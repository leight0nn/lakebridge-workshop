/*
Order Processing Operations
==========================
Complexity Score: 5.1/10
Migration Hours: 6
Category: OLTP
SQL Features: CRUD Operations, Transactions

Business Purpose:
Standard order processing operations including order creation,
updates, and status management for transactional systems.
*/

-- Order creation and updates with transaction handling
BEGIN TRANSACTION order_processing;

-- Create new order with validation
INSERT INTO orders (
    o_custkey,
    o_orderstatus,
    o_totalprice,
    o_orderdate,
    o_orderpriority,
    o_clerk,
    o_shippriority,
    o_comment
)
SELECT
    c.c_custkey,
    'O' AS o_orderstatus,  -- Open status
    0.00 AS o_totalprice,  -- Will be updated after line items
    GETDATE() AS o_orderdate,
    '3-MEDIUM' AS o_orderpriority,
    'SYSTEM_AUTO' AS o_clerk,
    1 AS o_shippriority,
    'Auto-generated order' AS o_comment
FROM customer c
WHERE c.c_custkey = 12345
    AND c.c_acctbal >= 0  -- Credit check
    AND NOT EXISTS (
        SELECT 1
        FROM orders o
        WHERE o.o_custkey = c.c_custkey
            AND o.o_orderdate = CAST(GETDATE() AS DATE)
            AND o.o_orderstatus = 'O'
    );

-- Get the new order key
DECLARE @new_order_key BIGINT = SCOPE_IDENTITY();

-- Add line items to the order
INSERT INTO lineitem (
    l_orderkey,
    l_partkey,
    l_suppkey,
    l_linenumber,
    l_quantity,
    l_extendedprice,
    l_discount,
    l_tax,
    l_returnflag,
    l_linestatus,
    l_shipdate,
    l_commitdate,
    l_receiptdate,
    l_shipinstruct,
    l_shipmode,
    l_comment
)
SELECT
    @new_order_key,
    ps.ps_partkey,
    ps.ps_suppkey,
    ROW_NUMBER() OVER (ORDER BY ps.ps_supplycost) AS l_linenumber,
    5 AS l_quantity,  -- Default quantity
    ps.ps_supplycost * 5 AS l_extendedprice,
    0.05 AS l_discount,  -- 5% discount
    0.08 AS l_tax,       -- 8% tax
    'N' AS l_returnflag,
    'O' AS l_linestatus,
    DATEADD(day, 7, GETDATE()) AS l_shipdate,
    DATEADD(day, 5, GETDATE()) AS l_commitdate,
    NULL AS l_receiptdate,
    'DELIVER IN PERSON' AS l_shipinstruct,
    'TRUCK' AS l_shipmode,
    'Standard delivery' AS l_comment
FROM partsupp ps
INNER JOIN part p ON ps.ps_partkey = p.p_partkey
WHERE ps.ps_availqty >= 5  -- Sufficient inventory
    AND ps.ps_supplycost > 0
    AND p.p_retailprice < 100  -- Low-cost items only
ORDER BY ps.ps_supplycost
OFFSET 0 ROWS FETCH NEXT 3 ROWS ONLY;  -- Limit to 3 line items

-- Update order total based on line items
UPDATE orders
SET o_totalprice = (
    SELECT SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax))
    FROM lineitem
    WHERE l_orderkey = @new_order_key
)
WHERE o_orderkey = @new_order_key;

-- Order status update operations
UPDATE orders
SET o_orderstatus = CASE
    WHEN o_orderstatus = 'O' AND EXISTS (
        SELECT 1
        FROM lineitem
        WHERE l_orderkey = orders.o_orderkey
            AND l_linestatus = 'F'  -- Filled
    ) THEN 'P'  -- Processing
    WHEN o_orderstatus = 'P' AND NOT EXISTS (
        SELECT 1
        FROM lineitem
        WHERE l_orderkey = orders.o_orderkey
            AND l_linestatus != 'F'
    ) THEN 'F'  -- Fulfilled
    ELSE o_orderstatus
END
WHERE o_orderdate >= DATEADD(day, -7, GETDATE());

-- Inventory updates after shipment
UPDATE partsupp
SET ps_availqty = ps_availqty - shipped.total_shipped
FROM partsupp ps
INNER JOIN (
    SELECT
        l.l_partkey,
        l.l_suppkey,
        SUM(l.l_quantity) AS total_shipped
    FROM lineitem l
    INNER JOIN orders o ON l.l_orderkey = o.o_orderkey
    WHERE l.l_shipdate = CAST(GETDATE() AS DATE)
        AND l.l_linestatus = 'F'
    GROUP BY l.l_partkey, l.l_suppkey
) shipped ON ps.ps_partkey = shipped.l_partkey AND ps.ps_suppkey = shipped.l_suppkey
WHERE ps.ps_availqty >= shipped.total_shipped;

-- Update line item receipt dates for delivered orders
UPDATE lineitem
SET l_receiptdate = GETDATE(),
    l_linestatus = 'F'
WHERE l_shipdate <= GETDATE()
    AND l_receiptdate IS NULL
    AND EXISTS (
        SELECT 1
        FROM orders
        WHERE o_orderkey = lineitem.l_orderkey
            AND o_orderstatus IN ('P', 'F')
    );

-- Customer account balance updates
UPDATE customer
SET c_acctbal = c_acctbal - pending_charges.total_amount
FROM customer c
INNER JOIN (
    SELECT
        o.o_custkey,
        SUM(o.o_totalprice) AS total_amount
    FROM orders o
    WHERE o.o_orderdate = CAST(GETDATE() AS DATE)
        AND o.o_orderstatus = 'F'
    GROUP BY o.o_custkey
) pending_charges ON c.c_custkey = pending_charges.o_custkey
WHERE c.c_acctbal >= pending_charges.total_amount;

-- Clean up old completed orders (archive operation)
DELETE FROM lineitem
WHERE l_orderkey IN (
    SELECT o.o_orderkey
    FROM orders o
    WHERE o.o_orderstatus = 'F'
        AND o.o_orderdate < DATEADD(year, -2, GETDATE())
);

DELETE FROM orders
WHERE o_orderstatus = 'F'
    AND o_orderdate < DATEADD(year, -2, GETDATE());

-- Generate processing summary for monitoring
SELECT
    'ORDER_PROCESSING_SUMMARY' AS report_type,
    COUNT(CASE WHEN o_orderstatus = 'O' THEN 1 END) AS open_orders,
    COUNT(CASE WHEN o_orderstatus = 'P' THEN 1 END) AS processing_orders,
    COUNT(CASE WHEN o_orderstatus = 'F' THEN 1 END) AS fulfilled_orders,
    SUM(CASE WHEN o_orderstatus = 'O' THEN o_totalprice ELSE 0 END) AS open_value,
    SUM(CASE WHEN o_orderstatus = 'F' AND o_orderdate = CAST(GETDATE() AS DATE) THEN o_totalprice ELSE 0 END) AS daily_revenue,
    AVG(DATEDIFF(day, o_orderdate, GETDATE())) AS avg_order_age_days,
    COUNT(DISTINCT o_custkey) AS active_customers
FROM orders
WHERE o_orderdate >= DATEADD(month, -1, GETDATE());

-- Exception handling and rollback conditions
IF @@ERROR != 0 OR @@ROWCOUNT = 0
BEGIN
    ROLLBACK TRANSACTION order_processing;
    RAISERROR('Order processing failed - transaction rolled back', 16, 1);
END
ELSE
BEGIN
    COMMIT TRANSACTION order_processing;
    PRINT 'Order processing completed successfully';
END;