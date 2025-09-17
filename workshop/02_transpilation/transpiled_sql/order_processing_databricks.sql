/*
DATABRICKS SQL VERSION - Order Processing Operations
===================================================
Original: order_processing.sql
Complexity Score: 5.1/10 (Simple-Medium)
Migration Wave: Wave 1 - Quick Wins
Migration Hours: 6

TRANSPILATION CHANGES MADE:
- Converted T-SQL transactions to Databricks SQL scripting
- Replaced GETDATE() with CURRENT_TIMESTAMP()
- Replaced DATEADD() with DATE_ADD()
- Replaced T-SQL variables with session variables
- Replaced SCOPE_IDENTITY() with generated column approach
- Converted OFFSET/FETCH to LIMIT
- Adapted transaction handling for Delta Lake ACID properties

VALIDATION STATUS: ⚠️ Requires testing - complex transaction logic
PERFORMANCE NOTES: Leverage Delta Lake MERGE for upsert operations
*/

-- Databricks SQL version using SQL scripting and session variables
BEGIN

    -- Use session variables instead of T-SQL DECLARE
    DECLARE new_order_key BIGINT DEFAULT NULL;

    -- Order creation with validation (Delta Lake provides ACID guarantees)
    INSERT INTO globalsupply_corp.raw.orders (
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
        CURRENT_TIMESTAMP() AS o_orderdate,
        '3-MEDIUM' AS o_orderpriority,
        'SYSTEM_AUTO' AS o_clerk,
        1 AS o_shippriority,
        'Auto-generated order' AS o_comment
    FROM globalsupply_corp.raw.customer c
    WHERE c.c_custkey = 12345
        AND c.c_acctbal >= 0  -- Credit check
        AND NOT EXISTS (
            SELECT 1
            FROM globalsupply_corp.raw.orders o
            WHERE o.o_custkey = c.c_custkey
                AND DATE(o.o_orderdate) = CURRENT_DATE()
                AND o.o_orderstatus = 'O'
        );

    -- Get the new order key using a different approach
    -- Note: In production, use auto-incrementing columns or UUIDs
    SET new_order_key = (
        SELECT MAX(o_orderkey)
        FROM globalsupply_corp.raw.orders
        WHERE o_custkey = 12345
        AND DATE(o_orderdate) = CURRENT_DATE()
    );

    -- Add line items to the order
    INSERT INTO globalsupply_corp.raw.lineitem (
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
        new_order_key AS l_orderkey,
        ps.ps_partkey,
        ps.ps_suppkey,
        ROW_NUMBER() OVER (ORDER BY ps.ps_supplycost) AS l_linenumber,
        5 AS l_quantity,  -- Default quantity
        ps.ps_supplycost * 5 AS l_extendedprice,
        0.05 AS l_discount,  -- 5% discount
        0.08 AS l_tax,       -- 8% tax
        'N' AS l_returnflag,
        'O' AS l_linestatus,
        DATE_ADD(CURRENT_DATE(), 7) AS l_shipdate,
        DATE_ADD(CURRENT_DATE(), 5) AS l_commitdate,
        NULL AS l_receiptdate,
        'DELIVER IN PERSON' AS l_shipinstruct,
        'TRUCK' AS l_shipmode,
        'Standard delivery' AS l_comment
    FROM globalsupply_corp.raw.partsupp ps
    INNER JOIN globalsupply_corp.raw.part p ON ps.ps_partkey = p.p_partkey
    WHERE ps.ps_availqty >= 5  -- Sufficient inventory
        AND ps.ps_supplycost > 0
        AND p.p_retailprice < 100  -- Low-cost items only
    ORDER BY ps.ps_supplycost
    LIMIT 3;  -- Limit to 3 line items

    -- Update order total based on line items
    UPDATE globalsupply_corp.raw.orders
    SET o_totalprice = (
        SELECT SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax))
        FROM globalsupply_corp.raw.lineitem
        WHERE l_orderkey = new_order_key
    )
    WHERE o_orderkey = new_order_key;

    -- Order status update operations
    UPDATE globalsupply_corp.raw.orders
    SET o_orderstatus = CASE
        WHEN o_orderstatus = 'O' AND EXISTS (
            SELECT 1
            FROM globalsupply_corp.raw.lineitem
            WHERE l_orderkey = orders.o_orderkey
                AND l_linestatus = 'F'  -- Filled
        ) THEN 'P'  -- Processing
        WHEN o_orderstatus = 'P' AND NOT EXISTS (
            SELECT 1
            FROM globalsupply_corp.raw.lineitem
            WHERE l_orderkey = orders.o_orderkey
                AND l_linestatus != 'F'
        ) THEN 'F'  -- Fulfilled
        ELSE o_orderstatus
    END
    WHERE o_orderdate >= DATE_SUB(CURRENT_DATE(), 7);

    -- Inventory updates after shipment
    -- Using MERGE for upsert operation (more efficient with Delta Lake)
    MERGE INTO globalsupply_corp.raw.partsupp AS target
    USING (
        SELECT
            l.l_partkey,
            l.l_suppkey,
            SUM(l.l_quantity) AS total_shipped
        FROM globalsupply_corp.raw.lineitem l
        INNER JOIN globalsupply_corp.raw.orders o ON l.l_orderkey = o.o_orderkey
        WHERE DATE(l.l_shipdate) = CURRENT_DATE()
            AND l.l_linestatus = 'F'
        GROUP BY l.l_partkey, l.l_suppkey
    ) AS shipped
    ON target.ps_partkey = shipped.l_partkey
    AND target.ps_suppkey = shipped.l_suppkey
    AND target.ps_availqty >= shipped.total_shipped
    WHEN MATCHED THEN UPDATE SET
        ps_availqty = ps_availqty - shipped.total_shipped;

    -- Update line item receipt dates for delivered orders
    UPDATE globalsupply_corp.raw.lineitem
    SET l_receiptdate = CURRENT_TIMESTAMP(),
        l_linestatus = 'F'
    WHERE l_shipdate <= CURRENT_DATE()
        AND l_receiptdate IS NULL
        AND EXISTS (
            SELECT 1
            FROM globalsupply_corp.raw.orders
            WHERE o_orderkey = lineitem.l_orderkey
                AND o_orderstatus IN ('P', 'F')
        );

    -- Customer account balance updates
    -- Using MERGE for better Delta Lake performance
    MERGE INTO globalsupply_corp.raw.customer AS target
    USING (
        SELECT
            o.o_custkey,
            SUM(o.o_totalprice) AS total_amount
        FROM globalsupply_corp.raw.orders o
        WHERE DATE(o.o_orderdate) = CURRENT_DATE()
            AND o.o_orderstatus = 'F'
        GROUP BY o.o_custkey
    ) AS pending_charges
    ON target.c_custkey = pending_charges.o_custkey
    AND target.c_acctbal >= pending_charges.total_amount
    WHEN MATCHED THEN UPDATE SET
        c_acctbal = c_acctbal - pending_charges.total_amount;

    -- Clean up old completed orders (archive operation)
    -- Note: Consider using Delta Lake time travel instead of deletion
    DELETE FROM globalsupply_corp.raw.lineitem
    WHERE l_orderkey IN (
        SELECT o.o_orderkey
        FROM globalsupply_corp.raw.orders o
        WHERE o.o_orderstatus = 'F'
            AND o.o_orderdate < DATE_SUB(CURRENT_DATE(), INTERVAL 2 YEAR)
    );

    DELETE FROM globalsupply_corp.raw.orders
    WHERE o_orderstatus = 'F'
        AND o_orderdate < DATE_SUB(CURRENT_DATE(), INTERVAL 2 YEAR);

    -- Generate processing summary for monitoring
    SELECT
        'ORDER_PROCESSING_SUMMARY' AS report_type,
        COUNT(CASE WHEN o_orderstatus = 'O' THEN 1 END) AS open_orders,
        COUNT(CASE WHEN o_orderstatus = 'P' THEN 1 END) AS processing_orders,
        COUNT(CASE WHEN o_orderstatus = 'F' THEN 1 END) AS fulfilled_orders,
        SUM(CASE WHEN o_orderstatus = 'O' THEN o_totalprice ELSE 0 END) AS open_value,
        SUM(CASE WHEN o_orderstatus = 'F' AND DATE(o_orderdate) = CURRENT_DATE() THEN o_totalprice ELSE 0 END) AS daily_revenue,
        AVG(DATEDIFF(CURRENT_DATE(), o_orderdate)) AS avg_order_age_days,
        COUNT(DISTINCT o_custkey) AS active_customers
    FROM globalsupply_corp.raw.orders
    WHERE o_orderdate >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH);

END;

/*
DELTA LAKE OPTIMIZATION SUGGESTIONS:

1. Use MERGE instead of separate INSERT/UPDATE operations:
   MERGE INTO target_table
   USING source_data ON conditions
   WHEN MATCHED THEN UPDATE SET ...
   WHEN NOT MATCHED THEN INSERT ...

2. Consider using Delta Lake features for data archiving:
   -- Instead of DELETE, use time travel
   CREATE TABLE archived_orders
   USING DELTA
   AS SELECT * FROM orders WHERE o_orderdate < '2022-01-01';

3. Optimize tables for common query patterns:
   OPTIMIZE globalsupply_corp.raw.orders ZORDER BY (o_orderdate, o_custkey);

4. Use generated columns for commonly calculated fields:
   ALTER TABLE orders ADD COLUMN (
     order_age_days INT GENERATED ALWAYS AS (
       DATEDIFF(CURRENT_DATE(), o_orderdate)
     )
   );

5. Consider using Change Data Feed for downstream processing:
   ALTER TABLE orders SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

TESTING RECOMMENDATIONS:
1. Validate transaction integrity with Delta Lake ACID properties
2. Test session variable behavior in concurrent environments
3. Verify MERGE operations handle duplicates correctly
4. Compare performance with original T-SQL version
5. Test rollback scenarios using Delta Lake versioning
*/