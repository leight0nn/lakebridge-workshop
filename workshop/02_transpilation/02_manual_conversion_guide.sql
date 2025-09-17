/*
GlobalSupply Corp - Manual SQL Server to Databricks Conversion Guide
====================================================================

This guide demonstrates common SQL Server T-SQL to Databricks SQL conversion patterns
based on the 5 target files from Module 1's assessment (Wave 1 & 2).

Files covered:
• financial_summary.sql (4.5/10) - Basic aggregation, simple joins
• order_processing.sql (5.1/10) - CRUD operations, transactions
• dynamic_reporting.sql (6.5/10) - Dynamic SQL, conditional logic
• window_functions_analysis.sql (7.2/10) - Advanced window functions
• customer_profitability.sql (7.8/10) - PIVOT operations

Business Purpose:
When Lakebridge automated transpilation is unavailable, this guide provides
manual conversion techniques for the most common patterns found in the
GlobalSupply Corp SQL Server workloads.
*/

-- ============================================================================
-- 1. DATE AND TIME FUNCTIONS
-- ============================================================================
-- Pattern found in: order_processing.sql, dynamic_reporting.sql

-- SQL Server T-SQL
SELECT
    GETDATE() AS current_time,
    DATEADD(day, 7, GETDATE()) AS ship_date,
    DATEADD(month, -1, GETDATE()) AS last_month,
    DATEDIFF(day, o_orderdate, GETDATE()) AS days_old,
    YEAR(o_orderdate) AS order_year,
    MONTH(o_orderdate) AS order_month
FROM orders;

-- Databricks SQL Equivalent
SELECT
    CURRENT_TIMESTAMP() AS current_time,
    DATE_ADD(CURRENT_DATE(), 7) AS ship_date,
    ADD_MONTHS(CURRENT_DATE(), -1) AS last_month,
    DATEDIFF(CURRENT_DATE(), o_orderdate) AS days_old,
    YEAR(o_orderdate) AS order_year,
    MONTH(o_orderdate) AS order_month
FROM orders;

-- ============================================================================
-- 2. VARIABLE DECLARATIONS AND ASSIGNMENT
-- ============================================================================
-- Pattern found in: dynamic_reporting.sql, order_processing.sql

-- SQL Server T-SQL
DECLARE @report_type VARCHAR(50) = 'SUPPLIER_PERFORMANCE';
DECLARE @date_from DATE = '2023-01-01';
DECLARE @date_to DATE = '2023-12-31';
DECLARE @min_order_value DECIMAL(18,2) = 1000.00;

-- Databricks SQL Equivalent (using session variables)
SET VAR report_type = 'SUPPLIER_PERFORMANCE';
SET VAR date_from = '2023-01-01';
SET VAR date_to = '2023-12-31';
SET VAR min_order_value = 1000.00;

-- Alternative: Use literal values or parameter substitution
-- For stored procedures, use CREATE PROCEDURE with parameters

-- ============================================================================
-- 3. TRANSACTION HANDLING
-- ============================================================================
-- Pattern found in: order_processing.sql

-- SQL Server T-SQL
BEGIN TRANSACTION order_processing;

INSERT INTO orders (o_custkey, o_orderstatus, o_totalprice, o_orderdate)
VALUES (12345, 'O', 0.00, GETDATE());

IF @@ERROR != 0
BEGIN
    ROLLBACK TRANSACTION order_processing;
    RAISERROR('Order creation failed', 16, 1);
END
ELSE
BEGIN
    COMMIT TRANSACTION order_processing;
END;

-- Databricks SQL Equivalent (Delta Lake provides ACID guarantees)
-- Use MERGE or multi-table transactions
BEGIN;

INSERT INTO orders (o_custkey, o_orderstatus, o_totalprice, o_orderdate)
VALUES (12345, 'O', 0.00, CURRENT_TIMESTAMP());

-- Delta Lake automatically handles ACID properties
-- No explicit transaction management needed for single statements
-- Use MERGE for complex multi-table operations

COMMIT;

-- ============================================================================
-- 4. STRING AGGREGATION FUNCTIONS
-- ============================================================================
-- Pattern found in: customer_profitability.sql

-- SQL Server T-SQL
SELECT
    c_custkey,
    STRING_AGG(p_name, ', ') AS product_list,
    STRING_AGG(CAST(l_quantity AS VARCHAR), '|') AS quantity_list
FROM customer c
JOIN orders o ON c.c_custkey = o.o_custkey
JOIN lineitem l ON o.o_orderkey = l.l_orderkey
JOIN part p ON l.l_partkey = p.p_partkey
GROUP BY c_custkey;

-- Databricks SQL Equivalent
SELECT
    c_custkey,
    ARRAY_JOIN(COLLECT_LIST(p_name), ', ') AS product_list,
    ARRAY_JOIN(COLLECT_LIST(CAST(l_quantity AS STRING)), '|') AS quantity_list
FROM customer c
JOIN orders o ON c.c_custkey = o.o_custkey
JOIN lineitem l ON o.o_orderkey = l.l_orderkey
JOIN part p ON l.l_partkey = p.p_partkey
GROUP BY c_custkey;

-- ============================================================================
-- 5. PIVOT OPERATIONS
-- ============================================================================
-- Pattern found in: customer_profitability.sql

-- SQL Server T-SQL
SELECT *
FROM (
    SELECT c_mktsegment, YEAR(o_orderdate) AS order_year, o_totalprice
    FROM customer c
    JOIN orders o ON c.c_custkey = o.o_custkey
) AS source_data
PIVOT (
    SUM(o_totalprice)
    FOR order_year IN ([2021], [2022], [2023])
) AS pivot_table;

-- Databricks SQL Equivalent
SELECT
    c_mktsegment,
    SUM(CASE WHEN YEAR(o_orderdate) = 2021 THEN o_totalprice ELSE 0 END) AS `2021`,
    SUM(CASE WHEN YEAR(o_orderdate) = 2022 THEN o_totalprice ELSE 0 END) AS `2022`,
    SUM(CASE WHEN YEAR(o_orderdate) = 2023 THEN o_totalprice ELSE 0 END) AS `2023`
FROM customer c
JOIN orders o ON c.c_custkey = o.o_custkey
GROUP BY c_mktsegment;

-- Alternative: Databricks SQL PIVOT syntax (available in newer versions)
SELECT *
FROM (
    SELECT c_mktsegment, YEAR(o_orderdate) AS order_year, o_totalprice
    FROM customer c
    JOIN orders o ON c.c_custkey = o.o_custkey
) PIVOT (
    SUM(o_totalprice)
    FOR order_year IN (2021, 2022, 2023)
);

-- ============================================================================
-- 6. WINDOW FUNCTIONS WITH FRAME SPECIFICATION
-- ============================================================================
-- Pattern found in: window_functions_analysis.sql

-- SQL Server T-SQL
SELECT
    c_custkey,
    o_orderdate,
    o_totalprice,

    -- Moving average with frame
    AVG(o_totalprice) OVER (
        PARTITION BY c_custkey
        ORDER BY o_orderdate
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS moving_avg_3_orders,

    -- Lead/Lag functions
    LAG(o_totalprice, 1) OVER (
        PARTITION BY c_custkey
        ORDER BY o_orderdate
    ) AS previous_order_value,

    LEAD(o_totalprice, 1) OVER (
        PARTITION BY c_custkey
        ORDER BY o_orderdate
    ) AS next_order_value
FROM orders;

-- Databricks SQL Equivalent (syntax is very similar)
SELECT
    c_custkey,
    o_orderdate,
    o_totalprice,

    -- Moving average with frame (same syntax)
    AVG(o_totalprice) OVER (
        PARTITION BY c_custkey
        ORDER BY o_orderdate
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS moving_avg_3_orders,

    -- Lead/Lag functions (same syntax)
    LAG(o_totalprice, 1) OVER (
        PARTITION BY c_custkey
        ORDER BY o_orderdate
    ) AS previous_order_value,

    LEAD(o_totalprice, 1) OVER (
        PARTITION BY c_custkey
        ORDER BY o_orderdate
    ) AS next_order_value
FROM orders;

-- ============================================================================
-- 7. TOP N vs LIMIT
-- ============================================================================
-- Pattern found in: financial_summary.sql, order_processing.sql

-- SQL Server T-SQL
SELECT TOP 100
    c_name,
    SUM(o_totalprice) AS total_revenue
FROM customer c
JOIN orders o ON c.c_custkey = o.o_custkey
GROUP BY c_name
ORDER BY total_revenue DESC;

-- Databricks SQL Equivalent
SELECT
    c_name,
    SUM(o_totalprice) AS total_revenue
FROM customer c
JOIN orders o ON c.c_custkey = o.o_custkey
GROUP BY c_name
ORDER BY total_revenue DESC
LIMIT 100;

-- ============================================================================
-- 8. CONDITIONAL AGGREGATION AND CASE STATEMENTS
-- ============================================================================
-- Pattern found in: financial_summary.sql

-- SQL Server T-SQL
SELECT
    r_name AS region,
    COUNT(CASE WHEN o_orderstatus = 'O' THEN 1 END) AS open_orders,
    COUNT(CASE WHEN o_orderstatus = 'F' THEN 1 END) AS fulfilled_orders,
    SUM(CASE WHEN o_orderstatus = 'F' THEN o_totalprice ELSE 0 END) AS fulfilled_revenue,
    AVG(CASE WHEN o_orderdate >= DATEADD(month, -3, GETDATE()) THEN o_totalprice END) AS recent_avg_order
FROM region r
JOIN nation n ON r.r_regionkey = n.n_regionkey
JOIN customer c ON n.n_nationkey = c.c_nationkey
JOIN orders o ON c.c_custkey = o.o_custkey
GROUP BY r_name;

-- Databricks SQL Equivalent
SELECT
    r_name AS region,
    COUNT(CASE WHEN o_orderstatus = 'O' THEN 1 END) AS open_orders,
    COUNT(CASE WHEN o_orderstatus = 'F' THEN 1 END) AS fulfilled_orders,
    SUM(CASE WHEN o_orderstatus = 'F' THEN o_totalprice ELSE 0 END) AS fulfilled_revenue,
    AVG(CASE WHEN o_orderdate >= ADD_MONTHS(CURRENT_DATE(), -3) THEN o_totalprice END) AS recent_avg_order
FROM region r
JOIN nation n ON r.r_regionkey = n.n_regionkey
JOIN customer c ON n.n_nationkey = c.c_nationkey
JOIN orders o ON c.c_custkey = o.o_custkey
GROUP BY r_name;

-- ============================================================================
-- 9. DYNAMIC SQL AND CONDITIONAL LOGIC
-- ============================================================================
-- Pattern found in: dynamic_reporting.sql

-- SQL Server T-SQL (Dynamic SQL construction)
DECLARE @sql NVARCHAR(MAX) = 'SELECT * FROM orders WHERE 1=1';
IF @region_filter IS NOT NULL
    SET @sql = @sql + ' AND region = ''' + @region_filter + '''';
IF @min_amount > 0
    SET @sql = @sql + ' AND total_amount >= ' + CAST(@min_amount AS VARCHAR);

EXEC sp_executesql @sql;

-- Databricks SQL Equivalent (using SQL scripting)
-- Option 1: Use stored procedures with conditional logic
CREATE OR REPLACE PROCEDURE get_filtered_orders(
    region_filter STRING DEFAULT NULL,
    min_amount DECIMAL(18,2) DEFAULT 0
)
LANGUAGE SQL
AS
BEGIN
    SELECT * FROM orders
    WHERE 1=1
    AND (region_filter IS NULL OR region = region_filter)
    AND (min_amount <= 0 OR total_amount >= min_amount);
END;

-- Option 2: Use CASE statements in WHERE clause
SELECT * FROM orders
WHERE 1=1
  AND (${region_filter} IS NULL OR region = ${region_filter})
  AND (${min_amount} <= 0 OR total_amount >= ${min_amount});

-- ============================================================================
-- 10. SYSTEM FUNCTIONS AND METADATA
-- ============================================================================
-- Pattern found in: order_processing.sql

-- SQL Server T-SQL
IF @@ERROR != 0 OR @@ROWCOUNT = 0
BEGIN
    PRINT 'Operation failed';
    SELECT SCOPE_IDENTITY() AS new_id;
END;

-- Databricks SQL Equivalent
-- Note: Error handling is different in Databricks
-- Use exception handling in procedures or check row counts explicitly

-- For row count validation:
CREATE OR REPLACE TEMPORARY VIEW operation_result AS
    INSERT INTO orders VALUES (1, 'O', 100.00, CURRENT_DATE())
    RETURNING *;

SELECT CASE
    WHEN COUNT(*) > 0 THEN 'Success'
    ELSE 'Failed'
END AS operation_status
FROM operation_result;

-- ============================================================================
-- CONVERSION BEST PRACTICES
-- ============================================================================

/*
KEY PRINCIPLES FOR MANUAL CONVERSION:

1. DATE FUNCTIONS:
   - Replace GETDATE() → CURRENT_TIMESTAMP() or CURRENT_DATE()
   - Replace DATEADD() → DATE_ADD(), ADD_MONTHS()
   - Replace DATEDIFF() → DATEDIFF() (same function name, different parameter order)

2. VARIABLES:
   - Replace DECLARE @var → SET VAR var (session variables)
   - Or use stored procedure parameters
   - Or use literal values/parameter substitution

3. TRANSACTIONS:
   - Delta Lake provides ACID guarantees automatically
   - Use explicit BEGIN/COMMIT for multi-statement transactions
   - MERGE operations are preferred for complex updates

4. AGGREGATION:
   - Replace STRING_AGG() → ARRAY_JOIN(COLLECT_LIST())
   - Most other aggregate functions work the same

5. PIVOT:
   - Use CASE statements for complex pivots
   - Or use newer Databricks PIVOT syntax when available

6. WINDOW FUNCTIONS:
   - Most syntax is identical between SQL Server and Databricks
   - Frame specifications work the same way

7. TOP N:
   - Replace SELECT TOP N → SELECT ... LIMIT N

8. DYNAMIC SQL:
   - Use stored procedures with conditional logic
   - Or parameterized queries with CASE statements
   - Avoid string concatenation for SQL construction

9. ERROR HANDLING:
   - Use proper exception handling in stored procedures
   - Check row counts explicitly rather than relying on @@ROWCOUNT
   - Use Delta Lake versioning for rollback scenarios

10. PERFORMANCE:
    - Take advantage of Delta Lake features (Z-ORDER, OPTIMIZE)
    - Use appropriate data types (avoid NVARCHAR if not needed)
    - Consider partitioning strategies for large tables
*/

-- ============================================================================
-- VALIDATION QUERIES
-- ============================================================================

-- Use these queries to validate your manual conversions:

-- 1. Check date function conversions
SELECT
    'SQL Server Style' AS method,
    COUNT(*) as order_count
FROM orders
WHERE o_orderdate >= '2023-01-01';

-- 2. Validate aggregation results
WITH validation_check AS (
    SELECT c_custkey, COUNT(*) as order_count
    FROM customer c
    JOIN orders o ON c.c_custkey = o.o_custkey
    GROUP BY c_custkey
)
SELECT
    AVG(order_count) as avg_orders_per_customer,
    MAX(order_count) as max_orders_per_customer
FROM validation_check;

-- 3. Test window function conversions
SELECT
    c_custkey,
    COUNT(*) OVER (PARTITION BY c_mktsegment) as segment_customer_count,
    ROW_NUMBER() OVER (ORDER BY c_acctbal DESC) as balance_rank
FROM customer
LIMIT 10;

/*
NEXT STEPS:
1. Test each conversion pattern with sample data
2. Validate results match expected SQL Server output
3. Run performance comparisons
4. Use 02_validation_tests.sql for comprehensive testing
5. Proceed to Module 3 for data reconciliation
*/