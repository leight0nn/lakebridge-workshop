# GlobalSupply Corp - TPC-H Test Data Generation Guide

## 📋 Overview

This guide provides comprehensive instructions for generating TPC-H compatible test data for Module 2 transpilation validation. The data model supports supply chain analytics with 8 core tables and realistic business relationships.

## 🏗️ Data Model Structure

### **Schema Architecture**
The data follows TPC-H standards adapted for supply chain management:

**Regional Hierarchy:**
```
Region (5 regions)
  → Nation (10 nations)
    → Customer/Supplier (25 customers, 15 suppliers)
```

**Order Flow:**
```
Customer → Orders (50 orders) → LineItem (100 line items)
```

**Product Catalog:**
```
Part (20 parts) → PartSupp (30 relationships) ← Supplier
```

### **Business Context**
The schema represents **GlobalSupply Corp's** operations with:
- **5 Global Regions**: North America, Europe, Asia Pacific, Latin America, Middle East Africa
- **10 Key Markets**: Major nations in each region
- **25 Diverse Customers**: Various market segments (MACHINERY, TECHNOLOGY, AUTOMOTIVE, etc.)
- **15 Global Suppliers**: Specialized by region and product category
- **20 Product Categories**: Industrial, electronic, automotive, construction components
- **Temporal Coverage**: 2022-2024 with realistic seasonal patterns

## 🔧 Prerequisites

### **Environment Setup**
1. **Unity Catalog Schema Created**
   ```sql
   -- Run first: 02_databricks_schema.sql
   CREATE CATALOG IF NOT EXISTS globalsupply_corp;
   CREATE SCHEMA IF NOT EXISTS globalsupply_corp.raw;
   ```

2. **Databricks SQL Warehouse Running**
   - Ensure proper permissions for catalog creation
   - Verify Unity Catalog is enabled

### **File Dependencies**
1. `02_databricks_schema.sql` - Unity Catalog structure
2. `02_sample_data_generation.sql` - Data insertion script (needs modification)
3. `02_data_validation.sql` - Data quality verification

## 📊 Data Generation Approach

### **Option 1: Manual Data Creation (Recommended)**

Due to Databricks SQL syntax requirements, create data using Databricks-compatible INSERT statements:

#### **Step 1: Create Base Reference Data**
```sql
-- Region data (5 regions)
INSERT INTO globalsupply_corp.raw.region VALUES
(1, 'NORTH AMERICA', 'Primary market with high-value customers'),
(2, 'EUROPE', 'Established market with premium products'),
(3, 'ASIA PACIFIC', 'Fast-growing market with manufacturing partnerships'),
(4, 'LATIN AMERICA', 'Developing market with cost-competitive suppliers'),
(5, 'MIDDLE EAST AFRICA', 'Strategic market with resource-based partnerships');

-- Nation data (10 nations)
INSERT INTO globalsupply_corp.raw.nation VALUES
(1, 'UNITED STATES', 1, 'Largest market with diverse customer segments'),
(2, 'CANADA', 1, 'Strong manufacturing base with resource industry'),
(3, 'GERMANY', 2, 'Premium manufacturing market'),
(4, 'FRANCE', 2, 'Design and luxury goods market'),
(5, 'JAPAN', 3, 'Technology and precision manufacturing'),
(6, 'CHINA', 3, 'Manufacturing hub with rapid growth'),
(7, 'BRAZIL', 4, 'Largest Latin American market'),
(8, 'MEXICO', 4, 'Strategic manufacturing gateway'),
(9, 'SAUDI ARABIA', 5, 'Energy sector partnerships'),
(10, 'SOUTH AFRICA', 5, 'Mining and resources market');
```

#### **Step 2: Create Customers and Suppliers**
```sql
-- Customer data (25 customers across segments)
INSERT INTO globalsupply_corp.raw.customer VALUES
(1001, 'ACME Manufacturing Corp', '123 Industrial Way, Detroit MI', 1, '1-313-555-0101', 125000.50, 'MACHINERY', 'Large automotive parts manufacturer'),
(1002, 'TechFlow Solutions', '456 Silicon Ave, San Jose CA', 1, '1-408-555-0202', 89500.25, 'TECHNOLOGY', 'Electronics distributor'),
-- ... continue with realistic customer data
```

#### **Step 3: Create Products and Supply Relationships**
```sql
-- Part data (20 diverse products)
INSERT INTO globalsupply_corp.raw.part VALUES
(1001, 'Precision Ball Bearing Assembly', 'Manufacturer#01', 'Brand#A', 'STEEL BEARING', 25, 'SM BOX', 89.50, 'High-precision bearing for industrial machinery'),
-- ... continue with product catalog

-- PartSupp relationships (30 supplier-part combinations)
INSERT INTO globalsupply_corp.raw.partsupp VALUES
(1001, 101, 1500, 67.80, 'Primary bearing supplier, bulk inventory maintained'),
-- ... continue with supply relationships
```

#### **Step 4: Create Orders with Temporal Distribution**
```sql
-- Orders spanning 2022-2024 (50 orders)
INSERT INTO globalsupply_corp.raw.orders VALUES
-- Q4 2022 Orders (Historical)
(10001, 1001, 'F', 25840.50, '2022-10-15', '1-URGENT', 'Clerk#001', 1, 'Large manufacturing order for Q4 production'),
-- Q1 2023 Orders (Growth period)
-- Q2 2023 Orders (Peak season)
-- 2024 Orders (Current operations with mixed statuses)
```

#### **Step 5: Create Line Items with Business Logic**
```sql
-- LineItem data (100 line items with realistic distribution)
INSERT INTO globalsupply_corp.raw.lineitem VALUES
(10001, 1001, 101, 1, 50, 4475.00, 0.05, 0.08, 'N', 'F', '2022-10-18', '2022-10-16', '2022-10-20', 'DELIVER IN PERSON', 'TRUCK', 'Precision bearings for production line'),
-- ... continue with order breakdowns
```

### **Option 2: CSV Import Method**

For larger datasets, consider CSV import:

```sql
-- Example CSV import (after creating CSV files)
COPY INTO globalsupply_corp.raw.customer
FROM '/path/to/customer_data.csv'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'delimiter' = ',')
COPY_OPTIONS ('mergeSchema' = 'false');
```

## 📈 Data Distribution Specifications

### **Geographic Distribution**
- **North America**: 30% of customers (high-value markets)
- **Europe**: 25% of customers (premium segments)
- **Asia Pacific**: 25% of customers (manufacturing focus)
- **Latin America**: 10% of customers (emerging markets)
- **Middle East Africa**: 10% of customers (resource sectors)

### **Temporal Distribution**
- **2022 Q4**: 10% of orders (historical baseline)
- **2023**: 60% of orders (full year operations)
- **2024**: 30% of orders (current operations with mixed statuses)

### **Order Status Distribution**
- **Fulfilled (F)**: 70% (completed historical orders)
- **Processing (P)**: 20% (current operations)
- **Open (O)**: 10% (new orders)

### **Market Segment Distribution**
- **MACHINERY**: 30% (industrial equipment)
- **TECHNOLOGY**: 20% (electronics and IT)
- **AUTOMOTIVE**: 20% (automotive parts)
- **BUILDING**: 15% (construction materials)
- **HEALTHCARE**: 10% (medical equipment)
- **FURNITURE**: 5% (furniture and home goods)

## 🔍 Data Quality Requirements

### **Referential Integrity**
- All foreign keys must reference valid primary keys
- No orphaned records allowed
- Proper dependency order during loading

### **Business Logic Constraints**
- Order dates ≤ ship dates ≤ receipt dates
- Positive quantities, prices, and account balances
- Realistic phone numbers and addresses by region
- Proper currency formatting (decimal precision)

### **Data Completeness**
- No NULL values in required primary key fields
- Realistic distribution across all dimensions
- Adequate data volume for analytics testing

## ✅ Validation Process

### **Step 1: Execute Data Validation Script**
```bash
# Run validation after data loading
databricks sql -f 02_data_validation.sql
```

### **Step 2: Verify Core Metrics**
Expected validation results:
- **Row Counts**: 5 regions, 10 nations, 25 customers, 15 suppliers, 20 parts, 30 partsupp, 50 orders, 100 lineitem
- **Referential Integrity**: 100% valid foreign key relationships
- **Business Logic**: Order statuses valid, dates consistent, positive values
- **Module 2 Compatibility**: Supports all 5 transpiled SQL files

### **Step 3: Business Scenario Testing**
Verify data supports:
- ✅ **Financial Summary**: Multi-region revenue analysis
- ✅ **Order Processing**: Mixed order statuses and realistic workflows
- ✅ **Customer Profitability**: Multiple market segments and order patterns
- ✅ **Window Functions**: Time-based analytics and rankings
- ✅ **Dynamic Reporting**: Flexible parameter-based queries

## 🚀 Deployment Instructions

### **Sequential Execution Order**
1. **Schema Setup**: `02_databricks_schema.sql`
2. **Data Loading**: Modified `02_sample_data_generation.sql` (with Databricks syntax)
3. **Validation**: `02_data_validation.sql`
4. **Module 2 Testing**: Execute transpiled SQL files

### **Performance Optimization**
After data loading:
```sql
-- Optimize tables for query performance
OPTIMIZE globalsupply_corp.raw.orders ZORDER BY (o_orderdate, o_custkey);
OPTIMIZE globalsupply_corp.raw.lineitem ZORDER BY (l_orderkey, l_shipdate);

-- Generate table statistics
ANALYZE TABLE globalsupply_corp.raw.orders COMPUTE STATISTICS;
ANALYZE TABLE globalsupply_corp.raw.lineitem COMPUTE STATISTICS;
```

## 🔧 Troubleshooting

### **Common Issues**

**1. Syntax Errors**
- Issue: `CURRENT_TIMESTAMP()` not recognized
- Solution: Use `CURRENT_TIMESTAMP` (without parentheses) or `NOW()`

**2. Date Format Issues**
- Issue: Date string format incompatibility
- Solution: Use `DATE('YYYY-MM-DD')` format consistently

**3. Foreign Key Violations**
- Issue: Child records created before parent records
- Solution: Follow dependency order (Region → Nation → Customer → Orders → LineItem)

**4. Decimal Precision**
- Issue: Financial calculations losing precision
- Solution: Use `DECIMAL(18,2)` explicitly for currency fields

### **Data Generation Alternatives**

**1. Python-based Generation**
```python
# Use Python with Databricks SQL Connector
import pandas as pd
from databricks import sql

# Generate data programmatically
# Insert via SQL connector
```

**2. Existing TPC-H Tools**
```bash
# Use standard TPC-H data generators
# Convert to Databricks-compatible format
# Import via COPY INTO commands
```

## 📊 Success Criteria

### **Deployment Readiness Checklist**
- [ ] All 8 tables populated with expected row counts
- [ ] 100% referential integrity validation passed
- [ ] Business logic validation completed
- [ ] Module 2 compatibility verified
- [ ] Performance optimization applied
- [ ] Sample queries execute successfully

### **Module Integration Validation**
- [ ] `financial_summary_databricks.sql` executes and returns realistic results
- [ ] `order_processing_databricks.sql` processes orders correctly
- [ ] `customer_profitability_databricks.sql` analyzes customer segments
- [ ] All validation tests pass with adequate data coverage

## 📞 Support Resources

### **Documentation References**
- TPC-H Specification: Data model standards
- Databricks Unity Catalog: Schema management
- Module 2 Transpilation Guide: SQL compatibility requirements

### **Key Files**
- `02_databricks_schema.sql`: Unity Catalog structure definition
- `02_data_validation.sql`: Comprehensive data quality verification
- `02_validation_tests.sql`: Module 2 transpiled SQL testing framework

---

**🎯 Objective**: Generate realistic, high-quality test data that enables comprehensive validation of Module 2 transpiled SQL files while maintaining referential integrity and business logic consistency for GlobalSupply Corp's supply chain analytics use cases.

**Next Step**: Execute data generation process and validate readiness for Module 2 transpiled SQL testing.