# Mock Data for Reconciliation Testing

This directory contains tools and data for simulated reconciliation testing when live SQL Server access is not available.

## 📁 Contents

- **`generate_mock_source.py`** - TPC-H style data generator for realistic business scenarios
- **`source_data.db`** - SQLite database (generated) containing simulated source data
- **`README.md`** - This documentation

## 🎯 Purpose

Provides realistic source data simulation for **GlobalSupply Corp** reconciliation scenarios without requiring actual SQL Server infrastructure. The generated data follows TPC-H patterns to represent authentic business workloads.

## 🏃‍♂️ Quick Start

### Generate Mock Data
```bash
cd mock_data
python generate_mock_source.py --scale 0.1
```

### Custom Data Volumes
```bash
# Smaller dataset for quick testing
python generate_mock_source.py --scale 0.05

# Larger dataset for performance testing  
python generate_mock_source.py --scale 0.5

# Custom output location
python generate_mock_source.py --scale 0.1 --output ../data/source.db
```

## 📊 Generated Schema

The mock data generator creates a TPC-H compatible schema:

### Tables Created
- **`customers`** - Customer demographics and account information
- **`suppliers`** - Supplier details and contact information  
- **`orders`** - Order transactions with dates and totals
- **`lineitem`** - Detailed line items with pricing and shipping

### Data Characteristics
- **Realistic distributions** - Geographic spread, date ranges, price variations
- **Business relationships** - Foreign key constraints and logical data relationships
- **Query-friendly indexes** - Optimized for typical reconciliation queries
- **Configurable volume** - Scale factor controls data size (0.1 = ~100K customers)

## 🔧 Scale Factor Guide

| Scale | Customers | Orders | Line Items | Use Case |
|-------|-----------|--------|------------|----------|
| 0.01  | ~1,500    | ~15K   | ~60K       | Quick testing |
| 0.05  | ~7,500    | ~75K   | ~300K      | Development |
| 0.1   | ~15,000   | ~150K  | ~600K      | Workshop default |
| 0.5   | ~75,000   | ~750K  | ~3M        | Performance testing |
| 1.0   | ~150,000  | ~1.5M  | ~6M        | Production simulation |

## 📈 Data Quality Features

### Realistic Business Patterns
- **Date distributions** - 7-year historical order data
- **Geographic spread** - 25 nations with appropriate phone/address patterns
- **Market segments** - BUILDING, AUTOMOBILE, MACHINERY, HOUSEHOLD, FURNITURE
- **Order complexity** - 1-7 line items per order with realistic pricing

### Built-in Variations
- **Account balances** - Range from negative to high-value accounts
- **Order statuses** - Open, Fulfilled, Pending with appropriate distributions
- **Shipping patterns** - Realistic delivery timeframes and methods
- **Price variations** - Market-appropriate pricing with discounts and taxes

## 🔍 Validation Scenarios

The mock data supports comprehensive reconciliation testing:

### Row Count Validation
- Exact count matching between source and target
- Configurable tolerance thresholds for data drift simulation

### Schema Validation  
- Column name and type verification
- Primary key and constraint validation
- Index performance comparison

### Data Quality Checks
- Referential integrity validation
- Range and format verification  
- Business rule compliance testing

### Aggregate Validation
- SUM, COUNT, AVG, MIN, MAX comparisons
- Financial total reconciliation
- Statistical distribution analysis

## 🛠️ Customization Options

### Modify Data Patterns
Edit `generate_mock_source.py` to customize:
- Market segments and business categories
- Date ranges and seasonality patterns
- Geographic distributions and regions
- Pricing models and discount structures

### Add Data Quality Issues
Intentionally introduce discrepancies for training:
- Missing records for troubleshooting practice
- Data type mismatches for schema validation
- Aggregate differences for reconciliation analysis

## 🔗 Integration

This mock data integrates seamlessly with:
- **Module 3 reconciliation analyzer** - Direct SQLite connectivity
- **Databricks targets** - Compatible with transpiled schemas from Module 2
- **Workshop exercises** - Supports all reconciliation learning objectives

---

*Ready to generate your reconciliation test data? Run the generator and start validating! 🎯*