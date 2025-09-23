"""
GlobalSupply Corp - Mock Source Data Generator

Creates realistic TPC-H style data in SQLite format to simulate SQL Server source
for reconciliation testing. This allows workshop participants to learn reconciliation
concepts without requiring actual SQL Server access.

Business Context:
Generates GlobalSupply Corp's historical data patterns including:
- Customer demographics and segments
- Order processing with realistic dates and amounts
- Line item details with pricing and quantities  
- Supplier information with geographic distribution

Features:
- TPC-H compatible schema for industry-standard testing
- Configurable data volumes for different workshop scenarios
- Built-in data quality issues for reconciliation training
- Realistic business data patterns and distributions
"""

import sqlite3
import random
import string
from datetime import datetime, timedelta
from pathlib import Path
import logging
from typing import List, Tuple
import argparse

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

class MockDataGenerator:
    """
    Generates TPC-H style mock data for GlobalSupply Corp reconciliation testing.
    """
    
    def __init__(self, db_path: str = "source_data.db", scale_factor: float = 0.1):
        """
        Initialize the mock data generator.
        
        Args:
            db_path: Path to SQLite database file
            scale_factor: Scale factor for data volume (0.1 = 10% of standard TPC-H)
        """
        self.db_path = Path(db_path)
        self.scale_factor = scale_factor
        self.conn = None
        
        # Base data volumes (will be scaled)
        self.base_customers = 150000
        self.base_suppliers = 10000  
        self.base_orders = 1500000
        self.base_lineitem_per_order = 4
        
        # Calculate scaled volumes
        self.num_customers = int(self.base_customers * scale_factor)
        self.num_suppliers = int(self.base_suppliers * scale_factor)
        self.num_orders = int(self.base_orders * scale_factor)
        
        logger.info(f"Initialized generator with scale factor {scale_factor}")
        logger.info(f"Target volumes: {self.num_customers:,} customers, {self.num_suppliers:,} suppliers, {self.num_orders:,} orders")
    
    def create_database(self):
        """Create SQLite database and tables with TPC-H schema."""
        if self.db_path.exists():
            self.db_path.unlink()
            logger.info(f"Removed existing database: {self.db_path}")
        
        self.conn = sqlite3.connect(self.db_path)
        cursor = self.conn.cursor()
        
        # Create customers table
        cursor.execute("""
            CREATE TABLE customers (
                c_custkey INTEGER PRIMARY KEY,
                c_name TEXT NOT NULL,
                c_address TEXT NOT NULL,
                c_nationkey INTEGER NOT NULL,
                c_phone TEXT NOT NULL,
                c_acctbal REAL NOT NULL,
                c_mktsegment TEXT NOT NULL,
                c_comment TEXT
            )
        """)
        
        # Create suppliers table
        cursor.execute("""
            CREATE TABLE suppliers (
                s_suppkey INTEGER PRIMARY KEY,
                s_name TEXT NOT NULL,
                s_address TEXT NOT NULL,
                s_nationkey INTEGER NOT NULL,
                s_phone TEXT NOT NULL,
                s_acctbal REAL NOT NULL,
                s_comment TEXT
            )
        """)
        
        # Create orders table
        cursor.execute("""
            CREATE TABLE orders (
                o_orderkey INTEGER PRIMARY KEY,
                o_custkey INTEGER NOT NULL,
                o_orderstatus TEXT NOT NULL,
                o_totalprice REAL NOT NULL,
                o_orderdate TEXT NOT NULL,
                o_orderpriority TEXT NOT NULL,
                o_clerk TEXT NOT NULL,
                o_shippriority INTEGER NOT NULL,
                o_comment TEXT,
                FOREIGN KEY (o_custkey) REFERENCES customers (c_custkey)
            )
        """)
        
        # Create lineitem table
        cursor.execute("""
            CREATE TABLE lineitem (
                l_orderkey INTEGER NOT NULL,
                l_partkey INTEGER NOT NULL,
                l_suppkey INTEGER NOT NULL,
                l_linenumber INTEGER NOT NULL,
                l_quantity REAL NOT NULL,
                l_extendedprice REAL NOT NULL,
                l_discount REAL NOT NULL,
                l_tax REAL NOT NULL,
                l_returnflag TEXT NOT NULL,
                l_linestatus TEXT NOT NULL,
                l_shipdate TEXT NOT NULL,
                l_commitdate TEXT NOT NULL,
                l_receiptdate TEXT NOT NULL,
                l_shipinstruct TEXT NOT NULL,
                l_shipmode TEXT NOT NULL,
                l_comment TEXT,
                PRIMARY KEY (l_orderkey, l_linenumber),
                FOREIGN KEY (l_orderkey) REFERENCES orders (o_orderkey),
                FOREIGN KEY (l_suppkey) REFERENCES suppliers (s_suppkey)
            )
        """)
        
        self.conn.commit()
        logger.info("Created database schema successfully")
    
    def generate_customers(self):
        """Generate customer data with realistic business patterns."""
        logger.info(f"Generating {self.num_customers:,} customers...")
        
        cursor = self.conn.cursor()
        batch_size = 10000
        
        market_segments = ['BUILDING', 'AUTOMOBILE', 'MACHINERY', 'HOUSEHOLD', 'FURNITURE']
        nations = list(range(25))  # TPC-H has 25 nations
        
        for batch_start in range(0, self.num_customers, batch_size):
            batch_end = min(batch_start + batch_size, self.num_customers)
            batch_data = []
            
            for i in range(batch_start + 1, batch_end + 1):
                name = f"Customer#{i:09d}"
                address = self._generate_address()
                nationkey = random.choice(nations)
                phone = self._generate_phone(nationkey)
                acctbal = round(random.uniform(-999.99, 9999.99), 2)
                mktsegment = random.choice(market_segments)
                comment = self._generate_comment()
                
                batch_data.append((i, name, address, nationkey, phone, acctbal, mktsegment, comment))
            
            cursor.executemany("""
                INSERT INTO customers (c_custkey, c_name, c_address, c_nationkey, 
                                     c_phone, c_acctbal, c_mktsegment, c_comment)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, batch_data)
            
            if batch_end % 50000 == 0:
                logger.info(f"Generated {batch_end:,} customers")
        
        self.conn.commit()
        logger.info(f"Completed customers generation: {self.num_customers:,} records")
    
    def generate_suppliers(self):
        """Generate supplier data."""
        logger.info(f"Generating {self.num_suppliers:,} suppliers...")
        
        cursor = self.conn.cursor()
        batch_size = 5000
        nations = list(range(25))
        
        for batch_start in range(0, self.num_suppliers, batch_size):
            batch_end = min(batch_start + batch_size, self.num_suppliers)
            batch_data = []
            
            for i in range(batch_start + 1, batch_end + 1):
                name = f"Supplier#{i:09d}"
                address = self._generate_address()
                nationkey = random.choice(nations)
                phone = self._generate_phone(nationkey)
                acctbal = round(random.uniform(-999.99, 9999.99), 2)
                comment = self._generate_comment()
                
                batch_data.append((i, name, address, nationkey, phone, acctbal, comment))
            
            cursor.executemany("""
                INSERT INTO suppliers (s_suppkey, s_name, s_address, s_nationkey,
                                     s_phone, s_acctbal, s_comment)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, batch_data)
        
        self.conn.commit()
        logger.info(f"Completed suppliers generation: {self.num_suppliers:,} records")
    
    def generate_orders(self):
        """Generate orders and lineitem data."""
        logger.info(f"Generating {self.num_orders:,} orders with line items...")
        
        cursor = self.conn.cursor()
        order_statuses = ['O', 'F', 'P']  # Open, Fulfilled, Pending
        priorities = ['1-URGENT', '2-HIGH', '3-MEDIUM', '4-NOT SPECIFIED', '5-LOW']
        
        # Generate date range (last 7 years)
        end_date = datetime.now()
        start_date = end_date - timedelta(days=7*365)
        
        batch_size = 5000
        lineitem_batch = []
        
        for batch_start in range(1, self.num_orders + 1, batch_size):
            batch_end = min(batch_start + batch_size, self.num_orders + 1)
            order_batch = []
            
            for order_id in range(batch_start, batch_end):
                # Order details
                custkey = random.randint(1, self.num_customers)
                status = random.choice(order_statuses)
                order_date = self._random_date(start_date, end_date)
                priority = random.choice(priorities)
                clerk = f"Clerk#{random.randint(1, 1000):04d}"
                ship_priority = random.randint(0, 1)
                comment = self._generate_comment()
                
                # Generate line items for this order
                num_lines = random.randint(1, 7)
                total_price = 0
                
                for line_num in range(1, num_lines + 1):
                    partkey = random.randint(1, 200000)
                    suppkey = random.randint(1, self.num_suppliers)
                    quantity = random.randint(1, 50)
                    base_price = round(random.uniform(1.0, 100.0), 2)
                    discount = round(random.uniform(0.0, 0.10), 2)
                    tax = round(random.uniform(0.0, 0.08), 2)
                    extended_price = round(quantity * base_price, 2)
                    total_price += extended_price * (1 - discount) * (1 + tax)
                    
                    # Line item dates
                    ship_date = order_date + timedelta(days=random.randint(1, 121))
                    commit_date = order_date + timedelta(days=random.randint(30, 90))
                    receipt_date = ship_date + timedelta(days=random.randint(1, 30))
                    
                    return_flag = 'A' if ship_date <= datetime.now() - timedelta(days=90) else 'N'
                    line_status = 'F' if ship_date <= datetime.now() else 'O'
                    
                    ship_instruct = random.choice(['DELIVER IN PERSON', 'COLLECT COD', 'NONE', 'TAKE BACK RETURN'])
                    ship_mode = random.choice(['TRUCK', 'MAIL', 'REG AIR', 'SHIP', 'AIR', 'FOB', 'RAIL'])
                    
                    lineitem_batch.append((
                        order_id, partkey, suppkey, line_num, quantity,
                        extended_price, discount, tax, return_flag, line_status,
                        ship_date.strftime('%Y-%m-%d'),
                        commit_date.strftime('%Y-%m-%d'),
                        receipt_date.strftime('%Y-%m-%d'),
                        ship_instruct, ship_mode, self._generate_comment()
                    ))
                
                order_batch.append((
                    order_id, custkey, status, round(total_price, 2),
                    order_date.strftime('%Y-%m-%d'), priority, clerk,
                    ship_priority, comment
                ))
            
            # Insert orders batch
            cursor.executemany("""
                INSERT INTO orders (o_orderkey, o_custkey, o_orderstatus, o_totalprice,
                                  o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, order_batch)
            
            if batch_end % 50000 == 0:
                logger.info(f"Generated {batch_end:,} orders")
        
        # Insert all line items
        logger.info(f"Inserting {len(lineitem_batch):,} line items...")
        cursor.executemany("""
            INSERT INTO lineitem (l_orderkey, l_partkey, l_suppkey, l_linenumber,
                                l_quantity, l_extendedprice, l_discount, l_tax,
                                l_returnflag, l_linestatus, l_shipdate, l_commitdate,
                                l_receiptdate, l_shipinstruct, l_shipmode, l_comment)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, lineitem_batch)
        
        self.conn.commit()
        logger.info(f"Completed orders generation: {self.num_orders:,} orders, {len(lineitem_batch):,} line items")
    
    def _generate_address(self) -> str:
        """Generate random address."""
        street_num = random.randint(1, 9999)
        street_names = ['Main St', 'Oak Ave', 'First St', 'Second Ave', 'Park Rd', 'Washington Blvd']
        return f"{street_num} {random.choice(street_names)}"
    
    def _generate_phone(self, nationkey: int) -> str:
        """Generate phone number based on nation."""
        area_code = 10 + nationkey
        number = random.randint(1000000, 9999999)
        return f"{area_code}-{number//10000}-{number%10000}"
    
    def _generate_comment(self) -> str:
        """Generate random comment."""
        words = ['express', 'packages', 'requests', 'accounts', 'deposits', 'quickly', 'carefully', 'regular']
        return ' '.join(random.choices(words, k=random.randint(3, 8)))
    
    def _random_date(self, start_date: datetime, end_date: datetime) -> datetime:
        """Generate random date between start and end."""
        time_between = end_date - start_date
        days_between = time_between.days
        random_days = random.randint(0, days_between)
        return start_date + timedelta(days=random_days)
    
    def create_indexes(self):
        """Create indexes for better query performance."""
        logger.info("Creating database indexes...")
        cursor = self.conn.cursor()
        
        indexes = [
            "CREATE INDEX idx_customers_nationkey ON customers (c_nationkey)",
            "CREATE INDEX idx_customers_mktsegment ON customers (c_mktsegment)",
            "CREATE INDEX idx_suppliers_nationkey ON suppliers (s_nationkey)",
            "CREATE INDEX idx_orders_custkey ON orders (o_custkey)",
            "CREATE INDEX idx_orders_orderdate ON orders (o_orderdate)",
            "CREATE INDEX idx_lineitem_orderkey ON lineitem (l_orderkey)",
            "CREATE INDEX idx_lineitem_suppkey ON lineitem (l_suppkey)",
            "CREATE INDEX idx_lineitem_shipdate ON lineitem (l_shipdate)"
        ]
        
        for index_sql in indexes:
            cursor.execute(index_sql)
        
        self.conn.commit()
        logger.info("Created database indexes")
    
    def generate_statistics(self):
        """Generate and log database statistics."""
        cursor = self.conn.cursor()
        
        stats = {}
        tables = ['customers', 'suppliers', 'orders', 'lineitem']
        
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            stats[table] = count
            logger.info(f"{table}: {count:,} records")
        
        # Additional statistics
        cursor.execute("SELECT MIN(o_orderdate), MAX(o_orderdate) FROM orders")
        min_date, max_date = cursor.fetchone()
        logger.info(f"Order date range: {min_date} to {max_date}")
        
        cursor.execute("SELECT SUM(o_totalprice) FROM orders")
        total_revenue = cursor.fetchone()[0]
        logger.info(f"Total order value: ${total_revenue:,.2f}")
        
        return stats
    
    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()
            logger.info(f"Database saved to: {self.db_path.absolute()}")

def main():
    """Main function to generate mock data."""
    parser = argparse.ArgumentParser(description="Generate TPC-H style mock data for reconciliation testing")
    parser.add_argument("--scale", type=float, default=0.1, help="Scale factor for data volume (default: 0.1)")
    parser.add_argument("--output", type=str, default="source_data.db", help="Output SQLite database file")
    
    args = parser.parse_args()
    
    logger.info("=== GlobalSupply Corp Mock Data Generator ===")
    logger.info(f"Scale factor: {args.scale}")
    logger.info(f"Output file: {args.output}")
    
    generator = MockDataGenerator(args.output, args.scale)
    
    try:
        generator.create_database()
        generator.generate_customers()
        generator.generate_suppliers()
        generator.generate_orders()
        generator.create_indexes()
        stats = generator.generate_statistics()
        
        logger.info("=== Generation Complete ===")
        logger.info(f"Generated {sum(stats.values()):,} total records")
        logger.info("Ready for reconciliation testing!")
        
    except Exception as e:
        logger.error(f"Error generating data: {e}")
        raise
    finally:
        generator.close()

if __name__ == "__main__":
    main()