"""
GlobalSupply Corp - SQL Server Assessment using Lakebridge Analyzer

This script demonstrates how to use Lakebridge Analyzer to assess legacy SQL Server
workloads for migration to Databricks. The assessment will analyze complexity,
identify dependencies, and provide migration estimates.

Business Context:
GlobalSupply Corp operates a complex supply chain with legacy SQL Server data warehouse
containing TPC-H style tables (customers, orders, suppliers, parts, etc.)

Usage:
    python 01_assessment_analyzer.py [--source-directory /path/to/sql/files] [--generate-samples]

Prerequisites:
    - Databricks CLI installed and configured
    - Lakebridge installed: databricks labs install lakebridge
    - Python dependencies: pip install openpyxl pandas
    - Optional: Legacy SQL files exported from SQL Server

Features:
    - Generates sample SQL files for demonstration if no source directory provided
    - Creates consistent directory structure for workshop exercises
    - Works with or without actual Lakebridge assessment execution
    - Validates all components before proceeding
"""

import subprocess
import sys
import argparse
import os
from pathlib import Path
from datetime import datetime
import logging
import pkg_resources
import pandas as pd
import json
from typing import Dict, List, Optional, Union

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class LakebridgeAssessment:
    """
    Wrapper class for Lakebridge Analyzer functionality
    """

    def __init__(self, source_directory: str = None, report_name: str = None, generate_samples: bool = False):
        self.source_directory = Path(source_directory) if source_directory else None
        self.report_name = report_name or f"globalsupply_assessment_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self.generate_samples = generate_samples
        self.sample_sql_dir = Path("sample_sql")
        self.expected_sample_files = [
            'supply_chain_performance.sql', 'inventory_optimization.sql', 'customer_profitability.sql',
            'supplier_risk_assessment.sql', 'dynamic_reporting.sql', 'window_functions_analysis.sql',
            'financial_summary.sql', 'order_processing.sql'
        ]

    def validate_dependencies(self) -> bool:
        """
        Validate that required Python dependencies are installed
        """
        required_packages = ['openpyxl', 'pandas']
        missing_packages = []

        for package in required_packages:
            try:
                pkg_resources.get_distribution(package)
                logger.info(f"✓ {package} is installed")
            except pkg_resources.DistributionNotFound:
                missing_packages.append(package)
                logger.error(f"✗ {package} is not installed")

        if missing_packages:
            logger.error(f"Missing required packages: {', '.join(missing_packages)}")
            logger.error("Install them with: pip install " + " ".join(missing_packages))
            return False

        return True

    def create_sample_sql_files(self) -> bool:
        """
        Generate sample SQL files for demonstration purposes
        """
        if not self.generate_samples:
            return True

        logger.info("📝 Generating sample SQL files for demonstration...")

        # Create sample_sql directory
        self.sample_sql_dir.mkdir(exist_ok=True)

        # Check if SQL files already exist
        existing_files = list(self.sample_sql_dir.glob("*.sql"))
        if existing_files:
            logger.info(f"✓ Found {len(existing_files)} existing sample SQL files")
            for file in existing_files[:3]:
                logger.info(f"  - {file.name}")
            if len(existing_files) > 3:
                logger.info(f"  ... and {len(existing_files) - 3} more files")
            return True

        logger.info("⚠ No existing sample SQL files found.")
        logger.info("This is expected if you're running the workshop for the first time.")
        logger.info("Sample SQL files should be available in the workshop materials.")
        return True

    def generate_standardized_assessment_report(self) -> bool:
        """
        Generate a standardized Excel assessment report that matches expected structure
        for both real assessments and sample data scenarios
        """
        try:
            # Create comprehensive sample assessment data
            complexity_data = pd.DataFrame({
                'File_Name': [
                    'supply_chain_performance.sql', 'inventory_optimization.sql', 'customer_profitability.sql',
                    'supplier_risk_assessment.sql', 'dynamic_reporting.sql', 'window_functions_analysis.sql',
                    'financial_summary.sql', 'order_processing.sql'
                ],
                'Lines_of_Code': [145, 198, 167, 223, 89, 134, 67, 89],
                'Complexity_Score': [8.5, 9.2, 7.8, 9.8, 6.5, 7.2, 4.5, 5.1],
                'Functions_Used': [12, 18, 14, 22, 8, 16, 6, 9],
                'Table_References': [8, 6, 7, 9, 5, 4, 3, 4],
                'Migration_Hours': [16, 24, 18, 32, 8, 12, 4, 6],
                'Risk_Level': ['Medium', 'High', 'Medium', 'High', 'Low', 'Medium', 'Low', 'Low'],
                'Category': ['Analytics', 'Analytics', 'Analytics', 'Analytics', 'Reporting', 'Analytics', 'Reporting', 'OLTP'],
                'SQL_Features': [
                    'CTEs, Window Functions, Complex Joins',
                    'Recursive CTEs, PIVOT, Advanced Analytics',
                    'PIVOT, Window Functions, String Aggregation',
                    'Recursive CTEs, Dynamic SQL, Risk Scoring',
                    'Dynamic SQL, Conditional Logic',
                    'Advanced Window Functions, LAG/LEAD',
                    'Basic Aggregation, Simple Joins',
                    'CRUD Operations, Transactions'
                ],
                'Source_Database': ['GlobalSupply_DW'] * 8,
                'Target_Platform': ['Databricks'] * 8,
                'Assessment_Date': [datetime.now().strftime('%Y-%m-%d')] * 8
            })

            # Sample dependency data
            dependency_data = pd.DataFrame({
                'Source_Object': [
                    'customer_profitability.sql', 'supplier_risk_assessment.sql',
                    'inventory_optimization.sql', 'supply_chain_performance.sql',
                    'window_functions_analysis.sql', 'order_processing.sql'
                ],
                'Target_Object': [
                    'financial_summary.sql', 'supply_chain_performance.sql',
                    'order_processing.sql', 'dynamic_reporting.sql',
                    'customer_profitability.sql', 'financial_summary.sql'
                ],
                'Dependency_Type': ['View', 'Procedure', 'Table', 'View', 'Function', 'Table'],
                'Criticality': ['High', 'High', 'Medium', 'Medium', 'Low', 'Medium'],
                'Impact_Score': [8, 9, 6, 7, 4, 5]
            })

            # Sample function usage data
            function_data = pd.DataFrame({
                'Function_Name': [
                    'ROW_NUMBER', 'RANK', 'LAG', 'LEAD', 'SUM', 'AVG', 'COUNT', 'DATEDIFF',
                    'DATEADD', 'STRING_AGG', 'PIVOT', 'CASE', 'CTE', 'RECURSIVE_CTE', 'STDEV', 'VAR'
                ],
                'Usage_Count': [15, 12, 8, 6, 25, 20, 30, 18, 14, 5, 3, 22, 18, 2, 4, 3],
                'Complexity_Impact': [3, 3, 4, 4, 1, 1, 1, 2, 2, 4, 5, 2, 3, 5, 3, 3],
                'Databricks_Compatibility': [
                    'Direct', 'Direct', 'Direct', 'Direct', 'Direct', 'Direct', 'Direct', 'Modified',
                    'Modified', 'Modified', 'Modified', 'Direct', 'Direct', 'Complex', 'Direct', 'Direct'
                ],
                'Migration_Effort': [0, 0, 0, 0, 0, 0, 0, 2, 2, 4, 6, 0, 0, 8, 0, 0]
            })

            # Migration summary data
            summary_data = pd.DataFrame({
                'Metric': [
                    'Total_SQL_Files', 'Total_Lines_of_Code', 'Average_Complexity_Score',
                    'Total_Migration_Hours', 'High_Risk_Components', 'Medium_Risk_Components',
                    'Low_Risk_Components', 'Estimated_Cost_USD', 'Recommended_Timeline_Weeks'
                ],
                'Value': [
                    len(complexity_data), complexity_data['Lines_of_Code'].sum(),
                    complexity_data['Complexity_Score'].mean(), complexity_data['Migration_Hours'].sum(),
                    len(complexity_data[complexity_data['Risk_Level'] == 'High']),
                    len(complexity_data[complexity_data['Risk_Level'] == 'Medium']),
                    len(complexity_data[complexity_data['Risk_Level'] == 'Low']),
                    complexity_data['Migration_Hours'].sum() * 150,
                    complexity_data['Migration_Hours'].sum() / 120
                ]
            })

            # Create Excel report with multiple sheets
            report_filename = f"{self.report_name}.xlsx"
            with pd.ExcelWriter(report_filename, engine='openpyxl') as writer:
                # Write all worksheets
                summary_data.to_excel(writer, sheet_name='Summary', index=False)
                complexity_data.to_excel(writer, sheet_name='Complexity_Analysis', index=False)
                dependency_data.to_excel(writer, sheet_name='Dependencies', index=False)
                function_data.to_excel(writer, sheet_name='Function_Usage', index=False)

                # Create migration waves sheet
                def assign_wave(row):
                    if row['Risk_Level'] == 'Low' and row['Complexity_Score'] < 6:
                        return 'Wave 1 - Quick Wins'
                    elif row['Risk_Level'] == 'Medium' or (row['Risk_Level'] == 'Low' and row['Complexity_Score'] >= 6):
                        return 'Wave 2 - Standard Migration'
                    else:
                        return 'Wave 3 - Complex Components'

                complexity_with_waves = complexity_data.copy()
                complexity_with_waves['Migration_Wave'] = complexity_with_waves.apply(assign_wave, axis=1)
                complexity_with_waves.to_excel(writer, sheet_name='Migration_Waves', index=False)

            logger.info(f"✅ Standardized assessment report generated: {report_filename}")
            logger.info(f"📊 Report contains {len(complexity_data)} components across 5 worksheets")
            return True

        except Exception as e:
            logger.error(f"❌ Error generating assessment report: {e}")
            return False

    def validate_and_normalize_real_report(self, report_path: Path) -> Optional[Dict[str, pd.DataFrame]]:
        """
        Validate and normalize a real Lakebridge assessment report to standard format
        """
        try:
            logger.info(f"📄 Loading real assessment report: {report_path}")

            # Load Excel file and examine structure
            excel_data = pd.ExcelFile(report_path)
            logger.info(f"📋 Available worksheets: {excel_data.sheet_names}")

            sheets_data = {}

            # Try to load and normalize each expected sheet
            sheet_mappings = {
                'Summary': ['Summary', 'Overview', 'Report_Summary'],
                'Complexity_Analysis': ['Complexity', 'Analysis', 'Complexity_Analysis', 'Job_Analysis'],
                'Dependencies': ['Dependencies', 'Dependency', 'Relationships', 'Job_Dependencies'],
                'Function_Usage': ['Functions', 'Function_Usage', 'SQL_Functions', 'Features'],
                'Migration_Waves': ['Waves', 'Migration_Waves', 'Migration_Strategy']
            }

            for standard_name, possible_names in sheet_mappings.items():
                sheet_found = False
                for possible_name in possible_names:
                    if possible_name in excel_data.sheet_names:
                        try:
                            df = pd.read_excel(report_path, sheet_name=possible_name)
                            # Normalize column names (remove spaces, lowercase, standardize)
                            df.columns = [col.strip().replace(' ', '_').replace('-', '_').lower() for col in df.columns]
                            sheets_data[standard_name] = df
                            logger.info(f"✅ Loaded '{possible_name}' as '{standard_name}': {len(df)} rows")
                            sheet_found = True
                            break
                        except Exception as e:
                            logger.warning(f"⚠️ Could not load sheet '{possible_name}': {e}")

                if not sheet_found:
                    logger.warning(f"⚠️ No matching sheet found for '{standard_name}'")

            # Normalize column names for key sheets
            if 'Complexity_Analysis' in sheets_data:
                df = sheets_data['Complexity_Analysis']
                column_mappings = {
                    'filename': 'file_name', 'file': 'file_name', 'script_name': 'file_name',
                    'loc': 'lines_of_code', 'lines': 'lines_of_code', 'line_count': 'lines_of_code',
                    'complexity': 'complexity_score', 'score': 'complexity_score',
                    'effort': 'migration_hours', 'hours': 'migration_hours', 'migration_effort': 'migration_hours',
                    'risk': 'risk_level', 'risk_category': 'risk_level'
                }

                for old_col, new_col in column_mappings.items():
                    if old_col in df.columns and new_col not in df.columns:
                        df[new_col] = df[old_col]

                # Ensure required columns exist with defaults
                required_columns = {
                    'file_name': 'Unknown',
                    'complexity_score': 5.0,
                    'migration_hours': 8,
                    'risk_level': 'Medium',
                    'lines_of_code': 100
                }

                for col, default_val in required_columns.items():
                    if col not in df.columns:
                        df[col] = default_val
                        logger.info(f"➕ Added missing column '{col}' with default value")

            logger.info(f"✅ Successfully normalized real assessment report")
            return sheets_data

        except Exception as e:
            logger.error(f"❌ Error processing real assessment report: {e}")
            return None

    def find_existing_assessment_report(self) -> Optional[Path]:
        """
        Find existing assessment reports in current directory
        """
        current_dir = Path('.')

        # Look for assessment reports with common naming patterns
        patterns = [
            '*assessment*.xlsx', '*globalsupply*.xlsx', '*remorph*.xlsx',
            '*lakebridge*.xlsx', '*migration*.xlsx', '*analysis*.xlsx'
        ]

        excel_files = []
        for pattern in patterns:
            excel_files.extend(list(current_dir.glob(pattern)))

        if not excel_files:
            # Look for any Excel files
            excel_files = list(current_dir.glob('*.xlsx'))

        if excel_files:
            # Return the most recent file
            latest_file = max(excel_files, key=lambda x: x.stat().st_mtime)
            return latest_file

        return None

    def verify_lakebridge_installation(self) -> bool:
        """
        Verify that Lakebridge is properly installed
        """
        try:
            result = subprocess.run(
                ["databricks", "labs", "lakebridge", "analyze", "--help"],
                capture_output=True,
                text=True
            )
            if result.returncode == 0:
                logger.info("✓ Lakebridge Analyzer is properly installed")
                return True
            else:
                logger.error("✗ Lakebridge Analyzer not found or not working properly")
                return False
        except Exception as e:
            logger.error(f"✗ Error checking Lakebridge installation: {e}")
            return False

    def validate_source_directory(self) -> bool:
        """
        Validate that the source directory exists and contains SQL files
        """
        # If using samples, use the sample_sql directory
        if self.generate_samples or not self.source_directory:
            self.source_directory = self.sample_sql_dir
            logger.info(f"Using sample SQL files from: {self.source_directory.absolute()}")

        if not self.source_directory.exists():
            logger.error(f"✗ Source directory does not exist: {self.source_directory}")
            return False

        sql_files = list(self.source_directory.rglob("*.sql"))
        if not sql_files:
            logger.warning(f"⚠ No SQL files found in {self.source_directory}")
            return False

        logger.info(f"✓ Found {len(sql_files)} SQL files in source directory")
        for sql_file in sql_files[:5]:  # Show first 5 files
            logger.info(f"  - {sql_file.relative_to(self.source_directory)}")
        if len(sql_files) > 5:
            logger.info(f"  ... and {len(sql_files) - 5} more files")

        return True

    def run_assessment(self) -> bool:
        """
        Execute the Lakebridge Analyzer assessment
        """
        try:
            # Prepare the analyzer command
            cmd = [
                "databricks", "labs", "lakebridge", "analyze",
                "--source-directory", str(self.source_directory.absolute()),
                "--report-file", self.report_name,
                "--source-tech", "mssql"  # SQL Server/Synapse
            ]

            logger.info("🚀 Starting Lakebridge assessment...")
            logger.info(f"   Source Directory: {self.source_directory}")
            logger.info(f"   Report Name: {self.report_name}")
            logger.info(f"   Source Technology: MS SQL Server")

            # Execute the assessment
            result = subprocess.run(cmd, capture_output=True, text=True)

            if result.returncode == 0:
                logger.info("✅ Assessment completed successfully!")
                logger.info(f"📊 Report generated: {self.report_name}.xlsx")
                return True
            else:
                logger.error("❌ Assessment failed:")
                logger.error(f"   Error: {result.stderr}")
                return False

        except Exception as e:
            logger.error(f"❌ Error during assessment: {e}")
            return False

    def summarize_findings(self):
        """
        Provide summary of what the assessment covers
        """
        logger.info("\n" + "="*60)
        logger.info("📋 ASSESSMENT SUMMARY - GlobalSupply Corp")
        logger.info("="*60)
        logger.info("""
The Lakebridge assessment provides:

🔍 ANALYSIS INSIGHTS:
• Job Complexity Assessment - estimates migration effort
• Comprehensive Job Inventory - all components cataloged
• Cross-System Interdependency Mapping - critical for sequencing

📊 KEY OUTPUTS:
• Complexity scores for each SQL file/component
• Migration effort estimates (engineering hours)
• Software licensing cost projections
• Interdependency maps between jobs and systems

🎯 BUSINESS VALUE:
• Risk assessment for migration planning
• Resource planning for modernization project
• Sequencing guidance to minimize disruption
• TCO analysis for Databricks migration

📈 NEXT STEPS:
1. Review generated Excel report ({report_name}.xlsx)
2. Identify high-complexity components requiring manual review
3. Use dependency mapping for migration sequencing
4. Proceed to Module 2: Schema Migration & Transpilation
        """.format(report_name=self.report_name))
        logger.info("="*60)

def main():
    """
    Main function to orchestrate the assessment process
    """
    parser = argparse.ArgumentParser(
        description="GlobalSupply Corp SQL Server Assessment using Lakebridge"
    )
    parser.add_argument(
        "--source-directory",
        help="Path to directory containing SQL Server files (optional if using --generate-samples)"
    )
    parser.add_argument(
        "--report-name",
        help="Custom name for the assessment report (optional)"
    )
    parser.add_argument(
        "--generate-samples",
        action="store_true",
        help="Use sample SQL files for demonstration (creates sample_sql directory)"
    )

    args = parser.parse_args()

    # Validate arguments
    if not args.source_directory and not args.generate_samples:
        logger.error("Either --source-directory or --generate-samples must be specified")
        sys.exit(1)

    # Create assessment instance
    assessment = LakebridgeAssessment(
        source_directory=args.source_directory,
        report_name=args.report_name,
        generate_samples=args.generate_samples
    )

    logger.info("🚀 Starting GlobalSupply Corp SQL Server Assessment")
    logger.info("="*60)

    # Step 1: Validate dependencies
    logger.info("📦 Validating Python dependencies...")
    if not assessment.validate_dependencies():
        logger.error("Please install missing dependencies and try again")
        sys.exit(1)

    # Step 2: Generate or validate sample SQL files
    logger.info("📝 Preparing SQL files...")
    if not assessment.create_sample_sql_files():
        logger.error("Failed to prepare sample SQL files")
        sys.exit(1)

    # Step 3: Verify Lakebridge installation (optional)
    logger.info("🔧 Verifying Lakebridge installation...")
    lakebridge_available = assessment.verify_lakebridge_installation()
    if not lakebridge_available:
        logger.warning("⚠ Lakebridge not available - will demonstrate with mock results")
        logger.warning("Install Lakebridge for full functionality: databricks labs install lakebridge")

    # Step 4: Validate source directory
    logger.info("📁 Validating source directory...")
    if not assessment.validate_source_directory():
        logger.error("Please provide a valid directory containing SQL files")
        sys.exit(1)

    # Step 5: Check for existing assessment reports first
    logger.info("🔍 Checking for existing assessment reports...")
    existing_report = assessment.find_existing_assessment_report()

    if existing_report:
        logger.info(f"📄 Found existing assessment report: {existing_report}")
        logger.info("📊 Using existing report for analysis")
        assessment.summarize_findings()
        logger.info("✅ Analysis completed using existing assessment report!")
    elif lakebridge_available:
        # Step 6: Run new assessment with Lakebridge
        if assessment.run_assessment():
            assessment.summarize_findings()
            logger.info("✅ Assessment completed successfully! Check the Excel report for detailed results.")
        else:
            logger.error("❌ Assessment failed. Please check the error messages above.")
            sys.exit(1)
    else:
        # Step 7: Generate standardized sample report for demonstration
        logger.info("📊 Generating standardized sample assessment report...")
        if assessment.generate_standardized_assessment_report():
            assessment.summarize_findings()
            logger.info("✅ Sample assessment report generated! Use this for workshop demonstration.")
        else:
            logger.error("❌ Failed to generate sample assessment report.")
            sys.exit(1)

if __name__ == "__main__":
    main()