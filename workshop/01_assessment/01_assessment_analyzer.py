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

    # Step 5: Run the assessment (if Lakebridge is available)
    if lakebridge_available:
        if assessment.run_assessment():
            assessment.summarize_findings()
            logger.info("✅ Assessment completed successfully! Check the Excel report for detailed results.")
        else:
            logger.error("❌ Assessment failed. Please check the error messages above.")
            sys.exit(1)
    else:
        # Mock assessment for demonstration
        logger.info("📊 Running demonstration mode (Lakebridge not available)")
        assessment.summarize_findings()
        logger.info("✅ Demonstration completed! Install Lakebridge for actual assessment results.")

if __name__ == "__main__":
    main()