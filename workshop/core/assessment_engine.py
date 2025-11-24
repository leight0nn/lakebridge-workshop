"""
Assessment Engine - Abstracted Logic for Module 1 Assessment

This module contains all the complex assessment logic extracted from the original
notebook, providing a clean interface for workshop participants.

Key Functions:
- load_assessment_data(): Intelligent data loading with fallback
- create_sample_data(): Generate realistic demonstration data
- process_assessment_results(): Clean and normalize assessment output
- generate_insights(): Create business insights and recommendations
"""

import pandas as pd
import json
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Union, Any
import subprocess

logger = logging.getLogger(__name__)


def find_assessment_report() -> Optional[Path]:
    """
    Intelligent assessment report discovery with multiple fallback strategies.
    
    Returns:
        Path to the most recent assessment report, or None if not found
    """
    current_dir = Path('.')
    
    # Look for assessment reports with comprehensive naming patterns
    patterns = [
        '*assessment*.xlsx', '*globalsupply*.xlsx', '*remorph*.xlsx',
        '*lakebridge*.xlsx', '*migration*.xlsx', '*analysis*.xlsx'
    ]
    
    excel_files = []
    for pattern in patterns:
        excel_files.extend(list(current_dir.glob(pattern)))
    
    if not excel_files:
        # Look for any Excel files as fallback
        excel_files = list(current_dir.glob('*.xlsx'))
    
    if excel_files:
        # Return the most recent file
        latest_file = max(excel_files, key=lambda x: x.stat().st_mtime)
        return latest_file
    
    return None


def normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize column names to standard format for consistent processing.
    
    Args:
        df: DataFrame with potentially inconsistent column names
        
    Returns:
        DataFrame with normalized column names
    """
    # Standardize column names (remove spaces, lowercase, handle variations)
    df.columns = [col.strip().replace(' ', '_').replace('-', '_').lower() for col in df.columns]
    return df


def map_columns_to_standard(df: pd.DataFrame, sheet_type: str) -> pd.DataFrame:
    """
    Map various column name variations to standardized names.
    
    Args:
        df: DataFrame with columns to map
        sheet_type: Type of sheet ('Complexity_Analysis', 'Dependencies', etc.)
        
    Returns:
        DataFrame with standardized column names
    """
    if sheet_type == 'Complexity_Analysis':
        column_mappings = {
            'filename': 'file_name', 'file': 'file_name', 'script_name': 'file_name',
            'name': 'file_name', 'object_name': 'file_name',
            'loc': 'lines_of_code', 'lines': 'lines_of_code', 'line_count': 'lines_of_code',
            'complexity': 'complexity_score', 'score': 'complexity_score',
            'effort': 'migration_hours', 'hours': 'migration_hours', 
            'migration_effort': 'migration_hours', 'work_hours': 'migration_hours',
            'risk': 'risk_level', 'risk_category': 'risk_level',
            'functions': 'functions_used', 'function_count': 'functions_used',
            'tables': 'table_references', 'table_count': 'table_references'
        }
    elif sheet_type == 'Dependencies':
        column_mappings = {
            'source': 'source_object', 'from_object': 'source_object',
            'target': 'target_object', 'to_object': 'target_object',
            'type': 'dependency_type', 'dep_type': 'dependency_type',
            'priority': 'criticality', 'importance': 'criticality'
        }
    elif sheet_type == 'Function_Usage':
        column_mappings = {
            'function': 'function_name', 'func_name': 'function_name',
            'count': 'usage_count', 'usage': 'usage_count',
            'complexity_score': 'complexity_impact', 'impact': 'complexity_impact',
            'compatibility': 'databricks_compatibility', 'compat': 'databricks_compatibility'
        }
    else:
        column_mappings = {}
    
    # Apply mappings
    for old_col, new_col in column_mappings.items():
        if old_col in df.columns and new_col not in df.columns:
            df[new_col] = df[old_col]
    
    return df


def ensure_required_columns(df: pd.DataFrame, sheet_type: str) -> pd.DataFrame:
    """
    Ensure all required columns exist with sensible defaults.
    
    Args:
        df: DataFrame to validate
        sheet_type: Type of sheet to validate for
        
    Returns:
        DataFrame with all required columns
    """
    if sheet_type == 'Complexity_Analysis':
        required_columns = {
            'file_name': 'Unknown',
            'complexity_score': 5.0,
            'migration_hours': 8,
            'risk_level': 'Medium',
            'lines_of_code': 100,
            'category': 'Other',
            'sql_features': 'Standard SQL'
        }
    elif sheet_type == 'Dependencies':
        required_columns = {
            'source_object': 'Unknown',
            'target_object': 'Unknown', 
            'dependency_type': 'Unknown',
            'criticality': 'Medium'
        }
    elif sheet_type == 'Function_Usage':
        required_columns = {
            'function_name': 'Unknown',
            'usage_count': 1,
            'complexity_impact': 3,
            'databricks_compatibility': 'Unknown'
        }
    else:
        required_columns = {}
    
    # Add missing columns with defaults
    for col, default_val in required_columns.items():
        if col not in df.columns:
            df[col] = default_val
    
    return df


def create_sample_data() -> Dict[str, pd.DataFrame]:
    """
    Create comprehensive sample data for workshop demonstration.
    
    Returns:
        Dictionary containing sample DataFrames for each analysis type
    """
    # Sample complexity analysis representing real-world SQL Server workloads
    complexity_data = pd.DataFrame({
        'file_name': [
            'supply_chain_performance.sql', 'inventory_optimization.sql', 'customer_profitability.sql',
            'supplier_risk_assessment.sql', 'dynamic_reporting.sql', 'window_functions_analysis.sql',
            'financial_summary.sql', 'order_processing.sql'
        ],
        'lines_of_code': [145, 198, 167, 223, 89, 134, 67, 89],
        'complexity_score': [8.5, 9.2, 7.8, 9.8, 6.5, 7.2, 4.5, 5.1],
        'functions_used': [12, 18, 14, 22, 8, 16, 6, 9],
        'table_references': [8, 6, 7, 9, 5, 4, 3, 4],
        'migration_hours': [16, 24, 18, 32, 8, 12, 4, 6],
        'risk_level': ['Medium', 'High', 'Medium', 'High', 'Low', 'Medium', 'Low', 'Low'],
        'category': ['Analytics', 'Analytics', 'Analytics', 'Analytics', 'Reporting', 'Analytics', 'Reporting', 'OLTP'],
        'sql_features': [
            'CTEs, Window Functions, Complex Joins',
            'Recursive CTEs, PIVOT, Advanced Analytics', 
            'PIVOT, Window Functions, String Aggregation',
            'Recursive CTEs, Dynamic SQL, Risk Scoring',
            'Dynamic SQL, Conditional Logic',
            'Advanced Window Functions, LAG/LEAD',
            'Basic Aggregation, Simple Joins',
            'CRUD Operations, Transactions'
        ]
    })
    
    # Sample dependency mapping with realistic relationships
    dependency_data = pd.DataFrame({
        'source_object': [
            'customer_profitability.sql', 'supplier_risk_assessment.sql', 'inventory_optimization.sql',
            'supply_chain_performance.sql', 'window_functions_analysis.sql', 'order_processing.sql'
        ],
        'target_object': [
            'financial_summary.sql', 'supply_chain_performance.sql', 'order_processing.sql',
            'dynamic_reporting.sql', 'customer_profitability.sql', 'financial_summary.sql'
        ],
        'dependency_type': ['View', 'Procedure', 'Table', 'View', 'Function', 'Table'],
        'criticality': ['High', 'High', 'Medium', 'Medium', 'Low', 'Medium']
    })
    
    # Sample function usage with Databricks compatibility assessment
    function_data = pd.DataFrame({
        'function_name': [
            'ROW_NUMBER', 'RANK', 'LAG', 'LEAD', 'SUM', 'AVG', 'COUNT', 'DATEDIFF', 
            'DATEADD', 'STRING_AGG', 'PIVOT', 'CASE', 'CTE', 'RECURSIVE_CTE', 'STDEV', 'VAR'
        ],
        'usage_count': [15, 12, 8, 6, 25, 20, 30, 18, 14, 5, 3, 22, 18, 2, 4, 3],
        'complexity_impact': [3, 3, 4, 4, 1, 1, 1, 2, 2, 4, 5, 2, 3, 5, 3, 3],
        'databricks_compatibility': [
            'Direct', 'Direct', 'Direct', 'Direct', 'Direct', 'Direct', 'Direct', 'Modified',
            'Modified', 'Modified', 'Modified', 'Direct', 'Direct', 'Complex', 'Direct', 'Direct'
        ]
    })
    
    return {
        'Complexity_Analysis': complexity_data,
        'Dependencies': dependency_data,
        'Function_Usage': function_data
    }


def load_real_assessment_report(report_file: Path) -> Optional[Dict[str, pd.DataFrame]]:
    """
    Load and normalize a real Lakebridge assessment report.
    
    Args:
        report_file: Path to Excel assessment report
        
    Returns:
        Dictionary of normalized DataFrames or None if loading fails
    """
    try:
        logger.info(f"Loading real assessment report: {report_file}")
        
        # Load the Excel file and examine structure
        excel_data = pd.ExcelFile(report_file)
        logger.info(f"Available worksheets: {excel_data.sheet_names}")
        
        # Map common sheet name variations to standard names
        sheet_mappings = {
            'Summary': ['Summary', 'Overview', 'Report_Summary'],
            'Complexity_Analysis': ['Complexity', 'Analysis', 'Complexity_Analysis', 'Job_Analysis', 'Complexity Analysis'],
            'Dependencies': ['Dependencies', 'Dependency', 'Relationships', 'Job_Dependencies'],
            'Function_Usage': ['Functions', 'Function_Usage', 'SQL_Functions', 'Features', 'Function Usage'],
            'Migration_Waves': ['Waves', 'Migration_Waves', 'Migration_Strategy', 'Migration Waves']
        }
        
        sheets_data = {}
        
        # Load and normalize each sheet
        for standard_name, possible_names in sheet_mappings.items():
            sheet_found = False
            for possible_name in possible_names:
                if possible_name in excel_data.sheet_names:
                    try:
                        df = pd.read_excel(report_file, sheet_name=possible_name)
                        if len(df) > 0:  # Only process non-empty sheets
                            # Apply all normalization steps
                            df = normalize_column_names(df)
                            df = map_columns_to_standard(df, standard_name)
                            df = ensure_required_columns(df, standard_name)
                            
                            sheets_data[standard_name] = df
                            logger.info(f"Loaded '{possible_name}' as '{standard_name}': {len(df)} rows, {len(df.columns)} columns")
                            sheet_found = True
                            break
                    except Exception as e:
                        logger.warning(f"Could not load sheet '{possible_name}': {e}")
            
            if not sheet_found:
                logger.warning(f"No matching sheet found for '{standard_name}'")
        
        # If we got at least some data, we're good
        if sheets_data:
            logger.info(f"Successfully loaded real assessment report with {len(sheets_data)} worksheets")
            return sheets_data
            
    except Exception as e:
        logger.error(f"Error loading Excel file: {e}")
    
    return None


def load_assessment_data() -> Dict[str, pd.DataFrame]:
    """
    Master function to load assessment data from various sources with intelligent fallback.
    
    This is the main entry point for workshop participants.
    
    Returns:
        Dictionary containing assessment data with consistent structure
    """
    # Step 1: Try to find existing assessment report
    report_file = find_assessment_report()
    
    if report_file:
        print(f"📄 Found assessment report: {report_file}")
        
        # Try to load real report
        real_data = load_real_assessment_report(report_file)
        if real_data:
            print(f"✅ Successfully loaded real assessment report")
            return real_data
    
    # Step 2: Fall back to sample data
    print("📊 No usable assessment report found, using sample workshop data")
    print("")
    print("💡 To use a real assessment report:")
    print("   1. Ensure Lakebridge is installed: databricks labs install lakebridge")
    print("   2. Run assessment: python 01_assessment_analyzer.py --generate-samples")
    print("   3. Or place your existing .xlsx report in this directory")
    
    return create_sample_data()


def process_assessment_results(data: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
    """
    Process raw assessment data into business insights.
    
    Args:
        data: Raw assessment data from load_assessment_data()
        
    Returns:
        Processed insights and recommendations
    """
    insights = {
        'timestamp': datetime.now().isoformat(),
        'data_source': 'real_report' if find_assessment_report() else 'sample_data'
    }
    
    # Process complexity analysis
    if 'Complexity_Analysis' in data and len(data['Complexity_Analysis']) > 0:
        df = data['Complexity_Analysis']
        
        insights['summary_statistics'] = {
            'total_files': len(df),
            'total_loc': df['lines_of_code'].sum() if 'lines_of_code' in df.columns else 0,
            'total_estimated_effort_hours': df['migration_hours'].sum() if 'migration_hours' in df.columns else 0,
            'average_complexity': df['complexity_score'].mean() if 'complexity_score' in df.columns else 5.0,
            'high_complexity_files': len(df[df['complexity_score'] > 8]) if 'complexity_score' in df.columns else 0
        }
        
        # Risk analysis
        if 'risk_level' in df.columns:
            risk_distribution = df['risk_level'].value_counts().to_dict()
            insights['risk_analysis'] = risk_distribution
        
        # Category analysis
        if 'category' in df.columns:
            category_distribution = df['category'].value_counts().to_dict()
            insights['category_analysis'] = category_distribution
        
        # Convert DataFrame to dict for easy access
        insights['file_analysis'] = {}
        for _, row in df.iterrows():
            filename = row.get('file_name', 'unknown')
            insights['file_analysis'][filename] = {
                'complexity_score': row.get('complexity_score', 5.0),
                'migration_hours': row.get('migration_hours', 8),
                'risk_level': row.get('risk_level', 'Medium'),
                'lines_of_code': row.get('lines_of_code', 100),
                'sql_features': row.get('sql_features', 'Standard SQL'),
                'category': row.get('category', 'Other')
            }
    
    # Process dependencies if available
    if 'Dependencies' in data and len(data['Dependencies']) > 0:
        dep_df = data['Dependencies']
        insights['dependency_analysis'] = {
            'total_dependencies': len(dep_df),
            'high_criticality_deps': len(dep_df[dep_df['criticality'] == 'High']) if 'criticality' in dep_df.columns else 0
        }
    
    # Process function usage if available
    if 'Function_Usage' in data and len(data['Function_Usage']) > 0:
        func_df = data['Function_Usage']
        if 'databricks_compatibility' in func_df.columns:
            compat_analysis = func_df['databricks_compatibility'].value_counts().to_dict()
            insights['function_analysis'] = compat_analysis
    
    return insights


def generate_recommendations(insights: Dict[str, Any]) -> List[str]:
    """
    Generate business recommendations based on assessment insights.
    
    Args:
        insights: Processed insights from process_assessment_results()
        
    Returns:
        List of actionable recommendations
    """
    recommendations = []
    
    stats = insights.get('summary_statistics', {})
    total_files = stats.get('total_files', 0)
    total_hours = stats.get('total_estimated_effort_hours', 0)
    avg_complexity = stats.get('average_complexity', 5.0)
    high_complexity_files = stats.get('high_complexity_files', 0)
    
    # Strategic recommendations based on complexity
    if avg_complexity > 7.5:
        recommendations.append("Consider hiring Databricks Professional Services for complex components")
        recommendations.append("Plan for extended timeline due to high overall complexity")
    elif avg_complexity < 5.0:
        recommendations.append("Excellent opportunity for rapid migration with low risk")
        recommendations.append("Consider accelerated timeline with current team")
    
    # Timeline and resource recommendations
    if total_hours > 100:
        recommendations.append("Recommend 3-4 person dedicated team for efficient execution")
        recommendations.append("Plan phased approach with 3 migration waves")
    else:
        recommendations.append("Suitable for 2-3 person team with part-time commitment")
    
    # Risk-based recommendations
    if high_complexity_files > 0:
        recommendations.append(f"Assign senior architects to {high_complexity_files} high-complexity components")
        recommendations.append("Develop proof-of-concepts for complex components before full migration")
    
    # Dependency recommendations
    if 'dependency_analysis' in insights:
        dep_stats = insights['dependency_analysis']
        if dep_stats.get('high_criticality_deps', 0) > 0:
            recommendations.append("Prioritize high-criticality dependencies in migration sequencing")
    
    # Function compatibility recommendations
    if 'function_analysis' in insights:
        func_stats = insights['function_analysis']
        complex_functions = func_stats.get('Complex', 0) + func_stats.get('Manual', 0)
        if complex_functions > 0:
            recommendations.append("Plan additional testing for complex SQL function conversions")
    
    # Business value recommendations
    recommendations.append("Begin with Module 2: Schema Migration & Transpilation")
    recommendations.append("Focus on quick wins to build team confidence and momentum")
    recommendations.append("Plan comprehensive data reconciliation testing (Module 3)")
    
    return recommendations