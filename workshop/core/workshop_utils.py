"""
Workshop Utilities - Core Functions for Lakebridge Workshop

This module provides a clean, simple interface for workshop participants to focus
on SQL migration concepts rather than Python implementation details.

Key Features:
- Simple function calls that abstract complex logic
- Consistent error handling and user feedback
- Integration with existing lakebridge_adapter
- Educational-focused output formatting
- Backward compatibility with existing notebooks

Usage:
    from workshop.core import workshop_utils as wu
    
    # Load assessment data
    data = wu.load_assessment_data()
    
    # Display results
    wu.display_summary(data)
    wu.create_dashboard(data)
"""

import sys
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
import logging

# Configure logging for workshop context
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)


def setup_workshop_environment() -> Dict[str, Any]:
    """
    Set up the workshop environment with necessary imports and configuration.
    
    Returns:
        Dict containing imported modules and configuration status
    """
    try:
        import pandas as pd
        import matplotlib.pyplot as plt
        import seaborn as sns
        import numpy as np
        import warnings
        
        # Configure plotting style
        warnings.filterwarnings('ignore')
        plt.style.use('default')
        sns.set_palette("husl")
        plt.rcParams['figure.figsize'] = (12, 8)
        plt.rcParams['font.size'] = 10
        
        print("✅ Workshop environment configured successfully")
        print("📊 Ready for GlobalSupply Corp migration analysis")
        
        return {
            'pd': pd,
            'plt': plt,
            'sns': sns,
            'np': np,
            'status': 'success'
        }
    except ImportError as e:
        print(f"❌ Missing required package: {e}")
        print("Please install: pip install pandas matplotlib seaborn numpy")
        return {'status': 'error', 'error': str(e)}


def print_section_header(title: str, emoji: str = "📊") -> None:
    """Print a formatted section header for workshop clarity."""
    print(f"\n{emoji} {title.upper()}")
    print("=" * (len(title) + 5))


def print_subsection(title: str, emoji: str = "📋") -> None:
    """Print a formatted subsection header."""
    print(f"\n{emoji} {title}")
    print("-" * (len(title) + 5))


def print_success(message: str) -> None:
    """Print a success message with consistent formatting."""
    print(f"✅ {message}")


def print_warning(message: str) -> None:
    """Print a warning message with consistent formatting."""
    print(f"⚠️ {message}")


def print_error(message: str) -> None:
    """Print an error message with consistent formatting."""
    print(f"❌ {message}")


def print_info(message: str) -> None:
    """Print an info message with consistent formatting."""
    print(f"💡 {message}")


def format_number(number: float, as_currency: bool = False) -> str:
    """Format numbers for display in workshop context."""
    if as_currency:
        return f"${number:,.0f}"
    else:
        return f"{number:,.1f}" if number != int(number) else f"{number:,}"


def calculate_effort_summary(hours: List[float]) -> Dict[str, Any]:
    """Calculate effort summary statistics for display."""
    total_hours = sum(hours)
    return {
        'total_hours': total_hours,
        'cost_estimate': total_hours * 150,  # $150/hour rate
        'weeks_single': total_hours / 40,
        'weeks_team': total_hours / 160,  # 4-person team
        'timeline_recommended': total_hours / 120  # 3-person team with coordination
    }


def create_risk_emoji(risk_level: str) -> str:
    """Return appropriate emoji for risk level."""
    risk_emojis = {
        'Low': '🟢',
        'Medium': '🟡', 
        'High': '🔴',
        'Critical': '🔴🔴'
    }
    return risk_emojis.get(risk_level, '⚪')


def create_complexity_indicator(complexity: float) -> str:
    """Create visual complexity indicator."""
    if complexity <= 4.0:
        return "🟢 Simple"
    elif complexity <= 7.0:
        return "🟡 Medium"
    elif complexity <= 9.0:
        return "🔴 High"
    else:
        return "🔴🔴 Complex"


def display_file_list(files_data: Dict[str, Any], title: str = "Files Analyzed") -> None:
    """Display a formatted list of files with key metrics."""
    print_subsection(title, "📄")
    
    for filename, info in files_data.items():
        clean_name = filename.replace('.sql', '')
        complexity = info.get('complexity_score', info.get('complexity', 5.0))
        hours = info.get('migration_hours', info.get('hours', 8))
        risk = info.get('risk_level', 'Medium')
        
        complexity_indicator = create_complexity_indicator(complexity)
        risk_emoji = create_risk_emoji(risk)
        
        print(f"  {complexity_indicator} {clean_name}")
        print(f"    {risk_emoji} Risk: {risk} | Effort: {hours}h | Complexity: {complexity}/10")


def display_key_metrics(data: Dict[str, Any], context: str = "Assessment") -> None:
    """Display key metrics in a consistent format."""
    print_section_header(f"{context} Key Metrics", "📈")
    
    if 'summary_statistics' in data:
        stats = data['summary_statistics']
        print(f"📁 Total Files: {stats.get('total_files', 0)}")
        print(f"📏 Lines of Code: {format_number(stats.get('total_loc', 0))}")
        print(f"⏱️ Total Hours: {format_number(stats.get('total_estimated_effort_hours', 0))}")
        print(f"💰 Cost Estimate: {format_number(stats.get('total_estimated_effort_hours', 0) * 150, True)}")
        print(f"📊 Avg Complexity: {stats.get('average_complexity', 0):.1f}/10")
    else:
        print("📊 Summary statistics not available")


def display_recommendations(recommendations: List[str], title: str = "Recommendations") -> None:
    """Display recommendations in a formatted list."""
    print_subsection(title, "💡")
    
    for i, rec in enumerate(recommendations, 1):
        print(f"  {i}. {rec}")


def create_migration_waves(files_data: Dict[str, Any]) -> Dict[str, List[str]]:
    """Organize files into migration waves based on complexity and risk."""
    waves = {
        'Wave 1 - Quick Wins': [],
        'Wave 2 - Standard Migration': [],
        'Wave 3 - Complex Components': []
    }
    
    for filename, info in files_data.items():
        risk = info.get('risk_level', 'Medium')
        complexity = info.get('complexity_score', info.get('complexity', 5.0))
        
        if risk == 'Low' and complexity < 6:
            waves['Wave 1 - Quick Wins'].append(filename)
        elif risk == 'Medium' or (risk == 'Low' and complexity >= 6):
            waves['Wave 2 - Standard Migration'].append(filename)
        else:
            waves['Wave 3 - Complex Components'].append(filename)
    
    return waves


def display_migration_strategy(files_data: Dict[str, Any]) -> None:
    """Display migration wave strategy."""
    print_section_header("Migration Wave Strategy", "🌊")
    
    waves = create_migration_waves(files_data)
    
    wave_descriptions = {
        'Wave 1 - Quick Wins': {
            'emoji': '🟢',
            'description': 'Low risk, straightforward migrations to build momentum',
            'timeline': '2-4 weeks',
            'team': '1-2 developers'
        },
        'Wave 2 - Standard Migration': {
            'emoji': '🟡', 
            'description': 'Standard complexity migrations with moderate risk',
            'timeline': '4-8 weeks',
            'team': '2-3 developers'
        },
        'Wave 3 - Complex Components': {
            'emoji': '🔴',
            'description': 'High complexity/risk components requiring expert attention',
            'timeline': '6-12 weeks',
            'team': '3-4 senior developers'
        }
    }
    
    for wave_name, files in waves.items():
        if not files:
            continue
            
        wave_info = wave_descriptions[wave_name]
        total_hours = sum(files_data[f].get('migration_hours', files_data[f].get('hours', 8)) for f in files)
        
        print(f"\n{wave_info['emoji']} {wave_name}:")
        print(f"   📄 Components: {len(files)}")
        print(f"   ⏱️ Total Hours: {total_hours}")
        print(f"   📅 Timeline: {wave_info['timeline']}")
        print(f"   👥 Team Size: {wave_info['team']}")
        print(f"   💡 Strategy: {wave_info['description']}")
        
        if len(files) <= 3:
            for filename in files:
                clean_name = filename.replace('.sql', '')
                complexity = files_data[filename].get('complexity_score', files_data[filename].get('complexity', 5.0))
                hours = files_data[filename].get('migration_hours', files_data[filename].get('hours', 8))
                print(f"     • {clean_name} (Complexity: {complexity}/10, {hours}h)")


def display_business_impact() -> None:
    """Display expected business impact and ROI."""
    print_section_header("Expected Business Benefits", "📈")
    
    benefits = [
        ("Query Performance", "3-5x improvement", "Faster analytics, better user experience"),
        ("Analytics Capability", "Advanced ML/AI", "Predictive supply chain optimization"),
        ("Infrastructure Cost", "20-30% reduction", "Cloud-native scaling and optimization"),
        ("Time-to-Insight", "10x faster", "Natural language queries with Genie"),
        ("Scalability", "Unlimited scale", "Handle peak loads without performance issues"),
        ("Innovation Speed", "2x faster", "Rapid prototyping of new analytics")
    ]
    
    for benefit, improvement, description in benefits:
        print(f"💡 {benefit}: {improvement}")
        print(f"   {description}")


def calculate_roi(total_cost: float, annual_savings: float = 200000) -> Dict[str, float]:
    """Calculate ROI metrics."""
    if annual_savings == 0:
        return {'payback_months': 0, 'roi_3year': 0}
    
    payback_months = (total_cost / (annual_savings / 12)) if annual_savings > 0 else 0
    roi_3year = ((annual_savings * 3 - total_cost) / total_cost * 100) if total_cost > 0 else 0
    
    return {
        'payback_months': payback_months,
        'roi_3year': roi_3year,
        'annual_savings': annual_savings,
        'total_cost': total_cost
    }


def display_roi_analysis(total_hours: float) -> None:
    """Display ROI analysis."""
    print_section_header("Cost-Benefit Analysis", "💰")
    
    total_cost = total_hours * 150
    roi = calculate_roi(total_cost)
    
    print(f"📊 Total Migration Effort: {total_hours} hours")
    print(f"💵 Estimated Cost: {format_number(total_cost, True)} (@ $150/hour)")
    print(f"📅 Timeline Options:")
    print(f"   • Sequential: {total_hours/40:.1f} weeks (1 developer)")
    print(f"   • Parallel: {total_hours/160:.1f} weeks (4 developers)")
    print(f"   • Recommended: {total_hours/120:.1f} weeks (3 developers + coordination)")
    
    print(f"\n📈 ROI Projection:")
    print(f"   • Annual Savings Estimate: {format_number(roi['annual_savings'], True)}")
    print(f"   • Payback Period: {roi['payback_months']:.1f} months")
    print(f"   • 3-Year ROI: {roi['roi_3year']:.0f}%")


def create_executive_summary(data: Dict[str, Any]) -> str:
    """Generate executive summary text."""
    if 'file_analysis' in data:
        total_files = len(data['file_analysis'])
        total_hours = sum(
            file_data.get('estimated_effort_hours', 8) 
            for file_data in data['file_analysis'].values()
            if isinstance(file_data, dict)
        )
    elif 'summary_statistics' in data:
        total_files = data['summary_statistics'].get('total_files', 0)
        total_hours = data['summary_statistics'].get('total_estimated_effort_hours', 0)
    else:
        total_files = 0
        total_hours = 0
    
    total_cost = total_hours * 150
    roi = calculate_roi(total_cost)
    
    summary = f"""
🎯 EXECUTIVE SUMMARY - GLOBALSUPPLY CORP MIGRATION

📊 SCOPE & FINDINGS:
• {total_files} SQL components analyzed for migration to Databricks
• Estimated migration effort: {total_hours} hours ({format_number(total_cost, True)})
• Recommended approach: 3-wave phased migration strategy
• Strong business case with {roi['roi_3year']:.0f}% 3-year ROI

📈 BUSINESS IMPACT:
• 3-5x query performance improvement
• Advanced ML/AI capabilities for supply chain optimization
• 20-30% infrastructure cost reduction
• 10x faster time-to-insight with natural language queries

💰 FINANCIAL PROJECTIONS:
• Investment Required: {format_number(total_cost, True)}
• Annual Savings: {format_number(roi['annual_savings'], True)}
• Payback Period: {roi['payback_months']:.1f} months
• 3-Year Net Value: {format_number(roi['annual_savings'] * 3 - total_cost, True)}

🚀 RECOMMENDATION: PROCEED WITH MIGRATION
The analysis demonstrates strong business justification with manageable 
technical risk and clear path to success.
"""
    return summary


def export_results(data: Dict[str, Any], filename_prefix: str = "globalsupply") -> List[str]:
    """Export workshop results to files."""
    try:
        import pandas as pd
        from datetime import datetime
        
        exported_files = []
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Export executive summary
        summary_file = f"{filename_prefix}_executive_summary.txt"
        with open(summary_file, 'w') as f:
            f.write(create_executive_summary(data))
        exported_files.append(summary_file)
        
        # Export detailed data if available
        if 'file_analysis' in data:
            csv_file = f"{filename_prefix}_detailed_analysis_{timestamp}.csv"
            file_data = []
            
            for filename, analysis in data['file_analysis'].items():
                if isinstance(analysis, dict):
                    file_data.append({
                        'filename': filename,
                        'complexity_score': analysis.get('complexity_score', 0),
                        'estimated_hours': analysis.get('estimated_effort_hours', 0),
                        'migration_wave': analysis.get('migration_wave', 1),
                        'risk_factors': ', '.join(analysis.get('risk_factors', []))
                    })
            
            if file_data:
                df = pd.DataFrame(file_data)
                df.to_csv(csv_file, index=False)
                exported_files.append(csv_file)
        
        return exported_files
        
    except Exception as e:
        print_error(f"Export failed: {e}")
        return []


def display_next_steps(module_number: int = 1) -> None:
    """Display next steps based on current module."""
    print_section_header("Next Steps", "🚀")
    
    if module_number == 1:
        print("📋 Immediate Actions (Next 2 weeks):")
        print("   → Review assessment results and business case")
        print("   → Secure executive sponsorship and budget approval") 
        print("   → Assemble migration team with SQL Server + Databricks expertise")
        print("   → Set up Databricks workspace and development environment")
        print("   → Begin Module 2: Schema Migration & Transpilation")
        
    elif module_number == 2:
        print("📋 Immediate Actions:")
        print("   → Review transpiled SQL files for syntax validation")
        print("   → Test simple queries in Databricks SQL environment")
        print("   → Validate Unity Catalog schema references")
        print("   → Begin Module 3: Data Reconciliation")
        
    print(f"\n🎯 Module {module_number + 1}: {'Schema Migration & Transpilation' if module_number == 1 else 'Data Reconciliation'}")
    print(f"Location: ../{'02_transpilation' if module_number == 1 else '03_reconciliation'}/")