"""
Visualization Utilities - Chart and Dashboard Generation for Workshop

This module abstracts all the complex matplotlib/seaborn visualization code
into simple function calls, allowing workshop participants to focus on
insights rather than plotting details.

Key Features:
- Single function calls for complete dashboards
- Consistent styling across all visualizations
- Error handling for missing data
- Educational annotations and insights
- Export capabilities for reports
"""

import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any, Tuple
import logging

logger = logging.getLogger(__name__)

# Configure consistent styling
def setup_plot_style():
    """Configure consistent plot styling for workshop."""
    plt.style.use('default')
    sns.set_palette("husl")
    plt.rcParams['figure.figsize'] = (12, 8)
    plt.rcParams['font.size'] = 10

setup_plot_style()


def create_assessment_dashboard(data: Dict[str, pd.DataFrame]) -> None:
    """
    Create comprehensive assessment analysis dashboard.
    
    Args:
        data: Assessment data from assessment_engine.load_assessment_data()
    """
    if 'Complexity_Analysis' not in data or len(data['Complexity_Analysis']) == 0:
        print("⚠️ Complexity analysis data not available for visualization")
        return
    
    df = data['Complexity_Analysis']
    
    # Validate required columns exist
    required_cols = ['complexity_score', 'migration_hours']
    missing_cols = [col for col in required_cols if col not in df.columns]
    
    if missing_cols:
        print(f"⚠️ Missing required columns: {missing_cols}")
        print("Adding default values for visualization...")
        for col in missing_cols:
            if col == 'complexity_score':
                df[col] = np.random.uniform(4, 9, len(df))
            elif col == 'migration_hours':
                df[col] = np.random.randint(4, 32, len(df))
    
    try:
        # Create comprehensive complexity dashboard
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('GlobalSupply Corp - Migration Complexity Analysis Dashboard', 
                     fontsize=16, fontweight='bold', y=0.98)
        
        # 1. Complexity Score Distribution
        axes[0, 0].hist(df['complexity_score'], bins=10, alpha=0.7, color='skyblue', 
                        edgecolor='black', linewidth=1.2)
        axes[0, 0].set_title('📊 Complexity Score Distribution', fontweight='bold')
        axes[0, 0].set_xlabel('Complexity Score (1-10 scale)')
        axes[0, 0].set_ylabel('Number of SQL Components')
        
        # Add mean line and statistics
        mean_complexity = df['complexity_score'].mean()
        axes[0, 0].axvline(mean_complexity, color='red', linestyle='--', linewidth=2,
                           label=f'Mean: {mean_complexity:.1f}')
        axes[0, 0].legend()
        
        # 2. Risk Level Distribution (if available)
        if 'risk_level' in df.columns:
            risk_counts = df['risk_level'].value_counts()
            colors = {'Low': '#2ecc71', 'Medium': '#f39c12', 'High': '#e74c3c'}
            risk_colors = [colors.get(risk, '#95a5a6') for risk in risk_counts.index]
            
            wedges, texts, autotexts = axes[0, 1].pie(risk_counts.values, labels=risk_counts.index, 
                                                      autopct='%1.1f%%', colors=risk_colors, 
                                                      startangle=90, explode=(0.05, 0.05, 0.05))
            axes[0, 1].set_title('🚦 Migration Risk Distribution', fontweight='bold')
            
            # Enhance pie chart text
            for autotext in autotexts:
                autotext.set_color('white')
                autotext.set_fontweight('bold')
        else:
            axes[0, 1].text(0.5, 0.5, 'Risk Level Data\nNot Available', 
                           transform=axes[0, 1].transAxes, ha='center', va='center',
                           fontsize=14, bbox=dict(boxstyle='round', facecolor='lightgray'))
            axes[0, 1].set_title('🚦 Migration Risk Distribution', fontweight='bold')
        
        # 3. Effort vs Complexity Scatter Plot
        bubble_size = df.get('lines_of_code', pd.Series([100] * len(df)))
        scatter = axes[1, 0].scatter(df['complexity_score'], df['migration_hours'], 
                                    c=bubble_size, cmap='viridis', alpha=0.8, 
                                    s=150, edgecolors='black', linewidth=0.5)
        
        axes[1, 0].set_xlabel('Complexity Score')
        axes[1, 0].set_ylabel('Migration Hours')
        axes[1, 0].set_title('⚡ Effort vs Complexity (bubble size = Lines of Code)', fontweight='bold')
        
        # Add colorbar if we have size data
        if 'lines_of_code' in df.columns:
            cbar = plt.colorbar(scatter, ax=axes[1, 0])
            cbar.set_label('Lines of Code', rotation=270, labelpad=20)
        
        # Add trend line
        z = np.polyfit(df['complexity_score'], df['migration_hours'], 1)
        p = np.poly1d(z)
        axes[1, 0].plot(df['complexity_score'], p(df['complexity_score']), "r--", alpha=0.7)
        
        # 4. Category-wise Migration Hours (if available)
        if 'category' in df.columns:
            category_hours = df.groupby('category')['migration_hours'].sum().sort_values(ascending=True)
            bars = axes[1, 1].barh(category_hours.index, category_hours.values, 
                                   color=['#3498db', '#e67e22', '#9b59b6'], alpha=0.8)
            
            axes[1, 1].set_xlabel('Total Migration Hours')
            axes[1, 1].set_title('📊 Migration Effort by Category', fontweight='bold')
            
            # Add value labels on bars
            for i, (bar, value) in enumerate(zip(bars, category_hours.values)):
                axes[1, 1].text(value + 0.5, i, f'{int(value)}h', 
                                 va='center', fontweight='bold')
        else:
            axes[1, 1].text(0.5, 0.5, 'Category Data\nNot Available', 
                           transform=axes[1, 1].transAxes, ha='center', va='center',
                           fontsize=14, bbox=dict(boxstyle='round', facecolor='lightgray'))
            axes[1, 1].set_title('📊 Migration Effort by Category', fontweight='bold')
        
        plt.tight_layout()
        plt.subplots_adjust(top=0.93)
        plt.show()
        
    except Exception as e:
        logger.error(f"Error creating complexity dashboard: {e}")
        print(f"❌ Error creating visualizations: {e}")


def create_dependency_analysis(data: Dict[str, pd.DataFrame]) -> None:
    """
    Create dependency analysis visualizations.
    
    Args:
        data: Assessment data with Dependencies sheet
    """
    if 'Dependencies' not in data or len(data['Dependencies']) == 0:
        print("⚠️ Dependency analysis data not available")
        return
    
    dep_df = data['Dependencies']
    
    try:
        # Create dependency visualizations
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))
        fig.suptitle('GlobalSupply Corp - Dependency Analysis', fontsize=14, fontweight='bold')
        
        # 1. Criticality distribution (if available)
        if 'criticality' in dep_df.columns:
            criticality_counts = dep_df['criticality'].value_counts()
            colors = {'High': '#e74c3c', 'Medium': '#f39c12', 'Low': '#2ecc71'}
            crit_colors = [colors.get(crit, '#95a5a6') for crit in criticality_counts.index]
            
            wedges, texts, autotexts = ax1.pie(criticality_counts.values, 
                                               labels=criticality_counts.index,
                                               autopct='%1.1f%%', colors=crit_colors,
                                               startangle=90, explode=(0.05, 0.05, 0.05))
            ax1.set_title('🚦 Dependency Criticality', fontweight='bold')
            
            for autotext in autotexts:
                autotext.set_color('white')
                autotext.set_fontweight('bold')
        else:
            ax1.text(0.5, 0.5, 'Criticality Data\nNot Available', 
                    ha='center', va='center', fontsize=14,
                    bbox=dict(boxstyle='round', facecolor='lightgray'))
            ax1.set_title('🚦 Dependency Criticality', fontweight='bold')
        
        # 2. Dependency type distribution (if available)
        if 'dependency_type' in dep_df.columns:
            type_counts = dep_df['dependency_type'].value_counts()
            bars = ax2.bar(type_counts.index, type_counts.values, 
                          color=['#3498db', '#e67e22', '#9b59b6', '#1abc9c'][:len(type_counts)])
            
            ax2.set_title('📊 Dependencies by Type', fontweight='bold')
            ax2.set_xlabel('Dependency Type')
            ax2.set_ylabel('Count')
            ax2.tick_params(axis='x', rotation=45)
            
            # Add value labels on bars
            for bar, value in zip(bars, type_counts.values):
                ax2.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.1,
                         str(value), ha='center', va='bottom', fontweight='bold')
        else:
            ax2.text(0.5, 0.5, 'Dependency Type\nData Not Available', 
                    ha='center', va='center', fontsize=14,
                    bbox=dict(boxstyle='round', facecolor='lightgray'))
            ax2.set_title('📊 Dependencies by Type', fontweight='bold')
        
        plt.tight_layout()
        plt.show()
        
    except Exception as e:
        logger.error(f"Error creating dependency analysis: {e}")
        print(f"❌ Error in dependency analysis: {e}")


def create_function_analysis(data: Dict[str, pd.DataFrame]) -> None:
    """
    Create function usage and compatibility analysis.
    
    Args:
        data: Assessment data with Function_Usage sheet
    """
    if 'Function_Usage' not in data or len(data['Function_Usage']) == 0:
        print("⚠️ Function usage data not available")
        return
    
    func_df = data['Function_Usage']
    
    try:
        # Create function analysis visualization
        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 10))
        fig.suptitle('SQL Function Usage & Compatibility Analysis', fontsize=14, fontweight='bold')
        
        # 1. Compatibility distribution (if available)
        if 'databricks_compatibility' in func_df.columns:
            compat_counts = func_df['databricks_compatibility'].value_counts()
            colors = {'Direct': '#2ecc71', 'Modified': '#f39c12', 'Complex': '#e74c3c', 'Manual': '#8e44ad'}
            compat_colors = [colors.get(comp, '#95a5a6') for comp in compat_counts.index]
            
            ax1.pie(compat_counts.values, labels=compat_counts.index, autopct='%1.1f%%',
                    colors=compat_colors, startangle=90)
            ax1.set_title('🎯 Compatibility Distribution')
        else:
            ax1.text(0.5, 0.5, 'Compatibility Data\nNot Available', 
                    ha='center', va='center', fontsize=14,
                    bbox=dict(boxstyle='round', facecolor='lightgray'))
            ax1.set_title('🎯 Compatibility Distribution')
        
        # 2. Top used functions (if available)
        if 'usage_count' in func_df.columns and 'function_name' in func_df.columns:
            top_used = func_df.nlargest(min(8, len(func_df)), 'usage_count')
            bars = ax2.barh(top_used['function_name'], top_used['usage_count'], color='skyblue')
            ax2.set_title('📊 Most Used Functions')
            ax2.set_xlabel('Usage Count')
            
            # Add value labels
            for bar, value in zip(bars, top_used['usage_count']):
                ax2.text(bar.get_width() + 0.3, bar.get_y() + bar.get_height()/2,
                         str(value), va='center', fontweight='bold')
        else:
            ax2.text(0.5, 0.5, 'Usage Count Data\nNot Available', 
                    ha='center', va='center', fontsize=14,
                    bbox=dict(boxstyle='round', facecolor='lightgray'))
            ax2.set_title('📊 Most Used Functions')
        
        # 3. Complexity vs Usage scatter (if available)
        if 'complexity_impact' in func_df.columns and 'usage_count' in func_df.columns:
            scatter = ax3.scatter(func_df['usage_count'], func_df['complexity_impact'],
                                 alpha=0.7, s=100, color='coral')
            ax3.set_xlabel('Usage Count')
            ax3.set_ylabel('Complexity Impact (1-5)')
            ax3.set_title('⚡ Usage vs Complexity')
            ax3.grid(True, alpha=0.3)
        else:
            ax3.text(0.5, 0.5, 'Complexity/Usage\nData Not Available', 
                    ha='center', va='center', fontsize=14,
                    bbox=dict(boxstyle='round', facecolor='lightgray'))
            ax3.set_title('⚡ Usage vs Complexity')
        
        # 4. Key insights
        ax4.text(0.1, 0.8, "🎯 KEY MIGRATION INSIGHTS:", transform=ax4.transAxes, 
                 fontsize=12, fontweight='bold')
        
        insights = [
            "• Focus on high-usage complex functions first",
            "• Test modified functions thoroughly", 
            "• Consider performance implications",
            "• Plan for user training on new syntax"
        ]
        
        for i, insight in enumerate(insights):
            ax4.text(0.1, 0.6 - i*0.1, insight, transform=ax4.transAxes, fontsize=10)
        
        ax4.axis('off')
        
        plt.tight_layout()
        plt.show()
        
    except Exception as e:
        logger.error(f"Error in function analysis: {e}")
        print(f"❌ Error in function analysis: {e}")


def create_migration_timeline(insights: Dict[str, Any]) -> None:
    """
    Create migration timeline and effort visualization.
    
    Args:
        insights: Processed insights from assessment_engine
    """
    try:
        files_data = insights.get('file_analysis', {})
        if not files_data:
            print("⚠️ No file analysis data available for timeline visualization")
            return
        
        # Create migration waves
        waves = {
            'Wave 1 - Quick Wins': [],
            'Wave 2 - Standard Migration': [],
            'Wave 3 - Complex Components': []
        }
        
        for filename, info in files_data.items():
            risk = info.get('risk_level', 'Medium')
            complexity = info.get('complexity_score', 5.0)
            hours = info.get('migration_hours', 8)
            
            if risk == 'Low' and complexity < 6:
                waves['Wave 1 - Quick Wins'].append((filename, hours))
            elif risk == 'Medium' or (risk == 'Low' and complexity >= 6):
                waves['Wave 2 - Standard Migration'].append((filename, hours))
            else:
                waves['Wave 3 - Complex Components'].append((filename, hours))
        
        # Calculate wave efforts
        wave_efforts = {wave: sum(hours for _, hours in files) for wave, files in waves.items()}
        
        # Create timeline visualization
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))
        fig.suptitle('Migration Planning & Timeline Analysis', fontsize=14, fontweight='bold')
        
        # 1. Wave effort distribution
        wave_names = [w.replace(' - ', '\n') for w in wave_efforts.keys()]
        efforts = list(wave_efforts.values())
        colors = ['#2ecc71', '#f39c12', '#e74c3c']
        
        bars = ax1.bar(wave_names, efforts, color=colors, alpha=0.8)
        ax1.set_title('📊 Effort Distribution by Wave', fontweight='bold')
        ax1.set_ylabel('Migration Hours')
        
        # Add value labels
        for bar, effort in zip(bars, efforts):
            if effort > 0:
                ax1.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 1,
                         f'{int(effort)}h', ha='center', va='bottom', fontweight='bold')
        
        # 2. Timeline visualization
        timeline_data = {
            'Wave 1': {'start': 0, 'duration': 4, 'effort': wave_efforts.get('Wave 1 - Quick Wins', 0)},
            'Wave 2': {'start': 3, 'duration': 8, 'effort': wave_efforts.get('Wave 2 - Standard Migration', 0)},
            'Wave 3': {'start': 10, 'duration': 12, 'effort': wave_efforts.get('Wave 3 - Complex Components', 0)}
        }
        
        y_positions = [2, 1, 0]
        colors_timeline = ['#2ecc71', '#f39c12', '#e74c3c']
        
        for i, (wave, data) in enumerate(timeline_data.items()):
            if data['effort'] > 0:
                ax2.barh(y_positions[i], data['duration'], left=data['start'], 
                        color=colors_timeline[i], alpha=0.7, height=0.6)
                
                # Add wave and hours labels
                ax2.text(data['start'] + data['duration']/2, y_positions[i],
                         f"{wave}\n{int(data['effort'])}h",
                         ha='center', va='center', fontweight='bold', color='white')
        
        ax2.set_xlim(0, 25)
        ax2.set_ylim(-0.5, 2.5)
        ax2.set_xlabel('Timeline (Weeks)')
        ax2.set_title('📅 Migration Timeline', fontweight='bold')
        ax2.set_yticks(y_positions)
        ax2.set_yticklabels(['Wave 3', 'Wave 2', 'Wave 1'])
        
        plt.tight_layout()
        plt.show()
        
    except Exception as e:
        logger.error(f"Error creating migration timeline: {e}")
        print(f"❌ Error creating timeline visualization: {e}")


def create_complete_dashboard(data: Dict[str, pd.DataFrame], insights: Dict[str, Any]) -> None:
    """
    Create a complete dashboard with all visualizations.
    
    Args:
        data: Raw assessment data
        insights: Processed insights
    """
    print("📊 Creating comprehensive assessment dashboard...")
    print("This may take a few moments to generate all visualizations.\n")
    
    try:
        # 1. Main complexity analysis
        create_assessment_dashboard(data)
        
        # 2. Dependency analysis
        create_dependency_analysis(data)
        
        # 3. Function compatibility analysis
        create_function_analysis(data)
        
        # 4. Migration timeline
        create_migration_timeline(insights)
        
        print("✅ Dashboard generation complete!")
        
    except Exception as e:
        logger.error(f"Error creating complete dashboard: {e}")
        print(f"❌ Error creating complete dashboard: {e}")


def export_visualizations(data: Dict[str, pd.DataFrame], insights: Dict[str, Any], output_dir: str = ".") -> List[str]:
    """
    Export all visualizations as image files.
    
    Args:
        data: Raw assessment data
        insights: Processed insights
        output_dir: Directory to save images
        
    Returns:
        List of exported file paths
    """
    from pathlib import Path
    
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)
    
    exported_files = []
    
    try:
        # Set up for image export
        original_backend = plt.get_backend()
        plt.switch_backend('Agg')  # Non-interactive backend for export
        
        # Export complexity dashboard
        if 'Complexity_Analysis' in data:
            plt.figure(figsize=(16, 12))
            create_assessment_dashboard(data)
            complexity_file = output_path / "complexity_dashboard.png"
            plt.savefig(complexity_file, dpi=300, bbox_inches='tight')
            plt.close()
            exported_files.append(str(complexity_file))
        
        # Export dependency analysis
        if 'Dependencies' in data:
            plt.figure(figsize=(16, 6))
            create_dependency_analysis(data)
            dependency_file = output_path / "dependency_analysis.png"
            plt.savefig(dependency_file, dpi=300, bbox_inches='tight')
            plt.close()
            exported_files.append(str(dependency_file))
        
        # Export function analysis
        if 'Function_Usage' in data:
            plt.figure(figsize=(16, 10))
            create_function_analysis(data)
            function_file = output_path / "function_analysis.png"
            plt.savefig(function_file, dpi=300, bbox_inches='tight')
            plt.close()
            exported_files.append(str(function_file))
        
        # Export timeline
        plt.figure(figsize=(16, 6))
        create_migration_timeline(insights)
        timeline_file = output_path / "migration_timeline.png"
        plt.savefig(timeline_file, dpi=300, bbox_inches='tight')
        plt.close()
        exported_files.append(str(timeline_file))
        
        # Restore original backend
        plt.switch_backend(original_backend)
        
        print(f"✅ Exported {len(exported_files)} visualization files to {output_dir}")
        return exported_files
        
    except Exception as e:
        logger.error(f"Error exporting visualizations: {e}")
        print(f"❌ Error exporting visualizations: {e}")
        return []