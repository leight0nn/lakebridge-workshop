"""
Lakebridge Workshop Adapter - Resilient Dependency Management

This adapter provides a resilient interface to Lakebridge functionality, ensuring
the workshop remains functional regardless of Lakebridge availability or version changes.

Key Features:
- Dynamic version detection and compatibility checking
- Graceful fallback to manual implementations
- Wrapper functions for core Lakebridge commands
- Comprehensive error handling and user guidance
- Support for both live and simulated workshop modes

Business Context:
Enables GlobalSupply Corp workshop participants to learn migration concepts
even when Lakebridge dependencies are unavailable or incompatible.
"""

import subprocess
import sys
import logging
import json
import os
from pathlib import Path
from typing import Dict, List, Optional, Union, Tuple, Any
from datetime import datetime
import importlib.util
import pkg_resources
from packaging import version

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class LakebridgeCompatibilityError(Exception):
    """Raised when Lakebridge compatibility issues are detected."""
    pass

class LakebridgeAdapter:
    """
    Resilient adapter for Lakebridge functionality with intelligent fallbacks.
    
    Provides consistent interface regardless of Lakebridge availability,
    enabling workshop continuity under all dependency scenarios.
    """
    
    # Supported Lakebridge version ranges
    SUPPORTED_VERSIONS = {
        'minimum': '0.1.0',
        'maximum': '2.0.0',
        'recommended': '0.3.0',
        'tested': ['0.2.0', '0.2.1', '0.3.0']
    }
    
    def __init__(self, fallback_mode: bool = False, simulate_unavailable: bool = False):
        """
        Initialize the Lakebridge adapter.
        
        Args:
            fallback_mode: Force fallback mode for testing
            simulate_unavailable: Simulate Lakebridge unavailability for testing
        """
        self.fallback_mode = fallback_mode
        self.simulate_unavailable = simulate_unavailable
        self.lakebridge_available = False
        self.lakebridge_version = None
        self.compatibility_status = "unknown"
        self.fallback_reason = None
        
        # Initialize adapter state
        self._check_lakebridge_availability()
        
        logger.info(f"LakebridgeAdapter initialized - Available: {self.lakebridge_available}, "
                   f"Version: {self.lakebridge_version}, Fallback: {self.fallback_mode}")
    
    def _check_lakebridge_availability(self) -> None:
        """Check if Lakebridge is available and compatible."""
        if self.simulate_unavailable:
            self.lakebridge_available = False
            self.fallback_reason = "Simulated unavailability for testing"
            return
        
        try:
            # Check if databricks CLI is available
            result = subprocess.run(['databricks', '--version'], 
                                  capture_output=True, text=True, timeout=10)
            if result.returncode != 0:
                raise subprocess.CalledProcessError(result.returncode, 'databricks --version')
            
            # Check if Lakebridge is installed
            try:
                result = subprocess.run(['databricks', 'labs', 'installed'], 
                                      capture_output=True, text=True, timeout=30)
                
                if result.returncode == 0 and 'lakebridge' in result.stdout.lower():
                    # Extract version information
                    self.lakebridge_version = self._extract_lakebridge_version(result.stdout)
                    self.compatibility_status = self._check_version_compatibility()
                    
                    if self.compatibility_status == "compatible" and not self.fallback_mode:
                        self.lakebridge_available = True
                        logger.info(f"Lakebridge v{self.lakebridge_version} detected and compatible")
                    else:
                        self.lakebridge_available = False
                        self.fallback_reason = f"Version compatibility: {self.compatibility_status}"
                else:
                    self.lakebridge_available = False
                    self.fallback_reason = "Lakebridge not installed via databricks labs"
                    
            except subprocess.TimeoutExpired:
                self.lakebridge_available = False
                self.fallback_reason = "Databricks labs command timeout"
                
        except (subprocess.CalledProcessError, FileNotFoundError, subprocess.TimeoutExpired) as e:
            self.lakebridge_available = False
            self.fallback_reason = f"Databricks CLI unavailable: {str(e)}"
            logger.warning(f"Databricks CLI check failed: {e}")
    
    def _extract_lakebridge_version(self, labs_output: str) -> Optional[str]:
        """Extract Lakebridge version from databricks labs output."""
        try:
            lines = labs_output.strip().split('\n')
            for line in lines:
                if 'lakebridge' in line.lower():
                    # Parse version from output format
                    parts = line.split()
                    for part in parts:
                        if part.replace('.', '').replace('-', '').replace('_', '').isdigit() or \
                           any(char.isdigit() for char in part):
                            # Basic version pattern detection
                            if '.' in part and len(part.split('.')) >= 2:
                                return part.strip()
            
            # Fallback: try to get version via direct command
            result = subprocess.run(['databricks', 'labs', 'show', 'lakebridge'], 
                                  capture_output=True, text=True, timeout=15)
            if result.returncode == 0:
                # Extract version from show output
                for line in result.stdout.split('\n'):
                    if 'version' in line.lower():
                        parts = line.split(':')
                        if len(parts) > 1:
                            return parts[1].strip()
                            
        except Exception as e:
            logger.warning(f"Could not extract Lakebridge version: {e}")
            
        return "unknown"
    
    def _check_version_compatibility(self) -> str:
        """Check if detected Lakebridge version is compatible."""
        if not self.lakebridge_version or self.lakebridge_version == "unknown":
            return "unknown_version"
        
        try:
            current_version = version.parse(self.lakebridge_version)
            min_version = version.parse(self.SUPPORTED_VERSIONS['minimum'])
            max_version = version.parse(self.SUPPORTED_VERSIONS['maximum'])
            
            if current_version < min_version:
                return f"too_old (minimum: {self.SUPPORTED_VERSIONS['minimum']})"
            elif current_version >= max_version:
                return f"too_new (maximum: {self.SUPPORTED_VERSIONS['maximum']})"
            else:
                return "compatible"
                
        except Exception as e:
            logger.warning(f"Version compatibility check failed: {e}")
            return "parse_error"
    
    def get_status(self) -> Dict[str, Any]:
        """Get comprehensive adapter status information."""
        return {
            'lakebridge_available': self.lakebridge_available,
            'lakebridge_version': self.lakebridge_version,
            'compatibility_status': self.compatibility_status,
            'fallback_mode': self.fallback_mode,
            'fallback_reason': self.fallback_reason,
            'supported_versions': self.SUPPORTED_VERSIONS,
            'recommendations': self._get_recommendations()
        }
    
    def _get_recommendations(self) -> List[str]:
        """Get user recommendations based on current status."""
        recommendations = []
        
        if not self.lakebridge_available:
            if "databricks cli" in (self.fallback_reason or "").lower():
                recommendations.append("Install Databricks CLI: pip install databricks-cli")
                recommendations.append("Configure authentication: databricks configure --token")
            elif "not installed" in (self.fallback_reason or "").lower():
                recommendations.append("Install Lakebridge: databricks labs install lakebridge")
            elif "compatibility" in (self.fallback_reason or "").lower():
                recommendations.append(f"Update Lakebridge to supported version ({self.SUPPORTED_VERSIONS['recommended']})")
                recommendations.append("Check COMPATIBILITY.md for version requirements")
        
        if self.fallback_mode:
            recommendations.append("Workshop will use simulated results for learning")
            recommendations.append("All concepts and workflows remain valid")
        
        return recommendations
    
    def analyze_legacy_sql(self, 
                          source_directory: str,
                          output_file: Optional[str] = None,
                          **kwargs) -> Dict[str, Any]:
        """
        Analyze legacy SQL files with Lakebridge or fallback implementation.
        
        Args:
            source_directory: Directory containing SQL files
            output_file: Optional output file path
            **kwargs: Additional arguments for analysis
            
        Returns:
            Analysis results dictionary
        """
        if self.lakebridge_available and not self.fallback_mode:
            return self._lakebridge_analyze(source_directory, output_file, **kwargs)
        else:
            return self._fallback_analyze(source_directory, output_file, **kwargs)
    
    def _lakebridge_analyze(self, 
                           source_directory: str,
                           output_file: Optional[str] = None,
                           **kwargs) -> Dict[str, Any]:
        """Execute actual Lakebridge analysis."""
        try:
            cmd = ['databricks', 'labs', 'lakebridge', 'analyze']
            cmd.extend(['--source-directory', source_directory])
            
            if output_file:
                cmd.extend(['--output-file', output_file])
            
            # Add additional arguments
            for key, value in kwargs.items():
                if value is not None:
                    cmd.extend([f'--{key.replace("_", "-")}', str(value)])
            
            logger.info(f"Executing Lakebridge analysis: {' '.join(cmd)}")
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
            
            if result.returncode == 0:
                logger.info("Lakebridge analysis completed successfully")
                
                # Parse output file if specified
                if output_file and Path(output_file).exists():
                    try:
                        with open(output_file, 'r') as f:
                            if output_file.endswith('.json'):
                                return json.load(f)
                            else:
                                return {'raw_output': f.read(), 'success': True}
                    except Exception as e:
                        logger.warning(f"Could not parse output file: {e}")
                        
                return {
                    'success': True,
                    'stdout': result.stdout,
                    'method': 'lakebridge_native'
                }
            else:
                logger.error(f"Lakebridge analysis failed: {result.stderr}")
                # Fall back to manual implementation
                return self._fallback_analyze(source_directory, output_file, **kwargs)
                
        except Exception as e:
            logger.error(f"Lakebridge analysis error: {e}")
            return self._fallback_analyze(source_directory, output_file, **kwargs)
    
    def _fallback_analyze(self, 
                         source_directory: str,
                         output_file: Optional[str] = None,
                         **kwargs) -> Dict[str, Any]:
        """Fallback SQL analysis implementation."""
        logger.info("Using fallback SQL analysis implementation")
        
        try:
            from .sql_analyzer import FallbackSQLAnalyzer
            analyzer = FallbackSQLAnalyzer()
            return analyzer.analyze_directory(source_directory, output_file, **kwargs)
        except ImportError:
            # Generate simulated results for workshop continuity
            return self._generate_simulated_analysis(source_directory, **kwargs)
    
    def _generate_simulated_analysis(self, source_directory: str, **kwargs) -> Dict[str, Any]:
        """Generate realistic simulated analysis results."""
        source_path = Path(source_directory)
        
        if not source_path.exists():
            logger.warning(f"Source directory not found: {source_directory}")
            return {'success': False, 'error': 'Source directory not found'}
        
        # Count SQL files
        sql_files = list(source_path.glob('**/*.sql'))
        
        # Generate realistic complexity scores based on file characteristics
        file_analyses = {}
        total_complexity = 0
        
        for sql_file in sql_files:
            try:
                content = sql_file.read_text()
                complexity = self._estimate_sql_complexity(content)
                total_complexity += complexity
                
                file_analyses[sql_file.name] = {
                    'complexity_score': complexity,
                    'line_count': len(content.splitlines()),
                    'contains_cte': 'WITH ' in content.upper(),
                    'contains_window_functions': any(func in content.upper() 
                                                   for func in ['ROW_NUMBER(', 'RANK(', 'DENSE_RANK(', 'LAG(', 'LEAD(']),
                    'contains_subqueries': content.count('(SELECT') > 0,
                    'migration_wave': self._assign_migration_wave(complexity),
                    'estimated_effort_hours': max(1, int(complexity * 2)),
                    'risk_factors': self._identify_risk_factors(content)
                }
            except Exception as e:
                logger.warning(f"Could not analyze {sql_file}: {e}")
                file_analyses[sql_file.name] = {'error': str(e)}
        
        avg_complexity = total_complexity / len(sql_files) if sql_files else 0
        
        simulated_results = {
            'success': True,
            'method': 'simulated_fallback',
            'analysis_metadata': {
                'timestamp': datetime.now().isoformat(),
                'source_directory': str(source_directory),
                'files_analyzed': len(sql_files),
                'fallback_reason': self.fallback_reason
            },
            'summary_statistics': {
                'total_files': len(sql_files),
                'average_complexity': round(avg_complexity, 2),
                'total_estimated_effort_hours': sum(
                    fa.get('estimated_effort_hours', 0) for fa in file_analyses.values()
                ),
                'high_complexity_files': sum(
                    1 for fa in file_analyses.values() 
                    if fa.get('complexity_score', 0) > 7.0
                )
            },
            'file_analysis': file_analyses,
            'migration_recommendations': {
                'wave_1_files': [f for f, a in file_analyses.items() 
                               if a.get('migration_wave') == 1],
                'wave_2_files': [f for f, a in file_analyses.items() 
                               if a.get('migration_wave') == 2],
                'wave_3_files': [f for f, a in file_analyses.items() 
                               if a.get('migration_wave') == 3]
            },
            'workshop_note': "Results generated by fallback analyzer for educational purposes"
        }
        
        # Save results if output file specified
        if kwargs.get('output_file'):
            output_file = kwargs.get('output_file')
            try:
                with open(output_file, 'w') as f:
                    json.dump(simulated_results, f, indent=2)
                logger.info(f"Simulated analysis results saved to {output_file}")
            except Exception as e:
                logger.warning(f"Could not save results to {output_file}: {e}")
        
        return simulated_results
    
    def _estimate_sql_complexity(self, sql_content: str) -> float:
        """Estimate SQL complexity based on content analysis."""
        complexity_score = 1.0  # Base complexity
        content_upper = sql_content.upper()
        
        # Complexity factors
        complexity_factors = {
            'JOIN': 0.5,
            'UNION': 0.8,
            'CASE': 0.3,
            'WITH': 1.0,  # CTEs
            'WINDOW': 1.2,
            'PARTITION BY': 1.0,
            'ROW_NUMBER': 0.8,
            'RANK': 0.8,
            'LEAD': 0.9,
            'LAG': 0.9,
            'RECURSIVE': 2.0,
            'PIVOT': 1.5,
            'UNPIVOT': 1.5,
            'MERGE': 1.8,
            'CURSOR': 2.5,
            'WHILE': 1.5,
            'IF': 0.4,
            'TRY': 1.0,
            'CATCH': 1.0,
            'RAISERROR': 0.8,
            'DYNAMIC': 2.0,
            'EXEC': 1.2,
            'OPENROWSET': 1.8,
            'BULK': 1.5
        }
        
        # Count occurrences and add to complexity
        for keyword, weight in complexity_factors.items():
            count = content_upper.count(keyword)
            complexity_score += count * weight
        
        # Additional complexity from structure
        subquery_count = sql_content.count('(SELECT')
        complexity_score += subquery_count * 0.7
        
        line_count = len(sql_content.splitlines())
        if line_count > 100:
            complexity_score += 1.0
        if line_count > 500:
            complexity_score += 2.0
        
        # Cap at reasonable maximum
        return min(complexity_score, 10.0)
    
    def _assign_migration_wave(self, complexity: float) -> int:
        """Assign migration wave based on complexity."""
        if complexity <= 4.0:
            return 1  # Low complexity - Wave 1
        elif complexity <= 7.0:
            return 2  # Medium complexity - Wave 2
        else:
            return 3  # High complexity - Wave 3
    
    def _identify_risk_factors(self, sql_content: str) -> List[str]:
        """Identify potential migration risk factors."""
        risks = []
        content_upper = sql_content.upper()
        
        risk_patterns = {
            'CURSOR': 'Uses cursors (procedural logic)',
            'WHILE': 'Contains WHILE loops',
            'GOTO': 'Uses GOTO statements',
            'RAISERROR': 'Custom error handling',
            'TRY': 'Exception handling blocks',
            'DYNAMIC': 'Dynamic SQL construction',
            'OPENROWSET': 'External data sources',
            'LINKED': 'Linked server dependencies',
            'BULK': 'Bulk operations',
            'MERGE': 'Complex MERGE statements',
            'RECURSIVE': 'Recursive CTEs',
            'PIVOT': 'Pivot operations',
            'UNPIVOT': 'Unpivot operations'
        }
        
        for pattern, description in risk_patterns.items():
            if pattern in content_upper:
                risks.append(description)
        
        # Additional structural risks
        if sql_content.count('(SELECT') > 5:
            risks.append('Heavy subquery usage')
        
        if len(sql_content.splitlines()) > 500:
            risks.append('Very large file size')
        
        return risks
    
    def transpile_sql(self, 
                     source_file: str,
                     source_dialect: str = 'tsql',
                     target_dialect: str = 'databricks',
                     output_file: Optional[str] = None,
                     **kwargs) -> Dict[str, Any]:
        """
        Transpile SQL from source to target dialect.
        
        Args:
            source_file: Source SQL file path
            source_dialect: Source SQL dialect (tsql, oracle, snowflake)
            target_dialect: Target SQL dialect (databricks)
            output_file: Optional output file path
            **kwargs: Additional transpilation arguments
            
        Returns:
            Transpilation results dictionary
        """
        if self.lakebridge_available and not self.fallback_mode:
            return self._lakebridge_transpile(source_file, source_dialect, target_dialect, output_file, **kwargs)
        else:
            return self._fallback_transpile(source_file, source_dialect, target_dialect, output_file, **kwargs)
    
    def _lakebridge_transpile(self, 
                             source_file: str,
                             source_dialect: str,
                             target_dialect: str,
                             output_file: Optional[str] = None,
                             **kwargs) -> Dict[str, Any]:
        """Execute actual Lakebridge transpilation."""
        try:
            cmd = ['databricks', 'labs', 'lakebridge', 'transpile']
            cmd.extend(['--source-file', source_file])
            cmd.extend(['--source-dialect', source_dialect])
            cmd.extend(['--target-dialect', target_dialect])
            
            if output_file:
                cmd.extend(['--output-file', output_file])
            
            # Add additional arguments
            for key, value in kwargs.items():
                if value is not None:
                    cmd.extend([f'--{key.replace("_", "-")}', str(value)])
            
            logger.info(f"Executing Lakebridge transpilation: {' '.join(cmd)}")
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
            
            if result.returncode == 0:
                logger.info("Lakebridge transpilation completed successfully")
                
                # Read transpiled output
                if output_file and Path(output_file).exists():
                    try:
                        with open(output_file, 'r') as f:
                            transpiled_sql = f.read()
                        return {
                            'success': True,
                            'transpiled_sql': transpiled_sql,
                            'method': 'lakebridge_native',
                            'source_file': source_file,
                            'output_file': output_file
                        }
                    except Exception as e:
                        logger.warning(f"Could not read output file: {e}")
                
                return {
                    'success': True,
                    'transpiled_sql': result.stdout,
                    'method': 'lakebridge_native',
                    'source_file': source_file
                }
            else:
                logger.error(f"Lakebridge transpilation failed: {result.stderr}")
                # Fall back to manual implementation
                return self._fallback_transpile(source_file, source_dialect, target_dialect, output_file, **kwargs)
                
        except Exception as e:
            logger.error(f"Lakebridge transpilation error: {e}")
            return self._fallback_transpile(source_file, source_dialect, target_dialect, output_file, **kwargs)
    
    def _fallback_transpile(self, 
                           source_file: str,
                           source_dialect: str,
                           target_dialect: str,
                           output_file: Optional[str] = None,
                           **kwargs) -> Dict[str, Any]:
        """Fallback SQL transpilation implementation."""
        logger.info("Using fallback SQL transpilation implementation")
        
        try:
            from .sql_transpiler import FallbackSQLTranspiler
            transpiler = FallbackSQLTranspiler()
            return transpiler.transpile_file(source_file, source_dialect, target_dialect, output_file, **kwargs)
        except ImportError:
            # Generate simulated transpilation for workshop continuity
            return self._generate_simulated_transpilation(source_file, source_dialect, target_dialect, output_file)
    
    def _generate_simulated_transpilation(self, 
                                        source_file: str,
                                        source_dialect: str,
                                        target_dialect: str,
                                        output_file: Optional[str] = None) -> Dict[str, Any]:
        """Generate simulated transpilation results."""
        source_path = Path(source_file)
        
        if not source_path.exists():
            return {'success': False, 'error': f'Source file not found: {source_file}'}
        
        try:
            source_sql = source_path.read_text()
            
            # Apply basic transformations for demo purposes
            transpiled_sql = self._apply_basic_transformations(source_sql, source_dialect, target_dialect)
            
            # Save transpiled output if requested
            if output_file:
                output_path = Path(output_file)
                output_path.parent.mkdir(parents=True, exist_ok=True)
                output_path.write_text(transpiled_sql)
                logger.info(f"Simulated transpilation saved to {output_file}")
            
            return {
                'success': True,
                'transpiled_sql': transpiled_sql,
                'method': 'simulated_fallback',
                'source_file': source_file,
                'source_dialect': source_dialect,
                'target_dialect': target_dialect,
                'output_file': output_file,
                'transformations_applied': [
                    'TOP -> LIMIT conversion',
                    'GETDATE() -> CURRENT_TIMESTAMP() conversion',
                    'ISNULL -> COALESCE conversion',
                    'Basic syntax adaptations'
                ],
                'workshop_note': "Transpilation generated by fallback implementation for educational purposes"
            }
            
        except Exception as e:
            logger.error(f"Simulated transpilation error: {e}")
            return {'success': False, 'error': str(e)}
    
    def _apply_basic_transformations(self, sql: str, source_dialect: str, target_dialect: str) -> str:
        """Apply basic SQL transformations for demo purposes."""
        transformed = sql
        
        # Common SQL Server to Databricks transformations
        if source_dialect.lower() == 'tsql' and target_dialect.lower() == 'databricks':
            # TOP clause transformation
            import re
            
            # TOP N -> LIMIT N
            transformed = re.sub(r'\bTOP\s+(\d+)\b', r'', transformed, flags=re.IGNORECASE)
            if 'TOP' in sql.upper():
                transformed += '\nLIMIT 100  -- Converted from TOP clause'
            
            # Function conversions
            transformations = {
                r'\bGETDATE\(\)': 'CURRENT_TIMESTAMP()',
                r'\bISNULL\(': 'COALESCE(',
                r'\bLEN\(': 'LENGTH(',
                r'\bCHARINDEX\(': 'POSITION(',
                r'\bSUBSTRING\(': 'SUBSTR(',
                r'\bDATEDIFF\(': 'DATEDIFF(',
                r'\bCONVERT\(': 'CAST(',
                r'\b\[dbo\]\.': '',  # Remove schema qualifiers
                r'\[([^\]]+)\]': r'\1'  # Remove square brackets
            }
            
            for pattern, replacement in transformations.items():
                transformed = re.sub(pattern, replacement, transformed, flags=re.IGNORECASE)
        
        # Add header comment
        header = f"""-- Transpiled from {source_dialect.upper()} to {target_dialect.upper()}
-- Generated by Lakebridge Workshop Fallback Transpiler
-- Note: This is a simplified transformation for educational purposes
-- Production transpilation requires Lakebridge or equivalent tools

"""
        
        return header + transformed
    
    def test_connection(self) -> Dict[str, Any]:
        """Test connection to Databricks and validate setup."""
        if not self.lakebridge_available:
            return {
                'success': False,
                'connection_status': 'lakebridge_unavailable',
                'fallback_active': True,
                'message': 'Lakebridge not available - using fallback mode'
            }
        
        try:
            # Test basic databricks connection
            result = subprocess.run(['databricks', 'workspace', 'list', '/'], 
                                  capture_output=True, text=True, timeout=30)
            
            if result.returncode == 0:
                return {
                    'success': True,
                    'connection_status': 'connected',
                    'lakebridge_version': self.lakebridge_version,
                    'message': 'Successfully connected to Databricks workspace'
                }
            else:
                return {
                    'success': False,
                    'connection_status': 'authentication_failed',
                    'error': result.stderr,
                    'message': 'Databricks authentication failed'
                }
                
        except Exception as e:
            return {
                'success': False,
                'connection_status': 'connection_error',
                'error': str(e),
                'message': 'Could not test Databricks connection'
            }


# Convenience functions for direct usage
def get_adapter(fallback_mode: bool = False) -> LakebridgeAdapter:
    """Get a configured Lakebridge adapter instance."""
    return LakebridgeAdapter(fallback_mode=fallback_mode)

def analyze_sql_files(source_directory: str, 
                     output_file: Optional[str] = None,
                     adapter: Optional[LakebridgeAdapter] = None,
                     **kwargs) -> Dict[str, Any]:
    """Convenience function for SQL analysis."""
    if adapter is None:
        adapter = get_adapter()
    
    return adapter.analyze_legacy_sql(source_directory, output_file, **kwargs)

def transpile_sql_file(source_file: str,
                      source_dialect: str = 'tsql',
                      target_dialect: str = 'databricks',
                      output_file: Optional[str] = None,
                      adapter: Optional[LakebridgeAdapter] = None,
                      **kwargs) -> Dict[str, Any]:
    """Convenience function for SQL transpilation."""
    if adapter is None:
        adapter = get_adapter()
    
    return adapter.transpile_sql(source_file, source_dialect, target_dialect, output_file, **kwargs)

def check_lakebridge_status() -> Dict[str, Any]:
    """Quick status check for Lakebridge availability."""
    adapter = LakebridgeAdapter()
    return adapter.get_status()

if __name__ == "__main__":
    # CLI interface for testing
    import argparse
    
    parser = argparse.ArgumentParser(description="Lakebridge Workshop Adapter")
    parser.add_argument('--status', action='store_true', help='Check Lakebridge status')
    parser.add_argument('--test-connection', action='store_true', help='Test Databricks connection')
    parser.add_argument('--fallback', action='store_true', help='Force fallback mode')
    
    args = parser.parse_args()
    
    if args.status or not any([args.test_connection]):
        # Default to status check
        status = check_lakebridge_status()
        print(json.dumps(status, indent=2))
    
    if args.test_connection:
        adapter = LakebridgeAdapter(fallback_mode=args.fallback)
        connection_test = adapter.test_connection()
        print(json.dumps(connection_test, indent=2))