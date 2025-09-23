# Lakebridge Workshop Compatibility Guide

## 📋 Overview

This document outlines Lakebridge version compatibility for the workshop environment, ensuring reliable functionality across different dependency scenarios.

## 🎯 Design Philosophy

The workshop is designed with **resilience-first** architecture:
- **Primary Mode**: Full Lakebridge integration when available
- **Fallback Mode**: Educational simulation when Lakebridge unavailable
- **Hybrid Mode**: Partial functionality with version mismatches

---

## 🔧 Supported Lakebridge Versions

### ✅ Fully Compatible

| Version | Status | Notes |
|---------|--------|-------|
| `0.3.0` | **Recommended** | Optimal workshop experience |
| `0.2.1` | Supported | All features tested |
| `0.2.0` | Supported | Minor limitations in advanced features |

### ⚠️ Partially Compatible

| Version | Status | Limitations |
|---------|--------|-------------|
| `0.1.9` | Limited | Missing reconciliation features |
| `0.1.8` | Limited | Transpilation quality issues |
| `0.4.0-beta` | Experimental | Untested new features |

### ❌ Incompatible

| Version | Status | Reason |
|---------|--------|--------|
| `< 0.1.8` | Unsupported | API incompatibilities |
| `>= 2.0.0` | Future | Breaking changes expected |

---

## 🛠️ Installation & Setup

### Option 1: Full Lakebridge Installation (Recommended)

```bash
# Install Databricks CLI
pip install databricks-cli

# Configure authentication  
databricks configure --token

# Install Lakebridge
databricks labs install lakebridge

# Verify installation
databricks labs lakebridge --version
```

### Option 2: Workshop-Only Mode (Fallback)

```bash
# Install minimal dependencies
pip install pandas sqlalchemy packaging PyYAML

# Clone workshop repository
git clone https://github.com/databricks-labs/lakebridge-workshop
cd lakebridge-workshop

# Test fallback mode
python workshop/core/lakebridge_adapter.py --status
```

---

## 🔍 Compatibility Detection

The workshop automatically detects your environment:

### Automatic Detection Process

1. **Databricks CLI Check**: Verifies `databricks` command availability
2. **Lakebridge Installation**: Checks `databricks labs installed` output
3. **Version Extraction**: Parses version from installation
4. **Compatibility Assessment**: Validates against supported ranges
5. **Mode Selection**: Chooses optimal operation mode

### Manual Override

```python
from workshop.core.lakebridge_adapter import LakebridgeAdapter

# Force fallback mode for testing
adapter = LakebridgeAdapter(fallback_mode=True)

# Simulate unavailable environment
adapter = LakebridgeAdapter(simulate_unavailable=True)

# Check status
status = adapter.get_status()
print(f"Mode: {'Fallback' if status['fallback_mode'] else 'Native'}")
```

---

## 📊 Feature Matrix

| Feature | Lakebridge Native | Fallback Mode | Notes |
|---------|------------------|---------------|-------|
| **Assessment** | ✅ Full Analysis | 🔄 Simulated | Realistic complexity scoring |
| **Transpilation** | ✅ Production Quality | 🔄 Basic Patterns | Educational transformations |
| **Reconciliation** | ✅ Live Validation | 🔄 Mock Data | SQLite-based simulation |
| **Reporting** | ✅ Complete | ✅ Complete | Identical output formats |
| **Learning Value** | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐ | High educational value both modes |

### Legend
- ✅ Full functionality
- 🔄 Simulated with educational value
- ❌ Not available

---

## 🎓 Workshop Module Compatibility

### Module 1: Assessment & Planning
- **Lakebridge Native**: Real complexity analysis with production insights
- **Fallback Mode**: Simulated analysis using file characteristics
- **Learning Impact**: Minimal - concepts fully demonstrated in both modes

### Module 2: SQL Transpilation  
- **Lakebridge Native**: Production-quality SQL conversion
- **Fallback Mode**: Pattern-based transformations for common scenarios
- **Learning Impact**: Low - core transpilation concepts covered

### Module 3: Data Reconciliation
- **Lakebridge Native**: Live data validation workflows
- **Fallback Mode**: SQLite-based reconciliation simulation
- **Learning Impact**: None - full workflow experience in both modes

---

## 🔧 Troubleshooting

### Common Issues & Solutions

#### Issue: "databricks command not found"
```bash
# Solution: Install Databricks CLI
pip install databricks-cli

# Verify installation
databricks --version
```

#### Issue: "Lakebridge not installed"
```bash
# Solution: Install via labs
databricks labs install lakebridge

# Alternative: Use fallback mode
export WORKSHOP_FALLBACK_MODE=true
```

#### Issue: Version compatibility warnings
```bash
# Check current version
databricks labs show lakebridge

# Update if needed
databricks labs upgrade lakebridge

# Or use specific version
databricks labs install lakebridge==0.3.0
```

#### Issue: Authentication failures
```bash
# Reconfigure Databricks auth
databricks configure --token

# Test connection
databricks workspace list /
```

### Advanced Troubleshooting

#### Debug Mode
```python
import logging
logging.basicConfig(level=logging.DEBUG)

from workshop.core.lakebridge_adapter import LakebridgeAdapter
adapter = LakebridgeAdapter()
```

#### Manual Version Override
```python
# Override version detection for testing
adapter = LakebridgeAdapter()
adapter.lakebridge_version = "0.3.0"
adapter.compatibility_status = "compatible"
adapter.lakebridge_available = True
```

---

## 🚀 Optimization Recommendations

### For Workshop Instructors

1. **Pre-Workshop Setup**:
   - Test Lakebridge installation on workshop environment
   - Verify all participants can access Databricks workspace
   - Prepare fallback mode announcement if issues detected

2. **During Workshop**:
   - Use adapter status check as ice-breaker activity
   - Demonstrate both modes if mixed environments
   - Emphasize that learning objectives remain consistent

3. **Post-Workshop**:
   - Share COMPATIBILITY.md with participants
   - Provide guidance for production Lakebridge setup
   - Offer office hours for installation support

### For Participants

1. **Before Workshop**:
   - Install Databricks CLI and configure authentication
   - Test Lakebridge installation if possible
   - Don't worry if installation fails - fallback mode works great!

2. **During Workshop**:
   - Run compatibility check early in Module 1
   - Ask questions about differences between modes
   - Focus on concepts rather than specific tool details

3. **After Workshop**:
   - Set up full Lakebridge environment for production use
   - Practice workflows learned in workshop
   - Contribute feedback on compatibility issues

---

## 📅 Version History & Roadmap

### Tested Combinations

| Date | Lakebridge Version | Workshop Version | Status | Notes |
|------|-------------------|------------------|--------|-------|
| 2024-12-01 | 0.3.0 | 1.0.0 | ✅ Fully Compatible | Initial release |
| 2024-11-15 | 0.2.1 | 0.9.0 | ✅ Compatible | Beta testing |
| 2024-11-01 | 0.2.0 | 0.9.0 | ⚠️ Minor Issues | Reconciliation limitations |

### Future Compatibility Plans

- **Q1 2025**: Support for Lakebridge 0.4.x series
- **Q2 2025**: Enhanced fallback capabilities
- **Q3 2025**: Integration with Lakebridge 1.0 stable
- **Q4 2025**: Advanced workshop features

---

## 🤝 Contributing

Found a compatibility issue? Help improve the workshop:

1. **Report Issues**: Use GitHub Issues with `compatibility` label
2. **Test New Versions**: Try workshop with beta Lakebridge releases  
3. **Update Documentation**: Submit PRs for version compatibility updates
4. **Share Experience**: Provide feedback on fallback mode effectiveness

### Issue Template

```markdown
**Lakebridge Version**: 0.x.x
**Workshop Module**: 1/2/3
**Environment**: OS, Python version
**Issue**: Brief description
**Expected**: What should happen
**Actual**: What actually happened
**Workaround**: If any found
```

---

## 📚 Additional Resources

- **Lakebridge Documentation**: [Official Docs](https://databricks-labs.github.io/lakebridge/)
- **Databricks CLI Setup**: [Installation Guide](https://docs.databricks.com/dev-tools/cli/index.html)
- **Workshop GitHub**: [Issues & Discussions](https://github.com/databricks-labs/lakebridge-workshop)
- **Community Support**: [Databricks Community](https://community.databricks.com/)

---

*This compatibility guide is automatically updated by our dependency monitoring system. Last updated: Generated via GitHub Actions.*