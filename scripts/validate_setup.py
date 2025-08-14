#!/usr/bin/env python
"""
Setup validation script for Dagster project
"""

import os
import sys
import importlib.util

def check_python_version():
    """Check if Python version is compatible"""
    version = sys.version_info
    if version.major < 3 or (version.major == 3 and version.minor < 8):
        print("[ERROR] Python 3.8+ required. Current version:", sys.version)
        return False
    print("[OK] Python version:", sys.version)
    return True

def check_virtual_environment():
    """Check if running in virtual environment"""
    in_venv = (
        hasattr(sys, 'real_prefix') or 
        (hasattr(sys, 'base_prefix') and sys.base_prefix != sys.prefix)
    )
    if in_venv:
        print("[OK] Running in virtual environment")
    else:
        print("[WARNING] Not running in virtual environment (recommended)")
    return True

def check_required_files():
    """Check if required files exist"""
    required_files = [
        "requirements.txt",
        "my_dagster_project",
        "config",
        "scripts"
    ]
    
    missing_files = []
    for file in required_files:
        if not os.path.exists(file):
            missing_files.append(file)
    
    if missing_files:
        print("[ERROR] Missing required files/directories:", missing_files)
        return False
    
    print("[OK] All required files/directories present")
    return True

def check_dependencies():
    """Check if required dependencies are installed"""
    required_packages = [
        "dagster",
        "dagster_pandas", 
        "dagster_webserver",
        "pandas",
        "yaml"
    ]
    
    missing_packages = []
    for package in required_packages:
        try:
            importlib.util.find_spec(package)
        except ImportError:
            missing_packages.append(package)
    
    if missing_packages:
        print("[ERROR] Missing required packages:", missing_packages)
        print("   Run: pip install -r requirements.txt")
        return False
    
    print("[OK] All required dependencies installed")
    return True

def check_dagster_components():
    """Check if Dagster components are properly configured"""
    try:
        from my_dagster_project.definitions import defs
        print("[OK] Dagster definitions loaded successfully")
        
        # Check if we can access assets
        if hasattr(defs, 'assets') and defs.assets:
            print(f"[OK] {len(defs.assets)} assets found in definitions")
        else:
            print("[WARNING] No assets found in definitions")
            
        # Check if we can access jobs
        if hasattr(defs, 'jobs') and defs.jobs:
            print(f"[OK] {len(defs.jobs)} jobs found in definitions")
        else:
            print("[WARNING] No jobs found in definitions")
            
        return True
    except Exception as e:
        print("[ERROR] Error loading Dagster definitions:", str(e))
        return False

def check_database_access():
    """Check if database files are accessible"""
    try:
        # Test importing core components
        from my_dagster_project.core.metadata_store import metadata_store
        from my_dagster_project.core.asset_record import asset_record
        print("[OK] Core database components loaded successfully")
        return True
    except Exception as e:
        print("[ERROR] Error loading database components:", str(e))
        return False

def check_cli_tools():
    """Check if CLI tools are accessible"""
    try:
        # Test importing CLI
        from my_dagster_project.scripts.asset_manager import cli
        print("[OK] CLI tools loaded successfully")
        return True
    except Exception as e:
        print("[ERROR] Error loading CLI tools:", str(e))
        return False

def main():
    """Main validation function"""
    print("=========================================")
    print("  Dagster Project Setup Validation      ")
    print("=========================================")
    print()
    
    checks = [
        ("Python Version", check_python_version),
        ("Virtual Environment", check_virtual_environment),
        ("Required Files", check_required_files),
        ("Dependencies", check_dependencies),
        ("Dagster Components", check_dagster_components),
        ("Database Access", check_database_access),
        ("CLI Tools", check_cli_tools)
    ]
    
    passed = 0
    total = len(checks)
    
    for name, check_func in checks:
        print(f"Checking {name}...")
        if check_func():
            passed += 1
        print()
    
    print("=========================================")
    print(f"Validation Summary: {passed}/{total} checks passed")
    
    if passed == total:
        print("[SUCCESS] All checks passed! Your setup is ready.")
        print()
        print("Next steps:")
        print("1. Run: dagster dev")
        print("2. Visit: http://localhost:3000")
        print("3. Start creating assets!")
    elif passed >= total * 0.8:
        print("[INFO] Most checks passed. Minor issues detected.")
        print("You can likely proceed, but review warnings above.")
    else:
        print("[ERROR] Several issues detected. Please address them before proceeding.")
    
    print("=========================================")

if __name__ == "__main__":
    main()