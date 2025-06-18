#!/usr/bin/env python3
"""
Check available ODBC drivers for SQL Server connectivity

This script helps diagnose ODBC driver availability and provides
installation instructions if needed.
"""

import sys
import os

def check_pyodbc():
    """Check if pyodbc is available"""
    try:
        import pyodbc
        print("✓ pyodbc is available")
        return True
    except ImportError:
        print("✗ pyodbc is not available")
        print("  Install with: pip install pyodbc")
        return False

def check_odbc_drivers():
    """Check available ODBC drivers"""
    try:
        import pyodbc
        drivers = pyodbc.drivers()
        
        print(f"\nAvailable ODBC drivers ({len(drivers)} total):")
        for i, driver in enumerate(drivers, 1):
            print(f"  {i:2d}. {driver}")
        
        # Check for SQL Server specific drivers
        sql_drivers = [d for d in drivers if 'SQL' in d.upper()]
        print(f"\nSQL Server related drivers ({len(sql_drivers)} found):")
        if sql_drivers:
            for i, driver in enumerate(sql_drivers, 1):
                print(f"  {i:2d}. {driver}")
        else:
            print("  None found")
        
        # Check for preferred drivers
        preferred_drivers = [
            "ODBC Driver 18 for SQL Server",
            "ODBC Driver 17 for SQL Server",
            "ODBC Driver 13 for SQL Server",
            "ODBC Driver 11 for SQL Server",
            "SQL Server Native Client 11.0",
            "SQL Server Native Client 10.0",
            "SQL Server"
        ]
        
        print(f"\nPreferred drivers (in order of preference):")
        found_preferred = False
        for driver in preferred_drivers:
            if driver in drivers:
                print(f"  ✓ {driver}")
                found_preferred = True
            else:
                print(f"  ✗ {driver}")
        
        return found_preferred, sql_drivers
        
    except ImportError:
        print("Cannot check drivers - pyodbc not available")
        return False, []

def check_system_info():
    """Check system information"""
    print("System Information:")
    print(f"  OS: {sys.platform}")
    print(f"  Python: {sys.version}")
    print(f"  Architecture: {os.uname().machine if hasattr(os, 'uname') else 'Unknown'}")

def provide_installation_instructions():
    """Provide installation instructions"""
    print("\n" + "="*60)
    print("INSTALLATION INSTRUCTIONS")
    print("="*60)
    
    if sys.platform == "darwin":  # macOS
        print("\nFor macOS:")
        print("1. Install via Homebrew (recommended):")
        print("   brew tap microsoft/mssql-release https://github.com/Microsoft/homebrew-mssql-release")
        print("   brew update")
        print("   HOMEBREW_ACCEPT_EULA=Y brew install msodbcsql18 mssql-tools18")
        print("\n2. Or run the setup script:")
        print("   ./setup_sqlserver_driver.sh")
        print("\n3. Or download manually from:")
        print("   https://docs.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server")
        
    elif sys.platform.startswith("linux"):  # Linux
        print("\nFor Linux:")
        print("1. Ubuntu/Debian:")
        print("   curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -")
        print("   curl https://packages.microsoft.com/config/ubuntu/20.04/prod.list > /etc/apt/sources.list.d/mssql-release.list")
        print("   apt-get update")
        print("   ACCEPT_EULA=Y apt-get install msodbcsql18")
        print("\n2. CentOS/RHEL:")
        print("   curl https://packages.microsoft.com/config/rhel/8/prod.repo > /etc/yum.repos.d/mssql-release.repo")
        print("   yum remove unixODBC-utf16 unixODBC-utf16-devel")
        print("   ACCEPT_EULA=Y yum install msodbcsql18")
        
    elif sys.platform == "win32":  # Windows
        print("\nFor Windows:")
        print("1. Download from:")
        print("   https://docs.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server")
        print("2. Run the installer as Administrator")
    
    print("\nAfter installation, run this script again to verify.")

def test_connection_string():
    """Test connection string generation"""
    try:
        # Import our consumer module
        sys.path.append(os.path.dirname(__file__))
        from kafka_sqlserver_consumer import SQLServerConfig
        
        print("\n" + "="*60)
        print("CONNECTION STRING TEST")
        print("="*60)
        
        config = SQLServerConfig(
            server="localhost",
            database="test_db",
            username="sa",
            password="test_password"
        )
        
        try:
            connection_string = config.get_connection_string()
            print("✓ Connection string generated successfully:")
            print(f"  {connection_string}")
        except Exception as e:
            print(f"✗ Failed to generate connection string: {e}")
            
    except ImportError as e:
        print(f"Cannot test connection string: {e}")

def main():
    """Main function"""
    print("ODBC Driver Checker for SQL Server")
    print("="*60)
    
    # Check system info
    check_system_info()
    print()
    
    # Check pyodbc
    if not check_pyodbc():
        print("\nPlease install pyodbc first:")
        print("  pip install pyodbc")
        return
    
    # Check drivers
    has_preferred, sql_drivers = check_odbc_drivers()
    
    if not has_preferred and not sql_drivers:
        print("\n⚠️  No SQL Server ODBC drivers found!")
        provide_installation_instructions()
    elif not has_preferred:
        print(f"\n⚠️  No preferred drivers found, but {len(sql_drivers)} SQL-related drivers available")
        print("Consider installing Microsoft ODBC Driver 18 for SQL Server for best compatibility")
    else:
        print("\n✓ SQL Server ODBC drivers are properly configured!")
    
    # Test connection string generation
    test_connection_string()
    
    print("\n" + "="*60)
    print("Check complete!")

if __name__ == "__main__":
    main()
