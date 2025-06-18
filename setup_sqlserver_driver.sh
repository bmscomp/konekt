#!/bin/bash

# Setup script for SQL Server ODBC driver on macOS

echo "Setting up SQL Server ODBC driver for macOS..."

# Check if Homebrew is installed
if ! command -v brew &> /dev/null; then
    echo "Error: Homebrew is not installed. Please install Homebrew first."
    echo "Visit: https://brew.sh/"
    exit 1
fi

# Install unixODBC if not already installed
echo "Installing unixODBC..."
brew install unixodbc

# Install Microsoft ODBC Driver for SQL Server
echo "Installing Microsoft ODBC Driver for SQL Server..."

# Download and install Microsoft ODBC Driver 18 for SQL Server
if [[ $(uname -m) == "arm64" ]]; then
    # Apple Silicon (M1/M2)
    echo "Detected Apple Silicon (ARM64)"
    DRIVER_URL="https://download.microsoft.com/download/1/f/f/1fffb537-26ab-4947-a46a-7a45c27f6f77/msodbcsql18_18.3.2.1-1_arm64.dmg"
else
    # Intel
    echo "Detected Intel x64"
    DRIVER_URL="https://download.microsoft.com/download/1/f/f/1fffb537-26ab-4947-a46a-7a45c27f6f77/msodbcsql18_18.3.2.1-1_amd64.dmg"
fi

# Create temporary directory
TEMP_DIR=$(mktemp -d)
cd "$TEMP_DIR"

echo "Downloading Microsoft ODBC Driver..."
curl -L -o msodbcsql.dmg "$DRIVER_URL"

if [ $? -eq 0 ]; then
    echo "Download completed. Please manually install the driver:"
    echo "1. Open the downloaded DMG file: $TEMP_DIR/msodbcsql.dmg"
    echo "2. Run the installer package"
    echo "3. Follow the installation instructions"
    echo ""
    echo "After installation, run: odbcinst -q -d"
    echo "You should see 'ODBC Driver 18 for SQL Server' in the list"
    
    # Open the DMG file
    open msodbcsql.dmg
else
    echo "Error: Failed to download the driver"
    exit 1
fi

# Alternative: Install via Homebrew tap (if available)
echo ""
echo "Alternative installation via Homebrew:"
echo "brew tap microsoft/mssql-release https://github.com/Microsoft/homebrew-mssql-release"
echo "brew update"
echo "HOMEBREW_ACCEPT_EULA=Y brew install msodbcsql18 mssql-tools18"

echo ""
echo "Setup script completed!"
echo "After installing the driver, you can test the connection with:"
echo "python -c \"import pyodbc; print([d for d in pyodbc.drivers() if 'SQL Server' in d])\""
