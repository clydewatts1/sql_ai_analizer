# SQL Lineage Analyzer

**Prototype Version**

This project is an early-stage prototype for analyzing SQL data lineage using AI (Google Gemini) and Teradata database integration. The application features a Tkinter-based GUI, multi-threaded connectivity checks, and a built-in prompt editor for customizing AI instructions.

A Tkinter-based GUI application that analyzes SQL files using Google's Gemini AI to generate data lineage mappings.

## Features

- **SQL Analysis**: Analyze SQL files to generate data lineage mappings
- **AI Integration**: Uses Google's Gemini AI for intelligent SQL analysis
- **Database Connectivity**: Connect to Teradata databases for schema discovery
- **Interactive Chat**: Chat with Gemini AI for SQL-related questions
- **Visual Outputs**: Generate Draw.io, Mermaid, and Graphviz diagrams
- **Markdown Explanations**: Save detailed analysis explanations in markdown format

## Prerequisites

- Python 3.8 or higher
- Virtual environment (recommended)

## Setup

1. **Clone or download** the project files
2. **Create a virtual environment** (if not already created):
   ```bash
   python -m venv venv
   ```

## Running the Application

### Option 1: Using the Launcher Scripts (Recommended)

**Windows Batch File:**
```bash
run_app.bat
```

**PowerShell Script:**
```powershell
.\run_app.ps1
```

These scripts will:
- Activate the virtual environment
- Install/update all required dependencies
- Launch the SQL Lineage Analyzer application

### Option 2: Manual Setup

1. **Activate the virtual environment:**
   ```bash
   # Windows
   venv\Scripts\activate

   # Linux/Mac
   source venv/bin/activate
   ```

2. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Run the application:**
   ```bash
   python sql_analyzer.py
   ```

### Option 4: Testing Auto-Analyze

**Test Script:**
```bash
test_auto_analyze.bat
```

This script verifies that:
- The `test.sql` file exists
- The virtual environment is properly set up
- The auto-analyze functionality works correctly

## Command-Line Options

The application supports the following command-line arguments:

- `--auto-analyze`: Automatically analyze the default `test.sql` file on startup
- `--auto-quit`: Automatically quit after analysis is complete
- `--test-mode`: Run in test mode (non-interactive)

## Features

- **Automatic Environment Setup**: Launcher scripts handle virtual environment activation and dependency installation
- **Auto-Analyze Mode**: Automatically loads and analyzes the default SQL file for testing purposes
- **Auto-Quit Mode**: Automatically closes the application after analysis completion
- **Command-Line Interface**: Support for various startup modes via command-line arguments
- **Environment Validation**: Built-in checks to ensure proper virtual environment setup

## Configuration

The application uses a configuration file stored at:
- Windows: `%USERPROFILE%\sql_an_analizer\config.yaml`

This file stores:
- Database connection parameters
- Gemini API key
- Other application settings

## Usage

1. **Configure API Key**: Enter your Google Gemini API key in the configuration section
2. **Database Setup** (optional): Configure database connection parameters if you want to use schema discovery
3. **Load SQL File**: Use the file browser to select a SQL file for analysis
4. **Run Analysis**: Click "Analyze SQL" to generate lineage mappings
5. **View Results**: Check the various output tabs for results and diagrams

### Command-Line Usage Examples

**Basic Usage:**
```bash
# Normal interactive mode
run_app.bat

# Auto-analyze mode (loads test.sql automatically)
run_app.bat --auto-analyze

# Auto-quit after analysis (closes automatically when done)
run_app.bat --auto-analyze --auto-quit

# PowerShell examples
.\run_app.ps1 -AutoAnalyze -AutoQuit
```

**Batch Processing:**
```bash
# Perfect for CI/CD pipelines or automated testing
# This mode runs completely unattended - no user interaction required
python sql_analyzer.py --auto-analyze --auto-quit
```

## Dependencies

- `google-genai>=2.0.0`: Google Gemini AI integration
- `teradatasql`: Teradata database connectivity
- `sqlfluff`: SQL parsing and formatting
- `pyyaml`: YAML configuration file handling
- `markdown`: Markdown processing
- `tkinterweb`: HTML display in Tkinter

## Troubleshooting

### Virtual Environment Issues
If you encounter virtual environment activation errors:
1. Make sure Python is installed
2. Recreate the virtual environment: `python -m venv venv`
3. Use the launcher scripts which handle activation automatically

### Module Import Errors
If you get import errors:
1. Make sure you're running from the activated virtual environment
2. Run `pip install -r requirements.txt` to install all dependencies
3. Use the launcher scripts which install dependencies automatically

### Database Connection Issues
- Ensure your Teradata database credentials are correct
- Check network connectivity to your database server
- Verify that the Teradata client libraries are properly installed

## Support

For issues or questions, please check the application logs and ensure all dependencies are properly installed.
