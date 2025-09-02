@echo off
REM SQL Lineage Analyzer Launcher
REM This script ensures the virtual environment is activated before running the application

REM Check for command-line flags
set AUTO_ANALYZE_FLAG=
set AUTO_QUIT_FLAG=
if "%1"=="--auto-analyze" (
    set AUTO_ANALYZE_FLAG=--auto-analyze
)
if "%1"=="--auto-quit" (
    set AUTO_QUIT_FLAG=--auto-quit
)
if "%2"=="--auto-analyze" (
    set AUTO_ANALYZE_FLAG=--auto-analyze
)
if "%2"=="--auto-quit" (
    set AUTO_QUIT_FLAG=--auto-quit
)

echo Activating virtual environment...
call venv\Scripts\activate.bat

if %ERRORLEVEL% neq 0 (
    echo Error: Failed to activate virtual environment
    echo Please make sure the virtual environment exists in the 'venv' directory
    echo You can create it by running: python -m venv venv
    pause
    exit /b 1
)

echo Installing/updating dependencies...
pip install -r requirements.txt

if %ERRORLEVEL% neq 0 (
    echo Error: Failed to install dependencies
    pause
    exit /b 1
)

echo Starting SQL Lineage Analyzer...
if defined AUTO_ANALYZE_FLAG (
    if defined AUTO_QUIT_FLAG (
        echo Running with flags: %AUTO_ANALYZE_FLAG% %AUTO_QUIT_FLAG%
        python sql_analyzer.py %AUTO_ANALYZE_FLAG% %AUTO_QUIT_FLAG%
    ) else (
        echo Running with flag: %AUTO_ANALYZE_FLAG%
        python sql_analyzer.py %AUTO_ANALYZE_FLAG%
    )
) else if defined AUTO_QUIT_FLAG (
    echo Running with flag: %AUTO_QUIT_FLAG%
    python sql_analyzer.py %AUTO_QUIT_FLAG%
) else (
    python sql_analyzer.py
)

pause
