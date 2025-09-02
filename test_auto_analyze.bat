@echo off
REM Test script for auto-analyze functionality
REM This script tests if the auto-analyze feature works correctly without user interaction

echo Testing auto-analyze functionality...

REM Check if test.sql exists
if not exist "test.sql" (
    echo ERROR: test.sql file not found!
    echo Please ensure test.sql exists in the current directory.
    pause
    exit /b 1
)

echo Found test.sql file.

REM Check if virtual environment exists
if not exist "venv\Scripts\activate.bat" (
    echo ERROR: Virtual environment not found!
    echo Please run the setup first.
    pause
    exit /b 1
)

echo Virtual environment found.

REM Run the application with auto-analyze and auto-quit (fully automated)
echo Running application with --auto-analyze --auto-quit flags...
echo This should complete automatically without requiring any user interaction.
call run_app.bat --auto-analyze --auto-quit

echo Test completed - application should have closed automatically.
pause
