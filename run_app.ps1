# SQL Lineage Analyzer Launcher
# This script ensures the virtual environment is activated before running the application

param(
    [switch]$AutoAnalyze,
    [switch]$AutoQuit,
    [switch]$TestMode
)

# Determine the flags to use
$flags = @()
if ($AutoAnalyze) {
    $flags += "--auto-analyze"
}
if ($AutoQuit) {
    $flags += "--auto-quit"
}
if ($TestMode) {
    $flags += "--test-mode"
}

Write-Host "Activating virtual environment..." -ForegroundColor Green
& ".\venv\Scripts\Activate.ps1"

if ($LASTEXITCODE -ne 0) {
    Write-Host "Error: Failed to activate virtual environment" -ForegroundColor Red
    Write-Host "Please make sure the virtual environment exists in the 'venv' directory" -ForegroundColor Yellow
    Write-Host "You can create it by running: python -m venv venv" -ForegroundColor Yellow
    Read-Host "Press Enter to exit"
    exit 1
}

Write-Host "Installing/updating dependencies..." -ForegroundColor Green
pip install -r requirements.txt

if ($LASTEXITCODE -ne 0) {
    Write-Host "Error: Failed to install dependencies" -ForegroundColor Red
    Read-Host "Press Enter to exit"
    exit 1
}

Write-Host "Starting SQL Lineage Analyzer..." -ForegroundColor Green
if ($flags.Count -gt 0) {
    $flagString = $flags -join " "
    Write-Host "Running with flags: $flagString" -ForegroundColor Cyan
    python sql_analyzer.py $flags
} else {
    python sql_analyzer.py
}

Read-Host "Press Enter to exit"
