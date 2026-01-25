@echo off
setlocal enabledelayedexpansion
title Phoenix GUI Compiler v5.0 (Gold Master)
echo [Anya-Corena] Starting Compilation v5.0 GOLD...
echo.

python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo [ERROR] Python is missing.
    pause
    exit /b
)

:: 1. VIRTUAL ENVIRONMENT setup
if not exist "venv_build" (
    echo [*] Creating build environment...
    python -m venv venv_build
)
call venv_build\Scripts\activate
echo [*] Installing/Verifying dependencies...
pip install pyinstaller pandas sqlalchemy psycopg2-binary PyQt6 --quiet

:: 2. SECRETS INITIALIZATION (Interactive Setup with Reconfiguration Option)
if exist ".env" (
    echo.
    echo [*] Found existing .env configuration.
    set /p RECONFIGURE="Do you want to reconfigure your database settings? (y/n): "
    if /i "!RECONFIGURE!"=="y" (
        del .env
        echo [*] Existing configuration removed. Starting setup...
    ) else (
        echo [*] Using existing .env configuration.
        goto SKIP_SETUP
    )
)

:RETRY_SETUP
echo.
echo ========================================================
echo   [SETUP] DATABASE CONFIGURATION
echo ========================================================
echo Let's configure your local database connection.

set /p DBHOST="Enter Host [localhost]: "
if "!DBHOST!"=="" set DBHOST=localhost

set /p DBPORT="Enter Port [5432]: "
if "!DBPORT!"=="" set DBPORT=5432

set /p DBNAME="Enter Database Name: "
set /p DBUSER="Enter User [postgres]: "
if "!DBUSER!"=="" set DBUSER=postgres

set /p DBPASS="Enter Password: "

echo.
echo [*] Verifying connection...
python -c "import psycopg2; psycopg2.connect(host='!DBHOST!', port=int('!DBPORT!'), dbname='!DBNAME!', user='!DBUSER!', password='!DBPASS!').close()"

if !errorlevel! neq 0 (
    echo.
    echo [ERROR] Connection failed!
    echo Please check your credentials and try again.
    echo.
    pause
    goto RETRY_SETUP
)

echo [SUCCESS] Connection verified! Saving .env...
(
    echo # Phoenix SQL Importer - Local Configuration
    echo DB_HOST=!DBHOST!
    echo DB_PORT=!DBPORT!
    echo DB_DATABASE=!DBNAME!
    echo DB_USER=!DBUSER!
    echo DB_PASSWORD=!DBPASS!
) > .env
echo [OK] .env file created successfully.

:SKIP_SETUP

:: 3. CLEANUP
echo.
echo [*] Cleaning old artifacts...
if exist "build" rmdir /s /q "build"
if exist "dist" rmdir /s /q "dist"
del /q *.spec >nul 2>&1

:: 4. COMPILATION
echo [*] Compiling PhoenixImporterGUI_v5.0.exe...
pyinstaller --noconfirm --onefile --noconsole --name "PhoenixImporterGUI_v5.0" --icon "resources/phoenix_icon.ico" --add-data "resources;resources" --clean phoenix_gui.py

echo [*] Moving executable to root...
move /Y dist\PhoenixImporterGUI_v5.0.exe . >nul

echo.
echo ========================================================
echo   [SUCCESS] PhoenixImporterGUI_v5.0.exe GENERATED.
echo ========================================================
pause
