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

:: 2. BUILD MODE SELECTION
echo.
echo ========================================================
echo   [BUILD MODE SELECTION]
echo ========================================================
echo 1. DEVELOPMENT BUILD (with your local database credentials)
echo 2. PRODUCTION BUILD (clean executable for distribution)
echo.
set /p BUILD_MODE="Select build mode (1/2): "

if "!BUILD_MODE!"=="2" goto PRODUCTION_BUILD
if not "!BUILD_MODE!"=="1" (
    echo [ERROR] Invalid selection. Defaulting to Development mode.
    set BUILD_MODE=1
)

:: ========== DEVELOPMENT MODE ==========
:DEVELOPMENT_BUILD
echo.
echo [MODE] Development Build Selected
echo ========================================================

if exist ".env" (
    echo.
    echo [*] Found existing .env configuration.
    set /p RECONFIGURE="Do you want to reconfigure your database settings? (y/n): "
    if /i "!RECONFIGURE!"=="y" (
        :: Read current values to show as defaults
        for /f "tokens=1,2 delims==" %%a in (.env) do (
            if "%%a"=="DB_HOST" set DEFAULT_HOST=%%b
            if "%%a"=="DB_PORT" set DEFAULT_PORT=%%b
            if "%%a"=="DB_DATABASE" set DEFAULT_DB=%%b
            if "%%a"=="DB_USER" set DEFAULT_USER=%%b
        )
        echo [*] Loading current configuration as defaults...
    ) else (
        echo [*] Using existing .env configuration.
        goto SKIP_SETUP
    )
) else (
    :: No .env exists, use generic defaults
    set DEFAULT_HOST=localhost
    set DEFAULT_PORT=5432
    set DEFAULT_DB=
    set DEFAULT_USER=postgres
)

:RETRY_SETUP
echo.
echo ========================================================
echo   [SETUP] DATABASE CONFIGURATION
echo ========================================================
echo Let's configure your local database connection.

set /p DBHOST="Enter Host [!DEFAULT_HOST!]: "
if "!DBHOST!"=="" set DBHOST=!DEFAULT_HOST!

set /p DBPORT="Enter Port [!DEFAULT_PORT!]: "
if "!DBPORT!"=="" set DBPORT=!DEFAULT_PORT!

set /p DBNAME="Enter Database Name [!DEFAULT_DB!]: "
if "!DBNAME!"=="" set DBNAME=!DEFAULT_DB!

set /p DBUSER="Enter User [!DEFAULT_USER!]: "
if "!DBUSER!"=="" set DBUSER=!DEFAULT_USER!

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
echo [OK] .env file created successfully with validated credentials.
goto SKIP_SETUP

:: ========== PRODUCTION MODE ==========
:PRODUCTION_BUILD
echo.
echo [MODE] Production Build Selected
echo ========================================================
echo [*] Generating clean .env template for distribution...
(
    echo # Phoenix SQL Importer - Local Configuration
    echo # Edit these values with your actual database credentials
    echo DB_HOST=localhost
    echo DB_PORT=5432
    echo DB_DATABASE=
    echo DB_USER=postgres
    echo DB_PASSWORD=
) > .env
echo [OK] Clean .env template created (no validation performed).

:SKIP_SETUP

:: 3. CLEANUP
echo.
echo [*] Cleaning old artifacts...
if exist "build" rmdir /s /q "build"
if exist "dist" rmdir /s /q "dist"
del /q *.spec >nul 2>&1

:: 4. COMPILATION
echo [*] Compiling PhoenixImporterGUI_v5.0.exe with embedded credentials...
pyinstaller --noconfirm --onefile --noconsole --name "PhoenixImporterGUI_v5.0" --icon "resources/phoenix_icon.ico" --add-data "resources;resources" --add-data ".env;." --clean phoenix_gui.py

echo [*] Moving executable to root...
move /Y dist\PhoenixImporterGUI_v5.0.exe . >nul

echo.
echo ========================================================
echo   [SUCCESS] PhoenixImporterGUI_v5.0.exe GENERATED.
echo ========================================================
pause
