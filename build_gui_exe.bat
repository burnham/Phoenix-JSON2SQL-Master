@echo off
title Phoenix GUI Compiler v5.0 (Gold Master)
echo [Anya-Corena] Iniciando compilacion v5.0 GOLD...
echo.

python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo [ERROR] Falta Python.
    pause
    exit /b
)

:: 0. SECRETS INITIALIZATION (Local Only)
if not exist ".env" (
    echo [*] Generating .env template for local credentials...
    echo # Phoenix SQL Importer - Local Configuration > .env
    echo # Edit this file with your real data and restart the app. >> .env
    echo DB_HOST=localhost >> .env
    echo DB_PORT=5432 >> .env
    echo DB_DATABASE=my_database >> .env
    echo DB_USER=postgres >> .env
    echo DB_PASSWORD=my_secret_password >> .env
    echo [OK] .env file created.
)

:: 1. LIMPIEZA PREVENTIVA
echo [*] Eliminando rastros antiguos...
if exist "build" rmdir /s /q "build"
if exist "dist" rmdir /s /q "dist"
del /q *.spec >nul 2>&1
taskkill /F /IM PhoenixImporterGUI.exe /T >nul 2>&1

:: 2. ENTORNO VIRTUAL
if not exist "venv_build" (
    echo [*] Creando entorno de compilacion - Solo la primera vez...
    python -m venv venv_build
    call venv_build\Scripts\activate
    echo [*] Instalando dependencias blindadas...
    pip install pyinstaller pandas sqlalchemy psycopg2-binary PyQt6
) else (
    call venv_build\Scripts\activate
)

echo.
echo [*] Compilando PhoenixImporterGUI.exe (Modo Standalone)...
:: Flags explicados:
:: --noconfirm: Sobrescribir sin preguntar
:: --onefile: Todo en un solo .exe
:: --noconsole: Sin ventana negra de CMD detras
:: --clean: Limpiar cache de PyInstaller antes de empezar
pyinstaller --noconfirm --onefile --noconsole --name "PhoenixImporterGUI_v5.0" --icon "resources/phoenix_icon.ico" --add-data "resources;resources" --clean phoenix_gui.py

echo [*] Moviendo ejecutable a la raiz...
move /Y dist\PhoenixImporterGUI_v5.0.exe . >nul

echo.
echo ========================================================
echo   [EXITO] PhoenixImporterGUI_v5.0.exe GENERADO.
echo ========================================================
pause
