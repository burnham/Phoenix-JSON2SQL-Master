@echo off
title Limpiador Tactico: GUI (VISUAL)
color 0b
cd /d "%~dp0"

echo [Anya-Corena] Ejecutando protocolo de limpieza...

:: 1. Archivos Temporales de Compilacion
echo [*] Borrando carpetas 'build' y 'dist'...
if exist "build" rmdir /s /q "build"
if exist "dist" rmdir /s /q "dist"

:: 2. Archivos de Especificacion
echo [*] Borrando .spec...
del /q *.spec >nul 2>&1

:: 3. Cache de Python (Junk)
echo [*] Borrando __pycache__ y logs...
if exist "__pycache__" rmdir /s /q "__pycache__"
if exist "logs" rmdir /s /q "logs"
del /q *.log >nul 2>&1

:: NOTA: No borramos 'venv_build' para que no tengas que instalar
:: las librerias cada vez que quieras hacer un cambio rapido.
:: Si quieres borrarlo, descomenta la siguiente linea:
:: rmdir /s /q "venv_build"

echo.
echo [EXITO] Espacio de trabajo limpio. Solo queda el Codigo Fuente y el EXE.
timeout /t 3 >nul