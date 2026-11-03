@echo off
rem Lanzador de centinela.ps1: evita la política de ejecución de PowerShell sin cambiarla en el sistema.
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%~dp0centinela.ps1" %*
set CODIGO=%ERRORLEVEL%
if "%~1"=="" pause
exit /b %CODIGO%
