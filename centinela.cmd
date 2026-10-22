@echo off
rem Lanzador de centinela.ps1: evita la politica de ejecucion de PowerShell sin cambiarla en el sistema.
rem Uso: centinela.cmd ^<comando^> [opciones]    (centinela.cmd ayuda)
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%~dp0centinela.ps1" %*
set CODIGO=%ERRORLEVEL%
if "%~1"=="" pause
exit /b %CODIGO%
