@echo off
cd /d "%~dp0"
bin\recogo-windows-amd64.exe serve
echo Server stopped.
pause
