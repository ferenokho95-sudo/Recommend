@echo off
setlocal
cd /d "%~dp0"
if not exist bin\recogo-windows-amd64.exe goto missing
start "RecoGo server" cmd /k call "%~dp0serve_windows.bat"
set /a attempts=0
:waitserver
bin\recogo-windows-amd64.exe health >nul 2>&1
if not errorlevel 1 goto ready
set /a attempts+=1
if %attempts% GEQ 30 goto fail
timeout /t 1 /nobreak >nul
goto waitserver
:ready
bin\recogo-windows-amd64.exe seed
if errorlevel 1 goto fail
bin\recogo-windows-amd64.exe train
if errorlevel 1 goto fail
start "" http://127.0.0.1:8080
echo Admin token defaults to recogo-local. Server runs in the other window.
pause
exit /b 0
:missing
echo Binary missing. Build it using Go: go build -o bin\recogo-windows-amd64.exe ./cmd/recogo
:fail
echo Startup failed. Check the messages above and the RecoGo server window.
pause
exit /b 1
