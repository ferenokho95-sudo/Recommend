@echo off
cd /d "%~dp0"
docker compose up --build -d --wait api
if errorlevel 1 goto fail
docker compose exec -T api /recogo seed --url http://127.0.0.1:8080
if errorlevel 1 goto fail
docker compose exec -T api /recogo train --url http://127.0.0.1:8080
if errorlevel 1 goto fail
start "" http://127.0.0.1:8080
echo RecoGo is running. MLflow: http://127.0.0.1:5000
pause
exit /b 0
:fail
echo Startup failed. Use docker compose logs --tail 100 to see details.
pause
exit /b 1
