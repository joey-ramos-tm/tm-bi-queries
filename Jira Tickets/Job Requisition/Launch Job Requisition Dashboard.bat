@echo off
echo ================================================================================
echo JOB REQUISITION DASHBOARD LAUNCHER
echo ================================================================================
echo.
echo Starting Flask API on port 5001...
echo.

cd /d "%~dp0"

REM Start the Flask API in a new window
start "Job Requisition API" cmd /k py job_requisition_api.py

REM Wait for the API to start
timeout /t 5 /nobreak > nul

REM Open the dashboard in default browser
echo Opening dashboard in browser...
start http://localhost:8008

echo.
echo ================================================================================
echo Dashboard launched successfully!
echo.
echo Dashboard URL: http://localhost:8008
echo API URL: http://localhost:8008/api
echo.
echo To close the dashboard, close the "Job Requisition API" window
echo ================================================================================
echo.
pause
