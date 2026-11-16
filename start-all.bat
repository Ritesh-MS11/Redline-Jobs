@echo off
setlocal enabledelayedexpansion
echo ============================================
echo   Job Queue System - Start Everything
echo ============================================
echo.

set ROOT=%~dp0
set REDISDIR=%ROOT%redis
set REDIS=%REDISDIR%\redis-server.exe
set REDISCLI=%REDISDIR%\redis-cli.exe

REM ── Check if Redis is already running on port 6379 ───────────────────────────
"%REDISCLI%" -p 6379 ping >nul 2>&1
if %ERRORLEVEL% EQU 0 (
    echo [Redis] Already running on port 6379.
    goto START_SERVICES
)

echo [1/4] Starting Redis server on port 6379...
start "Redis Server" /d "%REDISDIR%" cmd /k "redis-server.exe --port 6379"

echo     Waiting for Redis to become ready...
set RETRIES=0
:WAIT_REDIS
timeout /t 1 /nobreak >nul
"%REDISCLI%" -p 6379 ping >nul 2>&1
if %ERRORLEVEL% EQU 0 (
    echo [Redis] Ready and responding on port 6379.
    goto START_SERVICES
)
set /a RETRIES+=1
if !RETRIES! GEQ 10 (
    echo.
    echo [ERROR] Redis did not start within 10 seconds.
    echo Please check the "Redis Server" window for error messages.
    pause
    exit /b 1
)
goto WAIT_REDIS

:START_SERVICES
echo.
echo [2/4] Starting API server on http://localhost:3000 ...
start "API Server" /d "%ROOT%" cmd /k "node src\api\server.js"
timeout /t 2 /nobreak >nul

echo [3/4] Starting 3 background Workers...
start "Worker-1" /d "%ROOT%" cmd /k "set WORKER_ID=worker-1 && node src\worker\index.js"
start "Worker-2" /d "%ROOT%" cmd /k "set WORKER_ID=worker-2 && node src\worker\index.js"
start "Worker-3" /d "%ROOT%" cmd /k "set WORKER_ID=worker-3 && node src\worker\index.js"
timeout /t 2 /nobreak >nul

echo.
echo ============================================
echo   All services running!
echo   Dashboard: http://localhost:3000/dashboard
echo ============================================
echo.
echo Opening dashboard in your browser...
start "" "http://localhost:3000/dashboard"
echo.
echo Press any key to stop all services...
pause >nul

echo.
echo Stopping services...
taskkill /F /FI "WINDOWTITLE eq API Server*" >nul 2>&1
taskkill /F /FI "WINDOWTITLE eq Worker-*" >nul 2>&1
echo Done.
