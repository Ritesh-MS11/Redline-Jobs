@echo off
setlocal enabledelayedexpansion
echo ============================================
echo   RESTART - Clean shutdown and restart
echo ============================================
echo.

set ROOT=%~dp0
set REDISDIR=%ROOT%redis
set REDISCLI=%REDISDIR%\redis-cli.exe

echo [1] Stopping all running Node.js workers & API...
taskkill /F /IM node.exe >nul 2>&1
echo     Done.

echo [2] Stopping Redis server on port 6379...
"%REDISCLI%" -p 6379 shutdown nosave >nul 2>&1
timeout /t 1 /nobreak >nul
taskkill /F /IM redis-server.exe >nul 2>&1
echo     Done.

echo.
echo [3] Starting Redis server fresh...
start "Redis Server" /d "%REDISDIR%" cmd /k "redis-server.exe --port 6379"

echo     Waiting for Redis to respond...
set RETRIES=0
:WAIT_REDIS
timeout /t 1 /nobreak >nul
"%REDISCLI%" -p 6379 ping >nul 2>&1
if %ERRORLEVEL% EQU 0 (
    echo [Redis] Ready on port 6379!
    goto START_SERVICES
)
set /a RETRIES+=1
if !RETRIES! GEQ 10 (
    echo [ERROR] Redis failed to start.
    pause
    exit /b 1
)
goto WAIT_REDIS

:START_SERVICES
echo.
echo [4] Starting API server...
start "API Server" /d "%ROOT%" cmd /k "node src\api\server.js"
timeout /t 2 /nobreak >nul

echo [5] Starting 3 Workers...
start "Worker-1" /d "%ROOT%" cmd /k "set WORKER_ID=worker-1 && node src\worker\index.js"
start "Worker-2" /d "%ROOT%" cmd /k "set WORKER_ID=worker-2 && node src\worker\index.js"
start "Worker-3" /d "%ROOT%" cmd /k "set WORKER_ID=worker-3 && node src\worker\index.js"
timeout /t 2 /nobreak >nul

echo.
echo ============================================
echo   System restarted successfully!
echo   Dashboard: http://localhost:3000/dashboard
echo ============================================
echo.
start "" "http://localhost:3000/dashboard"
pause
