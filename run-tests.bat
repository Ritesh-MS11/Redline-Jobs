@echo off
echo ============================================
echo   Running Jest Integration Tests
echo   (uses isolated Redis on port 6380)
echo ============================================
echo.

set ROOT=%~dp0
set REDIS=%ROOT%redis\redis-server.exe
set REDISCLI=%ROOT%redis\redis-cli.exe

echo [1] Starting ISOLATED Redis for tests on port 6380...
start "Redis-Test-6380" cmd /k "%REDIS% --port 6380"
timeout /t 2 /nobreak >nul

%REDISCLI% -p 6380 ping >nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo Waiting a bit more for Redis to start...
    timeout /t 3 /nobreak >nul
    %REDISCLI% -p 6380 ping >nul 2>&1
    if %ERRORLEVEL% NEQ 0 (
        echo [ERROR] Redis on port 6380 did not start!
        pause
        exit /b 1
    )
)
echo Redis test instance is up on port 6380.
echo.

echo [2] Running Jest tests...
cd /d %ROOT%
set REDIS_PORT=6380
call npx jest --testTimeout=30000 --forceExit
set TEST_EXIT=%ERRORLEVEL%

echo.
echo [3] Stopping isolated test Redis (port 6380 only)...
%REDISCLI% -p 6380 shutdown nosave 2>nul

if %TEST_EXIT% EQU 0 (
    echo.
    echo ============================================
    echo   ALL 16 TESTS PASSED!
    echo ============================================
) else (
    echo.
    echo ============================================
    echo   SOME TESTS FAILED - see output above
    echo ============================================
)
echo.
pause
exit /b %TEST_EXIT%
