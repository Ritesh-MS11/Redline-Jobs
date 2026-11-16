@echo off
echo ============================================
echo   Sending Test Jobs to the API
echo ============================================
echo.

set REDISCLI=%~dp0redis\redis-cli.exe

echo [1/4] Enqueueing sendEmail job...
powershell -NoProfile -Command "try { $r = Invoke-RestMethod -Uri 'http://localhost:3000/jobs' -Method POST -ContentType 'application/json' -Body '{\"type\":\"sendEmail\",\"payload\":{\"to\":\"alice@example.com\",\"subject\":\"Order Confirmation\"}}'; Write-Host ('   -> Enqueued Job ID: ' + $r.jobId) -ForegroundColor Green } catch { Write-Host ('   -> Error: ' + $_.Exception.Message) -ForegroundColor Red }"

echo.
echo [2/4] Enqueueing resizeImage job...
powershell -NoProfile -Command "try { $r = Invoke-RestMethod -Uri 'http://localhost:3000/jobs' -Method POST -ContentType 'application/json' -Body '{\"type\":\"resizeImage\",\"payload\":{\"url\":\"profile.png\",\"width\":600,\"height\":600}}'; Write-Host ('   -> Enqueued Job ID: ' + $r.jobId) -ForegroundColor Green } catch { Write-Host ('   -> Error: ' + $_.Exception.Message) -ForegroundColor Red }"

echo.
echo [3/4] Enqueueing flaky job (simulates 40%% failure & exponential retry)...
powershell -NoProfile -Command "try { $r = Invoke-RestMethod -Uri 'http://localhost:3000/jobs' -Method POST -ContentType 'application/json' -Body '{\"type\":\"flaky\",\"payload\":{\"demo\":true}}'; Write-Host ('   -> Enqueued Job ID: ' + $r.jobId) -ForegroundColor Green } catch { Write-Host ('   -> Error: ' + $_.Exception.Message) -ForegroundColor Red }"

echo.
echo [4/4] Current Queue Statistics:
powershell -NoProfile -Command "try { $s = Invoke-RestMethod -Uri 'http://localhost:3000/queue/stats'; Write-Host ('   Pending:    ' + $s.pending); Write-Host ('   Processing: ' + $s.processing); Write-Host ('   Completed:  ' + $s.completed); Write-Host ('   Delayed:    ' + $s.delayed); Write-Host ('   Dead:       ' + $s.dead) } catch { Write-Host ('   -> Error fetching stats: ' + $_.Exception.Message) -ForegroundColor Red }"

echo.
echo ============================================
echo Open your dashboard to watch them process:
echo http://localhost:3000/dashboard
echo ============================================
echo.
pause
