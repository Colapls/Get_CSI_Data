@echo off
REM ============================================================
REM   CSI Web Collector - Windows starter
REM   ASCII-only to avoid cmd.exe codepage issues
REM   - Auto-kill stale processes on port 2234
REM   - Auto-install missing deps
REM   - Default port 2234 (vs csi_visualizer 2233)
REM   - Auto-open browser after 2s
REM ============================================================

setlocal EnableExtensions EnableDelayedExpansion

set PORT=2234
set HOST=127.0.0.1

cd /d "%~dp0"

echo ============================================================
echo   CSI Web Collector - Web startup
echo ============================================================

REM ============================================================
REM 1) Release port 2234 (kill any stale listener).
REM    Use PowerShell -> ASCII temp file -> for /f loop.
REM    Avoids unicode issues with piping PS straight to for /f.
REM    No IF/ELSE wrapper: for /f over an empty file just does
REM    nothing, and IF blocks containing `>nul 2>&1` can trigger
REM    spurious `timeout was unexpected` errors on some Windows.
REM ============================================================
echo [start] Checking port %PORT% ...

set "PS_TMP=C:\Windows\Temp\csi_port_%PORT%.txt"
if exist "%PS_TMP%" del "%PS_TMP%" >nul 2>&1

REM Dump LISTENING PIDs to temp file
powershell -NoProfile -Command "(Get-NetTCPConnection -LocalPort %PORT% -State Listen -ErrorAction SilentlyContinue | Select-Object -ExpandProperty OwningProcess) | ForEach-Object { [Console]::Out.WriteLine($_) }" > "%PS_TMP%" 2>nul

REM Read PIDs and kill (empty PIDs -> loop does nothing)
set "PORT_PIDS="
for /f "usebackq delims=" %%P in ("%PS_TMP%") do (
    set "PORT_PIDS=!PORT_PIDS! %%P"
)

echo [start] Stale PIDs on port %PORT%:%PORT_PIDS%
for %%P in (!PORT_PIDS!) do (
    echo [start]   Killing PID %%P ...
    taskkill /F /PID %%P >nul 2>&1
)
ping -n 3 127.0.0.1 >nul
echo [start] Port cleanup done.

REM Fallback: any other state still on 2234 (TIME_WAIT, ESTABLISHED, etc.)
powershell -NoProfile -Command "(Get-NetTCPConnection -LocalPort %PORT% -ErrorAction SilentlyContinue | Select-Object -ExpandProperty OwningProcess) | ForEach-Object { [Console]::Out.WriteLine($_) }" > "%PS_TMP%" 2>nul
for /f "usebackq delims=" %%P in ("%PS_TMP%") do (
    taskkill /F /PID %%P >nul 2>&1
)

if exist "%PS_TMP%" del "%PS_TMP%" >nul 2>&1

REM ============================================================
REM 2) Check Python dependencies
REM ============================================================
echo [start] Checking Python dependencies...
python -c "import flask, flask_socketio, numpy, requests, PIL" 2>nul
if errorlevel 1 (
    echo [start] Installing requirements.txt ...
    python -m pip install -r requirements.txt
    if errorlevel 1 (
        echo [start] ERROR: pip install failed
        pause
        exit /b 1
    )
)

REM ============================================================
REM 3) Clear __pycache__ so template changes take effect
REM ============================================================
if exist __pycache__            rd /s /q __pycache__            >nul 2>&1
if exist templates\__pycache__ rd /s /q templates\__pycache__ >nul 2>&1
if exist static\__pycache__    rd /s /q static\__pycache__    >nul 2>&1

REM ============================================================
REM 4) Start the Flask + SocketIO app
REM ============================================================
echo [start] Starting CSI Web Collector on port %PORT% ...
echo [start] Browser will auto-open http://%HOST%:%PORT%/
echo.

REM Open browser 2 seconds after startup (non-blocking)
start "" /b cmd /c "ping -n 3 127.0.0.1 >nul && start http://%HOST%:%PORT%/" >nul 2>&1

python app.py
if errorlevel 1 (
    echo.
    echo [start] ERROR: app.py exited with error
    pause
)

endlocal
