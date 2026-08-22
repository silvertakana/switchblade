@echo off
rem Switchblade (local-model-router) launcher.
rem Run at Windows logon (Startup shortcut) and every few minutes via the
rem scheduled task "LocalModelRouter keepalive". Idempotent: if the router
rem is already listening on 8787 it does nothing and exits 0, so the task
rem can run on any schedule safely.
set NODE=C:\Program Files\nodejs\node.exe
set LOG="%~dp0router.log"
if not exist "%NODE%" (
  echo [%date% %time%] node not found at %NODE% >> %LOG%
  exit /b 1
)
cd /d "%~dp0"
rem Skip if the router is already listening on 8787.
netstat -ano | findstr /C:":8787" | findstr /C:"LISTENING" >nul
if not errorlevel 1 (
  exit /b 0
)
echo [%date% %time%] router down - starting >> %LOG%
"%NODE%" server.mjs >> router.log 2>&1
exit /b %errorlevel%
