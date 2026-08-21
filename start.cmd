@echo off
rem Local model router launcher. Run at Windows logon via a scheduled task.
rem Uses the full node path (scheduled tasks do not inherit the user PATH).
set NODE=C:\Program Files\nodejs\node.exe
set LOG="C:\dev\local-model-router\router.log"
if not exist "%NODE%" (
  echo node not found at %NODE% >> %LOG%
  exit /b 1
)
rem Skip if the router is already listening on 8787 (e.g. manually started).
netstat -ano | findstr /C:":8787" | findstr /C:"LISTENING" >nul
if not errorlevel 1 (
  echo [%date% %time%] already running on 8787, skipping start >> %LOG%
  exit /b 0
)
cd /d "C:\dev\local-model-router"
"%NODE%" server.mjs >> router.log 2>&1
