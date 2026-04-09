@echo off
REM ============================================================
REM  plex-http-proxy startup script (Windows)
REM
REM  This stops Plex, starts the proxy on port 32400,
REM  then restarts Plex (which will fall back to port 32401).
REM
REM  Edit the paths below if your install locations differ.
REM ============================================================

set PROXY_EXE=%~dp0target\release\lg-plex-fixer.exe
set PLEX_EXE=C:\Program Files\Plex\Plex Media Server\Plex Media Server.exe

echo Stopping Plex Media Server...
taskkill /F /IM "Plex Media Server.exe" >nul 2>&1
timeout /t 2 /nobreak >nul

echo Starting proxy on port 32400...
start "" "%PROXY_EXE%"
timeout /t 2 /nobreak >nul

echo Starting Plex Media Server (will use port 32401)...
start "" "%PLEX_EXE%"

echo.
echo Done. Proxy on 32400, Plex on 32401.
echo Close this window when done, or press Ctrl+C.
pause
