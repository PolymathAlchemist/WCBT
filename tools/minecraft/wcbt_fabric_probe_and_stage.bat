@echo off
setlocal EnableExtensions EnableDelayedExpansion

REM =========================================================
REM WCBT Fabric Server Probe + Snapshot Stager (Windows)
REM - Produces a deterministic report of what exists
REM - Detects world folders by finding level.dat
REM - Hashes mods (SHA256) for exact restore equivalence
REM - Optionally stages a clean snapshot folder for WCBT
REM =========================================================

set "SERVER_ROOT=%~dp0"
set "SERVER_ROOT=%SERVER_ROOT:~0,-1%"
set "OUT_DIR=%SERVER_ROOT%\_wcbt_probe"
set "STAGE_DIR=%SERVER_ROOT%\_wcbt_snapshot"

if exist "%OUT_DIR%" rmdir /s /q "%OUT_DIR%"
mkdir "%OUT_DIR%"

echo Server root: %SERVER_ROOT%
echo Output dir : %OUT_DIR%
echo.

REM -----------------------------
REM 1) Basic identity report
REM -----------------------------
(
  echo [ROOT]
  echo %SERVER_ROOT%
  echo.
  echo [TIMESTAMP]
  powershell -NoProfile -Command "Get-Date -Format o"
  echo.
  echo [JAVA]
  where java
  echo.
  echo [SERVER FILES PRESENT]
  dir /b /a
) > "%OUT_DIR%\root_report.txt"

REM -----------------------------
REM 2) Required items check
REM -----------------------------
set "MISSING=0"

call :need "mods"
call :need "config"
call :need "libraries"
call :need "server.properties"
call :need "eula.txt"

REM One of these should exist in Fabric setups
if not exist "%SERVER_ROOT%\fabric-server-launch.jar" (
  echo WARN: fabric-server-launch.jar not found>>"%OUT_DIR%\required_check.txt"
)

if not exist "%SERVER_ROOT%\server.jar" (
  echo WARN: server.jar not found>>"%OUT_DIR%\required_check.txt"
)

if "%MISSING%"=="1" (
  echo.
  echo ERROR: Missing required items. See: %OUT_DIR%\required_check.txt
  echo Aborting.
  exit /b 1
)

REM -----------------------------
REM 3) Detect world folders
REM    Any directory containing level.dat is treated as a world root
REM -----------------------------
(
  echo [WORLD FOLDERS DETECTED]
  for /d %%D in ("%SERVER_ROOT%\*") do (
    if exist "%%D\level.dat" (
      echo %%~nxD
    )
  )
) > "%OUT_DIR%\world_folders.txt"

findstr /r /c:".\+" "%OUT_DIR%\world_folders.txt" >nul
if errorlevel 1 (
  echo.
  echo ERROR: No world folders detected (no level.dat found under any top-level folder).
  echo Ask him to confirm where the world folder lives, or whether it is nested.
  echo.
  echo Current top-level folders are listed in: %OUT_DIR%\root_report.txt
  exit /b 1
)

REM -----------------------------
REM 4) Size report (top-level)
REM -----------------------------
powershell -NoProfile -Command ^
  "Get-ChildItem -LiteralPath '%SERVER_ROOT%' -Force | " ^
  "Select-Object Name, @{n='Type';e={if($_.PSIsContainer){'Dir'}else{'File'}}}, " ^
  "@{n='Bytes';e={if($_.PSIsContainer){(Get-ChildItem -LiteralPath $_.FullName -Force -Recurse -ErrorAction SilentlyContinue | Measure-Object -Property Length -Sum).Sum}else{$_.Length}}} | " ^
  "Sort-Object Type, Name | Format-Table -AutoSize | Out-String -Width 300 | " ^
  "Set-Content -Encoding UTF8 '%OUT_DIR%\size_report.txt'"

REM -----------------------------
REM 5) Hash mods (SHA256)
REM -----------------------------
(
  echo [MODS SHA256]
  for %%F in ("%SERVER_ROOT%\mods\*.jar") do (
    echo %%~nxF
    certutil -hashfile "%%F" SHA256 | findstr /r /v "hash of|CertUtil"
    echo.
  )
) > "%OUT_DIR%\mods_sha256.txt"

REM -----------------------------
REM 6) Optional: stage a clean snapshot folder
REM    Includes:
REM      - detected world folders
REM      - mods, config, libraries
REM      - server.properties, eula.txt, ops/whitelist/usercache/bans if present
REM      - fabric/server jars and launcher properties if present
REM    Excludes by default:
REM      - logs (large + regenerable)
REM -----------------------------
set /p "DO_STAGE=Create _wcbt_snapshot folder for WCBT? (Y/N): "
if /i not "%DO_STAGE%"=="Y" goto :done

if exist "%STAGE_DIR%" rmdir /s /q "%STAGE_DIR%"
mkdir "%STAGE_DIR%"

echo.
echo Staging snapshot to: %STAGE_DIR%

REM Copy core dirs
xcopy "%SERVER_ROOT%\mods" "%STAGE_DIR%\mods" /E /I /H /Y >nul
xcopy "%SERVER_ROOT%\config" "%STAGE_DIR%\config" /E /I /H /Y >nul
xcopy "%SERVER_ROOT%\libraries" "%STAGE_DIR%\libraries" /E /I /H /Y >nul

REM Copy detected worlds
for /f "usebackq delims=" %%W in ("%OUT_DIR%\world_folders.txt") do (
  if /i "%%W"=="[WORLD FOLDERS DETECTED]" (
    REM header line
  ) else (
    echo Copying world folder: %%W
    xcopy "%SERVER_ROOT%\%%W" "%STAGE_DIR%\%%W" /E /I /H /Y >nul
  )
)

REM Copy core config/state files if present
for %%F in (server.properties eula.txt ops.json whitelist.json usercache.json banned-players.json banned-ips.json) do (
  if exist "%SERVER_ROOT%\%%F" copy "%SERVER_ROOT%\%%F" "%STAGE_DIR%\" >nul
)

REM Copy fabric/server jars and launcher props if present
for %%F in (fabric-server-launch.jar server.jar fabric-server-launcher.properties) do (
  if exist "%SERVER_ROOT%\%%F" copy "%SERVER_ROOT%\%%F" "%STAGE_DIR%\" >nul
)

REM Optionally include .fabric and versions (some setups rely on these caches)
if exist "%SERVER_ROOT%\.fabric" xcopy "%SERVER_ROOT%\.fabric" "%STAGE_DIR%\.fabric" /E /I /H /Y >nul
if exist "%SERVER_ROOT%\versions" xcopy "%SERVER_ROOT%\versions" "%STAGE_DIR%\versions" /E /I /H /Y >nul

echo.
echo Snapshot staged. Point WCBT backup source at:
echo   %STAGE_DIR%
echo.

:done
echo Reports written to:
echo   %OUT_DIR%
echo.
pause
exit /b 0

REM -----------------------------
REM Helpers
REM -----------------------------
:need
if exist "%SERVER_ROOT%\%~1" (
  echo OK: %~1>>"%OUT_DIR%\required_check.txt"
) else (
  echo MISSING: %~1>>"%OUT_DIR%\required_check.txt"
  set "MISSING=1"
)
exit /b 0
