@echo off
setlocal enabledelayedexpansion

REM ------------------------------------------------------------
REM WCBT cloc by-file report (Markdown)
REM Ensures execution from project root
REM ------------------------------------------------------------

REM Move to project root (parent of tools)
cd /d "%~dp0\.."

set RAW=raw-output_cloc-by-file.md
set CLEAN=cloc-by-file.md

REM Step 1: generate raw Markdown from cloc
cloc --vcs=git --by-file --md --hide-rate > %RAW%

REM Step 2: reformat for strict Markdown parsers (PyCharm)
powershell -NoProfile -Command ^
  "$lines = Get-Content '%RAW%'; ^
   $out = @(); ^
   foreach ($line in $lines) { ^
     if ($line -match '^cloc\|github') { continue } ^
     if ($line -match '^--- \| ---') { continue } ^
     if ($line -match '^\|[- ]+\|') { $out += $line; continue } ^
     if ($line -match '^\|') { $out += $line; continue } ^
   }; ^
   $out | Set-Content '%CLEAN%'"

echo.
echo cloc reports generated at project root:
echo   %RAW%
echo   %CLEAN%
echo.
