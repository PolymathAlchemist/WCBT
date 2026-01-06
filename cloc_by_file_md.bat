@echo off
setlocal

cd /d "%~dp0" || exit /b 1

set "OUT=cloc-by-file.md"

REM Overwrite the output with a simple Markdown header
(
  echo # cloc by-file report
  echo.
  echo Generated via:
  echo `cloc --vcs=git --by-file --md --hide-rate`
  echo.
  echo Note: Reformatted using PyCharm Markdown table formatter.
  echo.
  echo ---
  echo.
) > "%OUT%"

REM Append cloc output directly (no .tmp)
cloc --vcs=git --by-file --md --hide-rate >> "%OUT%" 2>>&1

echo Wrote "%OUT%"
echo.
