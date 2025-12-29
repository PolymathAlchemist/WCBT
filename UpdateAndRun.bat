@echo off
setlocal
pushd %~dp0

echo.
echo =============================================
echo   WCBT - Update / Install / Run (Windows)
echo =============================================
echo.

REM --- Must be in repo root
if not exist pyproject.toml (
  echo [ERROR] pyproject.toml not found.
  echo Run this from the WCBT repository root.
  goto end
)

REM --- Check Python exists
python --version >nul 2>&1
if %errorlevel% neq 0 (
  echo [ERROR] Python not found on PATH.
  echo Install Python 3.11+ from https://www.python.org/downloads/
  goto end
)

REM --- Check Python >= 3.11 (escaped parentheses)
python -c "import sys; v=sys.version_info; sys.exit(0 if (v.major>3 or (v.major==3 and v.minor>=11)) else 1)" >nul 2>&1
if %errorlevel% neq 0 (
  echo [ERROR] Python 3.11+ required.
  python --version
  goto end
)

REM --- Optional git update
if exist .git (
  git --version >nul 2>&1
  if %errorlevel% equ 0 (
    echo [INFO] Updating repository...
    call git pull --rebase --autostash
    if %errorlevel% neq 0 (
      echo [ERROR] Git update failed.
      goto end
    )
  )
)

REM --- Ensure uv
uv --version >nul 2>&1
if %errorlevel% neq 0 (
  echo [INFO] Installing uv...
  python -m pip install --upgrade pip
  python -m pip install --upgrade uv
  if %errorlevel% neq 0 (
    echo [ERROR] Failed to install uv.
    goto end
  )
)

REM --- Create venv if missing
if not exist .venv (
  echo [INFO] Creating virtual environment...
  call uv venv
  if %errorlevel% neq 0 goto end
)

REM --- Sync deps
echo [INFO] Installing dependencies...
call uv sync --all-extras
if %errorlevel% neq 0 (
  echo [ERROR] Dependency install failed.
  goto end
)

REM --- Run WCBT
echo.
echo [INFO] Starting WCBT...
echo.
call uv run wcbt %*
set exitcode=%errorlevel%

echo.
echo [INFO] WCBT exited with code %exitcode%.
echo.

:end
pause
popd
exit /b %exitcode%
