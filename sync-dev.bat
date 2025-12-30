@echo off
cd /d %~dp0\..
uv sync --extra dev
uv run python -m pytest
