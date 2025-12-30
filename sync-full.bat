cd /d C:\dev\wcbt
uv sync --extra dev --extra gui --extra security --extra compression
uv run python -m pytest
