@echo off
python -m src.entrypoints.app
:a
echo.
echo Enter 'r' to run again
cmd /k "doskey r=python -m src.entrypoints.app"
goto a
