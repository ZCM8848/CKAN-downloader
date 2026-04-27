@echo off
chcp 65001 >nul
echo [INFO] Installing dependencies and PyInstaller...
pip install -r requirements.txt pyinstaller

echo [INFO] Building executable...
pyinstaller --onefile --name CKAN-downloader main.py

echo [INFO] Build complete! Check the dist\ folder.
pause
