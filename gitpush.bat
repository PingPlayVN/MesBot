@echo off
cd /d "%~dp0"
git add .
git commit -m "new update"
git push
echo Done!
pause