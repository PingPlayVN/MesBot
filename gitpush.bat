@echo off
cd /d "%~dp0"
git add .
git commit -m "update noi tu"
git push
echo Done!
pause