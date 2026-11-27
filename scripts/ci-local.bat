@echo off
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%~dp0ci-local.ps1" %*
