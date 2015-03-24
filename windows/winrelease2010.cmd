@echo off
Echo This script should be run just after You have built
Echo VS2010 library files for realm core.
echo Press Any Key to create release package for VS2010
echo or press CTRL+C to abort
pause
set location=%~dp0
call %location%winrelease.cmd 2010
