@echo off
echo Tightdb core c++ binding Windows Build.
Echo alpha 0.01
echo ----------------------------------------------------------
Echo Currently this batch file will create a release version of
echo the already built source in the directory where the batch 
echo file resides. The release version will be placed in 
echo (git checkout directory)\release\VS2012\files
echo the directory is created if it does not exist.
echo prior contents of VS2012 will be deleted, except .7z files.
echo the contents of the directory will be the library files 
echo from lib, as well as header files from src and 
echo subdirectories.
echo in other words, release\VS2012 will contain 
echo the static files c++ binding to be consumed by VS2012 on 
echo windows.
echo after the files have been assembled, they are compressed 
echo with 7zip to create an archive redistributable called 
echo core_vs2012_yyyy_mm_dd_hh_mm_computername.7z
echo the .7z files as well as a decompressor will be placed in
echo (git checkout directory)\release\vs2012\release
echo only the release directory is needed by c++ binding 
echo users (on windows, using VS2012 c++)
echo ----------------------------------------------------------
echo parametres (%0)
echo where this file is located (%~dp0)
set location=%~dp0
set date=xdatex
set time=xtimex
set developer=xdeveloperx
echo this batchfile is located at %location%
echo -
echo Press Any key to proceed, or close the window to abort
pause
:remove any files from a prior release
echo cleaning up from earlier releases
del %location%release\vs2012\files /Q
del %location%release\vs2012\files\src\tightdb /Q
del %location%release\vs2012\files\src\tightdb\build /Q
del %location%release\vs2012\files\src\win32 /Q
del %location%release\vs2012\files\src\win32\pthread /Q
rem we keep the release directory as is - it might contain
rem earlier builds, we should not delete those
:create release directory structure in case it isn't there
echo creating release directory structure
md %location%release
md %location%release\vs2012
md %location%release\vs2012\files\
md %location%release\vs2012\files\src
md %location%release\vs2012\release
echo copying release files to release directory
:copy the archiver to the release directory
copy %location%7z.exe %location%\release\vs2012\release
;copy library files to the files directory
copy %location%lib\*.lib %location%\release\vs2012\files
:copy the header files to the files directory
xcopy %location%src\*.h* %location%\release\vs2012\files\src /S
:archive the files directory and put it in the release directory
echo creating release archive
cd %location%release\vs2012\files
%location%7z.exe a  -r %location%\release\vs2012\release\tightdb_cpp_VS2012_%xdatex%_%xtimex%_%xdeveloperx% *.*
:revision history
:0.01 
:First version. still needs to set up date time and developer environment variables correctly
:Only support for vs2012
:still need to be able to call the compiler to get things built
