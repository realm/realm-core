if .%1==. goto error
@echo off
echo Tightdb core c++ binding Windows Release.
Echo beta 0.1
echo ----------------------------------------------------------
echo In general, relase.cmd will create release directories
echo with release versions of the already built solution in the
echo same directory as where the solution file is.
echo The release version will be placed in a directory called
echo release, in two sub directories files\ and release\
echo files\ will be an uncompressed release of all needed files
echo laid out in a logical way, and release\ will contain
echo a zipped version of the files\directory - named according
echo to the time the winrelease.cmd file was run
echo the zipfile will also contain the username from the
echo computer that generated the zipfile
echo only the release\files directory is needed by c++ binding 
echo developers (on windows)
echo the zip file in release\vs2012\release can be shipped to
echo c++ binding users and coders of bindings to other languages
echo on the windows platform.
echo ----------------------------------------------------------
echo parameters (%0)
echo where this file is located (%~dp0)
set vsversion=%1
set location=%~dp0
:note - this will not work if Your locale is not similar to Danish
set reldate=%date:~6,4%-%date:~3,2%-%date:~0,2%
set reltime=%time::=%
set reltime=%reltime: =%
set reltime=%reltime:,=%
set reldeveloper=%USERNAME: =%
set reldeveloper=%reldeveloper:,=%
echo this batchfile is located at %location%
echo -
echo - Release date will be set to %reldate%
echo - Release time will be set to %reltime%
echo - Release user will be set to %reldeveloper%
echo - Release for VS%vsversion% will be built
echo -
echo Press Any key to proceed, or CTRL+C to abort
pause
:remove any files from a prior release
echo cleaning up from earlier releases
del %location%release\vs%vsversion%\files /Q /S
rem we keep the release directory as is - it might contain
rem earlier builds, we should not delete those
:create release directory structure in case it isn't there
echo creating release directory structure
md %location%release
md %location%release\vs%vsversion%
md %location%release\vs%vsversion%\files\
md %location%release\vs%vsversion%\files\src
md %location%release\vs%vsversion%\release
echo copying release files to release directory
:copy library files to the files directory
copy %location%lib\*.lib %location%\release\vs%vsversion%\files
:copy the header files to the files directory
xcopy %location%src\*.h* %location%\release\vs%vsversion%\files\src /S /y
:archive the files directory and put it in the release directory
echo creating release archive
set releasefilename=%location%\release\vs%vsversion%\release\tightdb_cpp_VS%vsversion%_%reldate%_%reltime%_%reldeveloper%
echo release file name set to %releasefilename%
cd %location%release\vs%vsversion%\files
echo %location%7z.exe a -tzip -r %location%release\vs%vsversion%\release\%releasefilename% *.*
%location%7z.exe a -tzip -r %releasefilename% *.*
:revision history
:0.01 
:First version. still needs to set up date time and developer environment variables correctly
:Only support for vs2012
:still need to be able to call the compiler to get things built
:0.02 language rewritten for clarity
:0.02 7zip now called so that it makes a .zip file instead of a 7z file
:0.02 file renamed to winrelease.bat from build.bat
:0.03 file renamed to winrelease.cmd from winrelease.bat
:0.04 Added time and date and username to the filename
:0.05 changed comments, reoved debug pause commands
:0.10 added support for VS2010 as well as VS2012
echo Finished!
pause
goto end
:error
echo Please run the Winrelease2010 or the Winrelease2012 command file
:end
