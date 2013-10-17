This directory contains scripts and code files used to collect and zip files for a windows relase.

Following is a short description of how to create a windows release of tightdb as c++ LIB binaries.

You will need both Visual Studio 2010 and Visual Studio 2012 to build the release.

Warning : Following the instructions below, after the build process has ended, vs2012 will 
have changed the project files. Do not check these changes back into github as 
VS2010 cannot read them correctly.

1. open tightDB.sln in VS2010
2. select Build->Batch Build. Unmark all , mark the 4 called tightDB static libraray click clean
3. select Build->Batch build. Click Rebuild
4. Run winrelease2010.cmd (located in <checkout directory>windows\)
5. close VS2010
6. open tightDB.sln in VS2012
7. right click "Solution 'TightDB' (8 projects) in Solution Explorer
8. select Update VC++ Projects
9. in the popup "Update VC++ Compiler and Libraries" click Update
10. wait while VS2012 updates the projects.
11. select Build->Batch Build. Unmark all , mark the 4 called tightDB static libraray click clean
12. build->batch build - click build all
13. run winrelease2012.cmd (located in <checkout directory>windows\)

The release files can now be found in the directories Windows\release\vs2010 and Windows\release\vs2012
the VS20NN directories contains two sub directories:
files\ 
release\  

files\
 is an unpacked version, 
release\
contains a timestamped zip file with the same files as are found in the files\ directory
