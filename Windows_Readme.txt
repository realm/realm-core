This is a short description of how to create a windows release of tightdb as c++ LIB binaries.

You will need both Visual Studio 2010 and Visual Studio 2012 to build the release.
After the build process has ended, vs2012 will have changed the project files. Do not check these changes
back into github as VS2010 cannot read them correctly.


1. open tightDB.sln in VS2010
2. select Build->Batch Build
3. build->batch build - unmark all , mark the 4 called tightDB static libraray
4. click clean
5. build->batch build - click build all
6. run winrelease2010.cmd (located in the same dir. as tightDB.sln  remember to press ENTER a few times until the script ends)
7. close VS2010
8. open tightDB.sln in VS2012
9. right click "Solution 'TightDB' (8 projects) in Solution Explorer
10. select Update VC++ Projects
11. in the popup "Update VC++ Compiler and Libraries" click Update
12. wait while VS2012 updates the projects.
13. select Build->Batch Build
14. build->batch build - unmark all , mark the 4 called tightDB static libraray
15. click clean
16. build->batch build - click build all
17. run winrelease2012.cmd (located in the same dir. as tightDB.sln  remember to press ENTER a few times until the script ends)

The release files can now be found in the dirs release\vs2010 and release\vs2012
the directories contains two sub directories files\ and release\  files is a unpacked version, release\contains a 
timestamped zip file with the same files as are found in t
he files\ directory