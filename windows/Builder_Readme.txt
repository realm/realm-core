This directory contains scripts and code files used to collect and zip files for a windows relase.

Following is a short description of how to create a windows release of realm as c++ LIB binaries.

Following ths procedure, You will have accomplihed the following tasks for all combinations of :
VS2010 / VS2012
Debug / Release
32bit / 64bit

* Verified all compiler warnings
* Run unit tests
* Run visual leak detector
* Built static libraries

The end result will be two zipfiles, one for vs2010 and one for vs2012, containing a binary release,
consisting of LIB files, and header files.


You will need both Visual Studio 2010 and Visual Studio 2012 to build the release.

Warning : Following the instructions below, vs2012 will have changed the project files. 
Do not check these changed files back into github as VS2010 cannot read them correctly.

If You have access to many cores, or two computers you can do vs2010 on one computer in one repository
, and at the same time, start from vs2012. on another computer in another repository

1. open Realm.sln in VS2010

2. select Build->Batch Build. Unmark *all* , mark the 4 called 
project:  configuration: Platform
Realm   Debug      Win32
Realm   Debug      x64
Realm   Release    Win32
Realm   Release    x64
click clean

3. select Build->Batch Build, click Rebuild. Wait for the build process to finish.

4. Look at the bottom of Output. Verify that all 8 projects built, and that 0 failed

5. Look through Output, look for warnings.

6. For each warning, click on it to view the source. The source should have a note about the specific warning, 
otherwise a c++ developer will have to evaluate the warning and fix the problem or write a note

7. Assuming all warnigs are noted in the source, and all 8 projects build, continue..

8.
Select Debug in Solution Configurations drop down
Select Win32 in solution platform drops down
Click the green run arrow. after a while,
top5 timings will be shown. No unit tests should be listed as failed.
Press ENTER. A VLD info will flash by and the console
window will close. Inspect the output window inside VS2010, make sure
no leaks reported from Visual Leak Detector

9.
Select Debug in Solution Configurations drop down
Select x64 in solution platform drops down
Click the green run arrow. after a while,
top5 timings will be shown. No unit tests should be listed as failed.
Press ENTER. A VLD info will flash by and the console
window will close. Inspect the output window inside VS2010, make sure
no leaks reported from Visual Leak Detector

10.
Select Release in Solution Configurations drop down
Select x64 in solution platform drops down
Click the green run arrow. after a while,
top5 timings will be shown. No unit tests should be listed as failed.
Press ENTER. 

11.
Select Release in Solution Configurations drop down
Select win32 in solution platform drops down
Click the green run arrow. after a while,
top5 timings will be shown. No unit tests should be listed as failed.
Press ENTER. 


12. select Build->Batch Build. Unmark *all* , mark the 4 called 
project:  configuration: Platform
Realm   Static library, debug    Win32
Realm   Static library, debug    x64
Realm   Static library, release  Win32
Realm   Static library, release  x64
click clean

13. select Build->Batch build. Click Rebuild (wait until the build has finished)

14. Look at the bottom of Output. Verify that all 8 projects built, and that 0 failed

15. Look through Output, look for warnings.

16. For each warning, click on it to view the source. The source should have a note about the specific warning, 
otherwise a c++ developer will have to evaluate the warning and fix the problem or write a note
6. Assuming all warnigs are noted in the source, and all 8 projects build, continue..

17. right click "Solution 'Realm' (8 projects) in Solution Explorer

18. Select 'open folder in windows explorer'

19. in the opened folder, go to the windows directory

20. in the windows folder, double click on winrelease2010.cmd


***Visual studio 2012 build 

21. Open Realm.sln with visual studio 2012 . right click "Solution 'Realm' (8 projects) in Solution Explorer

22. select Update VC++ Projects

23. in the popup "Update VC++ Compiler and Libraries" click Update

24. wait while VS2012 updates the projects.

25. select Build->Batch Build. Unmark *all* , mark the 4 called 
project:  configuration: Platform
Realm   Debug      Win32
Realm   Debug      x64
Realm   Release    Win32
Realm   Release    x64
click clean

26. select Build->Batch Build, click Rebuild. Wait for the build process to finish.

27. Look at the bottom of Output. Verify that all 8 projects built, and that 0 failed

28. Look through Output, look for warnings.

29. For each warning, click on it to view the source. The source should have a note about the specific warning, 
otherwise a c++ developer will have to evaluate the warning and fix the problem or write a note

30. Assuming all warnigs are noted in the source, and all 8 projects build, continue..

31.
Select Debug in Solution Configurations drop down
Select Win32 in solution platform drops down
Click the green run arrow. after a while,
top5 timings will be shown. No unit tests should be listed as failed.
Press ENTER. A VLD info will flash by and the console
window will close. Inspect the output window inside VS2010, make sure
no leaks reported from Visual Leak Detector

32.
Select Debug in Solution Configurations drop down
Select x64 in solution platform drops down
Click the green run arrow. after a while,
top5 timings will be shown. No unit tests should be listed as failed.
Press ENTER. A VLD info will flash by and the console
window will close. Inspect the output window inside VS2010, make sure
no leaks reported from Visual Leak Detector

33.
Select Release in Solution Configurations drop down
Select x64 in solution platform drops down
Click the green run arrow. after a while,
top5 timings will be shown. No unit tests should be listed as failed.
Press ENTER. 

34.
Select Release in Solution Configurations drop down
Select win32 in solution platform drops down
Click the green run arrow. after a while,
top5 timings will be shown. No unit tests should be listed as failed.
Press ENTER. 


35. select Build->Batch Build. Unmark *all* , mark the 4 called 
project:  configuration: Platform
Realm   Static library, debug    Win32
Realm   Static library, debug    x64
Realm   Static library, release  Win32
Realm   Static library, release  x64
click clean

36. select Build->Batch build. Click Rebuild (wait until the build has finished)

37. Look at the bottom of Output. Verify that all 8 projects built, and that 0 failed

38. Look through Output, look for warnings.

39. For each warning, click on it to view the source. The source should have a note about the specific warning, 
otherwise a c++ developer will have to evaluate the warning and fix the problem or write a note

40. Assuming all warnigs are noted in the source, and all 8 projects build, continue..

41. right click "Solution 'Realm' (8 projects) in Solution Explorer

42. Select 'open folder in windows explorer'

43. in the opened folder, go to the windows directory

44. in the windows folder, double click on winrelease2012.cmd




The release files can now be found in the directories Windows\release\vs2010 and Windows\release\vs2012
the VS20NN directories contains two sub directories:
files\ 
release\  

files\
 is an unpacked version, 
release\
contains a timestamped zip file with the same files as are found in the files\ directory
