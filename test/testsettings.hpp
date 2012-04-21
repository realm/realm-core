#ifndef TESTSETTINGS_H
#define TESTSETTINGS_H

#ifndef TEST_DURATION
	#define TEST_DURATION 0	   // Only brief unit tests. < 1 sec
	//#define TEST_DURATION 1  // All unit tests, plus monkey tests. ~1 minute 
	//#define TEST_DURATION 2  // Same as 2, but longer monkey tests. 8 minutes
	//#define TEST_DURATION 3
#endif

#endif