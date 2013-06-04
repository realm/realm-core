#define NOMINMAX

#include <cstring>
#include <iostream>

#include <UnitTest++.h>
#include <TestReporter.h> // Part of UnitTest++
#include <tightdb.hpp>
#include <tightdb/utilities.hpp>

#include <../importer.hpp>

using namespace std;
using namespace UnitTest;
using namespace tightdb;


int main(int argc, char* argv[])
{
    Importer importer;
	tightdb::Table table;

	//***************************************
	
	//	Prototype! Has only been tested with flight data cvs files!

	// And the code is really messy. To be cleaned, etc

	//***************************************


	// Supports: 
	//		Newline inside data fields (yay!)
	//		double-quoted and non-quoted fields, and these can be mixed arbitrarely
	//		double-quotes inside data field
	//		*nix + Windows line feed
	//		TightDB types String, Integer, Float and Double
	//		Auto detection of float precision to prioritize Float over Double
	//		Auto detection of header and naming of TightDB columns accordingly

	// Arguments to import_csv():

    // null_to_0 imports value rows as TightDB value types (Integer, Float or Double) even though they contain empty
	// strings (null / ""). Else they are converted to String

	// type_detection_rows tells how many rows to read before analyzing data types (to see if numeric rows are really
	// numeric everywhere, and not strings that happen to just mostly contain numeric characters

	importer.import_csv("d:/csv/perf.csv", table, true, 1000);



}
