#include <cstring>
#include <iostream>
#include <UnitTest++.h>
#include <tightdb/utilities.hpp>
#include <tightdb.hpp>
#include <tightdb/tightdb_nmmintrin.h>
#if defined(_MSC_VER) && defined(_DEBUG)
//    #include "C:\\Program Files (x86)\\Visual Leak Detector\\include\\vld.h"
#endif

using namespace std;
using namespace tightdb;

TIGHTDB_TABLE_1(Courses,
                Name, String)

TIGHTDB_TABLE_2(Students,
                Name, String,
                Attends, Subtable<Courses>)

int main(int argc, char* argv[])
{
    Students students;
    Courses courses;

    students.add();    
    students[0].Name = "Peter";
    students[0].Attends->add("Physics 101");
    students[0].Attends->add("English 503");
    
    students.add();    
    students[1].Name = "Alice";
    students[1].Attends->add("English 503");
    
    students.add();    
    students[2].Name = "Bob";
    students[2].Attends->add("Chemistry Intro");
    students[2].Attends->add("Physics 101");
    
    //Students::Query query = students.where();




//    s1.add("Peter", );

//    s1.add("Peter", 

    

}