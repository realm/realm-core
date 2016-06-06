/*************************************************************************
 *
 * REALM CONFIDENTIAL
 * __________________
 *
 *  [2011] - [2015] Realm Inc
 *  All Rights Reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Realm Incorporated and its suppliers,
 * if any.  The intellectual and technical concepts contained
 * herein are proprietary to Realm Incorporated
 * and its suppliers and may be covered by U.S. and Foreign Patents,
 * patents in process, and are protected by trade secret or copyright law.
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from Realm Incorporated.
 *
 **************************************************************************/

#include <unistd.h>

#include <realm.hpp>

using namespace std;
using namespace realm;

int main(int argc, char* argv[])
{
    extern char* optarg;
    char* infilename;
    int ch;
    while ((ch = getopt(argc, argv, "i:")) != -1) {
        switch (ch) {
            case 'i':
                infilename = strdup(optarg);
                break;
            default:
                cerr << "Wrong arguments" << endl;
                exit(-1);
        }
    }

    SharedGroup sg(infilename, false, SharedGroup::durability_Full);
    sg.compact();
    sg.close();
}