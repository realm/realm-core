/*************************************************************************
 *
 * Copyright 2016 Realm Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **************************************************************************/

#include <cstdio>
#include <cstdlib>
#include <realm.hpp>

using namespace realm;

REALM_TABLE_7(IndexTable, s1, String, n1, Int, n2, Int, n3, Int, n4, Int, n5, Int, s2, String)


int main()
{
    Group* g = new Group();

    BasicTableRef<IndexTable> t = g->add_table<IndexTable>("test");
    srandom(1);
    printf("Adding rows\n");
    for (size_t i = 0; i < 10000000; ++i) {
        long n1 = random() % 1000;
        long n2 = random() % 1000;
        long n3 = random() % 1000;
        long n4 = random() % 1000;
        long n5 = random() % 1000;
        char s1[512];
        sprintf(s1, "%ldHello%ld", n1, n2);
        char s2[512];
        sprintf(s2, "%ldWorld%ld", n3, n4);
        t->add(s1, n1, n2, n3, n4, n5, s2);
        if (i % 50000 == 0) {
            printf("(%ld) ", i);
        }
    }
    printf("\nOptimizing\n");
    t->optimize();
    printf("Creating index\n");
    t->column().s1.add_search_index();
    printf("Writing to disk\n");
    g->write("test.realm");
}
