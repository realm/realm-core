#include <stdio.h>
#include <stdlib.h>
#include <realm.hpp>

using namespace realm;

REALM_TABLE_7(IndexTable,
                s1, String,
                n1, Int,
                n2, Int,
                n3, Int,
                n4, Int,
                n5, Int,
                s2, String)



int main(int argc, char *argv[]) {
    Group *g = new Group();

    BasicTableRef<IndexTable> t = g->add_table<IndexTable>("test");
    srandom(1);
    printf("Adding rows\n");
    for(size_t i=0; i<10000000; ++i) {
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
    t->column().s1.set_index();
    printf("Writing to disk\n");
    g->write("test.realm");
}
