#include <tightdb.hpp>
#include <gperftools/profiler.h>

using namespace tightdb;

int main()
{
    //int pid = fork();
    srand(time(NULL));
    const int reads_per_write = 0; // 500000;
    ProfilerStart("gnyf.prof");
    SharedGroup db("test2.tightdb");

    for (size_t round = 0; round < 20; ++round) {
    
        for (size_t i = 0; i < 1000000; ++i) {
            if (reads_per_write != 0 && (i % reads_per_write) == 0)
            {
                WriteTransaction trx(db);
            
                TableRef t = trx.get_table("test");
                
                size_t key = rand() % 1000000;
                size_t ndx = t->lower_bound_int(0, key);
            
                StringData str = t->get_string(1, ndx);
                t->set_string(1, ndx, str);
                trx.commit();
//                const char* s = str.data();         
            }
            else {
                ReadTransaction trx(db);
            
                ConstTableRef t = trx.get_table("test");
                
                size_t key = rand() % 1000000;
                size_t ndx = t->lower_bound_int(0, key);
            
                StringData str = t->get_string(1, ndx);
                const char* s = str.data();         
            }
        }
    
        printf("round %d done\n", (int)round);
    }
    ProfilerStop();
    /*if (pid > 0) { // parent
        int status = 0;
        wait(&status); // wait for child to complete
        
        printf("done 1\n");
    }
    else printf("done 2\n");*/
    
    return 0;
}
