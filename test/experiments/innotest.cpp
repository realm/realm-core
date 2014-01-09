#include <tightdb.hpp>

using namespace tightdb;

int main()
{
    //int pid = fork();
    srand(time(NULL));
    
    SharedGroup db("test2.tightdb");

    for (size_t round = 0; round < 10; ++round) {
    
        for (size_t i = 0; i < 1000000; ++i) {
            {
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
    
    /*if (pid > 0) { // parent
        int status = 0;
        wait(&status); // wait for child to complete
        
        printf("done 1\n");
    }
    else printf("done 2\n");*/
    
    return 0;
}
