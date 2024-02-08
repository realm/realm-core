#include <ctime>
#include <iostream>
#include <fstream>
#include <cstring>
#include <deque>
#include <mutex>
#include <condition_variable>
#include <realm.hpp>
#include <realm/cluster.hpp>
#include <realm/db.hpp>
#include <realm/history.hpp>

using namespace realm;

void import(const char* filename)
{
    DBOptions options;
    auto db = DB::create(make_in_realm_history(), filename);
    auto tr = db->start_write();
    auto t = tr->get_table("Hits");
    auto col_keys = t->get_column_keys();
    auto time_start = std::chrono::high_resolution_clock::now();
    auto time_end = time_start;
    {
        std::cout << std::endl << "count of AdvEngineID <> 0" << std::endl;
        time_start = std::chrono::high_resolution_clock::now();
        size_t q;
        for (int i = 0; i < 10; ++i) {
            auto k = t->get_column_key("AdvEngineID");
            q = t->where().not_equal(k, 0).count();
        }
        time_end = std::chrono::high_resolution_clock::now();
        std::cout << "result = " << q << " in "
                  << std::chrono::duration_cast<std::chrono::milliseconds>(time_end - time_start).count() << " msecs"
                  << std::endl;
    }
    {
        std::cout << std::endl << "Query result for AdvEngineID <> 0" << std::endl;
        time_start = std::chrono::high_resolution_clock::now();
        TableView q;
        for (int i = 0; i < 10; ++i) {
            auto k = t->get_column_key("AdvEngineID");
            q = t->where().not_equal(k, 0).find_all();
        }
        time_end = std::chrono::high_resolution_clock::now();
        std::cout << "result with size " << q.size() << " in "
                  << std::chrono::duration_cast<std::chrono::milliseconds>(time_end - time_start).count() << " msecs"
                  << std::endl;
        time_start = std::chrono::high_resolution_clock::now();
        size_t count = 0;
        for (int i = 0; i < 10; ++i) {
            auto limit = q.size();
            auto k = t->get_column_key("AdvEngineID");
            for (size_t i = 0; i < limit; ++i) {
                count += q[i].get<Int>(k);
            }
        }
        time_end = std::chrono::high_resolution_clock::now();
        std::cout << "Iterating over result to get count " << count << " in "
                  << std::chrono::duration_cast<std::chrono::milliseconds>(time_end - time_start).count() << " msecs"
                  << std::endl;
    }
    {
        std::cout << std::endl << "Max of EventDate" << std::endl;
        time_start = std::chrono::high_resolution_clock::now();
        Mixed q;
        for (int i = 0; i < 10; ++i) {
            auto k = t->get_column_key("EventDate");
            q = *(t->max(k));
        }
        // auto q = t->where().not_equal(k, 0).count();
        time_end = std::chrono::high_resolution_clock::now();
        std::cout << "result = " << q << " in "
                  << std::chrono::duration_cast<std::chrono::milliseconds>(time_end - time_start).count() << " msecs"
                  << std::endl;
    }
}

int main(int argc, const char* argv[])
{
    if (argc == 1) {
        import("./hits.realm");
    }
    if (argc == 2) {
        import(argv[1]);
    }
}
