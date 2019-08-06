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

#include <iostream>
#include <chrono>
#include <cassert>
#include <cstring>
#include <thread>
#include <vector>

#include "db.hpp"

int main(int argc, char* argv[]) {
    const int limit = 10000;
    const char* fields = "uuuu";

    Db& db = Db::create("perf.core2");

    std::chrono::time_point<std::chrono::high_resolution_clock> start, end;
    Snapshot& ss = db.create_changes();
    Table t = ss.create_table(fields);
    Field<uint64_t> field_x0 = ss.get_field<uint64_t>(t,0);
    Field<uint64_t> field_x1 = ss.get_field<uint64_t>(t,1);
    Field<uint64_t> field_x2 = ss.get_field<uint64_t>(t,2);
    Field<uint64_t> field_x3 = ss.get_field<uint64_t>(t,3);

    std::cout << "inserting " << limit << " keys..." << std::flush;
    start = std::chrono::high_resolution_clock::now();
    for (uint64_t key = 0; key < limit; key++) {
        ss.insert(t, {key << 1});
    }
    end = std::chrono::high_resolution_clock::now();
    std::chrono::nanoseconds ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end-start) / limit;
    std::cout << "   ...done in " << ns.count() << " nsecs/key" << std::endl;
/*
    std::cout << "validating " << limit << " keys not present..." << std::flush;
    start = std::chrono::high_resolution_clock::now();
    for (uint64_t key = 0; key < limit; key++) {
        if (ss.exists(t, { (key << 1) + 1} ))
            std::cout << "Found a key that was never inserted: " << key << std::endl;
    }
    end = std::chrono::high_resolution_clock::now();
    ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end-start) / limit;
    std::cout << "   ...done in " << ns.count() << " nsecs/key" << std::endl;
    std::cout << "validating " << limit << " keys present..." << std::flush;
    start = std::chrono::high_resolution_clock::now();
    for (uint64_t key = 0; key < limit; key++) {
        if (!ss.exists(t, {key << 1}))
            std::cout << "Missing a key that should be there: " << key << std::endl;
    }
    end = std::chrono::high_resolution_clock::now();
    ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end-start) / limit;
    std::cout << "   ...done in " << ns.count() << " nsecs/key" << std::endl;

    std::cout << "checking empty/zero default values for " << limit << " keys..." << std::flush;
    start = std::chrono::high_resolution_clock::now();
    for (uint64_t key = 0; key < limit; key++) {
        assert(ss.get(t, {key << 1})(field_x0) == 0);
    }
    end = std::chrono::high_resolution_clock::now();
    ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end-start) / limit;
    std::cout << "   ...done in " << ns.count() << " nsecs/key" << std::endl;
    ss.print_stat(std::cout);
*/
    std::cout << std::endl << "setting values for later (4 random values/object) " << limit << " keys..." << std::flush;
    start = std::chrono::high_resolution_clock::now();

    for (uint64_t key = 0; key < limit; key ++) {
        auto o = ss.change(t, { key << 1 });
        uint64_t a = rand() % 10000L;
        uint64_t b = rand() % 10000L;
        uint64_t c = rand() % 10000L;
        uint64_t d = rand() % 10000L;
        o.set(field_x0, a);
        o.set(field_x1, b);
        o.set(field_x2, c);
        o.set(field_x3, d);
    }

    end = std::chrono::high_resolution_clock::now();
    ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end-start) / limit;
    std::cout << "   ...done in " << ns.count() << " nsecs/key" << std::endl;

    std::cout << "first access " << std::flush;
    int sum = 0;
    start = std::chrono::high_resolution_clock::now();
    for (uint64_t key = 0; key < limit; key++) {
        auto o = ss.get(t, {key << 1});
        sum += o(field_x0);
    }
    end = std::chrono::high_resolution_clock::now();
    ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end-start) / limit;
    std::cout << "   ...done in " << ns.count() << " nsecs/query" << std::endl;
    auto baseline = ns;

    std::cout << "2nd access, same field " << std::flush;
    sum = 0;
    start = std::chrono::high_resolution_clock::now();
    for (uint64_t key = 0; key < limit; key++) {
        auto o = ss.get(t, {key << 1});
        sum += o(field_x0);
        sum += o(field_x0);
    }
    end = std::chrono::high_resolution_clock::now();
    ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end-start) / limit;
    std::cout << "   ...done in " << (ns - baseline).count() << " nsecs/query" << std::endl;

    std::cout << "2nd access, other field " << std::flush;
    sum = 0;
    start = std::chrono::high_resolution_clock::now();
    for (uint64_t key = 0; key < limit; key++) {
        auto o = ss.get(t, {key << 1});
        sum += o(field_x0);
        sum += o(field_x1);
    }
    end = std::chrono::high_resolution_clock::now();
    ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end-start) / limit;
    std::cout << "   ...done in " << (ns - baseline).count() << " nsecs/query" << std::endl;

    int count = 0; // count all entries satisfying search criteria used later
    auto query = [&](Object& o) 
        { 
            return o(field_x0) < 1000 && o(field_x1) < 1000 && o(field_x2) < 1000 && o(field_x3) < 1000;
        };

    std::cout << "Querying" << std::flush;
    start = std::chrono::high_resolution_clock::now();
    for (uint64_t key = 0; key < limit; key++) {
        auto o = ss.get(t, {key << 1});
        if (query(o)) count++;
    }
    end = std::chrono::high_resolution_clock::now();
    ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end-start) / limit;
    std::cout << "   ...done in " << ns.count() << " nsecs/query";
    std::cout << "   ...with search finding " << count << std::endl << std::endl;

    ss.print_stat(std::cout);
    std::cout << "Committing to stable storage" << std::flush;
    start = std::chrono::high_resolution_clock::now();
    db.commit(std::move(ss));
    end = std::chrono::high_resolution_clock::now();
    std::chrono::milliseconds ms = std::chrono::duration_cast<std::chrono::milliseconds>(end-start);
    std::cout << "   ...done in " << ms.count() << " msecs" << std::endl << std::endl;

    const Snapshot& s4 = db.open_snapshot();
    int count2 = 0;
    std::cout << "Searching in key order (4 fields) for " << limit << " keys..." << std::flush;
    start = std::chrono::high_resolution_clock::now();
    for (uint64_t key = 0; key < limit; key++) {
        auto o = s4.get(t, {key << 1});
        if (query(o)) count2++;
    }
    end = std::chrono::high_resolution_clock::now();
    ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end-start) / limit;
    std::cout << "   ...found " << count2 << " elements in " << ns.count() << " nsecs/element" << std::endl;

    auto job = [&](int partitions, int partition_number, std::vector<Row>* results) 
        {
            s4.for_each_partition(partitions, partition_number, t, [&](Object& o) {
                    if (query(o)) {
                        results->push_back(o.r);
                    }
                });
        };

    std::cout << "searching with for_each (4 fields) for " << limit << " keys..." << std::flush;
    start = std::chrono::high_resolution_clock::now();
    std::vector<Row> results;
    job(1,0,&results);
    end = std::chrono::high_resolution_clock::now();
    ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end-start) / limit;
    std::cout << "   ... found " << results.size() << " elements in " << ns.count() << " nsecs/element" << std::endl;

    std::cout << "searching in parallel (4 threads) for " << limit << " keys..." << std::flush;
    // 'cause we can! Due to thread-start/join overhead, you need a large table
    // to see any gain. The tradeoff will be different, if you have a pool of
    // worker threads already created and waiting.
    {
        std::vector<Row> res0;
        std::vector<Row> res1;
        std::vector<Row> res2;
        std::vector<Row> res3;
        start = std::chrono::high_resolution_clock::now();
        std::thread T0(job, 4, 0, &res0);
        std::thread T1(job, 4, 1, &res1);
        std::thread T2(job, 4, 2, &res2);
        std::thread T3(job, 4, 3, &res3);
        T0.join();
        T1.join();
        T2.join();
        T3.join();
        end = std::chrono::high_resolution_clock::now();
        ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end-start) / limit;
        count2 = res0.size() + res1.size() + res2.size() + res3.size();
        std::cout << "   ...finding " << count2 << " elements in " << ns.count() << " nsecs/element" << std::endl;
    }
    db.release(std::move(s4));

    Snapshot& s5 = db.create_changes();
    auto job2 = [&](std::vector<Row>* work_to_do) {
        int limit = work_to_do->size();
        for (int i = 0; i < limit; ++i) {
            s5.change(t, (*work_to_do)[i]).set(field_x1, 1000UL);
        }
    };

    std::cout << "Changing all objects found " << std::flush;
    start = std::chrono::high_resolution_clock::now();
    job2(&results);
    end = std::chrono::high_resolution_clock::now();
    ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end-start) / count;
    std::cout << "   ...done in " << ns.count() << " nsecs/element" << std::endl << std::endl;

    ss.print_stat(std::cout);
    std::cout << "Committing to stable storage" << std::flush;
    start = std::chrono::high_resolution_clock::now();
    db.commit(std::move(s5));
    end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration_cast<std::chrono::milliseconds>(end-start);
    std::cout << "   ...done in " << ms.count() << " msecs" << std::endl << std::endl;

}
