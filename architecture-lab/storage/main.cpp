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
    const int limit = 3000000;
    const char* fields = "uifdtruuuuUTs";

    Db& db = Db::create("testing.core2");

    std::chrono::time_point<std::chrono::high_resolution_clock> start, end;
    Snapshot& ss = db.create_changes();
    Table t = ss.create_table(fields);
    Field<uint64_t> field_a = ss.get_field<uint64_t>(t, 0);
    Field<int64_t> field_b = ss.get_field<int64_t>(t, 1);
    Field<float> field_c = ss.get_field<float>(t, 2);
    Field<double> field_d = ss.get_field<double>(t, 3);
    Field<Table> field_e = ss.get_field<Table>(t, 4);
    Field<Row> field_f = ss.get_field<Row>(t, 5);
    Field<uint64_t> field_x0 = ss.get_field<uint64_t>(t,6);
    Field<uint64_t> field_x1 = ss.get_field<uint64_t>(t,7);
    Field<uint64_t> field_x2 = ss.get_field<uint64_t>(t,8);
    Field<uint64_t> field_x3 = ss.get_field<uint64_t>(t,9);
    Field<List<uint64_t>> field_y = ss.get_field<List<uint64_t>>(t,10);
    Field<List<Table>> field_t = ss.get_field<List<Table>>(t,11);
    Field<String> field_s = ss.get_field<String>(t,12);

    std::cout << "inserting " << limit << " keys..." << std::flush;
    start = std::chrono::high_resolution_clock::now();
    for (uint64_t key = 0; key < limit; key++) {
        ss.insert(t, {key << 1});
        if (!ss.exists(t, {key << 1}))
            std::cout << "Missing a key that should be there: " << key << std::endl;
    }
    end = std::chrono::high_resolution_clock::now();
    std::chrono::nanoseconds ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end-start) / limit;
    std::cout << "   ...done in " << ns.count() << " nsecs/key" << std::endl;

    // FIXME: Just a happy go lucky test of lists - expand and put later
    Object o = ss.get(t, {2});
    ListAccessor<uint64_t> la = o(field_y);
    uint64_t list_sz = la.get_size();
    assert(list_sz == 0);
    la.set_size(10);
    list_sz = la.get_size();
    assert(list_sz == 10);
    for (unsigned j=0; j<10; ++j) la.wr(j, j*j+j);
    for (unsigned j=0; j<10; ++j) assert(la.rd(j) == j*j+j);
    ListAccessor<Table> ta = o(field_t);
    ta.set_size(1);
    ta.wr(0, t);
    o.set(field_s, "dette er en streng");
    std::string s = o(field_s);
    assert(s == "dette er en streng");

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
        assert(ss.get(t, {key << 1})(field_a) == 0);
    }
    end = std::chrono::high_resolution_clock::now();
    ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end-start) / limit;
    std::cout << "   ...done in " << ns.count() << " nsecs/key" << std::endl;
    ss.print_stat(std::cout);

    std::cout << std::endl << "setting values for " << limit << " keys..." << std::flush;
    start = std::chrono::high_resolution_clock::now();
    for (uint64_t key = 0; key < limit; key++) {
        ss.change(t, {key << 1}).set(field_a, key);
    }
    end = std::chrono::high_resolution_clock::now();
    ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end-start) / limit;
    std::cout << "   ...done in " << ns.count() << " nsecs/key" << std::endl;

    std::cout << "checking values for " << limit << " keys..." << std::flush;
    start = std::chrono::high_resolution_clock::now();
    for (uint64_t key = 0; key < limit; key++) {
        assert(ss.get(t, {key << 1})(field_a) == key);
    }
    end = std::chrono::high_resolution_clock::now();
    ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end-start) / limit;
    std::cout << "   ...done in " << ns.count() << " nsecs/key" << std::endl;

    std::cout << "Multiple writes to same object and field" << std::flush;
    start = std::chrono::high_resolution_clock::now();
    for (uint64_t key = 0; key < 100000; key++) {
        ss.change(t, {0}).set(field_b, -1L);
    }
    end = std::chrono::high_resolution_clock::now();
    ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end-start) / 100000;
    std::cout << "   ...done in " << ns.count() << " nsecs/write" << std::endl;

    std::cout << "Multiple reads from same object and field" << std::flush;
    start = std::chrono::high_resolution_clock::now();
    for (uint64_t key = 0; key < 100000; key++) {
        assert(ss.get(t, {0})(field_b) == -1);
    }
    end = std::chrono::high_resolution_clock::now();
    ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end-start) / 100000;
    std::cout << "   ...done in " << ns.count() << " nsecs/read" << std::endl;
    std::cout << "Preparing, please hold..." << std::flush;
    // priming (to create all relevant arrays)
    int count = 0; // count all entries satisfying search criteria used later
    int count2 = 0;
    auto query = [&](Object& o) 
        { 
            return o(field_x0) < 1000 && o(field_x1) < 1000 && o(field_x2) < 1000 && o(field_x3) < 1000;
        };

    for (uint64_t key = 0; key < limit; key ++) {
        auto o = ss.change(t, { key << 1 });
        o.set(field_b, 42L);
        o.set(field_c, float(1.0));
        o.set(field_d, 1.0);
        o.set(field_e, {key});
        o.set(field_f, {key});
        uint64_t a = rand() % 10000L;
        uint64_t b = rand() % 10000L;
        uint64_t c = rand() % 10000L;
        uint64_t d = rand() % 10000L;
        o.set(field_x0, a);
        o.set(field_x1, b);
        o.set(field_x2, c);
        o.set(field_x3, d);
        assert(o(field_x0) == a);
        assert(o(field_x1) == b);
        assert(o(field_x2) == c);
        assert(o(field_x3) == d);
        if (a < 1000 && b < 1000 && c < 1000 && d < 1000) count++;
        if (query(o)) count2++;
    }
    std::cout << "Later search should find " << count << " elements" << std::endl;
    assert(count == count2);

    std::cout << "Writing to different fields of same object" << std::flush;
    start = std::chrono::high_resolution_clock::now();
    for (uint64_t key = 0; key < limit; key++) {
        auto o = ss.change(t, { key << 1 });
        o.set(field_b, 1L);
        o.set(field_c, float(0.6 * key));
        o.set(field_d, 0.7 * key);
        o.set(field_e, {key + 12});
        o.set(field_f, {key + 43});
    }
    end = std::chrono::high_resolution_clock::now();
    ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end-start) / 5 / limit;
    std::cout << "   ...done in " << ns.count() << " nsecs/write" << std::endl;

    count2 = 0;
    std::cout << "Reading from multiple fields of same object" << std::flush;
    start = std::chrono::high_resolution_clock::now();
    for (uint64_t key = 0; key < limit; key++) {
        auto o = ss.get(t, {key << 1});
        assert(o(field_b) == 1L);
        assert(o(field_c) - float(0.6 * key) < 0.001);
        assert(o(field_d) - 0.7 * key < 0.00001);
        assert(o(field_e).key == key + 12);
        assert(o(field_f).key == key + 43);
        if (query(o)) count2++;
    }
    end = std::chrono::high_resolution_clock::now();
    ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end-start) / 9 / limit;
    std::cout << "   ...done in " << ns.count() << " nsecs/read";
    std::cout << "   ...with search finding " << count2 << std::endl << std::endl;

    ss.print_stat(std::cout);
    std::cout << "Committing to stable storage" << std::flush;
    start = std::chrono::high_resolution_clock::now();
    db.commit(std::move(ss));
    end = std::chrono::high_resolution_clock::now();
    std::chrono::milliseconds ms = std::chrono::duration_cast<std::chrono::milliseconds>(end-start);
    std::cout << "   ...done in " << ms.count() << " msecs" << std::endl << std::endl;

    const Snapshot& s2 = db.open_snapshot();
    {
        Object o = s2.get(t, {2});
        ListAccessor<uint64_t> la = o(field_y);
        uint64_t list_sz = la.get_size();
        assert(list_sz == 10);
        for (unsigned j=0; j<10; ++j) assert(la.rd(j) == j*j+j);
        std::string s = o(field_s);
        assert(s == "dette er en streng");
    }
    std::cout << "checking values (after commit, from file) for " << limit << " keys..." << std::flush;
    start = std::chrono::high_resolution_clock::now();
    for (uint64_t key = 0; key < limit; key++) {
        assert(s2.get(t, {key << 1})(field_a) == key);
    }
    end = std::chrono::high_resolution_clock::now();
    ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end-start) / limit;
    std::cout << "   ...done in " << ns.count() << " nsecs/key" << std::endl;
    db.release(std::move(s2));

    Snapshot& s3 = db.create_changes();
    {
        Object o = s3.get(t, {2});
        ListAccessor<uint64_t> la = o(field_y);
        uint64_t list_sz = la.get_size();
        assert(list_sz == 10);
        for (unsigned j=0; j<10; ++j) assert(la.rd(j) == j*j+j);
        for (unsigned j=0; j<10; ++j) la.wr(j, j*j-j);
        for (unsigned j=0; j<10; ++j) assert(la.rd(j) == j*j-j);
    }
    std::cout << "setting values for " << limit << " keys..." << std::flush;
    start = std::chrono::high_resolution_clock::now();
    for (uint64_t key = 0; key < limit; key++) {
        s3.change(t, {key << 1}).set(field_a, key + 47);
    }
    end = std::chrono::high_resolution_clock::now();
    ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end-start) / limit;
    std::cout << "   ...done in " << ns.count() << " nsecs/key" << std::endl << std::endl;

    ss.print_stat(std::cout);
    std::cout << "Committing to stable storage" << std::flush;
    start = std::chrono::high_resolution_clock::now();
    db.commit(std::move(s3));
    end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration_cast<std::chrono::milliseconds>(end-start);
    std::cout << "   ...done in " << ms.count() << " msecs" << std::endl << std::endl;

    const Snapshot& s4 = db.open_snapshot();
    {
        Object o = s4.get(t, {2});
        ListAccessor<uint64_t> la = o(field_y);
        uint64_t list_sz = la.get_size();
        assert(list_sz == 10);
        for (unsigned j=0; j<10; ++j) assert(la.rd(j) == j*j-j);
    }
    std::cout << "checking values (after commit, from file) for " << limit << " keys..." << std::flush;
    start = std::chrono::high_resolution_clock::now();
    for (uint64_t key = 0; key < limit; key++) {
        assert(s4.get(t, {key << 1})(field_a) == key + 47);
    }
    end = std::chrono::high_resolution_clock::now();
    ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end-start) / limit;
    std::cout << "   ...done in " << ns.count() << " nsecs/key" << std::endl;

    count2 = 0;
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
