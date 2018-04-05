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

#include <cstdlib>
#include <algorithm>
#include <iostream>

#include <realm/util/file.hpp>
#include <realm/group_shared.hpp>
#include <realm/history.hpp>
#include <realm/commit_log.hpp>
#include <realm/lang_bind_helper.hpp>

#include "../util/timer.hpp"
#include "../util/random.hpp"
#include "../util/benchmark_results.hpp"

using namespace realm;
using namespace realm::util;
using namespace realm::test_util;


namespace {

std::unique_ptr<Replication> make_history(std::string path)
{
    return make_client_history(path);
    //    return make_in_realm_history(path);
}


// Make a Realm of considerable size. Then perform as series of write
// transactions via one SharedGroup. At the same time (by the same thread)
// occasionally advance a read transaction via another SharedGroup. This will
// produce a situation with a varying number of concurrently locked snapshots.
class PeakFileSizeTask {
public:
    PeakFileSizeTask()
    {
        std::string path = "/tmp/benchmark-history-types.realm";
        util::File::try_remove(path);

        reader_history = make_history(path);
        reader_shared_group.reset(new DB(*reader_history));

        writer_history = make_history(path);
        writer_shared_group.reset(new DB(*writer_history));

        WriteTransaction wt(*writer_shared_group);
        TableRef table = wt.add_table("table");
        for (size_t i = 0; i < num_cols; ++i)
            table->add_column(type_Int, "");
        table->add_empty_row(num_rows);
        for (size_t i = 0; i < num_rows; ++i) {
            if (i % REALM_MAX_BPNODE_SIZE == 0) { // REALM_MAX_BPNODE_SIZE = 1000
                for (size_t j = 0; j < num_cols; ++j)
                    table->set_int(j, i, 65536L + long(i) + long(j));
            }
        }
        wt.commit();

        reader_shared_group->begin_read();
    }

    void run()
    {
        for (size_t i = 0; i < num_transactions; ++i) {
            if (i % max_num_locked_snapshots == 0)
                LangBindHelper::advance_read(*reader_shared_group);
            WriteTransaction wt(*writer_shared_group);
            TableRef table = wt.get_table("table");
            for (size_t j = 0; j < num_modifications; ++j) {
                size_t col_ndx = (j + i) % num_cols;
                size_t row_ndx = (size_t((double(num_rows - 1) / num_modifications - 1) * j) + i) % num_rows;
                table->set_int(col_ndx, row_ndx, 262144L + long(j) + long(i));
            }
            wt.commit();
        }
    }

private:
    const size_t num_cols = 8;
    const size_t num_rows = 10000;
    const size_t num_transactions = 10000;
    const size_t num_modifications = 20;
    const size_t max_num_locked_snapshots = 8;

    std::unique_ptr<Replication> reader_history;
    std::unique_ptr<DB> reader_shared_group;

    std::unique_ptr<Replication> writer_history;
    std::unique_ptr<DB> writer_shared_group;
};


class Task {
public:
    Task(int num_readers, bool grow)
        : m_num_readers(num_readers)
        , m_grow(grow)
    {
        std::string path = "/tmp/benchmark-history-types.realm";
        util::File::try_remove(path);

        reader_histories.reset(new std::unique_ptr<Replication>[ num_readers ]);
        reader_shared_groups.reset(new std::unique_ptr<DB>[ num_readers ]);

        for (int i = 0; i < num_readers; ++i) {
            reader_histories[i] = make_history(path);
            reader_shared_groups[i].reset(new DB(*reader_histories[i]));
        }

        writer_history = make_history(path);
        writer_shared_group.reset(new DB(*writer_history));

        WriteTransaction wt(*writer_shared_group);
        TableRef table = wt.add_table("table");
        table->add_column(type_Int, "i");
        if (!grow)
            table->add_empty_row();
        wt.commit();
    }

    void run()
    {
        for (int i = 0; i < 64; ++i) {
            int reader_ndx = i % m_num_readers;
            if (m_num_readers > 0) {
                reader_shared_groups[reader_ndx]->end_read();
                reader_shared_groups[reader_ndx]->begin_read();
            }
            WriteTransaction wt(*writer_shared_group);
            TableRef table = wt.get_table("table");
            if (m_grow) {
                table->add_empty_row();
                table->set_int(0, i, i);
            }
            else {
                table->set_int(0, 0, i);
            }
            wt.commit();
        }
    }

private:
    const int m_num_readers;
    const bool m_grow;

    std::unique_ptr<std::unique_ptr<Replication>[]> reader_histories;
    std::unique_ptr<std::unique_ptr<DB>[]> reader_shared_groups;

    std::unique_ptr<Replication> writer_history;
    std::unique_ptr<DB> writer_shared_group;
};

} // unnamed namespace


int main()
{
    int max_lead_text_size = 25;
    BenchmarkResults results(max_lead_text_size);

    Timer timer(Timer::type_UserTime);
    {
        /*
        for (int i = 0; i != 1; ++i) {
            PeakFileSizeTask task;
            timer.reset();
            task.run();
            results.submit("dummy", timer);
        }
        results.finish("dummy", "Dummy");
        */
        // No readers (no grow)
        for (int i = 0; i != 25; ++i) {
            Task task(0, false);
            timer.reset();
            task.run();
            results.submit("0_readers_no_grow", timer);
        }
        results.finish("0_readers_no_grow", "No readers (no grow)");

        // One reader (no grow)
        for (int i = 0; i != 25; ++i) {
            Task task(1, false);
            timer.reset();
            task.run();
            results.submit("1_readers_no_grow", timer);
        }
        results.finish("1_readers_no_grow", "One reader (no grow)");

        // Two readers (no grow)
        for (int i = 0; i != 25; ++i) {
            Task task(2, false);
            timer.reset();
            task.run();
            results.submit("2_readers_no_grow", timer);
        }
        results.finish("2_readers_no_grow", "Two readers (no grow)");

        // Five readers (no grow)
        for (int i = 0; i != 25; ++i) {
            Task task(5, false);
            timer.reset();
            task.run();
            results.submit("5_readers_no_grow", timer);
        }
        results.finish("5_readers_no_grow", "Five readers (no grow)");

        // Fifteen readers (no grow)
        for (int i = 0; i != 25; ++i) {
            Task task(15, false);
            timer.reset();
            task.run();
            results.submit("15_readers_no_grow", timer);
        }
        results.finish("15_readers_no_grow", "Fifteen readers (no grow)");

        // No readers (grow)
        for (int i = 0; i != 25; ++i) {
            Task task(0, true);
            timer.reset();
            task.run();
            results.submit("0_readers_grow", timer);
        }
        results.finish("0_readers_grow", "No readers (grow)");

        // One reader (grow)
        for (int i = 0; i != 25; ++i) {
            Task task(1, true);
            timer.reset();
            task.run();
            results.submit("1_readers_grow", timer);
        }
        results.finish("1_readers_grow", "One reader (grow)");

        // Two readers (grow)
        for (int i = 0; i != 25; ++i) {
            Task task(2, true);
            timer.reset();
            task.run();
            results.submit("2_readers_grow", timer);
        }
        results.finish("2_readers_grow", "Two readers (grow)");

        // Five readers (grow)
        for (int i = 0; i != 25; ++i) {
            Task task(5, true);
            timer.reset();
            task.run();
            results.submit("5_readers_grow", timer);
        }
        results.finish("5_readers_grow", "Five readers (grow)");

        // Fifteen readers (grow)
        for (int i = 0; i != 25; ++i) {
            Task task(15, true);
            timer.reset();
            task.run();
            results.submit("15_readers_grow", timer);
        }
        results.finish("15_readers_grow", "Fifteen readers (grow)");
    }
}
