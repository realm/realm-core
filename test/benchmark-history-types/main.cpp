#include <cstdlib>
#include <algorithm>
#include <iostream>

#include <realm/util/file.hpp>
#include <realm/group_shared.hpp>
#include <realm/history.hpp>
#include <realm/commit_log.hpp>

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


class Task {
public:
    Task(int num_readers, bool grow):
        m_num_readers(num_readers),
        m_grow(grow)
    {
        std::string path = "/tmp/benchmark-history-types.realm";
        util::File::try_remove(path);

        reader_histories.reset(new std::unique_ptr<Replication>[num_readers]);
        reader_shared_groups.reset(new std::unique_ptr<SharedGroup>[num_readers]);

        for (int i = 0; i < num_readers; ++i) {
            reader_histories[i] = make_history(path);
            reader_shared_groups[i].reset(new SharedGroup(*reader_histories[i]));
        }

        writer_history = make_history(path);
        writer_shared_group.reset(new SharedGroup(*writer_history));

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
    std::unique_ptr<std::unique_ptr<SharedGroup>[]> reader_shared_groups;

    std::unique_ptr<Replication> writer_history;
    std::unique_ptr<SharedGroup> writer_shared_group;
};

} // unnamed namespace


int main()
{
    int max_lead_text_size = 25;
    BenchmarkResults results(max_lead_text_size);

    Timer timer(Timer::type_UserTime);
    {
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
