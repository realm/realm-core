#include <realm/util/file.hpp>
#include "util/timer.hpp"
#include "util/benchmark_results.hpp"

using namespace realm;

constexpr size_t page_size = 4096;
constexpr size_t chunk_size = 8 * page_size;
constexpr size_t chunks_count = 25000;
constexpr size_t file_size = chunks_count * chunk_size;

constexpr char data[chunk_size] = {0};

static void benchmark_write(util::File& file, test_util::Timer& timer, test_util::BenchmarkResults results, const char* ident, const char* lead_text) {
    for (size_t i = 0; i < chunks_count; i++) {
        file.write(data);
        timer.reset();
        file.sync();
        results.submit(ident, timer);
    }
    results.finish(ident, lead_text, "runtime_secs");
}

static void benchmark_map(util::File& file, test_util::Timer& timer, test_util::BenchmarkResults results, const char* ident, const char* lead_text) {
    void* map = file.map(util::File::access_ReadWrite, file_size);
    char* destination = reinterpret_cast<char*>(map);
    for (size_t i = 0; i < chunks_count; i++) {
        memcpy(destination, data, chunk_size);
        timer.reset();
        util::File::sync_map(file.get_descriptor(), destination, chunk_size);
        results.submit(ident, timer);
        destination += chunk_size;
    }
    results.finish(ident, lead_text, "runtime_secs");
    util::File::unmap(map, file_size);
}

int main() {
    test_util::BenchmarkResults results(32, "benchmark-flush");
    test_util::Timer total_timer(test_util::Timer::type_UserTime);
    test_util::Timer chunks_timer(test_util::Timer::type_UserTime);

    util::File::AccessMode am = util::File::access_ReadWrite;
    util::File::CreateMode cm = util::File::create_Auto;
    int flags = util::File::flag_Trunc;

    {
        util::File file;
        file.open("benchmark.tmp", am, cm, flags);
        file.prealloc(file_size);
        total_timer.reset();
        benchmark_write(file, chunks_timer, results, "write_buffered", "buffered File::write");
        results.submit_single("write_buffered_total", "buffered File::write (total)", "runtime_secs", total_timer);
    }
    {
        util::File file;
        file.open("benchmark.tmp", am, cm, flags | util::File::flag_Direct);
        file.prealloc(file_size);
        total_timer.reset();
        benchmark_write(file, chunks_timer, results, "write_direct", "direct File::write");
        results.submit_single("write_direct_total", "direct File::write (total)", "runtime_secs", total_timer);
    }

    {
        util::File file;
        file.open("benchmark.tmp", am, cm, flags);
        file.prealloc(file_size);
        total_timer.reset();
        benchmark_map(file, chunks_timer, results, "map_buffered", "buffered File::map");
        results.submit_single("map_buffered_total", "buffered File::map (total)", "runtime_secs", total_timer);
    }
    {
        util::File file;
        file.open("benchmark.tmp", am, cm, flags | util::File::flag_Direct);
        file.prealloc(file_size);
        total_timer.reset();
        benchmark_map(file, chunks_timer, results, "map_direct", "direct File::map");
        results.submit_single("map_direct_total", "direct File::map (total)", "runtime_secs", total_timer);
    }
}