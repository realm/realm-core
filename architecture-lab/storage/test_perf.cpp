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
#include <iomanip>
#include <sstream>
#include <string>
#include <fstream>

#include <chrono>
#include <cassert>
#include <cstring>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <vector>
#include <list>
#include <unordered_map>

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>

#include "db.hpp"

#define CHUNK_SIZE 10

struct chunk {
    uint16_t symbols[CHUNK_SIZE];
    int prefix_index = -1;
    bool operator==(const chunk& a2) const
    {
        return memcmp(symbols, a2.symbols, 2 * CHUNK_SIZE) == 0 && prefix_index == a2.prefix_index;
    };
};
template <>
struct std::hash<std::vector<uint16_t>> {
    std::size_t operator()(const std::vector<uint16_t>& c) const noexcept
    {
        auto seed = c.size();
        for (auto& x : c) {
            seed = (seed + 3) * (x + 7);
        }
        return seed;
    }
};

template <>
struct std::hash<chunk> {
    std::size_t operator()(chunk const& c) const noexcept
    {
        std::size_t ret = (uint64_t)c.symbols[0] << 48;
        ret ^= (uint64_t)c.symbols[1] << 32;
        ret ^= (uint64_t)c.symbols[2] << 16;
        ret ^= (uint64_t)c.symbols[3];
        ret ^= (uint64_t)c.symbols[4] << 48;
        ret ^= (uint64_t)c.symbols[5] << 32;
        ret ^= (uint64_t)c.symbols[6] << 16;
        ret ^= (uint64_t)c.symbols[7];
        ret ^= (uint64_t)c.symbols[8] << 48;
        ret ^= (uint64_t)c.symbols[9] << 32;
#if 0
        std::size_t ret = (unsigned long)c.chars[0] << 56;
        ret ^= (unsigned long)c.chars[1] << 48;
        ret ^= (unsigned long)c.chars[2] << 40;
        ret ^= (unsigned long)c.chars[3] << 32;
#if (CHUNK_SIZE >= 12)
        ret ^= (unsigned long)c.chars[4] << 24;
        ret ^= (unsigned long)c.chars[5] << 16;
        ret ^= (unsigned long)c.chars[6] << 8;
        ret ^= (unsigned long)c.chars[7];
        ret ^= (unsigned long)c.chars[8] << 56;
        ret ^= (unsigned long)c.chars[9] << 48;
        ret ^= (unsigned long)c.chars[10] << 40;
        ret ^= (unsigned long)c.chars[11] << 32;
#endif
#if (CHUNK_SIZE >= 20)
        ret ^= (unsigned long)c.chars[12] << 24;
        ret ^= (unsigned long)c.chars[13] << 16;
        ret ^= (unsigned long)c.chars[14] << 8;
        ret ^= (unsigned long)c.chars[15];
        ret ^= (unsigned long)c.chars[16] << 56;
        ret ^= (unsigned long)c.chars[17] << 48;
        ret ^= (unsigned long)c.chars[18] << 40;
        ret ^= (unsigned long)c.chars[19] << 32;
#endif
#endif
        ret ^= (unsigned long)c.prefix_index;
        // std::cout << " *" << ret << "* " << std::flush;
        return ret;
    }
};

struct encoding_entry {
    uint16_t exp_a;
    uint16_t exp_b;
    uint16_t symbol = 0; // unused symbol 0.
};

int hash(uint16_t a, uint16_t b)
{
    // range of return value must match size of encoding table
    uint32_t tmp = a + 3;
    tmp *= b + 7;
    return (tmp ^ (tmp >> 16)) & 0xFFFF;
}
#define COMPRESS_BEFORE_INTERNING 1

class string_compressor {
public:
    std::vector<std::vector<uint16_t>> symbols;
    std::vector<std::string> strings;
    std::unordered_map<std::vector<uint16_t>, int> symbol_map;
    std::unordered_map<std::string, int> string_map;
    std::vector<encoding_entry> encoding_table;
    std::vector<encoding_entry> decoding_table;
    bool separators[256];
    uint16_t symbol_buffer[8192];
    string_compressor()
    {
        encoding_table.resize(65536);
        for (int j = 0; j < 0x20; ++j)
            separators[j] = true;
        for (int j = 0x20; j < 0x100; ++j)
            separators[j] = false;
        separators['/'] = true;
        separators[':'] = true;
        separators['?'] = true;
        separators['<'] = true;
        separators['>'] = true;
        separators['['] = true;
        separators[']'] = true;
        separators['{'] = true;
        separators['}'] = true;
    }
    int compress_symbols(uint16_t symbols[], int size, int max_runs, int breakout_limit = 1)
    {
        bool table_full = decoding_table.size() >= 65536 - 256;
        // std::cout << "Input: ";
        // for (int i = 0; i < size; ++i)
        //     std::cout << symbols[i] << " ";
        // std::cout << std::endl;
        for (int runs = 0; runs < max_runs; ++runs) {
            uint16_t* to = symbols;
            int p;
            uint16_t* from = symbols;
            for (p = 0; p < size - 1;) {
                uint16_t a = from[0];
                uint16_t b = from[1];
                auto index = hash(a, b);
                auto& e = encoding_table[index];
                if (e.symbol && e.exp_a == a && e.exp_b == b) {
                    // existing matching entry -> compress
                    *to++ = e.symbol;
                    p += 2;
                }
                else if (e.symbol || table_full) {
                    // existing conflicting entry or at capacity -> don't compress
                    *to++ = a;
                    p++;
                    // trying to stay aligned yields slightly worse results, so disable for now:
                    // *to++ = from[1];
                    // p++;
                }
                else {
                    // no matching entry yet, create new one -> compress
                    e.symbol = decoding_table.size() + 256;
                    e.exp_a = a;
                    e.exp_b = b;
                    // std::cout << "             new symbol " << e.symbol << " -> " << e.exp_a << " " << e.exp_b
                    //           << std::endl;
                    decoding_table.push_back({a, b, e.symbol});
                    table_full = decoding_table.size() >= 65536 - 256;
                    *to++ = e.symbol;
                    p += 2;
                }
                from = symbols + p;
            }
            // potentially move last unpaired symbol over
            if (p < size) {
                *to++ = *from++; // need to increment for early out check below
            }
            size = to - symbols;
            // std::cout << " -- Round " << runs << " -> ";
            // for (int i = 0; i < size; ++i)
            //     std::cout << symbols[i] << " ";
            // std::cout << std::endl;
            if (size <= breakout_limit)
                break; // early out, gonna use at least one chunk anyway
            if (from == to)
                break; // early out, no symbols were compressed on last run
        }
        return size;
    }
    void decompress_and_verify(uint16_t symbols[], int size, const char* first, const char* past)
    {
        uint16_t decompressed[8192];
        uint16_t* from = symbols;
        uint16_t* to = decompressed;

        auto decompress = [&](uint16_t symbol, auto& recurse) -> void {
            if (symbol < 256)
                *to++ = symbol;
            else {
                auto& e = decoding_table[symbol - 256];
                recurse(e.exp_a, recurse);
                recurse(e.exp_b, recurse);
            }
        };

        while (size--) {
            decompress(*from++, decompress);
        }
        // walk back on any trailing zeroes:
        while (to[-1] == 0 && to > decompressed) {
            --to;
        }
        size = to - decompressed;
        // std::cout << "reverse -> ";
        // for (int i = 0; i < size; ++i) {
        //     std::cout << decompressed[i] << " ";
        // }
        // std::cout << std::endl;
        assert(size == past - first);
        uint16_t* checked = decompressed;
        while (first < past) {
            assert((0xFF & *first++) == *checked++);
        }
    }
    int compress(uint16_t symbols[], const char* first, const char* past)
    {
        // expand into 16 bit symbols:
        int size = past - first;
        assert(size < 8180);
        uint16_t* to = symbols;
        int out_size = 0;
        for (const char* p = first; p < past;) {
            // form group from !seps followed by seps
            uint16_t* group_start = to;
            while (p < past && !separators[0xFFUL & *p])
                *to++ = *p++ & 0xFF;
            while (p < past && separators[0xFFUL & *p])
                *to++ = *p++ & 0xFF;
            int group_size = to - group_start;
            // compress the group
            group_size = compress_symbols(group_start, group_size, 2);
            to = group_start + group_size;
            out_size += group_size;
        }
        // compress all groups together
        // size = out_size;
        size = compress_symbols(symbols, out_size, 2, 4);
        compressed_symbols += size;
        return size;
    }
    int handle(const char* _first, const char* _past)
    {
#if COMPRESS_BEFORE_INTERNING
        int size = _past - _first;
        total_chars += size;
        size = compress(symbol_buffer, _first, _past);
        // decompress_and_verify(symbol_buffer, size, _first, _past);

        std::vector<uint16_t> symbol(size);
        for (int j = 0; j < size; ++j)
            symbol[j] = symbol_buffer[j];
        auto it = symbol_map.find(symbol);
        if (it == symbol_map.end()) {
            auto id = symbols.size();
            symbols.push_back(symbol);
            symbol_map[symbol] = id;
            unique_symbol_size += size;
            return id;
        }
        else {
            return it->second;
        }
#else // INTERN BEFORE COMPRESSING:
        int size = _past - _first;
        total_chars += size;
        std::string s(_first, _past);
        auto it = string_map.find(s);
        if (it == string_map.end()) {
            auto id = strings.size();
            strings.push_back(s);
            string_map[s] = id;
            auto size = compress(symbol_buffer, _first, _past);
            std::vector<uint16_t> symbol(size);
            for (int j = 0; j < size; ++j)
                symbol[j] = symbol_buffer[j];
            symbols.push_back(symbol);
            unique_symbol_size += size;
            return id;
        }
        else {
            return it->second;
        }
#endif
    }
    int symbol_table_size()
    {
        return decoding_table.size();
    }
    int next_id = 0;
    int next_prefix_id = -1;
    int64_t total_chars = 0;
    int64_t compressed_symbols = 0;
    int64_t unique_symbol_size = 0;
};

// controls
#define USE_UNALIGNED 0
#define USE_INTERPOLATION 0
#define USE_LOCAL_DIR 1
#define USE_SPARSE 1
#define USE_BASE_OFFSET 0 // defunct
#define USE_EMPTY_IMPROVEMENT 1

int unsigned_bits_needed(uint64_t val)
{
#if USE_UNALIGNED
    /* This is with unaligned accesses */
    if (val >> 48)
        return 64;
    if (val >> 32)
        return 48;
    if (val >> 24)
        return 32;
    if (val >> 16)
        return 24;
    if (val >> 12)
        return 16;
    if (val >> 8)
        return 12;
    if (val >> 6)
        return 8;
    if (val >> 5)
        return 6;
    if (val >> 4)
        return 5;
    if (val >> 3)
        return 4;
    if (val >> 2)
        return 3;
    if (val >> 1)
        return 2;
    if (val)
        return 1;
    return 0;
#else
    // This is with our current aligned accesses

    if (val < 16) {
        if (val < 2)
            return val;
        return 4;
    }
    if (val < 256)
        return 8;
    if (val < 65536)
        return 16;
    if (val >> 32)
        return 64;
    return 32;

#endif
}

int signed_bits_needed(int64_t val)
{
    // make sure we have room for negation and shift
    if (val & 0xFF00000000000000ULL)
        return 64;
    if (val < 0)
        return unsigned_bits_needed((-val) << 1);
    return unsigned_bits_needed(val << 1);
}

int align(int alignment, int size)
{
    int mask = alignment - 1;
    int unaligned = size & mask;
    if (unaligned)
        size += alignment - unaligned;
    return size;
}
enum EncType { Array = 0, Empty = 1, Sprse = 2, Indir = 3, Linear = 4, Offst = 5 };

std::string EncName[] = {"array", "empty", "sparse", "indir", "lnreg", "offst"};

struct leaf_compression_analyzer {
    constexpr static int leaf_size = 256;
    int64_t values[leaf_size];
    std::unordered_map<int64_t, int> unique_values;
    int entry_count = 0;
    uint64_t total_bytes = 0;
    uint64_t type_counts[6] = {0, 0, 0, 0, 0, 0};

    void note_value(int64_t value)
    {
        unique_values[value]++;
        values[entry_count] = value;
        entry_count++;
        if (entry_count == leaf_size) {
            post_process();
        }
    }

    void post_process()
    {
        int64_t default_value = 0;
        int64_t max_value = std::numeric_limits<int64_t>::min();
        int64_t min_value = std::numeric_limits<int64_t>::max();
        int default_value_count = 0;
        auto it = unique_values.end();
        int bits_per_value = 0;
        // a) determine how many bits are needed to rep greatest value.
        // b) determine most frequent value - which will become default value.
        for (auto entry = unique_values.begin(); entry != unique_values.end(); ++entry) {
            auto bits = signed_bits_needed(entry->first);
            if (bits > bits_per_value) {
                bits_per_value = bits;
            }
            if (entry->second > default_value_count) {
                default_value = entry->first;
                default_value_count = entry->second;
                it = entry;
            }
            if (entry->first > max_value)
                max_value = entry->first;
            if (entry->first < min_value)
                min_value = entry->first;
        }
        auto range = max_value - min_value;
        int num_unique_values = unique_values.size();
        int non_default_values = 0;
        for (int k = 0; k < entry_count; k++) {
            if (values[k] != default_value)
                non_default_values++;
        }
        // now we can estimate the cost of different layouts:
        // starting point is our current encoding:
        // assuming a large db we include 8 bytes for the ref pointing to the data array
        int leaf_cost = 8 + 8 + (bits_per_value * entry_count + 7) / 8;
        leaf_cost = align(8, leaf_cost);
        EncType enc_type = EncType::Array;

        // with special case where everything is 0, encoded as ref == 0

        if (default_value == 0 && non_default_values == 0) {
            leaf_cost = 8 + (USE_EMPTY_IMPROVEMENT ? 0 : 8);
            enc_type = EncType::Empty;
            // std::cout << "Empty      " << leaf_cost << " bytes" << std::endl;
        }

#if USE_SPARSE
        // next evaluate a sparse encoding with actual entries marked in a bit mask
        {
            int alt_leaf_cost = 8 + 16 + (entry_count + 7) / 8 +
                                (bits_per_value * (non_default_values + (default_value ? 1 : 0) + 7) / 8);
            // std::cout << "Sparse     " << leaf_cost << " bytes" << std::endl;
            alt_leaf_cost = align(8, alt_leaf_cost);
            if (alt_leaf_cost < leaf_cost) {
                leaf_cost = alt_leaf_cost;
                enc_type = EncType::Sprse;
            }
        }
#endif
#if USE_LOCAL_DIR
        // with few unique values, use a local dict and let each entry be an index into it
        // experiments suggests a win for surprisingly high number of different values
        if (num_unique_values <= 3 * entry_count / 4) {
            // local dict cost:
            int alt_leaf_cost = 8 + 16 + (bits_per_value * num_unique_values + 7) / 8;
            // array of indirections cost:
            alt_leaf_cost += (entry_count * unsigned_bits_needed(num_unique_values - 1) + 7) / 8;
            // std::cout << "Local dict " << leaf_cost << " bytes" << std::endl;
            alt_leaf_cost = align(8, alt_leaf_cost);
            if (alt_leaf_cost < leaf_cost) {
                leaf_cost = alt_leaf_cost;
                enc_type = EncType::Indir;
            }
        }
#endif
#if USE_BASE_OFFSET
        // if we have many values but within a narrow band, try base+offset encoding
        // This is wrong:
        /*
        if (unsigned_bits_needed(range) < bits_per_value) {
            auto range_bits = unsigned_bits_needed(range);
            auto alt_leaf_cost =
                8 + 16 + (signed_bits_needed(min_value + range / 2) + 7) / 8 + (entry_count * range_bits + 7) / 8;
            alt_leaf_cost = align(8, alt_leaf_cost);
            if (alt_leaf_cost < leaf_cost) {
                leaf_cost = alt_leaf_cost;
                enc_type = EncType::Offst;
            }
        }
        */
#endif
#if USE_INTERPOLATION
        // determine cost of storing offsets from simple linear interpolation
        if (entry_count) {
            double ratio = double(values[entry_count - 1] - values[0]) / entry_count;
            // to work for large values, we need to represent estimate as sum of integer and double
            double adjustment = 0.0;
            int64_t base = values[0];
            max_value = std::numeric_limits<int64_t>::min();
            min_value = std::numeric_limits<int64_t>::max();
            for (int j = 0; j < entry_count; ++j) {
                auto observation = values[j];
                int64_t offset = observation - base - adjustment;
                if (offset > max_value)
                    max_value = offset;
                if (offset < min_value)
                    min_value = offset;
            }
            auto range = max_value - min_value;
            // header: div flags + base (int64_t) + ratio (double)
            auto alt_leaf_cost = 8 + 24 + (entry_count * unsigned_bits_needed(range) + 7) / 8;
            alt_leaf_cost = align(8, alt_leaf_cost);
            if (alt_leaf_cost < leaf_cost) {
                leaf_cost = alt_leaf_cost;
                enc_type = EncType::Linear;
            }
            adjustment += ratio;
        }
#endif
        type_counts[enc_type]++;
        total_bytes += leaf_cost;
        unique_values.clear();
        entry_count = 0;
    }
};

struct compressor_driver {
    struct entry {
        long* res;
        const char* first;
        const char* last;
    };
    std::vector<entry> work;
    string_compressor* compressor;
    leaf_compression_analyzer* leaf_analyzer;
    void set_compressor(string_compressor* _compressor)
    {
        compressor = _compressor;
    }
    void set_leaf_analyzer(leaf_compression_analyzer* _leaf_analyzer)
    {
        leaf_analyzer = _leaf_analyzer;
    }
    compressor_driver(long size)
    {
        work.reserve(size);
    }
    void add_to_work(long* res, const char* first, const char* last)
    {
        work.push_back({res, first, last});
    }
    void perform()
    {
        for (auto& entry : work) {
            auto val = compressor->handle(entry.first, entry.last);
            leaf_analyzer->note_value(val);
            *entry.res = val;
        }
        work.clear();
    }
};

struct results {
    long* values;
    long first_line = -1;
    long num_lines;
    long num_fields;
    results(long num_lines, long num_fields)
        : num_lines(num_lines)
        , num_fields(num_fields)
    {
        values = new long[num_lines * num_fields];
    }
    ~results()
    {
        delete values;
    }
    void finalize(long _first_line, long limit)
    {
        first_line = _first_line;
        num_lines = limit - _first_line;
    };
};

template <typename work_type>
class concurrent_queue {
    std::list<work_type> queue;
    std::mutex mutex;
    std::condition_variable changed;
    bool open = true;

public:
    void close()
    {
        std::unique_lock<std::mutex> lock(mutex);
        open = false;
        changed.notify_all();
    }
    void put(work_type work_item)
    {
        std::unique_lock<std::mutex> lock(mutex);
        queue.push_back(work_item);
        changed.notify_all();
    }
    work_type get()
    { // throws if closed and queue empty
        std::unique_lock<std::mutex> lock(mutex);
        while (queue.size() == 0 && open) {
            changed.wait(lock);
        }
        if (queue.size() > 0) {
            auto e = queue.front();
            queue.pop_front();
            return e;
        }
        if (!open) {
            throw std::runtime_error("Concurrent queue closed");
        }
        exit(-3);
    }
};

struct driver_workload {
    std::vector<std::unique_ptr<compressor_driver>> drivers;
    results* res;
    driver_workload(int max)
    {
        drivers.resize(max);
    }
};

int main(int argc, char* argv[])
{
    const int limit = 1000000;
    constexpr int max_fields = 105;
    char compressible[max_fields + 1] =
        "iisissiiiiiiissiiiiiiiiiisiiisiiiissiiisiiiiisiiiisiiiiisiiiiiissiiiiiiiiissiiiiiiiiiisississssssssssiiii";

    char fields[max_fields + 1] =
        "iiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii";
    /*
    {
        std::cout << "Determining field types...." << std::endl;
        std::ifstream infile(argv[1]);
        std::string line;
        long num_line = 0;
        while (std::getline(infile, line)) {
            long num_value = 0;
            if ((num_line % 100000) == 0)
                std::cout << num_line << " " << std::flush;
            std::istringstream iss(line);
            ++num_line;
            std::string tmp;
            while (std::getline(iss, tmp, '\t')) {
                //std::cout << num_line << " : " << num_value << " : " << tmp << std::endl;
                if (fields[num_value] == 'i') {
                    // confirm that we can ingest this field as an integer
                    for (int i = 0; i < tmp.length(); ++i) {
                        if (i == 0 && tmp[i] == '-') continue;
                        if (tmp[i] >= '0' && tmp[i] <= '9') continue;
                        fields[num_value] = 's';
                        break;
                    }
                }
                ++num_value;
            }
        }
        std::cout << "Fields: " << fields << std::endl;
    }
    */
    Db& db = Db::create("perf.core2");

    Snapshot& ss = db.create_changes();
    Table t = ss.create_table(fields, 100000000);
    Field<String> f_s[max_fields];
    Field<int64_t> f_i[max_fields];
    for (int j = 0; j < max_fields; ++j) {
        if (fields[j] == 'i')
            f_i[j] = ss.get_field<int64_t>(t, j);
        else
            f_s[j] = ss.get_field<String>(t, j);
    }
    int fd = open(argv[1], O_RDONLY);
    assert(fd >= 0);
    off_t size = lseek(fd, 0, SEEK_END);
    void* file_start = mmap(0, size, PROT_READ, MAP_PRIVATE, fd, 0);
    assert(file_start != (void*)-1);
    long step_size = 5000000;
    int num_work_packages = 8;
    concurrent_queue<results*> to_reader;
    for (int i = 0; i < num_work_packages; ++i)
        to_reader.put(new results(step_size, max_fields));
    concurrent_queue<results*> to_writer;
    concurrent_queue<std::shared_ptr<driver_workload>> to_compressor;
    concurrent_queue<std::shared_ptr<driver_workload>> free_drivers;
    leaf_compression_analyzer leaf_analyzers[max_fields];

    std::thread writer([&]() {
        std::cout << "Initial scan / object creation" << std::endl;
        std::chrono::time_point<std::chrono::high_resolution_clock> start, end;
        long total_lines = 0;
        start = std::chrono::high_resolution_clock::now();
        const char* line_start = (const char*)file_start;
        while (line_start < file_start + size) {
            while (*line_start++ != '\n')
                ;
            if ((total_lines % 100000) == 0)
                std::cout << "." << std::flush;
            if ((total_lines % 1000000) == 0)
                std::cout << total_lines << std::endl;
            ss.insert(t, {total_lines << 1});
            // if (!ss.exists(t, {total_lines << 1})) {
            //     std::cout << "Weird - " << total_lines << " is missing" << std::endl;
            // }
            // auto o = ss.get(t, {total_lines << 1});
            total_lines++;
        }
        end = std::chrono::high_resolution_clock::now();
        std::chrono::milliseconds ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
        std::cout << "   ...done in " << ms.count() << " millisecs" << std::endl;
        ss.print_stat(std::cout);
        std::cout << "Committing to stable storage" << std::flush;
        start = std::chrono::high_resolution_clock::now();
        db.commit(std::move(ss));
        end = std::chrono::high_resolution_clock::now();
        ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
        std::cout << "   ...done in " << ms.count() << " msecs" << std::endl << std::endl;


        std::cout << "Optimizing access order..." << std::endl;
        std::vector<Row> row_order;
        row_order.reserve(total_lines);
        {
            const Snapshot& s3 = db.open_snapshot();
            s3.for_each(t, [&](Object& o) {
                row_order.push_back(o.r); /* std::cout << o.r.key << " " << std::flush; */
            });
            db.release(std::move(s3));
        }
        std::cout << "Committing data...." << std::endl;
        while (1) {

            results* res;
            try {
                res = to_writer.get();
            }
            catch (...) {
                std::cout << "Writing Done" << std::endl;
                break;
            }

            start = std::chrono::high_resolution_clock::now();
            const Snapshot& s3 = db.open_snapshot();
            db.release(std::move(s3));
            Snapshot& s2 = db.create_changes(); // create_changes();
            auto num_line = res->first_line;
            auto limit = res->first_line + res->num_lines;
            std::cout << "Writing " << num_line << " to " << limit << " width " << res->num_fields << std::endl;
            auto write_object = [&](long num_line, long*& val_ptr) {
                auto row = row_order[num_line];
                Object o = s2.get(t, row);
                auto num_value = 0;
                while (num_value < res->num_fields) {
                    auto val = *val_ptr++;
                    o.set(f_i[num_value++], val);
                }
            };
            auto write_range = [&](long first, long past) {
                // std::cout << "constructing [" << first << " - " << past << "[" << std::endl;
                auto val_ptr = res->values + (first - res->first_line) * res->num_fields;
                for (auto line = first; line < past; ++line) {
                    write_object(line, val_ptr);
                }
            };
            // split in 5 chunks and guard them against races by writing 500 entries
            // at the borders of each of the chunks.
            auto step = 1000000; // res->num_lines / 5;
            // 4 separating zone:
            for (auto line = num_line + step; line < limit; line += step) {
                write_range(line, line + 500);
            }
            // write 5 much larger in-between ranges in parallel
            std::vector<std::unique_ptr<std::thread>> threads;
            threads.push_back(std::make_unique<std::thread>([&]() { write_range(num_line, num_line + step); }));
            threads.push_back(
                std::make_unique<std::thread>([&]() { write_range(num_line + step + 500, num_line + 2 * step); }));
            threads.push_back(std::make_unique<std::thread>(
                [&]() { write_range(num_line + 2 * step + 500, num_line + 3 * step); }));
            threads.push_back(std::make_unique<std::thread>(
                [&]() { write_range(num_line + 3 * step + 500, num_line + 4 * step); }));
            write_range(num_line + 4 * step + 500, limit);
            for (auto& p : threads) {
                if (p)
                    p->join();
            }
            to_reader.put(res);
            end = std::chrono::high_resolution_clock::now();
            std::chrono::milliseconds ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
            std::cout << "   ...transaction built in " << ms.count() << " millisecs" << std::endl;
            start = end;
            db.commit(std::move(s2));
            end = std::chrono::high_resolution_clock::now();
            ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
            std::cout << "   ...committed in " << ms.count() << " msecs" << std::endl << std::endl;
        }
    });

    std::vector<std::unique_ptr<string_compressor>> compressors;
    compressors.resize(105);
    for (int j = 0; j < 105; j++) {
        if (compressible[j] == 's') {
            compressors[j] = std::make_unique<string_compressor>();
        }
    }

    std::thread compressor([&]() {
        std::chrono::time_point<std::chrono::high_resolution_clock> start, end;
        while (1) {
            std::shared_ptr<driver_workload> drivers;
            try {
                drivers = to_compressor.get();
            }
            catch (...) {
                to_writer.close();
                break;
            }
            start = std::chrono::high_resolution_clock::now();
            std::vector<std::unique_ptr<std::thread>> threads;
            for (int j = 0; j < 105; j++) {
                if (compressible[j] == 's') {
                    auto& driver = drivers->drivers[j];
                    driver->set_compressor(compressors[j].get());
                    driver->set_leaf_analyzer(&leaf_analyzers[j]);
                    // driver->perform();
                    threads.push_back(std::make_unique<std::thread>([&]() { driver->perform(); }));
                }
            }
            for (auto& p : threads) {
                p->join();
            }
            to_writer.put(drivers->res);
            free_drivers.put(drivers);
            end = std::chrono::high_resolution_clock::now();
            auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
            std::cout << "   ...compressed in " << ms.count() << " millisecs" << std::endl;
        }
    });
    // now populate dataset
    {
        std::cout << std::endl << "Ingesting data.... " << std::endl;
        std::chrono::time_point<std::chrono::high_resolution_clock> start, end;
        long num_line = 0;
        for (int k = 0; k < 4; k++) {
            auto drivers = std::make_shared<driver_workload>(105);
            for (int j = 0; j < 105; j++) {
                if (compressible[j] == 's') {
                    drivers->drivers[j] = std::make_unique<compressor_driver>(5000000);
                }
            }
            free_drivers.put(drivers);
        }
        const char* read_ptr = (const char*)file_start;
        while (read_ptr < file_start + size) {
            long limit = num_line + step_size;
            long first_line = num_line;
            results* res = to_reader.get();
            auto drivers = free_drivers.get();
            start = std::chrono::high_resolution_clock::now();
            while (num_line < limit && read_ptr < file_start + size) {
                long num_value = 0;
                if ((num_line % 100000) == 0)
                    std::cout << num_line << " " << std::flush;
                // std::cout << "Index " << num_line << " - key " << row.key << std::endl;
                long* line_results = res->values + (num_line - first_line) * max_fields;
                ++num_line;
                while (num_value < max_fields) {
                    const char* read_ptr2 = read_ptr;
                    while (*read_ptr2 != '\t' && *read_ptr2 != 0 && *read_ptr2 != '\n')
                        ++read_ptr2;
                    if (compressible[num_value] == 's') {
                        drivers->drivers[num_value]->add_to_work(line_results + num_value, read_ptr, read_ptr2);
                    }
                    else {
                        int64_t val;
                        if (read_ptr == read_ptr2)
                            val = 0;
                        else
                            val = atol(read_ptr);
                        line_results[num_value] = val;
                        leaf_analyzers[num_value].note_value(val);
                    }
                    ++num_value;
                    read_ptr = read_ptr2 + 1;
                }
            }
            res->finalize(first_line, num_line);
            drivers->res = res;
            to_compressor.put(drivers);
            end = std::chrono::high_resolution_clock::now();
            std::chrono::milliseconds ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
            std::cout << std::endl << "   ...read in " << ms.count() << " millisecs" << std::endl;
            start = end;
        }
        std::cout << "shutting down..." << std::endl;
        to_compressor.close();
        compressor.join();
        // destroy drivers...
        for (int k = 0; k < 4; k++) {
            auto p = free_drivers.get();
        }
        uint64_t from_size = 0;
        uint64_t dict_size = 0;
        uint64_t symbol_size = 0;
        uint64_t ref_size = 0;
        uint64_t compressed_size = 0;
        uint64_t dict_entries = 0;
        std::cout << "String compression results:" << std::endl;
        for (int i = 0; i < max_fields; ++i) {
            if (compressors[i]) {
                uint64_t col_from_size = compressors[i]->total_chars;
                // add one byte to each for zero termination:
                col_from_size += num_line;
                auto ids = compressors[i]->symbols.size();
                // assume 4 bytes overhead for each unique string:
                uint64_t col_dict_entries = compressors[i]->symbols.size();
                uint64_t col_dict_size = 2 * compressors[i]->unique_symbol_size + 16 * col_dict_entries;
                uint64_t col_symbol_size = compressors[i]->symbol_table_size() * sizeof(encoding_entry);
                uint64_t col_total = col_dict_size + col_symbol_size;
                uint64_t col_compressed = compressors[i]->compressed_symbols;
                std::cout << "Field " << std::right << std::setw(3) << i << " from " << std::setw(11)
                          << compressors[i]->total_chars << " to " << std::setw(11) << col_compressed * 2
                          << " bytes + " << std::setw(9) << col_symbol_size << " for symboltable \tInterned into "
                          << std::setw(11) << col_dict_entries << " unique values stored in " << std::setw(11)
                          << col_dict_size << " bytes" << std::endl;
                compressors[i].reset();
                dict_size += col_dict_size;
                dict_entries += col_dict_entries;
                symbol_size += col_symbol_size;
                from_size += col_from_size;
                compressed_size += col_compressed * 2;
            }
        }
        std::cout << "Leaf compression results:" << std::endl;
        for (int i = 0; i < max_fields; ++i) {
            auto col_ref_size = leaf_analyzers[i].total_bytes;
            ref_size += col_ref_size;
            uint64_t total_arrays = 0;
            for (int t = EncType::Array; t <= EncType::Offst; t++) {
                total_arrays += leaf_analyzers[i].type_counts[t];
            }
            std::cout << "Field " << std::right << std::setw(3) << i << " leafs compressed to " << std::setw(11)
                      << col_ref_size << " (";
            for (int t = EncType::Array; t <= EncType::Offst; t++) {
                std::cout << EncName[t] << ": " << std::right << std::setw(3)
                          << leaf_analyzers[i].type_counts[t] * 100 / total_arrays << " %  ";
            }
            std::cout << ")" << std::endl;
        }
        uint64_t cluster_tree_overhead = 4 * num_line;
        std::cout << std::endl
                  << std::right << "Summary:" << std::endl
                  << " - Read file with size: " << std::setw(11) << size << " bytes. Encoding:" << std::endl
                  << " - String compression:  " << std::setw(11) << from_size << " -> " << std::setw(11)
                  << compressed_size << " bytes of symbols + " << std::setw(11) << symbol_size
                  << " for symbol tables." << std::endl
                  << " - String interning:    " << std::setw(11) << compressed_size << " -> " << std::setw(11)
                  << dict_size << " bytes for dictionaries with " << dict_entries << " unique values" << std::endl
                  << " - Leaf size:           " << std::setw(11) << ref_size << std::endl
                  << " - ClusterTree overhead:" << std::setw(11) << cluster_tree_overhead << std::endl
                  << "------------------------" << std::endl;
        auto total = ref_size + dict_size + symbol_size + cluster_tree_overhead;
        std::cout << "Size estimate: " << std::right << std::setw(11) << total << "   (data compressed to "
                  << 1000 * total / size << " pml of original size)" << std::endl;

        compressors.clear();
        for (int i = 0; i < num_work_packages; ++i)
            delete to_reader.get();

        writer.join();
    }
}
