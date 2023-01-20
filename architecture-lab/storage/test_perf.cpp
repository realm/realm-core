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
#include <sstream>
#include <string>
#include <fstream>

#include <chrono>
#include <cassert>
#include <cstring>
#include <thread>
#include <vector>
#include <unordered_map>

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>

#include "db.hpp"

#define CHUNK_SIZE 20

struct chunk {
    char chars[CHUNK_SIZE];
    int prefix_index = -1;
    bool operator==(const chunk& a2) const
    {
        return memcmp(chars, a2.chars, CHUNK_SIZE) == 0 && prefix_index == a2.prefix_index;
    };
};

template <>
struct std::hash<chunk> {
    std::size_t operator()(chunk const& c) const noexcept
    {
        std::size_t ret = c.chars[0] << 56;
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
        ret ^= (unsigned long)c.prefix_index;
        // std::cout << " *" << ret << "* " << std::flush;
        return ret;
    }
};

struct string_compressor {
    std::vector<chunk> chunks;
    std::unordered_map<chunk, int> map;
    int handle(const char* first, const char* past)
    {
        int size = past - first;
        total_chars += size;
        const char* last = first + CHUNK_SIZE;
        int prefix = -1;
        chunk c;
        while (first < past) {
            if (last >= past) {
                last = past;
                memset(c.chars, 0, CHUNK_SIZE);
            }
            memcpy(c.chars, first, last - first);
            c.prefix_index = prefix;
            auto it = map.find(c);
            if (it == map.end()) {
                prefix = chunks.size();
                map[c] = prefix;
                chunks.push_back(c);
            }
            else {
                prefix = it->second;
            }
            first += CHUNK_SIZE;
            last += CHUNK_SIZE;
        }
        // std::cout << "   " << tmp << " -> " << prefix << std::endl;
        return prefix;
    }
    int next_id = 0;
    int next_prefix_id = -1;
    int64_t total_chars = 0;
};

struct compressor_driver {
    struct entry {
        long* res;
        const char* first;
        const char* last;
    };
    std::vector<entry> work;
    string_compressor& compressor;
    compressor_driver(string_compressor& compressor, long size)
        : compressor(compressor)
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
            *entry.res = compressor.handle(entry.first, entry.last);
        }
        work.clear();
    }
};

struct results {
    long* values;
    long first_line;
    long num_lines;
    long num_fields;
    results(long first_line, long num_lines, long num_fields)
        : first_line(first_line)
        , num_lines(num_lines)
        , num_fields(num_fields)
    {
        values = new long[num_lines * num_fields];
    }
    ~results()
    {
        delete values;
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

    std::chrono::time_point<std::chrono::high_resolution_clock> start, end;
    Snapshot& ss = db.create_changes();
    Table t = ss.create_table(fields);
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

    long total_lines = 0;
    std::thread writer([&]() {
        std::cout << "Initial scan / object creation" << std::endl;
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
    });
    writer.join();
    // now populate dataset
    {
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
        long step_size = 1000000;
        std::cout << std::endl << "Ingesting data (" << total_lines << " objects).... " << std::endl;
        long num_line = 0;
        std::vector<std::unique_ptr<string_compressor>> compressors;
        compressors.resize(105);
        std::vector<std::unique_ptr<compressor_driver>> drivers;
        drivers.resize(105);
        const char* read_ptr = (const char*)file_start;
        while (read_ptr < file_start + size) {
            long limit = std::min(total_lines, num_line + step_size);
            start = std::chrono::high_resolution_clock::now();
            results* res = new results(num_line, limit - num_line, max_fields);
            while (num_line < limit) {
                long num_value = 0;
                if ((num_line % 100000) == 0)
                    std::cout << num_line << " " << std::flush;
                // std::cout << "Index " << num_line << " - key " << row.key << std::endl;
                long* line_results = res->values + (num_line - res->first_line) * max_fields;
                ++num_line;
                while (num_value < max_fields) {
                    const char* read_ptr2 = read_ptr;
                    while (*read_ptr2 != '\t' && *read_ptr2 != 0 && *read_ptr2 != '\n')
                        ++read_ptr2;
                    if (compressible[num_value] == 's') {
                        if (!compressors[num_value]) {
                            compressors[num_value] = std::make_unique<string_compressor>();
                            drivers[num_value] =
                                std::make_unique<compressor_driver>(*compressors[num_value], 1000000);
                        }
                        auto& compressor = *compressors[num_value];
                        // long compressed = compressor.handle(read_ptr, read_ptr2);
                        // line_results[num_value] = compressed;
                        drivers[num_value]->add_to_work(line_results + num_value, read_ptr, read_ptr2);
                    }
                    else if (read_ptr == read_ptr2)
                        line_results[num_value] = 0;
                    else
                        line_results[num_value] = atol(read_ptr);

                    ++num_value;
                    read_ptr = read_ptr2 + 1;
                }
            }
            end = std::chrono::high_resolution_clock::now();
            std::chrono::milliseconds ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
            std::cout << std::endl << "   ...read in " << ms.count() << " millisecs" << std::endl;
            start = end;
            {
                std::vector<std::unique_ptr<std::thread>> threads;
                for (auto& p : drivers) {
                    if (p) {
                        threads.push_back(std::make_unique<std::thread>([&]() { p->perform(); }));
                    }
                }
                for (auto& p : threads) {
                    p->join();
                }
            }
            end = std::chrono::high_resolution_clock::now();
            ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
            std::cout << "   ...compressed in " << ms.count() << " millisecs" << std::endl;
            start = end;
            {
                const Snapshot& s3 = db.open_snapshot();
                db.release(std::move(s3));
                Snapshot& s2 = db.create_changes(); // create_changes();
                auto num_line = res->first_line;
                auto num_value = 0;
                auto val_ptr = res->values;
                auto limit = res->first_line + res->num_lines;
                while (num_line < limit) {
                    Object o;
                    if (num_value == 0) {
                        auto row = row_order[num_line];
                        // if (!s2.exists(t, row)) {
                        //     std::cout << "Weird - " << num_line << " with key " << row.key << " is missing"
                        //               << std::endl;
                        // }
                        o = s2.get(t, row);
                    }
                    auto val = *val_ptr++;
                    o.set(f_i[num_value++], val);
                    if (num_value == res->num_fields) {
                        num_value = 0;
                        num_line++;
                    }
                }
                delete res;
                end = std::chrono::high_resolution_clock::now();
                std::chrono::milliseconds ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
                std::cout << "   ...transaction built in " << ms.count() << " millisecs" << std::endl;
                start = std::chrono::high_resolution_clock::now();
                db.commit(std::move(s2));
                end = std::chrono::high_resolution_clock::now();
                ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
                std::cout << "   ...committed in " << ms.count() << " msecs" << std::endl << std::endl;
            }
        }
        uint64_t from_size = 0;
        uint64_t to_size = 0;
        for (int i = 0; i < max_fields; ++i) {
            if (compressors[i]) {
                from_size += compressors[i]->total_chars;
                to_size += compressors[i]->map.size() * sizeof(chunk);
                std::cout << "Field " << i << " with " << compressors[i]->map.size() << " chunks ("
                          << compressors[i]->map.size() * sizeof(chunk) << " bytes) from total "
                          << compressors[i]->total_chars << " chars" << std::endl;
            }
        }
        std::cout << "Total effect: from " << from_size << " to " << to_size << " bytes ("
                  << 100 - (to_size * 100) / from_size << " pct reduction)" << std::endl;
    }
}
