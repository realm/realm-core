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

#include "db.hpp"

struct string_compressor {
    std::unordered_map<std::string, int> map;
    int handle(std::string tmp)
    {
        total_chars += tmp.size();
        int retval = next_id;
        auto it = map.find(tmp);
        if (it == map.end()) {
            map[tmp] = retval = next_id++;
            /*
                        // build and hash prefixes
                        if (tmp.size() > 8)
                            for (int term = (tmp.size() - 1) & ~0x7; term > 0; term -= 8) {
                                tmp.resize(term);
                                it = map.find(tmp);
                                if (it != map.end())
                                    break;
                                map[tmp] = next_prefix_id--;
                            }
            */
        }
        else {
            retval = it->second;
        }
        return retval;
    }
    int next_id = 0;
    int next_prefix_id = -1;
    int64_t total_chars = 0;
};

int main(int argc, char* argv[])
{
    const int limit = 1000000;
    constexpr int max_fields = 105;
    char fields[max_fields + 1] =
        "iisissiiiiiiissiiiiiiiiiisiiisiiiissiiisiiiiisiiiisiiiiisiiiiiissiiiiiiiiissiiiiiiiiiisississssssssssiiii";
    /*
    char fields[max_fields + 1] =
    "iiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii";
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
    long total_lines;
    {
        std::cout << "Creating default objects....\n" << std::flush;
        std::ifstream infile(argv[1]);
        std::string line;
        long num_line = 0;
        start = std::chrono::high_resolution_clock::now();
        while (std::getline(infile, line)) {
            if ((num_line % 100000) == 0)
                std::cout << "." << std::flush;
            if ((num_line % 1000000) == 0)
                std::cout << num_line << std::endl;
            ss.insert(t, {num_line << 1});
            if (!ss.exists(t, {num_line << 1})) {
                std::cout << "Weird - " << num_line << " is missing" << std::endl;
            }
            auto o = ss.get(t, {num_line << 1});
            ++num_line;
        }
        total_lines = num_line;
        end = std::chrono::high_resolution_clock::now();
        std::chrono::nanoseconds ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start) / limit;
        std::cout << "   ...done in " << ns.count() << " nsecs/key" << std::endl;
        ss.print_stat(std::cout);
        std::cout << "Committing to stable storage" << std::flush;
        start = std::chrono::high_resolution_clock::now();
        db.commit(std::move(ss));
        end = std::chrono::high_resolution_clock::now();
        std::chrono::milliseconds ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
        std::cout << "   ...done in " << ms.count() << " msecs" << std::endl << std::endl;
    }
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
        std::cout << std::endl << "Ingesting data (" << total_lines << " objects).... " << std::endl;
        std::ifstream infile(argv[1]);
        std::string line;
        long num_line = 0;
        std::vector<std::unique_ptr<string_compressor>> compressors;
        compressors.resize(105);
        while (num_line < total_lines) {
            long limit = std::min(total_lines, num_line + 5000000);
            start = std::chrono::high_resolution_clock::now();
            const Snapshot& s3 = db.open_snapshot();
            db.release(std::move(s3));
            Snapshot& s2 = db.create_changes(); // create_changes();
            while (num_line < limit) {
                if (!std::getline(infile, line)) {
                    std::cout << "Kludder i den!" << std::endl;
                    exit(-1);
                }
                long num_value = 0;
                if ((num_line % 100000) == 0)
                    std::cout << num_line << " " << std::flush;
                auto row = row_order[num_line];
                // std::cout << "Index " << num_line << " - key " << row.key << std::endl;
                if (!s2.exists(t, row)) {
                    std::cout << "Weird - " << num_line << " with key " << row.key << " is missing" << std::endl;
                }
                auto o = s2.get(t, row);
                std::istringstream iss(line);
                ++num_line;
                std::string tmp;
                while (std::getline(iss, tmp, '\t')) {
                    // std::cout << num_line << " : " << num_value << " : " << tmp << std::endl;
                    if (fields[num_value] == 's') {
                        if (!compressors[num_value]) {
                            compressors[num_value] = std::make_unique<string_compressor>();
                        }
                        auto& compressor = *compressors[num_value];
                        compressor.handle(tmp);
                        o.set(f_s[num_value], tmp);
                    }
                    else if (tmp.empty())
                        o.set(f_i[num_value], 0L);
                    else
                        o.set(f_i[num_value], stol(tmp));
                    ++num_value;
                }
            }
            // db.release(std::move(s3));
            end = std::chrono::high_resolution_clock::now();
            std::chrono::nanoseconds ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start) / limit;
            std::cout << "   ...done in " << ns.count() << " nsecs/key" << std::endl;
            s2.print_stat(std::cout);
            std::cout << "Committing to stable storage" << std::flush;
            start = std::chrono::high_resolution_clock::now();
            db.commit(std::move(s2));
            end = std::chrono::high_resolution_clock::now();
            std::chrono::milliseconds ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
            std::cout << "   ...done in " << ms.count() << " msecs" << std::endl << std::endl;
        }
        for (int i = 0; i < max_fields; ++i) {
            if (compressors[i]) {
                std::cout << "Field " << i << " with " << compressors[i]->next_id << " unique elements and "
                          << compressors[i]->map.size() << " chunks from total " << compressors[i]->total_chars
                          << " chars" << std::endl;
            }
        }
    }
}
