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

#include <random>
#include <cstdint>

#include "hash.hpp"


uint64_t hash_a_table[8][256];
uint64_t hash_b_table[8][256];


void init_one_hash(std::mt19937_64& rand_gen, uint64_t table[8][256]) {
    for (int j=0; j<8; ++j)
        for (int k=0; k<256; ++k)
            table[j][k] = rand_gen();
}

void init_hashes() {
    std::mt19937_64 rand_gen;
    init_one_hash(rand_gen, hash_a_table);
    init_one_hash(rand_gen, hash_b_table);
}

inline uint64_t hash(uint64_t table[8][256], uint64_t key) {
    uint8_t c = key;
    uint64_t res = table[0][c];
    c = key >> 8;
    res ^= table[1][c];
    c = key >> 16;
    res ^= table[2][c];
    c = key >> 24;
    res ^= table[3][c];
    key >>= 32;
    c = key;
    res ^= table[4][c];
    c = key >> 8;
    res ^= table[5][c];
    c = key >> 16;
    res ^= table[6][c];
    c = key >> 24;
    res ^= table[7][c];
    return res;
}

uint64_t hash_a(uint64_t key) {
    return hash(hash_a_table, key);
}

uint64_t hash_b(uint64_t key) {
    return hash(hash_b_table, key);
}
