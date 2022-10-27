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

#include "testsettings.hpp"
#ifdef TEST_ALLOC

#include <list>
#include <map>
#include <string>
#include <unordered_map>
#include <vector>

#include <realm/alloc_slab.hpp>
#include <realm/array.hpp>
#include <realm/group.hpp>
#include <realm/impl/simulated_failure.hpp>
#include <realm/util/file.hpp>

#include "test.hpp"
#include "util/test_only.hpp"

using namespace realm;
using namespace realm::util;


// Test independence and thread-safety
// -----------------------------------
//
// All tests must be thread safe and independent of each other. This
// is required because it allows for both shuffling of the execution
// order and for parallelized testing.
//
// In particular, avoid using std::rand() since it is not guaranteed
// to be thread safe. Instead use the API offered in
// `test/util/random.hpp`.
//
// All files created in tests must use the TEST_PATH macro (or one of
// its friends) to obtain a suitable file system path. See
// `test/util/test_path.hpp`.
//
//
// Debugging and the ONLY() macro
// ------------------------------
//
// A simple way of disabling all tests except one called `Foo`, is to
// replace TEST(Foo) with ONLY(Foo) and then recompile and rerun the
// test suite. Note that you can also use filtering by setting the
// environment varible `UNITTEST_FILTER`. See `README.md` for more on
// this.
//
// Another way to debug a particular test, is to copy that test into
// `experiments/testcase.cpp` and then run `sh build.sh
// check-testcase` (or one of its friends) from the command line.


namespace {

void set_capacity(char* header, size_t value)
{
    typedef unsigned char uchar;
    uchar* h = reinterpret_cast<uchar*>(header);
    h[0] = uchar((value >> 19) & 0x000000FF);
    h[1] = uchar((value >> 11) & 0x000000FF);
    h[2] = uchar((value >> 3) & 0x000000FF);
}

size_t get_capacity(const char* header)
{
    typedef unsigned char uchar;
    const uchar* h = reinterpret_cast<const uchar*>(header);
    return (size_t(h[0]) << 19) + (size_t(h[1]) << 11) + (h[2] << 3);
}

} // anonymous namespace


TEST(Alloc_1)
{
    SlabAlloc alloc;
    CHECK(!alloc.is_attached());
    alloc.attach_empty();
    CHECK(alloc.is_attached());
    CHECK(!alloc.nonempty_attachment());

    MemRef mr1 = alloc.alloc(8);
    MemRef mr2 = alloc.alloc(16);
    MemRef mr3 = alloc.alloc(256);
    MemRef mr4 = alloc.alloc(96);
    // This will grow the file with 0x20000, but the 32 bytes are not enough to
    // create a new free block
    MemRef mr5 = alloc.alloc(0x20000 - 32);

    // Set size in headers (needed for Alloc::free())
    set_capacity(mr1.get_addr(), 8);
    set_capacity(mr2.get_addr(), 16);
    set_capacity(mr3.get_addr(), 256);
    set_capacity(mr4.get_addr(), 96);
    set_capacity(mr5.get_addr(), 0x20000 - 32);

    // Are pointers 64bit aligned
    CHECK_EQUAL(0, intptr_t(mr1.get_addr()) & 0x7);
    CHECK_EQUAL(0, intptr_t(mr2.get_addr()) & 0x7);
    CHECK_EQUAL(0, intptr_t(mr3.get_addr()) & 0x7);
    CHECK_EQUAL(0, intptr_t(mr4.get_addr()) & 0x7);

    // Do refs translate correctly
    CHECK_EQUAL(static_cast<void*>(mr1.get_addr()), alloc.translate(mr1.get_ref()));
    CHECK_EQUAL(static_cast<void*>(mr2.get_addr()), alloc.translate(mr2.get_ref()));
    CHECK_EQUAL(static_cast<void*>(mr3.get_addr()), alloc.translate(mr3.get_ref()));
    CHECK_EQUAL(static_cast<void*>(mr4.get_addr()), alloc.translate(mr4.get_ref()));

    alloc.free_(mr3.get_ref(), mr3.get_addr());
    alloc.free_(mr4.get_ref(), mr4.get_addr());
    alloc.free_(mr1.get_ref(), mr1.get_addr());
    alloc.free_(mr2.get_ref(), mr2.get_addr());
    alloc.free_(mr5.get_ref(), mr5.get_addr());

    // SlabAlloc destructor will verify that all is free'd
}


TEST(Alloc_AttachFile)
{
    GROUP_TEST_PATH(path);
    {
        SlabAlloc alloc;
        SlabAlloc::Config cfg;
        alloc.attach_file(path, cfg);
        CHECK(alloc.is_attached());
        CHECK(alloc.nonempty_attachment());
        alloc.detach();
        CHECK(!alloc.is_attached());
        alloc.attach_file(path, cfg);
        CHECK(alloc.is_attached());
        alloc.detach();
        CHECK(!alloc.is_attached());
        cfg.read_only = true;
        cfg.no_create = true;
        alloc.attach_file(path, cfg);
        CHECK(alloc.is_attached());
    }
}


// FIXME: Fails on Windows
#ifndef _MSC_VER
TEST(Alloc_BadFile)
{
    GROUP_TEST_PATH(path_1);
    GROUP_TEST_PATH(path_2);

    {
        File file(path_1, File::mode_Append);
        file.write("foo");
    }

    {
        SlabAlloc alloc;
        SlabAlloc::Config cfg;
        cfg.read_only = true;
        cfg.no_create = true;
        CHECK_THROW(alloc.attach_file(path_1, cfg), InvalidDatabase);
        CHECK(!alloc.is_attached());
        CHECK_THROW(alloc.attach_file(path_1, cfg), InvalidDatabase);
        CHECK(!alloc.is_attached());
        cfg.read_only = false;
        cfg.no_create = false;
        CHECK_THROW(alloc.attach_file(path_1, cfg), InvalidDatabase);
        CHECK(!alloc.is_attached());
        alloc.attach_file(path_2, cfg);
        CHECK(alloc.is_attached());
        alloc.detach();
        CHECK(!alloc.is_attached());
        CHECK_THROW(alloc.attach_file(path_1, cfg), InvalidDatabase);
    }
}
#endif


TEST(Alloc_AttachBuffer)
{
    GROUP_TEST_PATH(path);

    // Produce a valid buffer
    std::unique_ptr<char[]> buffer;
    size_t buffer_size;
    {
        File::try_remove(path);
        {
            SlabAlloc alloc;
            SlabAlloc::Config cfg;
            alloc.attach_file(path, cfg);
        }
        {
            File file(path);
            buffer_size = size_t(file.get_size());
            buffer.reset(new char[buffer_size]);
            CHECK(bool(buffer));
            file.read(buffer.get(), buffer_size);
        }
        File::remove(path);
    }

    {
        SlabAlloc alloc;
        SlabAlloc::Config cfg;
        alloc.attach_buffer(buffer.get(), buffer_size);
        CHECK(alloc.is_attached());
        CHECK(alloc.nonempty_attachment());
        alloc.detach();
        CHECK(!alloc.is_attached());
        alloc.attach_buffer(buffer.get(), buffer_size);
        CHECK(alloc.is_attached());
        alloc.detach();
        CHECK(!alloc.is_attached());
        alloc.attach_file(path, cfg);
        CHECK(alloc.is_attached());
        alloc.detach();
        CHECK(!alloc.is_attached());
        alloc.attach_buffer(buffer.get(), buffer_size);
        CHECK(alloc.is_attached());
        alloc.own_buffer();
        buffer.release();
        alloc.detach();
        CHECK(!alloc.is_attached());
    }
}


TEST(Alloc_BadBuffer)
{
    GROUP_TEST_PATH(path);

    // Produce an invalid buffer
    char buffer[32];
    for (size_t i = 0; i < sizeof buffer; ++i)
        buffer[i] = char((i + 192) % 128);

    {
        SlabAlloc alloc;
        CHECK_THROW(alloc.attach_buffer(buffer, sizeof buffer), InvalidDatabase);
        CHECK(!alloc.is_attached());
        CHECK_THROW(alloc.attach_buffer(buffer, sizeof buffer), InvalidDatabase);
        CHECK(!alloc.is_attached());
        SlabAlloc::Config cfg;
        alloc.attach_file(path, cfg);
        CHECK(alloc.is_attached());
        alloc.detach();
        CHECK(!alloc.is_attached());
        CHECK_THROW(alloc.attach_buffer(buffer, sizeof buffer), InvalidDatabase);
        CHECK(!alloc.is_attached());
    }
}


TEST(Alloc_Fuzzy)
{
    SlabAlloc alloc;
    std::vector<MemRef> refs;
    alloc.attach_empty();
    const size_t iterations = 10000;

    for (size_t iter = 0; iter < iterations; iter++) {
        int action = rand() % 100;

        if (action > 45) {
            // allocate slightly more often than free so that we get a growing mem pool
            size_t siz = rand() % 10 + 1;
            siz *= 8;
            MemRef r = alloc.alloc(siz);
            refs.push_back(r);
            set_capacity(r.get_addr(), siz);

            // write some data to the allcoated area so that we can verify it later
            memset(r.get_addr() + 3, static_cast<char>(reinterpret_cast<intptr_t>(r.get_addr())), siz - 3);
        }
        else if (refs.size() > 0) {
            // free random entry
            size_t entry = rand() % refs.size();
            alloc.free_(refs[entry].get_ref(), refs[entry].get_addr());
            refs.erase(refs.begin() + entry);
        }

        if (iter + 1 == iterations || refs.size() > 10) {
            // free everything when we have 10 allocations, or when we exit, to not leak
            while (refs.size() > 0) {
                MemRef r = refs[0];
                size_t siz = get_capacity(r.get_addr());

                // verify that all the data we wrote during allocation is intact
                for (size_t c = 3; c < siz; c++) {
                    if (r.get_addr()[c] != static_cast<char>(reinterpret_cast<intptr_t>(r.get_addr()))) {
                        // faster than using 'CHECK' for each character, which is slow
                        CHECK(false);
                    }
                }

                alloc.free_(r.get_ref(), r.get_addr());
                refs.erase(refs.begin());
            }
        }
    }
}

NONCONCURRENT_TEST_IF(Alloc_MapFailureRecovery, _impl::SimulatedFailure::is_enabled())
{
    GROUP_TEST_PATH(path);

    // Write a minimal file in streaming form so that it'll try to map the footer
    Group().write(path);

    SlabAlloc::Config cfg;
    SlabAlloc alloc;

    { // Initial Header mapping fails
        _impl::SimulatedFailure::prime_mmap([](size_t) {
            return true;
        });
        CHECK_THROW(alloc.attach_file(path, cfg), InvalidDatabase);
        CHECK(!alloc.is_attached());
    }

    { // Initial Footer mapping fails
        _impl::SimulatedFailure::prime_mmap([](size_t) {
            static int c = 0;
            return ++c > 1;
        });
        CHECK_THROW(alloc.attach_file(path, cfg), InvalidDatabase);
        CHECK(!alloc.is_attached());
    }

    const size_t page_size = util::page_size();

    { // Verify we can still open the file with the same allocator
        _impl::SimulatedFailure::prime_mmap(nullptr);
        alloc.attach_file(path, cfg);
        CHECK(alloc.is_attached());
        CHECK(alloc.get_baseline() == page_size);

        alloc.init_mapping_management(1);
    }

    { // Extendind the first mapping
        const auto initial_baseline = alloc.get_baseline();
        const auto initial_version = alloc.get_mapping_version();
        const char* initial_translated = alloc.translate(1000);

        _impl::SimulatedFailure::prime_mmap([](size_t) {
            return true;
        });
        // Does not expand so it succeeds
        alloc.update_reader_view(page_size);

        CHECK_THROW(alloc.update_reader_view(page_size * 2), std::bad_alloc);
        CHECK_EQUAL(initial_baseline, alloc.get_baseline());
        CHECK_EQUAL(initial_version, alloc.get_mapping_version());
        CHECK_EQUAL(initial_translated, alloc.translate(1000));

        _impl::SimulatedFailure::prime_mmap(nullptr);
        alloc.get_file().resize(page_size * 2);
        alloc.update_reader_view(page_size * 2);
        CHECK_EQUAL(alloc.get_baseline(), page_size * 2);
        // These two no longer applies:
        // CHECK_EQUAL(initial_version + 1, alloc.get_mapping_version());
        // CHECK_NOT_EQUAL(initial_translated, alloc.translate(1000));

        // Delete the old mapping. Will double-delete it if we incorrectly added
        // the mapping in the call that failed.
        alloc.purge_old_mappings(2, 2);
    }

    // Expand the first mapping to a full section
    static constexpr auto section_size = Allocator::section_size();
    alloc.get_file().resize(section_size * 2);
    alloc.update_reader_view(Allocator::section_size());
    alloc.purge_old_mappings(3, 3);

    { // Add a new complete section after a complete section
        const auto initial_baseline = alloc.get_baseline();
        const auto initial_version = alloc.get_mapping_version();
        const char* initial_translated = alloc.translate(1000);

        _impl::SimulatedFailure::prime_mmap([](size_t) {
            return true;
        });

        CHECK_THROW(alloc.update_reader_view(section_size * 2), std::bad_alloc);
        CHECK_EQUAL(initial_baseline, alloc.get_baseline());
        CHECK_EQUAL(initial_version, alloc.get_mapping_version());
        CHECK_EQUAL(initial_translated, alloc.translate(1000));

        _impl::SimulatedFailure::prime_mmap(nullptr);
        alloc.update_reader_view(section_size * 2);
        CHECK_EQUAL(alloc.get_baseline(), section_size * 2);
        CHECK_EQUAL(initial_version, alloc.get_mapping_version()); // did not alter an existing mapping
        CHECK_EQUAL(initial_translated, alloc.translate(1000));    // first section was not remapped
        CHECK_EQUAL(0, *alloc.translate(section_size * 2 - page_size));

        alloc.purge_old_mappings(4, 4);
    }

    alloc.get_file().resize(section_size * 4);

    { // Add complete section and a a partial section after that
        const auto initial_baseline = alloc.get_baseline();
        const auto initial_version = alloc.get_mapping_version();
        const char* initial_translated_1 = alloc.translate(1000);
        const char* initial_translated_2 = alloc.translate(section_size + 1000);

        _impl::SimulatedFailure::prime_mmap([](size_t size) {
            // Let the first allocation succeed and only the second one fail
            return size < section_size;
        });

        CHECK_THROW(alloc.update_reader_view(section_size * 3 + page_size), std::bad_alloc);
        CHECK_EQUAL(initial_baseline, alloc.get_baseline());
        CHECK_EQUAL(initial_version, alloc.get_mapping_version());
        CHECK_EQUAL(initial_translated_1, alloc.translate(1000));
        CHECK_EQUAL(initial_translated_2, alloc.translate(section_size + 1000));

        _impl::SimulatedFailure::prime_mmap(nullptr);
        alloc.update_reader_view(section_size * 3 + page_size);
        CHECK_EQUAL(alloc.get_baseline(), section_size * 3 + page_size);
        CHECK_EQUAL(initial_version, alloc.get_mapping_version()); // did not alter an existing mapping
        CHECK_EQUAL(initial_translated_1, alloc.translate(1000));
        CHECK_EQUAL(initial_translated_2, alloc.translate(section_size + 1000));
        CHECK_EQUAL(0, *alloc.translate(section_size * 2 + 1000));

        alloc.purge_old_mappings(5, 5);
    }
}

// This test reproduces the sporadic issue that was seen for large refs (addresses)
// on 32-bit iPhone 5 Simulator runs on certain host machines.
TEST(Alloc_ToAndFromRef)
{
    constexpr size_t ref_type_width = sizeof(ref_type) * 8;
    constexpr ref_type interesting_refs[] = {
        0, 8,
        ref_type(1ULL << (ref_type_width - 1)), // 32-bit: 0x80000000, 64-bit: 0x8000000000000000
        ref_type(3ULL << (ref_type_width - 2)), // 32-bit: 0xC0000000, 64-bit: 0xC000000000000000
    };

    constexpr size_t num_interesting_refs = sizeof(interesting_refs) / sizeof(interesting_refs[0]);
    for (size_t i = 0; i < num_interesting_refs; ++i) {
        ref_type ref = interesting_refs[i];
        int_fast64_t ref_as_int = from_ref(ref);
        ref_type back_to_ref = to_ref(ref_as_int);
        CHECK_EQUAL(ref, back_to_ref);
    }
}

TEST(Alloc_EncryptionPageRefresher)
{
    constexpr size_t top_array_size = 12;         // s_group_max_size
    constexpr size_t top_array_free_pos_ndx = 3;  // s_free_pos_ndx
    constexpr size_t top_array_free_size_ndx = 4; // s_free_size_ndx
    constexpr size_t total_size = 50;

    auto make_top_ref_with_allocations = [](SlabAlloc& alloc, RefRanges allocations, size_t total_size, Array& top) {
        RefRanges free_space;
        size_t last_alloc = 0;
        for (auto& allocation : allocations) {
            free_space.push_back({last_alloc, allocation.begin});
            last_alloc = allocation.end;
        }
        if (last_alloc < total_size) {
            free_space.push_back({last_alloc, total_size});
        }
        constexpr bool context_flag = false;
        top.create(Node::Type::type_HasRefs, context_flag, top_array_size, 0);
        Array free_positions(alloc);
        free_positions.create(Node::Type::type_Normal, context_flag, free_space.size(), 0);
        Array free_sizes(alloc);
        free_sizes.create(Node::Type::type_Normal, context_flag, free_space.size(), 0);
        for (size_t i = 0; i < free_space.size(); ++i) {
            free_positions.set(i, free_space[i].begin);
            free_sizes.set(i, free_space[i].end - free_space[i].begin);
        }
        top.set_as_ref(top_array_free_pos_ndx, free_positions.get_ref());
        top.set_as_ref(top_array_free_size_ndx, free_sizes.get_ref());
    };
    auto add_expected_refreshes_for_arrays = [](SlabAlloc& alloc, std::vector<VersionedTopRef> top_refs,
                                                RefRanges& to_refresh) {
        for (auto& v : top_refs) {
            // all top refs up to the max top ref size
            to_refresh.push_back({v.top_ref, v.top_ref + Array::header_size + top_array_size});
            // the ref + header size of free_pos and free_size
            Array top(alloc), pos(alloc), sizes(alloc);
            top.init_from_ref(v.top_ref);
            pos.init_from_ref(top.get_as_ref(top_array_free_pos_ndx));
            sizes.init_from_ref(top.get_as_ref(top_array_free_size_ndx));
            to_refresh.push_back({pos.get_ref(), pos.get_ref() + Array::header_size});
            to_refresh.push_back({sizes.get_ref(), sizes.get_ref() + Array::header_size});
            // now the full size of pos and size
            to_refresh.push_back({pos.get_ref(), pos.get_ref() + pos.get_byte_size()});
            to_refresh.push_back({sizes.get_ref(), sizes.get_ref() + sizes.get_byte_size()});
        }
    };

    auto check_range_refreshes = [&](std::vector<RefRanges> versions, RefRanges expected_diffs) {
        SlabAlloc alloc;
        alloc.attach_empty();
        CHECK(alloc.is_attached());
        std::vector<VersionedTopRef> top_refs;
        size_t version_dummy = 0;
        for (auto& v : versions) {
            Array top(alloc);
            make_top_ref_with_allocations(alloc, v, total_size, top);
            top_refs.push_back({top.get_ref(), ++version_dummy});
        }
        add_expected_refreshes_for_arrays(alloc, top_refs, expected_diffs);
        RefRanges actual_allocations;
        alloc.refresh_pages_for_versions(top_refs, [&](RefRanges allocs) {
            actual_allocations.insert(actual_allocations.end(), allocs.begin(), allocs.end());
        });
        std::sort(actual_allocations.begin(), actual_allocations.end(), [](const RefRange& a, const RefRange& b) {
            return a.begin < b.begin;
        });
        std::sort(expected_diffs.begin(), expected_diffs.end(), [](const RefRange& a, const RefRange& b) {
            return a.begin < b.begin;
        });
        CHECK_EQUAL(actual_allocations.size(), expected_diffs.size());
        for (size_t i = 0; i < actual_allocations.size(); ++i) {
            CHECK_EQUAL(actual_allocations[i].begin, expected_diffs[i].begin);
            CHECK_EQUAL(actual_allocations[i].end, expected_diffs[i].end);
        }
        alloc.detach();
    };

    std::vector<RefRanges> allocated_blocks = {{{5, 10}, {20, 30}}, {{5, 10}, {20, 40}}};
    RefRanges expected_diff = {{30, 40}};
    check_range_refreshes(allocated_blocks, expected_diff);

    allocated_blocks = {{{5, 10}, {20, 30}}, {{0, 10}, {20, 50}}};
    expected_diff = {{0, 5}, {30, 50}};
    check_range_refreshes(allocated_blocks, expected_diff);

    allocated_blocks = {{{5, 10}, {20, 30}}, {{0, 50}}};
    expected_diff = {{0, 5}, {10, 20}, {30, 50}};
    check_range_refreshes(allocated_blocks, expected_diff);

    allocated_blocks = {{{5, 10}, {20, 30}}, {}};
    expected_diff = {};
    check_range_refreshes(allocated_blocks, expected_diff);

    allocated_blocks = {{{5, 10}, {20, 30}}, {{5, 30}}};
    expected_diff = {{10, 20}};
    check_range_refreshes(allocated_blocks, expected_diff);

    allocated_blocks = {{{5, 10}, {20, 30}}, {{9, 29}}};
    expected_diff = {{10, 20}};
    check_range_refreshes(allocated_blocks, expected_diff);

    allocated_blocks = {{{5, 10}, {20, 30}}, {{10, 20}}};
    expected_diff = {{10, 20}};
    check_range_refreshes(allocated_blocks, expected_diff);
}

#endif // TEST_ALLOC
