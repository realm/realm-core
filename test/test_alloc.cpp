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
#include <realm/impl/simulated_failure.hpp>
#include <realm/util/allocator.hpp>
#include <realm/util/file.hpp>

#include "test.hpp"

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
        CHECK_EQUAL(initial_version + 1, alloc.get_mapping_version());
        CHECK_NOT_EQUAL(initial_translated, alloc.translate(1000));

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

namespace {

class TestSlabAlloc : public SlabAlloc {

public:
    size_t test_get_upper_section_boundary(size_t start_pos)
    {
        return get_upper_section_boundary(start_pos);
    }
    size_t test_get_lower_section_boundary(size_t start_pos)
    {
        return get_lower_section_boundary(start_pos);
    }
    size_t test_get_section_base(size_t index)
    {
        return get_section_base(index);
    }
    size_t test_get_section_index(size_t ref)
    {
        return get_section_index(ref);
    }
};

} // end anonymous namespace


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

namespace {
class MyAllocator : public util::AllocatorBase {
public:
    MyAllocator(size_t size = 1000)
        : m_size(size)
        , m_buffer(new char[size])
        , m_ptr(m_buffer.get())
    {
    }
    void* allocate(std::size_t sz, std::size_t) override
    {
        if (m_ptr + sz > m_buffer.get() + m_size)
            throw std::bad_alloc{};
        void* p = m_ptr;
        m_ptr += sz;
        return p;
    }
    void free(void*, size_t sz) noexcept override
    {
        m_freed += sz;
    }
    bool check()
    {
        return m_ptr - m_freed == m_buffer.get();
    }
    size_t get_allocated() const noexcept
    {
        return m_ptr - m_buffer.get();
    }

private:
    size_t m_size;
    std::unique_ptr<char[]> m_buffer;
    char* m_ptr;
    size_t m_freed = 0;
};

template <class T>
using Alloc = util::STLAllocator<T, MyAllocator>;
using Vector = std::vector<int, Alloc<int>>;
} // namespace

TEST(Allocator_STLAllocator)
{
    MyAllocator alloc;
    {
        Vector vec{alloc};
        vec.push_back(3);
    }
    CHECK(alloc.check());
}

TEST(Allocator_MakeUnique)
{
    auto alloc = MyAllocator{};
    {
        auto ptr1 = util::make_unique<std::vector<std::string>, MyAllocator>(alloc, 5);
        CHECK_EQUAL(ptr1->size(), 5);
        auto ptr2 =
            util::make_unique<std::vector<int>, MyAllocator>(alloc, std::initializer_list<int>{1, 2, 3, 4, 5});
        CHECK_EQUAL(ptr2->size(), 5);
    }
    CHECK(alloc.check());
}

TEST(Allocator_MoveAssignmentNoCopy)
{
    MyAllocator alloc;
    std::vector<char, STLAllocator<char, MyAllocator>> vec1(100, char(123), alloc);
    // FIXME: This check is weird because MSVC's std::vector apparently
    // allocates an extra 8 bytes in Debug mode on move-assignment/construction!
    // Presumably, this is due to debug-iterators.
    CHECK_LESS(alloc.get_allocated(), 200);
    // Move-construction
    auto vec2 = std::move(vec1);
    CHECK_LESS(alloc.get_allocated(), 200);
    std::vector<char, STLAllocator<char, MyAllocator>> vec3(alloc);
    vec2 = std::move(vec3);
    CHECK_LESS(alloc.get_allocated(), 200);
}

TEST(Allocator_Polymorphic)
{
    struct Foo {
        virtual ~Foo() {}
        char buffer1[10];
    };
    struct Bar : Foo {
        char buffer2[10];
    };

    MyAllocator alloc;
    {
        // Instance of std::unique_ptr with pointer to base class allocator.
        std::unique_ptr<char[], STLDeleter<char[]>> abstract{nullptr,
                                                             STLDeleter<char[]>{DefaultAllocator::get_default()}};

        // Instance of std::unique_ptr with pointer to base class, and base
        // class allocator.
        std::unique_ptr<Foo, STLDeleter<Foo>> abstract2{nullptr, STLDeleter<Foo>{DefaultAllocator::get_default()}};

        {
            // Instance of std::unique_ptr with pointer to concrete allocator
            // implementation.
            auto concrete = util::make_unique<char[]>(alloc, 10);
            auto concrete2 = util::make_unique<Bar>(alloc);

            // These assignments should replace both pointer and deleter.
            abstract = std::move(concrete);
            abstract2 = std::move(concrete2);
        }
    }
    CHECK(alloc.check());

    MyAllocator alloc2;
    {
        std::vector<char, STLAllocator<char>> abstract{DefaultAllocator::get_default()};
        {
            std::vector<char, STLAllocator<char>> vec{alloc2};
            vec.resize(10);
            abstract = std::move(vec);
        }
    }
    CHECK(alloc2.check());
}

namespace {
// Test that STLAllocator can be used as the allocator in common STL types.
using MyString = std::basic_string<char, std::char_traits<char>, STLAllocator<char, DefaultAllocator>>;
template <class K, class V>
using MyMap = std::map<K, V, std::less<>, STLAllocator<std::pair<const K, V>>>;
template <class T>
using MyVector = std::vector<T, STLAllocator<T>>;
template <class T>
using MyList = std::list<T, STLAllocator<T>>;
template <class K, class V>
using MyUnorderedMap = std::unordered_map<K, V, std::hash<K>, std::equal_to<K>, STLAllocator<std::pair<const K, V>>>;
} // unnamed namespace

namespace std {
template <>
struct hash<MyString> {
    size_t operator()(const MyString& str) const
    {
        return std::hash<std::string>{}(std::string{str.c_str(), str.size()});
    }
};
} // namespace std

TEST(Allocator_StandardContainers)
{
    auto& alloc = DefaultAllocator::get_default();
    MyMap<MyString, MyVector<MyString>> m{alloc};
    MyVector<MyString> vec{10, MyString{"bar", alloc}, alloc};
    m.emplace(MyString{"foo", alloc}, std::move(vec));
    MyList<MyString> list{10, MyString{"baz", alloc}, alloc};
    MyUnorderedMap<MyString, MyString> m2{alloc};
    m2.emplace(MyString{"foo", alloc}, MyString{"bar", alloc});
}

#endif // TEST_ALLOC
