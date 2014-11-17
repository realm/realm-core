#include "testsettings.hpp"
#ifdef TEST_ALLOC

#include <string>

#include <tightdb/util/unique_ptr.hpp>
#include <tightdb/util/file.hpp>
#include <tightdb/alloc_slab.hpp>

#include "test.hpp"

using namespace std;
using namespace tightdb;
using namespace tightdb::util;


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
    h[0] = uchar((value >> 16) & 0x000000FF);
    h[1] = uchar((value >>  8) & 0x000000FF);
    h[2] = uchar( value        & 0x000000FF);
}

size_t get_capacity(const char* header)
{
    typedef unsigned char uchar;
    const uchar* h = reinterpret_cast<const uchar*>(header);
    return (std::size_t(h[0]) << 16) + (std::size_t(h[1]) << 8) + h[2];
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

    // Set size in headers (needed for Alloc::free())
    set_capacity(mr1.m_addr, 8);
    set_capacity(mr2.m_addr, 16);
    set_capacity(mr3.m_addr, 256);

    // Are pointers 64bit aligned
    CHECK_EQUAL(0, intptr_t(mr1.m_addr) & 0x7);
    CHECK_EQUAL(0, intptr_t(mr2.m_addr) & 0x7);
    CHECK_EQUAL(0, intptr_t(mr3.m_addr) & 0x7);

    // Do refs translate correctly
    CHECK_EQUAL(static_cast<void*>(mr1.m_addr), alloc.translate(mr1.m_ref));
    CHECK_EQUAL(static_cast<void*>(mr2.m_addr), alloc.translate(mr2.m_ref));
    CHECK_EQUAL(static_cast<void*>(mr3.m_addr), alloc.translate(mr3.m_ref));

    alloc.free_(mr3.m_ref, mr3.m_addr);
    alloc.free_(mr2.m_ref, mr2.m_addr);
    alloc.free_(mr1.m_ref, mr1.m_addr);

    // SlabAlloc destructor will verify that all is free'd
}


TEST(Alloc_AttachFile)
{
    GROUP_TEST_PATH(path);
    {
        SlabAlloc alloc;
        bool is_shared     = false;
        bool read_only     = false;
        bool no_create     = false;
        bool skip_validate = false;
        alloc.attach_file(path, is_shared, read_only, no_create, skip_validate, 0);
        CHECK(alloc.is_attached());
        CHECK(alloc.nonempty_attachment());
        alloc.detach();
        CHECK(!alloc.is_attached());
        alloc.attach_file(path, is_shared, read_only, no_create, skip_validate, 0);
        CHECK(alloc.is_attached());
        alloc.detach();
        CHECK(!alloc.is_attached());
        read_only = true;
        no_create = true;
        alloc.attach_file(path, is_shared, read_only, no_create, skip_validate, 0);
        CHECK(alloc.is_attached());
    }
}


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
        bool is_shared     = false;
        bool read_only     = true;
        bool no_create     = true;
        bool skip_validate = false;
        CHECK_THROW(alloc.attach_file(path_1, is_shared, read_only, no_create,
                                      skip_validate, 0), InvalidDatabase);
        CHECK(!alloc.is_attached());
        CHECK_THROW(alloc.attach_file(path_1, is_shared, read_only, no_create,
                                      skip_validate, 0), InvalidDatabase);
        CHECK(!alloc.is_attached());
        read_only = false;
        no_create = false;
        CHECK_THROW(alloc.attach_file(path_1, is_shared, read_only, no_create,
                                      skip_validate, 0), InvalidDatabase);
        CHECK(!alloc.is_attached());
        alloc.attach_file(path_2, is_shared, read_only, no_create, skip_validate, 0);
        CHECK(alloc.is_attached());
        alloc.detach();
        CHECK(!alloc.is_attached());
        CHECK_THROW(alloc.attach_file(path_1, is_shared, read_only, no_create,
                                      skip_validate, 0), InvalidDatabase);
    }
}


TEST(Alloc_AttachBuffer)
{
    GROUP_TEST_PATH(path);

    // Produce a valid buffer
    UniquePtr<char[]> buffer;
    size_t buffer_size;
    {
        File::try_remove(path);
        {
            SlabAlloc alloc;
            bool is_shared     = false;
            bool read_only     = false;
            bool no_create     = false;
            bool skip_validate = false;
            alloc.attach_file(path, is_shared, read_only, no_create, skip_validate, 0);
        }
        {
            File file(path);
            buffer_size = size_t(file.get_size());
            buffer.reset(static_cast<char*>(malloc(buffer_size)));
            CHECK(bool(buffer));
            file.read(buffer.get(), buffer_size);
        }
        File::remove(path);
    }

    {
        SlabAlloc alloc;
        alloc.attach_buffer(buffer.get(), buffer_size);
        CHECK(alloc.is_attached());
        CHECK(alloc.nonempty_attachment());
        alloc.detach();
        CHECK(!alloc.is_attached());
        alloc.attach_buffer(buffer.get(), buffer_size);
        CHECK(alloc.is_attached());
        alloc.detach();
        CHECK(!alloc.is_attached());
        bool is_shared     = false;
        bool read_only     = false;
        bool no_create     = false;
        bool skip_validate = false;
        alloc.attach_file(path, is_shared, read_only, no_create, skip_validate, 0);
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
    for (size_t i=0; i<sizeof buffer; ++i)
        buffer[i] = char((i+192)%128);

    {
        SlabAlloc alloc;
        CHECK_THROW(alloc.attach_buffer(buffer, sizeof buffer), InvalidDatabase);
        CHECK(!alloc.is_attached());
        CHECK_THROW(alloc.attach_buffer(buffer, sizeof buffer), InvalidDatabase);
        CHECK(!alloc.is_attached());
        bool is_shared     = false;
        bool read_only     = false;
        bool no_create     = false;
        bool skip_validate = false;
        alloc.attach_file(path, is_shared, read_only, no_create, skip_validate, 0);
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
    vector<MemRef> refs;
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
            set_capacity(r.m_addr, siz);

            // write some data to the allcoated area so that we can verify it later
            memset(r.m_addr + 3, static_cast<char>(reinterpret_cast<intptr_t>(r.m_addr)), siz - 3);
        }
        else if(refs.size() > 0) {
            // free random entry
            size_t entry = rand() % refs.size();
            alloc.free_(refs[entry].m_ref, refs[entry].m_addr);
            refs.erase(refs.begin() + entry);
        }

        if (iter + 1 == iterations || refs.size() > 10) {
            // free everything when we have 10 allocations, or when we exit, to not leak
            while(refs.size() > 0) {
                MemRef r = refs[0];
                size_t siz = get_capacity(r.m_addr);

                // verify that all the data we wrote during allocation is intact
                for (size_t c = 3; c < siz; c++) {
                    if (r.m_addr[c] != static_cast<char>(reinterpret_cast<intptr_t>(r.m_addr))) {
                        // faster than using 'CHECK' for each character, which is slow
                        CHECK(false);
                    }
                }

                alloc.free_(r.m_ref, r.m_addr);
                refs.erase(refs.begin());
            }

        }
    }
}

#endif // TEST_ALLOC
