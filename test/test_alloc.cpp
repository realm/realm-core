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

// Note: You can now temporarely declare unit tests with the ONLY(TestName) macro instead of TEST(TestName). This
// will disable all unit tests except these. Remember to undo your temporary changes before committing.

namespace {

void set_capacity(char* header, size_t value)
{
    typedef unsigned char uchar;
    uchar* h = reinterpret_cast<uchar*>(header);
    h[0] = uchar((value >> 16) & 0x000000FF);
    h[1] = uchar((value >>  8) & 0x000000FF);
    h[2] = uchar( value        & 0x000000FF);
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
        alloc.attach_file(path, is_shared, read_only, no_create, skip_validate);
        CHECK(alloc.is_attached());
        CHECK(alloc.nonempty_attachment());
        alloc.detach();
        CHECK(!alloc.is_attached());
        alloc.attach_file(path, is_shared, read_only, no_create, skip_validate);
        CHECK(alloc.is_attached());
        alloc.detach();
        CHECK(!alloc.is_attached());
        read_only = true;
        no_create = true;
        alloc.attach_file(path, is_shared, read_only, no_create, skip_validate);
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
                                      skip_validate), InvalidDatabase);
        CHECK(!alloc.is_attached());
        CHECK_THROW(alloc.attach_file(path_1, is_shared, read_only, no_create,
                                      skip_validate), InvalidDatabase);
        CHECK(!alloc.is_attached());
        read_only = false;
        no_create = false;
        CHECK_THROW(alloc.attach_file(path_1, is_shared, read_only, no_create,
                                      skip_validate), InvalidDatabase);
        CHECK(!alloc.is_attached());
        alloc.attach_file(path_2, is_shared, read_only, no_create, skip_validate);
        CHECK(alloc.is_attached());
        alloc.detach();
        CHECK(!alloc.is_attached());
        CHECK_THROW(alloc.attach_file(path_1, is_shared, read_only, no_create,
                                      skip_validate), InvalidDatabase);
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
            alloc.attach_file(path, is_shared, read_only, no_create, skip_validate);
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
        alloc.attach_file(path, is_shared, read_only, no_create, skip_validate);
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
        alloc.attach_file(path, is_shared, read_only, no_create, skip_validate);
        CHECK(alloc.is_attached());
        alloc.detach();
        CHECK(!alloc.is_attached());
        CHECK_THROW(alloc.attach_buffer(buffer, sizeof buffer), InvalidDatabase);
        CHECK(!alloc.is_attached());
    }
}

#endif // TEST_ALLOC
