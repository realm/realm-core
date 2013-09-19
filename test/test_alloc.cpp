#include "testsettings.hpp"
#ifdef TEST_ALLOC

#include <UnitTest++.h>

#include <tightdb/alloc_slab.hpp>
#include <tightdb/file.hpp>

using namespace tightdb;


namespace {

void set_capacity(char* header, size_t value)
{
    typedef unsigned char uchar;
    uchar* h = reinterpret_cast<uchar*>(header);
    h[1] = uchar((value >> 16) & 0x000000FF);
    h[2] = uchar((value >>  8) & 0x000000FF);
    h[3] = uchar( value        & 0x000000FF);
}

} // anonymous namespace


TEST(Alloc1)
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
    File::try_remove("test.tightdb");

    {
        SlabAlloc alloc;
        bool is_shared = false;
        bool read_only = false;
        bool no_create = false;
        alloc.attach_file("test.tightdb", is_shared, read_only, no_create);
        CHECK(alloc.is_attached());
        CHECK(alloc.nonempty_attachment());
        alloc.detach();
        CHECK(!alloc.is_attached());
        alloc.attach_file("test.tightdb", is_shared, read_only, no_create);
        CHECK(alloc.is_attached());
        alloc.detach();
        CHECK(!alloc.is_attached());
        read_only = true;
        no_create = true;
        alloc.attach_file("test.tightdb", is_shared, read_only, no_create);
        CHECK(alloc.is_attached());
    }

    File::remove("test.tightdb");
}


TEST(Alloc_BadFile)
{
    File::try_remove("test.tightdb");
    File::try_remove("test2.tightdb");

    {
        File file("test.tightdb", File::mode_Append);
        file.write("foo");
    }

    {
        SlabAlloc alloc;
        bool is_shared = false;
        bool read_only = true;
        bool no_create = true;
        CHECK_THROW(alloc.attach_file("test.tightdb", is_shared, read_only, no_create), InvalidDatabase);
        CHECK(!alloc.is_attached());
        CHECK_THROW(alloc.attach_file("test.tightdb", is_shared, read_only, no_create), InvalidDatabase);
        CHECK(!alloc.is_attached());
        read_only = false;
        no_create = false;
        CHECK_THROW(alloc.attach_file("test.tightdb", is_shared, read_only, no_create), InvalidDatabase);
        CHECK(!alloc.is_attached());
        alloc.attach_file("test2.tightdb", is_shared, read_only, no_create);
        CHECK(alloc.is_attached());
        alloc.detach();
        CHECK(!alloc.is_attached());
        CHECK_THROW(alloc.attach_file("test.tightdb", is_shared, read_only, no_create), InvalidDatabase);
    }

    File::remove("test.tightdb");
    File::remove("test2.tightdb");
}


TEST(Alloc_AttachBuffer)
{
    // Produce a valid buffer
    UniquePtr<char[]> buffer;
    size_t buffer_size;
    {
        File::try_remove("test.tightdb");
        {
            SlabAlloc alloc;
            bool is_shared = false;
            bool read_only = false;
            bool no_create = false;
            alloc.attach_file("test.tightdb", is_shared, read_only, no_create);
        }
        {
            File file("test.tightdb");
            buffer_size = size_t(file.get_size());
            buffer.reset(static_cast<char*>(malloc(buffer_size)));
            CHECK(bool(buffer));
            file.read(buffer.get(), buffer_size);
        }
        File::remove("test.tightdb");
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
        bool is_shared = false;
        bool read_only = false;
        bool no_create = false;
        alloc.attach_file("test.tightdb", is_shared, read_only, no_create);
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
    File::try_remove("test.tightdb");

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
        bool is_shared = false;
        bool read_only = false;
        bool no_create = false;
        alloc.attach_file("test.tightdb", is_shared, read_only, no_create);
        CHECK(alloc.is_attached());
        alloc.detach();
        CHECK(!alloc.is_attached());
        CHECK_THROW(alloc.attach_buffer(buffer, sizeof buffer), InvalidDatabase);
        CHECK(!alloc.is_attached());
    }

    File::remove("test.tightdb");
}

#endif // TEST_ALLOC
