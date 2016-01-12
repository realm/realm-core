#include <map>

#include <realm/array.hpp>
#include <realm/impl/destroy_guard.hpp>

#include "test.hpp"

using namespace realm;
using namespace realm::_impl;


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

class Foo {
public:
    Foo(bool* destroyed_flag):
        m_destroyed_flag(destroyed_flag)
    {
    }

    void destroy()
    {
        *m_destroyed_flag = true;
    }

private:
    bool* const m_destroyed_flag;
};

class FooAlloc: public Allocator {
public:
    FooAlloc():
        m_offset(8)
    {
    }

    ~FooAlloc() noexcept
    {
    }

    MemRef do_alloc(size_t size) override
    {
        ref_type ref = m_offset;
        char*& addr = m_map[ref]; // Throws
        REALM_ASSERT(!addr);
        addr = new char[size]; // Throws
        m_offset += size;
        return MemRef(addr, ref +2); //set bit 1 to indicate writable
    }

    void do_free(ref_type ref, const char* addr) noexcept override
    {
        typedef std::map<ref_type, char*>::iterator iter;
        if (ref & 0x2) 
            ref -= 2;
        iter i = m_map.find(ref);
        REALM_ASSERT(i != m_map.end());
        char* addr_2 = i->second;
        REALM_ASSERT(addr_2 == addr);
        static_cast<void>(addr_2);
        m_map.erase(i);
        delete[] addr;
    }

    char* do_translate(ref_type ref) const noexcept override
    {
        typedef std::map<ref_type, char*>::const_iterator iter;
        if (ref & 0x2) 
            ref -= 2;
        iter i = m_map.find(ref);
        REALM_ASSERT(i != m_map.end());
        char* addr = i->second;
        return addr;
    }

    bool empty()
    {
        return m_map.empty();
    }

    void clear()
    {
        typedef std::map<ref_type, char*>::const_iterator iter;
        iter end = m_map.end();
        for (iter i = m_map.begin(); i != end; ++i) {
            char* addr = i->second;
            delete[] addr;
        }
        m_map.clear();
    }

#ifdef REALM_DEBUG
    void verify() const override
    {
    }
#endif

private:
    ref_type m_offset;
    std::map<ref_type, char*> m_map;
};

} // anonymous namespace


TEST(DestroyGuard_General)
{
    // Destroy
    {
        bool destroyed_flag = false;
        {
            Foo foo(&destroyed_flag);
            DestroyGuard<Foo> dg(&foo);
            CHECK_EQUAL(&foo, dg.get());
        }
        CHECK(destroyed_flag);
    }
    // Release
    {
        bool destroyed_flag = false;
        {
            Foo foo(&destroyed_flag);
            DestroyGuard<Foo> dg(&foo);
            CHECK_EQUAL(&foo, dg.release());
        }
        CHECK(!destroyed_flag);
    }
    // Reset
    {
        bool destroyed_flag_1 = false;
        bool destroyed_flag_2 = false;
        {
            DestroyGuard<Foo> dg;
            Foo foo_1(&destroyed_flag_1);
            dg.reset(&foo_1);
            Foo foo_2(&destroyed_flag_2);
            dg.reset(&foo_2);
            CHECK(destroyed_flag_1);
        }
        CHECK(destroyed_flag_2);
    }
}


TEST(DestroyGuard_ArrayShallow)
{
    // Test that when DestroyGuard<> is used with Array
    // (`ShallowArrayDestroyGuard`), it works in a shallow fashion.
    FooAlloc alloc;
    Array root(alloc);
    {
        ShallowArrayDestroyGuard dg(&root);
        root.create(Array::type_HasRefs);
        {
            bool context_flag = false;
            MemRef child_mem = Array::create_empty_array(Array::type_Normal, context_flag, alloc);
            int_fast64_t v(child_mem.m_ref);
            root.add(v);
        }
    }
    CHECK(!root.is_attached());
    CHECK(!alloc.empty());
    alloc.clear();
}


TEST(DestroyGuard_ArrayDeep)
{
    // Destroy
    {
        FooAlloc alloc;
        {
            Array arr(alloc);
            arr.create(Array::type_Normal);
            DeepArrayDestroyGuard dg(&arr);
            CHECK_EQUAL(&arr, dg.get());
        }
        CHECK(alloc.empty());
    }
    // Release
    {
        FooAlloc alloc;
        {
            Array arr(alloc);
            arr.create(Array::type_Normal);
            DeepArrayDestroyGuard dg(&arr);
            CHECK_EQUAL(&arr, dg.release());
        }
        CHECK(!alloc.empty());
        alloc.clear();
    }
    // Reset
    {
        FooAlloc alloc;
        {
            Array arr_1(alloc), arr_2(alloc);
            DeepArrayDestroyGuard dg;
            arr_1.create(Array::type_Normal);
            dg.reset(&arr_1);
            arr_2.create(Array::type_Normal);
            dg.reset(&arr_2);
        }
        CHECK(alloc.empty());
    }
    // Deep
    {
        FooAlloc alloc;
        Array root(alloc);
        {
            DeepArrayDestroyGuard dg(&root);
            root.create(Array::type_HasRefs);
            {
                bool context_flag = false;
                MemRef child_mem =
                    Array::create_empty_array(Array::type_Normal, context_flag, alloc);
                int_fast64_t v(child_mem.m_ref);
                root.add(v);
            }
        }
        CHECK(!root.is_attached());
        CHECK(alloc.empty());
    }
}


TEST(DestroyGuard_ArrayRefDeep)
{
    // Destroy
    {
        FooAlloc alloc;
        {
            bool context_flag = false;
            MemRef mem = Array::create_empty_array(Array::type_Normal, context_flag, alloc);
            DeepArrayRefDestroyGuard dg(mem.m_ref, alloc);
            CHECK_EQUAL(mem.m_ref, dg.get());
        }
        CHECK(alloc.empty());
    }
    // Release
    {
        FooAlloc alloc;
        {
            bool context_flag = false;
            MemRef mem = Array::create_empty_array(Array::type_Normal, context_flag, alloc);
            DeepArrayRefDestroyGuard dg(mem.m_ref, alloc);
            CHECK_EQUAL(mem.m_ref, dg.release());
        }
        CHECK(!alloc.empty());
        alloc.clear();
    }
    // Reset
    {
        FooAlloc alloc;
        {
            bool context_flag = false;
            DeepArrayRefDestroyGuard dg(alloc);
            MemRef mem_1 = Array::create_empty_array(Array::type_Normal, context_flag, alloc);
            dg.reset(mem_1.m_ref);
            MemRef mem_2 = Array::create_empty_array(Array::type_Normal, context_flag, alloc);
            dg.reset(mem_2.m_ref);
        }
        CHECK(alloc.empty());
    }
    // Deep
    {
        FooAlloc alloc;
        {
            ref_type root_ref;
            {
                Array root(alloc);
                root.create(Array::type_HasRefs);
                bool context_flag = false;
                MemRef child_mem =
                    Array::create_empty_array(Array::type_Normal, context_flag, alloc);
                int_fast64_t v(child_mem.m_ref);
                root.add(v);
                root_ref = root.get_ref();
            }
            DeepArrayRefDestroyGuard dg(root_ref, alloc);
        }
        CHECK(alloc.empty());
    }
}
