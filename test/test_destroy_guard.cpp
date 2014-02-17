#include <map>

#include <UnitTest++.h>

#include <tightdb/array.hpp>
#include <tightdb/impl/destroy_guard.hpp>

using namespace std;
using namespace tightdb;
using namespace tightdb::_impl;


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
        m_baseline = 8;
    }

    ~FooAlloc() TIGHTDB_NOEXCEPT
    {
    }

    MemRef do_alloc(size_t size) TIGHTDB_OVERRIDE
    {
        ref_type ref = m_offset;
        char*& addr = m_map[ref]; // Throws
        TIGHTDB_ASSERT(!addr);
        addr = new char[size]; // Throws
        m_offset += size;
        return MemRef(addr, ref);
    }

    void do_free(ref_type ref, const char* addr) TIGHTDB_NOEXCEPT TIGHTDB_OVERRIDE
    {
        typedef map<ref_type, char*>::iterator iter;
        iter i = m_map.find(ref);
        TIGHTDB_ASSERT(i != m_map.end());
        char* addr_2 = i->second;
        TIGHTDB_ASSERT(addr_2 == addr);
        static_cast<void>(addr_2);
        m_map.erase(i);
        delete[] addr;
    }

    char* do_translate(ref_type ref) const TIGHTDB_NOEXCEPT TIGHTDB_OVERRIDE
    {
        typedef map<ref_type, char*>::const_iterator iter;
        iter i = m_map.find(ref);
        TIGHTDB_ASSERT(i != m_map.end());
        char* addr = i->second;
        return addr;
    }

    bool empty()
    {
        return m_map.empty();
    }

    void clear()
    {
        typedef map<ref_type, char*>::const_iterator iter;
        iter end = m_map.end();
        for (iter i = m_map.begin(); i != end; ++i) {
            char* addr = i->second;
            delete[] addr;
        }
        m_map.clear();
    }

#ifdef TIGHTDB_DEBUG
    void Verify() const TIGHTDB_OVERRIDE
    {
    }
#endif

private:
    ref_type m_offset;
    map<ref_type, char*> m_map;
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
            ref_type child_ref = Array::create_empty_array(Array::type_Normal, alloc);
            int_fast64_t v(child_ref);
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
                ref_type child_ref = Array::create_empty_array(Array::type_Normal, alloc);
                int_fast64_t v(child_ref);
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
            ref_type ref = Array::create_empty_array(Array::type_Normal, alloc);
            DeepArrayRefDestroyGuard dg(ref, alloc);
            CHECK_EQUAL(ref, dg.get());
        }
        CHECK(alloc.empty());
    }
    // Release
    {
        FooAlloc alloc;
        {
            ref_type ref = Array::create_empty_array(Array::type_Normal, alloc);
            DeepArrayRefDestroyGuard dg(ref, alloc);
            CHECK_EQUAL(ref, dg.release());
        }
        CHECK(!alloc.empty());
        alloc.clear();
    }
    // Reset
    {
        FooAlloc alloc;
        {
            DeepArrayRefDestroyGuard dg(alloc);
            ref_type ref_1 = Array::create_empty_array(Array::type_Normal, alloc);
            dg.reset(ref_1);
            ref_type ref_2 = Array::create_empty_array(Array::type_Normal, alloc);
            dg.reset(ref_2);
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
                ref_type child_ref = Array::create_empty_array(Array::type_Normal, alloc);
                int_fast64_t v(child_ref);
                root.add(v);
                root_ref = root.get_ref();
            }
            DeepArrayRefDestroyGuard dg(root_ref, alloc);
        }
        CHECK(alloc.empty());
    }
}
