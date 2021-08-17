#include "testsettings.hpp"

#ifdef TEST_UTIL_INTRUSIVE_PTR

#include <functional>

#include "realm/util/intrusive_ptr.hpp"

#include "test.hpp"

namespace realm::util {
namespace {

struct TestType {
    int count = 0;
};

void intrusive_ptr_add_ref(TestType* ptr)
{
    ptr->count += 1;
}

void intrusive_ptr_release(TestType* ptr)
{
    ptr->count -= 1;
}

TEST(Util_IntrusivePtr)
{

    // Construct an IntrusivePtr from a pointer, this should increment the ref count to 1
    {
        TestType obj;
        IntrusivePtr<TestType> ptr(&obj);
        CHECK(obj.count == 1);
        CHECK(ptr);
    }

    // Construct an IntrusivePtr without incrementing the ref count - this should still decrement the
    // ref count when the ptr gets destroyed.
    {
        TestType obj;
        {
            IntrusivePtr<TestType> ptr1_noadd(&obj, false);
            CHECK(obj.count == 0);
        }
        CHECK(obj.count == -1);
    }

    // Check move construction
    {
        TestType obj_to_move;
        IntrusivePtr<TestType> ptr_to_move(&obj_to_move);
        IntrusivePtr<TestType> ptr_moved_to(std::move(ptr_to_move));
        CHECK(obj_to_move.count == 1);
        // This should also invalidate the moved-from IntrusivePtr.
        CHECK(!ptr_to_move);
        CHECK(ptr_to_move == nullptr);
        CHECK(ptr_moved_to == &obj_to_move);
    }

    // Check move assignment.
    {
        TestType obj_to_move;
        IntrusivePtr<TestType> ptr_to_move(&obj_to_move);
        IntrusivePtr<TestType> ptr_assign_moved_to;
        ptr_assign_moved_to = std::move(ptr_to_move);
        CHECK(!ptr_to_move);
        CHECK(ptr_assign_moved_to);
        CHECK(ptr_assign_moved_to == &obj_to_move);
    }

    // check std::swap
    {
        TestType obj1, obj2;
        IntrusivePtr ptr1(&obj1), ptr2(&obj2);
        CHECK(obj2.count == 1);
        std::swap(ptr1, ptr2);
        CHECK(ptr1 == IntrusivePtr(&obj2));
        CHECK(ptr2 == &obj1);
    }

    // check accessors
    {
        TestType obj;
        IntrusivePtr ptr(&obj);
        CHECK(ptr == &obj);
        CHECK(ptr.get() == &obj);
        CHECK(ptr->count == 1);
        CHECK((*ptr).count == 1);
    }

    // Check that copy-construction works
    {
        TestType obj;
        IntrusivePtr ptr_to_copy(&obj);
        IntrusivePtr ptr_to_copy_to(ptr_to_copy);
        CHECK(ptr_to_copy->count == 2);
        CHECK(ptr_to_copy == ptr_to_copy_to);
        CHECK(ptr_to_copy == &obj);
    }

    // Check that copy-assignment works.
    {
        TestType obj;
        IntrusivePtr ptr_to_copy(&obj);
        IntrusivePtr<TestType> ptr_to_copy_to;
        ptr_to_copy_to = ptr_to_copy;
        CHECK(obj.count == 2);
    }

    // Check that std::hash integration works.
    {
        TestType obj;
        IntrusivePtr ptr_to_hash(&obj);
        size_t hash_of_intrusive_ptr = std::hash<IntrusivePtr<TestType>>{}(ptr_to_hash);
        size_t hash_of_raw_ptr = std::hash<TestType*>{}(&obj);
        CHECK(hash_of_raw_ptr == hash_of_intrusive_ptr);
    }

    // Check that release works
    {
        TestType obj_to_release;
        {
            IntrusivePtr to_release(&obj_to_release);
            auto released = to_release.release();
            CHECK(!to_release);
            CHECK(released == &obj_to_release);
        }
        CHECK(obj_to_release.count == 1);
    }

    // check that reset works
    {
        TestType obj1, obj2;
        IntrusivePtr ptr(&obj1);
        CHECK(ptr);
        CHECK(ptr == &obj1);
        ptr.reset();
        CHECK(obj1.count == 0);
        CHECK(!ptr);
        CHECK(nullptr == ptr);
        ptr.reset(&obj1);
        CHECK(obj1.count == 1);
        CHECK(ptr);
        CHECK(ptr == &obj1);
        ptr.reset(&obj2, false);
        CHECK(obj2.count == 0);
        CHECK(ptr);
        CHECK(ptr == &obj2);
    }
}

} // namespace
} // namespace realm::util

#endif
