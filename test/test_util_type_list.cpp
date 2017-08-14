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
#ifdef TEST_UTIL_TYPE_LIST

#include <type_traits>
#include <map>
#include <tuple>

#include "test.hpp"

using namespace realm;

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


// In order to check that some static functions are called they must have
// a side-effect on a static variable.
static util::Mutex magicMutex;
static int magic;

struct Person {
    void set_name(std::string name)
    {
        m_name = name;
    }
    void set_age(int age)
    {
        m_age = age;
    }
    void set_married(bool married)
    {
        m_married = married;
    }

    std::string m_name;
    int m_age = 0;
    bool m_married = false;
    std::map<int, Person> m_children;
};

template <class T, int i>
struct DoSomething;

template <int idx>
struct DoSomething<std::string, idx> {
    static void exec()
    {
        magic += idx;
    }
    static void exec(Person* p)
    {
        p->set_name("John Doe");
    }
    static void exec(Person* p, int child)
    {
        p->m_children[child].set_name("John Doe Jr.");
    }
    template <class L>
    static void exec(Person* p, int child, L tuple)
    {
        p->m_children[child].set_name(std::get<idx>(tuple));
    }
};

template <int idx>
struct DoSomething<int, idx> {
    static void exec()
    {
        magic += 2 * idx;
    }
    static void exec(Person* p)
    {
        p->set_age(30);
    }
    static void exec(Person* p, int child)
    {
        p->m_children[child].set_age(10);
    }
    template <class L>
    static void exec(Person* p, int child, L tuple)
    {
        p->m_children[child].set_age(std::get<idx>(tuple));
    }
};

template <int idx>
struct DoSomething<bool, idx> {
    static void exec()
    {
        magic += 3 * idx;
    }
    static void exec(Person* p)
    {
        p->set_married(true);
    }
    static void exec(Person* p, int child)
    {
        p->m_children[child].set_married(false);
    }
    template <class L>
    static void exec(Person* p, int child, L tuple)
    {
        p->m_children[child].set_married(std::get<idx>(tuple));
    }
};

template <class T, int i>
struct NotEqual;

template <int idx>
struct NotEqual<std::string, idx> {
    template <class L>
    static bool exec(Person* p, L tuple)
    {
        return p->m_name != std::get<idx>(tuple);
    }
};

template <int idx>
struct NotEqual<int, idx> {
    template <class L>
    static bool exec(Person* p, L tuple)
    {
        return p->m_age != std::get<idx>(tuple);
    }
};

template <int idx>
struct NotEqual<bool, idx> {
    template <class L>
    static bool exec(Person* p, L tuple)
    {
        return p->m_married != std::get<idx>(tuple);
    }
};


TEST(TypeList_Basic)
{
    Person person;
    auto person_info = std::make_tuple(std::string("Paul"), 20, true);
    auto person_info1 = std::make_tuple(std::string("John Doe"), 30, true);

    using Dummy1 = util::TypeAppend<void, std::string>::type;
    using Dummy2 = util::TypeAppend<Dummy1, int>::type;
    using TypeList = util::TypeAppend<Dummy2, bool>::type;

    {
        util::LockGuard lk(magicMutex);
        magic = 0;
        int type_cnt = util::TypeCount<TypeList>::value;
        CHECK_EQUAL(type_cnt, 3);
        util::ForEachType<TypeList, DoSomething, 1>::exec();
        CHECK_EQUAL(magic, 14); // 1 + 2*2 + 3*3
    }

    util::ForEachType<TypeList, DoSomething, 1>::exec(&person);
    CHECK_EQUAL(person.m_name, "John Doe");
    CHECK_EQUAL(person.m_age, 30);

    util::ForEachType<TypeList, DoSomething>::exec(&person, 1);
    CHECK_EQUAL(person.m_name, "John Doe");
    CHECK_EQUAL(person.m_age, 30);
    CHECK_EQUAL(person.m_children[1].m_name, "John Doe Jr.");
    CHECK_EQUAL(person.m_children[1].m_age, 10);

    util::ForEachType<TypeList, DoSomething>::exec(&person, 2, person_info);
    CHECK_EQUAL(person.m_name, "John Doe");
    CHECK_EQUAL(person.m_age, 30);
    CHECK_EQUAL(person.m_children[1].m_name, "John Doe Jr.");
    CHECK_EQUAL(person.m_children[1].m_age, 10);
    CHECK_EQUAL(person.m_children[2].m_name, "Paul");
    CHECK_EQUAL(person.m_children[2].m_age, 20);
    CHECK_EQUAL(person.m_children[2].m_married, true);

    bool equal = !util::HasType<TypeList, NotEqual>::exec(&person, person_info1);
    CHECK_EQUAL(equal, true);
}

#endif
