#include <cstring>
#include <typeinfo>
#include <limits>
#include <vector>
#include <map>
#include <sstream>
#include <fstream>
#include <iostream>

#include <unistd.h>
#include <sys/wait.h>

#include <tightdb.hpp>
#include <tightdb/impl/destroy_guard.hpp>
#include <tightdb/column_basic.hpp>
#include <tightdb/column_string.hpp>
#include <tightdb/column_string_enum.hpp>
#include <tightdb/column_mixed.hpp>
#include <tightdb/array_binary.hpp>
#include <tightdb/array_string_long.hpp>
#include <tightdb/lang_bind_helper.hpp>
#ifdef TIGHTDB_ENABLE_REPLICATION
#  include <tightdb/replication.hpp>
#  include <tightdb/commit_log.hpp>
#endif

#include "../test.hpp"
#include "../util/demangle.hpp"
#include "../util/random.hpp"
#include "../util/thread_wrapper.hpp"
#include "../util/random.hpp"

using namespace std;
using namespace tightdb;
using namespace tightdb::util;
using namespace tightdb::test_util;
using namespace tightdb::_impl;

TEST(Foo)
{
    SHARED_GROUP_TEST_PATH(path);
    UniquePtr<LangBindHelper::TransactLogRegistry> tlr(getWriteLogs(path));
    UniquePtr<Replication> repl(makeWriteLogCollector(path));
    SharedGroup sg(*repl);
    Group* group = const_cast<Group*>(&sg.begin_read());

    LangBindHelper::promote_to_write(sg, *tlr);

    TableRef class_EmployeeObject = group->get_table("class_EmployeeObject");
    TableRef class_CompanyObject = group->get_table("class_CompanyObject");

    class_EmployeeObject->add_column(tightdb::DataType(0), "age");
    class_CompanyObject->add_column_link(tightdb::DataType(13), "employees", *class_EmployeeObject);

    LangBindHelper::commit_and_continue_as_read(sg);

    LangBindHelper::promote_to_write(sg, *tlr);
    class_EmployeeObject->add_empty_row(2);
    class_CompanyObject->add_empty_row();
    {
        LinkViewRef ll = class_CompanyObject->get_linklist(0,0);
        ll->add(0);
        ll->add(1);
    }
    LangBindHelper::commit_and_continue_as_read(sg);

    LinkViewRef people_in_company = class_CompanyObject->get_linklist(0,0);

    LangBindHelper::promote_to_write(sg, *tlr);
    people_in_company->remove(0);
    LangBindHelper::commit_and_continue_as_read(sg);

    LangBindHelper::promote_to_write(sg, *tlr);
    people_in_company->clear();
    LangBindHelper::commit_and_continue_as_read(sg);
}



// ==15309== Stack overflow in thread 1: can't grow stack to 0x7fe001ffc
/*
TEST(Foo)
{
    SHARED_GROUP_TEST_PATH(path);
    UniquePtr<LangBindHelper::TransactLogRegistry> tlr(getWriteLogs(path));
    UniquePtr<Replication> repl(makeWriteLogCollector(path));
    SharedGroup sg(*repl);
    Group* group = const_cast<Group*>(&sg.begin_read());

    LangBindHelper::promote_to_write(sg, *tlr);

    TableRef class_EmployeeObject = group->get_table("class_EmployeeObject");
    TableRef class_CompanyObject = group->get_table("class_CompanyObject");

    class_EmployeeObject->add_column(tightdb::DataType(2), "name");
    class_EmployeeObject->add_column(tightdb::DataType(0), "age");
    class_EmployeeObject->add_column(tightdb::DataType(1), "hired");
    class_CompanyObject->add_column(tightdb::DataType(2), "name");
    class_CompanyObject->add_column_link(tightdb::DataType(13), "employees", *class_EmployeeObject);

    LangBindHelper::commit_and_continue_as_read(sg);

    LangBindHelper::promote_to_write(sg, *tlr);
    class_EmployeeObject->add_empty_row(2);
    class_CompanyObject->add_empty_row();
    {
        LinkViewRef ll = class_CompanyObject->get_linklist(1,0);
        ll->add(0);
        ll->add(1);
    }
    LangBindHelper::commit_and_continue_as_read(sg);

    LinkViewRef people_in_company = class_CompanyObject->get_linklist(1,0);

    LangBindHelper::promote_to_write(sg, *tlr);
    people_in_company->remove(0);
    LangBindHelper::commit_and_continue_as_read(sg);

    LangBindHelper::promote_to_write(sg, *tlr);
    people_in_company->clear();
    LangBindHelper::commit_and_continue_as_read(sg);
}
*/
