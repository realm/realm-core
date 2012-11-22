#include <iostream>
#include <tightdb.hpp>
#include <tightdb/group_shared.hpp>
#include <tightdb/lang_bind_helper.hpp>

using namespace tightdb;
using namespace std;

namespace {

#define CHECK(v) if (!(v)) cerr << __LINE__ << ": CHECK failed" << endl
#define CHECK_EQUAL(a, b) if ((a)!=(b)) cerr << __LINE__ << ": CHECK_EQUAL failed: " << (a) << " vs " << (b) << endl

} // namespace

int main()
{
    remove("xxx.db");
    remove("xxx.db.lock");
    SharedGroup db("xxx.db");
    CHECK(db.is_valid());
    {
        Group& group = db.begin_write();
        TableRef table = group.get_table("my_table");
        {
            Spec& spec = table->get_spec();
            spec.add_column(COLUMN_TYPE_INT, "alpha");
            spec.add_column(COLUMN_TYPE_BOOL, "beta");
            spec.add_column(COLUMN_TYPE_INT, "gamma");
            spec.add_column(COLUMN_TYPE_DATE, "delta");
            spec.add_column(COLUMN_TYPE_STRING, "epsilon");
            spec.add_column(COLUMN_TYPE_BINARY, "zeta");
            {
                Spec subspec = spec.add_subtable_column("eta");
                subspec.add_column(COLUMN_TYPE_INT, "foo");
                {
                    Spec subsubspec = subspec.add_subtable_column("bar");
                    subsubspec.add_column(COLUMN_TYPE_INT, "value");
                }
            }
            spec.add_column(COLUMN_TYPE_MIXED, "theta");
        }
        table->update_from_spec();
        table->insert_empty_row(0, 1);
    }
    db.commit();

    {
        Group& group = db.begin_write();
        static_cast<void>(group);
    }
    db.commit();

    {
        Group& group = db.begin_write();
        TableRef table = group.get_table("my_table");
        table->set_int(0, 0, 1);
    }
    db.commit();

    {
        Group& group = db.begin_write();
        TableRef table = group.get_table("my_table");
        table->set_int(0, 0, 2);
    }
    db.commit();

    {
        Group& group = db.begin_write();
        TableRef table = group.get_table("my_table");
        table = table->get_subtable(6, 0);
        table->insert_int(0, 0, 0);
        table->insert_subtable(1, 0);
        table->insert_done();
        table = group.get_table("my_table");
        table->set_int(0, 0, 3);
    }
    db.commit();

    {
        Group& group = db.begin_write();
        TableRef table = group.get_table("my_table");
        table->set_int(0, 0, 4);
    }
    db.commit();

    {
        Group& group = db.begin_write();
        TableRef table = group.get_table("my_table");
        table = table->get_subtable(6, 0);
        table = table->get_subtable(1, 0);
        table->insert_empty_row(0, 1);
    }
    db.commit();

    {
        Group& group = db.begin_write();
        TableRef table = group.get_table("my_table");
        table = table->get_subtable(6, 0);
        table = table->get_subtable(1, 0);
        table->insert_empty_row(1, 1);
    }
    db.commit();

    {
        Group& group = db.begin_write();
        TableRef table = group.get_table("my_table");
        table = table->get_subtable(6, 0);
        table = table->get_subtable(1, 0);
        table->set_int(0, 0, 0);
        table = group.get_table("my_table");
        table->set_int(0, 0, 5);
        table = group.get_table("my_table");
        table = table->get_subtable(6, 0);
        table->set_int(0, 0, 1);
    }
    db.commit();

    {
        Group& group = db.begin_write();
        TableRef table = group.get_table("my_table");
        table = table->get_subtable(6, 0);
        table = table->get_subtable(1, 0);
        table->set_int(0, 1, 1);
        table = group.get_table("my_table");
        table->set_int(0, 0, 6);
        table = group.get_table("my_table");
        table = table->get_subtable(6, 0);
        table->set_int(0, 0, 2);
    }
    db.commit();

    return 0;
}
