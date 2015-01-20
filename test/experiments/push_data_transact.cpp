#include <deque>
#include <string>
#include <sstream>
#include <iostream>

#include <tightdb.hpp>
#include <tightdb/group_shared.hpp>

using namespace std;
using namespace tightdb;


namespace {

TIGHTDB_TABLE_2(MyTable,
                number, Int,
                text, String)

} // anonymous namespace


int main(int argc, const char* const argv[])
{
    string database_file  = "/tmp/push_data.tdb";

    deque<string> positional_args;
    for (int i=1; i<argc; ++i) {
        const string arg = argv[i];
        if (arg.size() < 2 || arg.substr(0,2) != "--") {
            positional_args.push_back(arg);
            continue;
        }

        if (arg == "--database-file") {
            if (i+1 < argc) {
                database_file = argv[++i];
                continue;
            }
        }
        goto bad_command_line;
    }

    if (positional_args.size() < 2) {
    bad_command_line:
        cerr <<
            "ERROR: Bad command line.\n\n"
            "Synopsis: "<<argv[0]<<"  NUM-REPS  TEXT...\n\n"
            "Options:\n"
            "  --database-file STRING   (default: \""<<database_file<<"\")\n";
        return 1;
    }

    int num_reps;
    {
        istringstream in(positional_args[0]);
        in >> noskipws >> num_reps;
        if (!in || !in.eof()) goto bad_command_line;
        positional_args.pop_front();
    }

    SharedGroup db(database_file.c_str());
    if (!db.is_valid()) throw RuntimeError("Failed to open database");

    {
        Group& group = db.begin_write();
        if (group.has_table("my_table") && !group.has_table<MyTable>("my_table"))
            throw RuntimeError("Table type mismatch");
        MyTable::Ref table = group.get_table<MyTable>("my_table");

        int counter = 0;
        for (int i=0; i<num_reps; ++i) {
            for (size_t j=0; j<positional_args.size(); ++j) {
                table->add(++counter, positional_args[j].c_str());
            }
        }
    }

    db.commit(); // FIXME: Must roll back on error

    return 0;
}
