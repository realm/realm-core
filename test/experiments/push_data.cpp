#include <deque>
#include <string>
#include <sstream>
#include <iostream>

#include <tightdb.hpp>
#include <tightdb/group_shared.hpp>

using namespace std;
using namespace realm;


namespace {

REALM_TABLE_2(MyTable,
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

    Group group(database_file.c_str());
    if (!group.is_valid()) throw runtime_error("Failed to open database");

    {
        if (group.has_table("my_table") && !group.has_table<MyTable>("my_table"))
            throw runtime_error("Table type mismatch");
        MyTable::Ref table = group.get_table<MyTable>("my_table");

        int counter = 0;
        for (int i=0; i<num_reps; ++i) {
            for (size_t j=0; j<positional_args.size(); ++j) {
                table->add(++counter, positional_args[j].c_str());
            }
        }
    }

    group.write("/tmp/xxx");
    rename("/tmp/xxx", database_file.c_str());

    return 0;
}
