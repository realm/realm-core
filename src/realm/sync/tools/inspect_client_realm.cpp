#include <getopt.h>
#include <string>
#include <iostream>
#include <sstream>

#include <realm.hpp>
#include <realm/sync/noinst/client_history_impl.hpp>

namespace {
using namespace realm;

void print_tables(const Group& group)
{
    for (auto table_key : group.get_table_keys()) {
        StringData table_name = group.get_table_name(table_key);
        std::cout << "Table: " << table_name << "\n";
        ConstTableRef table = group.get_table(table_key);
        size_t nrows = table->size();
        std::cout << "  " << nrows << " rows\n";
        for (auto col_key : table->get_column_keys()) {
            StringData column_name = table->get_column_name(col_key);
            DataType column_type = table->get_column_type(col_key);
            std::string column_type_str = get_data_type_name(column_type);
            std::cout << "  " << column_name << ", " << column_type_str;
            if (column_type == type_Link || column_type == type_LinkList) {
                ConstTableRef target_table = table->get_link_target(col_key);
                StringData target_name = target_table->get_name();
                std::cout << ", " << target_name;
            }
            bool has_search_index = table->has_search_index(col_key);
            std::cout << ", " << (has_search_index ? "search_index" : "no_search_index") << "\n";
        }
        std::cout << "\n";
    }
}

void inspect_client_realm(const std::string& path)
{
    auto history = sync::make_client_replication();
    auto sg = DB::create(*history, path);
    ReadTransaction rt{sg};
    const Group& group = rt.get_group();

    print_tables(group);
    std::cout << "\n\n";
}

void usage(char* prog)
{
    std::cerr << "Synopsis: " << prog << " PATH\n";
}

} // namespace
int main(int argc, char* argv[])
{
    if (argc != 2) {
        usage(argv[0]);
        std::exit(EXIT_FAILURE);
    }

    const std::string path = argv[1];

    inspect_client_realm(path);

    return 0;
}
