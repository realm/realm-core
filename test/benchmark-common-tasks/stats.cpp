#include <cstdlib>
#include <stdio.h>
#include <string>
#include <iostream>

#include "realm.hpp"

using namespace realm;

void print_useage(std::string program_name) {
    std::cout << "This program will create a realm file with the \n"
        << "specified name (first argument) in the current directory \n"
        << "with a binary blob of the specified size (second argument).\n"
        << "If a file with the same name exists, it will be overwritten.\n"
        << "For example: \n"
        << program_name << " simple_realm500.realm 500\n";
}

void delete_file_if_exists(std::string file_name)
{
    remove(file_name.c_str());
}

void create_realm_with_data(std::string file_name, size_t data_size)
{
    delete_file_if_exists(file_name);
    SharedGroup sg(file_name);
    Group& g = sg.begin_write();
    TableRef table = g.add_table("t0");
    table->add_column(type_Binary, "bin_col_0");
    table->add_empty_row(1);
    std::string blob(data_size, 'a');
    BinaryData binary(blob.data(), data_size);
    table->set_binary(0, 0, binary); // copies data into realm
    sg.commit();
    sg.close();
}

int main(int argc, const char* argv[])
{
    if (argc == 3) {
        std::string file_name = argv[1];
        long int data_size = atol(argv[2]);
        if (data_size < 0) {
            std::cout << "Error: Data size (second parameter) cannot be negative.\n";
            print_useage(argv[0]);
            return 1;
        }
        create_realm_with_data(file_name, static_cast<size_t>(data_size));
    } else {
        print_useage(argv[0]);
        return 0;
    }
    return 0;
}
