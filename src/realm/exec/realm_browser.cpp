#include <realm.hpp>
#include <iostream>

using namespace realm;

bool get_table_ndx(size_t& ndx)
{
    std::cout << "Table ndx? ";
    std::string inp;
    getline(std::cin, inp);
    const char* buf = inp.c_str();
    char* endp;
    ndx = strtol(buf, &endp, 0);
    return *endp == '\0';
}

bool get_range(size_t size, size_t& begin, size_t& end)
{
    std::cout << "Size " << size << ". Range? ";
    std::string inp;
    getline(std::cin, inp);
    if (inp.size() == 0) {
        begin = 0;
        end = size;
        return true;
    }
    const char* buf = inp.c_str();
    char* endp;
    begin = strtol(buf, &endp, 0);
    if (*endp == '\0') {
        end = begin + 1;
        return begin < size;
    }
    if (*endp == '-') {
        ++endp;
        if (*endp != '\0')
            end = strtol(endp, &endp, 0);
        else
            end = size;
        if (*endp == '\0') {
            return begin < end && end <= size;
        }
    }
    return false;
}

void print_objects(ConstTableRef table, size_t begin, size_t end)
{
    printf("                 Object key");
    auto col_keys = table->get_column_keys();
    for (auto col : col_keys) {
        printf("%21s", table->get_column_name(col).data());
    }
    printf("\n");
    for (size_t row = begin; row < end; row++) {
        printf("%5zu ", row);
        ConstObj obj = table->get_object(row);
        printf(" %20zx", obj.get_key().value);
        for (auto col : col_keys) {
            switch (table->get_column_type(col)) {
                case type_Int:
                    printf(" %20ld", obj.get<Int>(col));
                    break;
                case type_Bool:
                    printf(" %20s", obj.get<Bool>(col) ? "true" : "false");
                    break;
                case type_Float:
                    break;
                case type_Double:
                    break;
                case type_String: {
                    auto str = obj.get<String>(col);
                    auto sz = str.size();
                    if (sz > 200) {
                        printf("  String sz: %8zu", sz);
                    }
                    else {
                        printf(" %20s", str.data());
                    }
                    break;
                }
                case type_Timestamp:
                    printf(" %20ld", obj.get<Timestamp>(col).get_seconds());
                    break;
                default:
                    printf(" ********************");
                    break;
            }
        }
        printf("\n");
    }
}

int main(int argc, char const* argv[])
{
    if (argc > 1) {
        Group g(argv[1]);
        auto table_keys = g.get_table_keys();
        for (size_t i = 0; i < table_keys.size(); i++) {
            std::cout << i << ". " << g.get_table_name(table_keys[i]) << " ";
        }
        std::cout << std::endl;
        size_t table_ndx;
        while (get_table_ndx(table_ndx)) {
            auto table = g.get_table(table_keys[table_ndx]);
            auto sz = table->size();
            size_t begin;
            size_t end;
            while (get_range(sz, begin, end)) {
                print_objects(table, begin, end);
            }
        }
    }
    return 0;
}
