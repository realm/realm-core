#include <realm.hpp>
#include <iostream>
#include <ctime>
#include "realm/util/time.hpp"

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
        Obj obj = table->get_object(row);
        printf(" %20zx", obj.get_key().value);
        for (auto col : col_keys) {
            auto col_type = table->get_column_type(col);
            if (table->get_column_attr(col).test(col_attr_Nullable) && obj.is_null(col)) {
                printf("               <null>");
                continue;
            }
            if (table->get_column_attr(col).test(col_attr_List) && col_type != type_LinkList) {
                printf("               <list>");
                continue;
            }
            switch (col_type) {
                case type_Int:
                    printf(" %20ld", obj.get<Int>(col));
                    break;
                case type_Bool:
                    printf(" %20s", obj.get<Bool>(col) ? "true" : "false");
                    break;
                case type_Float:
                    printf(" %20f", obj.get<Float>(col));
                    break;
                case type_Double:
                    printf(" %20f", obj.get<Double>(col));
                    break;
                case type_String: {
                    std::string str = obj.get<String>(col);
                    if (str.size() == 0) {
                        str = "<empty>";
                    }
                    if (str.size() > 20) {
                        str = str.substr(0, 17) + "...";
                    }
                    printf(" %20s", str.c_str());
                    break;
                }
                case type_Binary: {
                    auto bin = obj.get<Binary>(col);
                    printf("   bin size: %8zu", bin.size());
                    break;
                }
                case type_Timestamp: {
                    auto value = obj.get<Timestamp>(col);
                    auto seconds = time_t(value.get_seconds());
                    auto tm = util::gmtime(seconds);
                    printf("  %4d-%02d-%02d %02d:%02d:%02d", tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour,
                           tm.tm_min, tm.tm_sec);
                    break;
                }
                case type_Link: {
                    printf("      -> %12zx", obj.get<ObjKey>(col).value);
                    break;
                }
                case type_LinkList: {
                    std::stringstream links;
                    links << "[" << std::hex;
                    auto lv = obj.get_linklist(col);
                    auto sz = lv.size();
                    if (sz > 0) {
                        links << lv.get(0).value;
                        for (size_t i = 1; i < sz; i++) {
                            links << "," << lv.get(i).value;
                        }
                    }
                    links << "]" << std::dec;
                    std::string str = links.str();
                    if (str.size() > 20) {
                        str = str.substr(0, 17) + "...";
                    }

                    printf(" %20s", str.c_str());
                    break;
                }
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
