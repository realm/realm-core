#include <realm.hpp>
#include <iostream>
#include <ctime>

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
    printf("      ");
    for (size_t col = 0; col < table->get_column_count(); col++) {
        printf("%21s", table->get_column_name(col).data());
    }
    printf("\n");
    for (size_t row = begin; row < end; row++) {
        printf("%5zu ", row);
        for (size_t col = 0; col < table->get_column_count(); col++) {
            if (table->is_null(col, row)) {
                printf("                 null");
                continue;
            }
            switch (table->get_column_type(col)) {
                case type_Int:
                    printf(" %20ld", table->get_int(col, row));
                    break;
                case type_Bool:
                    printf(" %20s", table->get_bool(col, row) ? "true" : "false");
                    break;
                case type_Float:
                    break;
                case type_Double:
                    break;
                case type_String: {
                    std::string str = table->get_string(col, row);
                    if (str.size() > 20) {
                        str = str.substr(0, 17) + "...";
                    }
                    printf(" %20s", str.c_str());
                    break;
                }
                case type_Timestamp: {
                    auto value = table->get_timestamp(col, row);
                    auto seconds = value.get_seconds();
                    auto tm = gmtime(&seconds);
                    printf("  %4d-%02d-%02d %02d:%02d:%02d", tm->tm_year + 1900, tm->tm_mon + 1, tm->tm_mday,
                           tm->tm_hour, tm->tm_min, tm->tm_sec);
                    break;
                }
                case type_Link: {
                    std::string link = "[" + std::to_string(table->get_link(col, row)) + "]";
                    printf(" %20s", link.c_str());
                    break;
                }
                case type_LinkList: {
                    std::string links = "[";
                    auto lv = table->get_linklist(col, row);
                    auto sz = lv->size();
                    if (sz > 0) {
                        links += std::to_string(lv->m_row_indexes.get(0));
                        for (size_t i = 1; i < sz; i++) {
                            links += ("," + std::to_string(lv->m_row_indexes.get(i)));
                        }
                    }
                    links += "]";
                    if (links.size() > 20) {
                        links = links.substr(0, 17) + "...";
                    }

                    printf(" %20s", links.c_str());
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
        g.verify();
        auto nb_tables = g.size();
        for (size_t i = 0; i < nb_tables; i++) {
            std::cout << i << ". " << g.get_table_name(i) << " ";
        }
        std::cout << std::endl;
        size_t table_ndx;
        while (get_table_ndx(table_ndx)) {
            auto table = g.get_table(table_ndx);
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
