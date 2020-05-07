#include <cstdint>
#include <cstring>
#include <limits>
#include <string>
#include <locale>
#include <sstream>
#include <iostream>
#include <iomanip>

#include <realm/util/string_view.hpp>
#include <realm/util/optional.hpp>
#include <realm/util/buffer_stream.hpp>
#include <realm/util/quote.hpp>
#include <realm/util/timestamp_formatter.hpp>
#include <realm/util/load_file.hpp>
#include <realm/group.hpp>
#include <realm/sync/version.hpp>

using namespace realm;

namespace {

template <class T>
std::string format_num_something(T num, const char* singular_form, const char* plural_form,
                                 std::locale loc = std::locale{})
{
    using lim = std::numeric_limits<T>;
    bool need_singular = (num == T(1) || (lim::is_signed && num == T(-1)));
    const char* form = (need_singular ? singular_form : plural_form);
    std::ostringstream out;
    out.imbue(loc);
    out << num << " " << form;
    return std::move(out).str();
}

std::string format_num_bytes(std::size_t num)
{
    return format_num_something(num, "byte", "bytes");
}

std::string format_num_rows(std::size_t num)
{
    return format_num_something(num, "row", "rows");
}

std::string format_num_links(std::size_t num)
{
    return format_num_something(num, "link", "links");
}


using TextColumn = std::vector<std::string>;

class Formatter {
public:
    Formatter(std::size_t limit, std::size_t offset, std::size_t max_string_size);

    std::string format_data_type(const Table&, ColKey col_ndx);

    template <class T>
    std::string format_value(const T&);

    std::string format_string(StringData);
    std::string format_binary(BinaryData);
    std::string format_timestamp(Timestamp);
    std::string format_object_id(ObjectId);
    std::string format_decimal(Decimal128);
    std::string format_list(ConstLstBase&);
    std::string format_link(ObjKey);
    std::string format_link_list(const ConstLnkLst&);

    template <DataType>
    std::string format_cell(ConstObj& obj, ColKey col_key);
    std::string format_cell_list(ConstObj& obj, ColKey col_key);

    template <DataType>
    void format_column(const Table&, ColKey col_ndx, TextColumn&);
    void format_column_list(const Table&, ColKey col_ndx, TextColumn&);

private:
    static constexpr const char* s_ellipsis = "...";
    const std::size_t m_limit;
    const std::size_t m_offset;
    const std::size_t m_max_string_size;
    util::ResettableExpandableBufferOutputStream m_out;
    util::TimestampFormatter m_timestamp_formatter;

    static util::TimestampFormatter::Config get_timestamp_formatter_config();
};

inline Formatter::Formatter(std::size_t limit, std::size_t offset, std::size_t max_string_size)
    : m_limit{limit}
    , m_offset{offset}
    , m_max_string_size{max_string_size}
    , m_timestamp_formatter{get_timestamp_formatter_config()}
{
    m_out << std::boolalpha;
}

std::string Formatter::format_data_type(const Table& table, ColKey col_ndx)
{
    DataType type = table.get_column_type(col_ndx);
    std::string str = realm::get_data_type_name(type);
    if (Table::is_link_type(ColumnType(type))) {
        ConstTableRef target_table = table.get_link_target(col_ndx);
        std::string prefix = target_table->is_embedded() ? "embedded " : "";
        str = prefix + str + " -> " + format_string(target_table->get_name());
    }
    return str;
}

template <class T>
std::string Formatter::format_value(const T& value)
{
    m_out.reset();
    m_out << value;
    return std::string{m_out.data(), m_out.size()};
}

std::string Formatter::format_string(StringData str)
{
    m_out.reset();
    util::StringView str_2{str.data(), str.size()};
    bool truncate = (str.size() > m_max_string_size);
    if (truncate) {
        m_out << util::quoted(str_2.substr(0, m_max_string_size)) << s_ellipsis;
    }
    else {
        m_out << util::quoted(str_2);
    }
    return std::string{m_out.data(), m_out.size()};
}

std::string Formatter::format_binary(BinaryData bin)
{
    m_out.reset();
    m_out << format_num_bytes(bin.size());
    return std::string{m_out.data(), m_out.size()};
}

std::string Formatter::format_timestamp(Timestamp timestamp)
{
    std::time_t time = std::time_t(timestamp.get_seconds());
    long nanoseconds = long(timestamp.get_nanoseconds());
    return std::string(m_timestamp_formatter.format(time, nanoseconds));
}

std::string Formatter::format_object_id(ObjectId id)
{
    return id.to_string();
}

std::string Formatter::format_decimal(Decimal128 id)
{
    return id.to_string();
}

std::string Formatter::format_list(ConstLstBase& list)
{
    m_out.reset();
    m_out << format_num_rows(list.size());
    return std::string{m_out.data(), m_out.size()};
}

std::string Formatter::format_link(ObjKey key)
{
    m_out.reset();
    m_out << "\\" << key.value;
    return std::string{m_out.data(), m_out.size()};
}

std::string Formatter::format_link_list(const ConstLnkLst& list)
{
    m_out.reset();
    m_out << format_num_links(list.size());
    return std::string{m_out.data(), m_out.size()};
}

template <DataType>
inline std::string Formatter::format_cell(ConstObj&, ColKey)
{
    return "unknown";
}

template <>
inline std::string Formatter::format_cell<type_Int>(ConstObj& obj, ColKey col_key)
{
    return format_value(obj.get<Int>(col_key));
}

template <>
inline std::string Formatter::format_cell<type_Bool>(ConstObj& obj, ColKey col_key)
{
    return format_value(obj.get<bool>(col_key));
}

template <>
inline std::string Formatter::format_cell<type_Float>(ConstObj& obj, ColKey col_key)
{
    return format_value(obj.get<float>(col_key));
}

template <>
inline std::string Formatter::format_cell<type_Double>(ConstObj& obj, ColKey col_key)
{
    return format_value(obj.get<double>(col_key));
}

template <>
inline std::string Formatter::format_cell<type_String>(ConstObj& obj, ColKey col_key)
{
    return format_string(obj.get<String>(col_key));
}

template <>
inline std::string Formatter::format_cell<type_Binary>(ConstObj& obj, ColKey col_key)
{
    return format_binary(obj.get<Binary>(col_key));
}

template <>
inline std::string Formatter::format_cell<type_Timestamp>(ConstObj& obj, ColKey col_key)
{
    return format_timestamp(obj.get<Timestamp>(col_key));
}

template <>
inline std::string Formatter::format_cell<type_ObjectId>(ConstObj& obj, ColKey col_key)
{
    return format_object_id(obj.get<ObjectId>(col_key));
}

template <>
inline std::string Formatter::format_cell<type_Decimal>(ConstObj& obj, ColKey col_key)
{
    return format_decimal(obj.get<Decimal128>(col_key));
}

template <>
inline std::string Formatter::format_cell<type_Link>(ConstObj& obj, ColKey col_key)
{
    return format_link(obj.get<ObjKey>(col_key));
}

template <>
inline std::string Formatter::format_cell<type_LinkList>(ConstObj& obj, ColKey col_key)
{
    auto ll = obj.get_linklist(col_key);
    return format_link_list(ll);
}

inline std::string Formatter::format_cell_list(ConstObj& obj, ColKey col_key)
{
    auto ll = obj.get_listbase_ptr(col_key);
    return format_list(*ll);
}

template <DataType type>
void Formatter::format_column(const Table& table, ColKey col_ndx, TextColumn& col)
{
    std::size_t end;
    std::size_t n = table.size();
    if (m_offset > n) {
        end = m_offset;
    }
    else if (m_limit == 0 || m_limit > n - m_offset) {
        end = n;
    }
    else {
        end = m_offset + m_limit;
    }
    for (std::size_t i = m_offset; i < end; ++i) {
        ConstObj obj = table.get_object(i);
        if (obj.is_null(col_ndx)) {
            col.push_back("null");
        }
        else {
            col.push_back(format_cell<type>(obj, col_ndx));
        }
    }
}

void Formatter::format_column_list(const Table& table, ColKey col_ndx, TextColumn& col)
{
    std::size_t end;
    std::size_t n = table.size();
    if (m_offset > n) {
        end = m_offset;
    }
    else if (m_limit == 0 || m_limit > n - m_offset) {
        end = n;
    }
    else {
        end = m_offset + m_limit;
    }
    for (std::size_t i = m_offset; i < end; ++i) {
        ConstObj obj = table.get_object(i);
        col.push_back(format_cell_list(obj, col_ndx));
    }
}

util::TimestampFormatter::Config Formatter::get_timestamp_formatter_config()
{
    util::TimestampFormatter::Config config;
    config.precision = util::TimestampFormatter::Precision::nanoseconds;
    return config;
}

} // unnamed namespace


int main(int argc, char* argv[])
{
    std::string realm_path;
    util::Optional<std::string> table_name;
    std::vector<std::string> column_names;
    std::size_t limit = 0;
    std::size_t offset = 0;
    std::size_t max_string_size = 30;
    std::string encryption_key;

    // Process command line
    {
        const char* prog = argv[0];
        --argc;
        ++argv;
        bool error = false;
        bool help = false;
        bool version = false;
        int argc_2 = 0;
        int i = 0;
        char* arg = nullptr;
        auto get_string_value = [&](std::string& var) {
            if (i < argc) {
                var = argv[i++];
                return true;
            }
            return false;
        };
        auto get_parsed_value_with_check = [&](auto& var, auto check_val) {
            std::string str_val;
            if (get_string_value(str_val)) {
                std::istringstream in(str_val);
                in.imbue(std::locale::classic());
                in.unsetf(std::ios_base::skipws);
                using value_type = typename std::remove_reference<decltype(var)>::type;
                value_type val = value_type{};
                in >> val;
                if (in && in.eof() && check_val(val)) {
                    var = val;
                    return true;
                }
            }
            return false;
        };
        auto get_parsed_value = [&](auto& var) {
            return get_parsed_value_with_check(var, [](auto) {
                return true;
            });
        };
        while (i < argc) {
            arg = argv[i++];
            if (arg[0] != '-') {
                argv[argc_2++] = arg;
                continue;
            }
            if (std::strcmp(arg, "-h") == 0 || std::strcmp(arg, "--help") == 0) {
                help = true;
                continue;
            }
            else if (std::strcmp(arg, "-l") == 0 || std::strcmp(arg, "--limit") == 0) {
                if (get_parsed_value(limit))
                    continue;
            }
            else if (std::strcmp(arg, "-o") == 0 || std::strcmp(arg, "--offset") == 0) {
                if (get_parsed_value(offset))
                    continue;
            }
            else if (std::strcmp(arg, "-m") == 0 || std::strcmp(arg, "--max-string-size") == 0) {
                if (get_parsed_value(max_string_size))
                    continue;
            }
            else if (std::strcmp(arg, "-e") == 0 || std::strcmp(arg, "--encryption-key") == 0) {
                if (get_string_value(encryption_key))
                    continue;
            }
            else if (std::strcmp(arg, "-v") == 0 || std::strcmp(arg, "--version") == 0) {
                version = true;
                continue;
            }
            std::cerr << "ERROR: Bad or missing value for option: " << arg << "\n";
            error = true;
        }
        argc = argc_2;

        i = 0;
        if (!get_string_value(realm_path)) {
            error = true;
        }
        else if (i < argc) {
            std::string str;
            if (!get_string_value(str)) {
                error = true;
            }
            else {
                table_name = str;
                while (i < argc) {
                    if (!get_string_value(str)) {
                        error = true;
                        break;
                    }
                    column_names.push_back(str);
                }
            }
        }

        if (help) {
            std::cerr << "Synopsis: " << prog
                      << " <realm file> [<table> [<column>...]]\n"
                         "\n"
                         "Options:\n"
                         "  -h, --help           Display command-line synopsis followed by the list of\n"
                         "                       available options.\n"
                         "  -l, --limit          Maximum number of rows to dump when dumping contents of\n"
                         "                       a table. Default is 0, which means unlimited.\n"
                         "  -o, --offset         The number of inital rows to skip when dumping contents\n"
                         "                       of a table. Default is zero.\n"
                         "  -m, --max-string-size  Truncate strings longer than this value. Default is\n"
                         "                       30.\n"
                         "  -e, --encryption-key  The file-system path of a file containing a 64-byte\n"
                         "                       encryption key to be used for accessing the specified\n"
                         "                       Realm file.\n"
                         "  -v, --version        Show the version of the Realm Sync release that this\n"
                         "                       command belongs to.\n";
            return EXIT_SUCCESS;
        }

        if (version) {
            const char* build_mode;
#if REALM_DEBUG
            build_mode = "Debug";
#else
            build_mode = "Release";
#endif
            std::cerr << "RealmSync/" REALM_SYNC_VER_STRING " (build_mode=" << build_mode << ")\n";
            return EXIT_SUCCESS;
        }

        if (error) {
            std::cerr << "ERROR: Bad command line.\n"
                         "Try `"
                      << prog << " --help`\n";
            return EXIT_FAILURE;
        }
    }

    std::vector<TextColumn> columns;
    bool first_row_is_header = false;
    Formatter formatter{limit, offset, max_string_size};
    {
        Group::OpenMode open_mode = Group::mode_ReadOnly;
        std::string encryption_key_2;
        const char* encryption_key_3 = nullptr;
        if (!encryption_key.empty()) {
            encryption_key_2 = util::load_file(encryption_key);
            encryption_key_3 = encryption_key_2.data();
        }
        const Group group{realm_path, encryption_key_3, open_mode};
        if (table_name) {
            ConstTableRef table = group.get_table(*table_name);
            if (!table) {
                std::cout << "ERROR: No such table\n";
                return EXIT_FAILURE;
            }
            if (!column_names.empty()) {
                first_row_is_header = true;
                std::size_t num_cols = column_names.size();
                table->get_column_count();
                for (std::size_t i = 0; i < num_cols; ++i) {
                    ColKey col_ndx = table->get_column_key(column_names[i]);
                    if (!col_ndx) {
                        std::cout << "ERROR: No such column\n";
                        return EXIT_FAILURE;
                    }
                    TextColumn col;
                    col.push_back(formatter.format_string(table->get_column_name(col_ndx)));
                    if (col_ndx.get_attrs().test(col_attr_List)) {
                        formatter.format_column_list(*table, col_ndx, col);
                    }
                    else {
                        switch (table->get_column_type(col_ndx)) {
                            case type_Int:
                                formatter.format_column<type_Int>(*table, col_ndx, col);
                                break;
                            case type_Bool:
                                formatter.format_column<type_Bool>(*table, col_ndx, col);
                                break;
                            case type_Float:
                                formatter.format_column<type_Float>(*table, col_ndx, col);
                                break;
                            case type_Double:
                                formatter.format_column<type_Double>(*table, col_ndx, col);
                                break;
                            case type_String:
                                formatter.format_column<type_String>(*table, col_ndx, col);
                                break;
                            case type_Binary:
                                formatter.format_column<type_Binary>(*table, col_ndx, col);
                                break;
                            case type_Timestamp:
                                formatter.format_column<type_Timestamp>(*table, col_ndx, col);
                                break;
                            case type_ObjectId:
                                formatter.format_column<type_ObjectId>(*table, col_ndx, col);
                                break;
                            case type_Decimal:
                                formatter.format_column<type_Decimal>(*table, col_ndx, col);
                                break;
                            case type_Link:
                                formatter.format_column<type_Link>(*table, col_ndx, col);
                                break;
                            case type_LinkList:
                                formatter.format_column<type_LinkList>(*table, col_ndx, col);
                                break;
                            case type_OldDateTime:
                            case type_OldTable:
                            case type_OldMixed:
                                break;
                        }
                    }
                    columns.push_back(std::move(col));
                }
            }
            else {
                first_row_is_header = true;
                TextColumn col_1, col_2, col_3, col_4;
                col_1.push_back("Column name");
                col_2.push_back("Column type");
                col_3.push_back("Nullable");
                col_4.push_back("Indexed");
                auto col_keys = table->get_column_keys();
                for (auto col : col_keys) {
                    col_1.push_back(formatter.format_string(table->get_column_name(col)));
                    col_2.push_back(formatter.format_data_type(*table, col));
                    col_3.push_back(formatter.format_value(table->is_nullable(col)));
                    col_4.push_back(formatter.format_value(table->has_search_index(col)));
                }
                columns.push_back(std::move(col_1));
                columns.push_back(std::move(col_2));
                columns.push_back(std::move(col_3));
                columns.push_back(std::move(col_4));
            }
        }
        else {
            first_row_is_header = true;
            TextColumn col_1, col_2;
            col_1.push_back("Table name");
            col_2.push_back("Number of rows");
            auto table_keys = group.get_table_keys();
            for (auto tk : table_keys) {
                ConstTableRef table = group.get_table(tk);
                col_1.push_back(formatter.format_string(table->get_name()));
                col_2.push_back(formatter.format_value(table->size()));
            }
            columns.push_back(std::move(col_1));
            columns.push_back(std::move(col_2));
        }
    }

    std::size_t max_num_rows = (first_row_is_header ? 1 : 0);
    std::size_t num_cols = columns.size();
    std::vector<std::size_t> column_widths;
    column_widths.resize(num_cols);
    for (std::size_t i = 0; i < num_cols; ++i) {
        std::size_t max_width = 0;
        const TextColumn& col = columns[i];
        std::size_t num_rows = col.size();
        if (num_rows > max_num_rows)
            max_num_rows = num_rows;
        for (std::size_t j = 0; j < num_rows; ++j) {
            std::size_t width = col[j].size();
            if (width > max_width)
                max_width = width;
        }
        column_widths[i] = max_width;
    }

    std::size_t column_spacing = 2;
    std::size_t total_width = 0;
    for (std::size_t i = 0; i < num_cols; ++i) {
        if (i > 0)
            total_width += column_spacing;
        total_width += column_widths[i];
    }

    for (std::size_t i = 0; i < max_num_rows; ++i) {
        std::size_t curs_pos = 0, col_pos = 0;
        for (std::size_t j = 0; j < num_cols; ++j) {
            if (j > 0)
                col_pos += column_widths[j - 1] + column_spacing;
            const TextColumn& col = columns[j];
            if (i < col.size()) {
                const std::string& str = col[i];
                std::size_t width = str.size();
                if (width > 0) {
                    std::size_t pad = col_pos - curs_pos;
                    std::cout << std::setw(int(pad)) << "";
                    std::cout << str;
                    curs_pos = col_pos + str.size();
                }
            }
        }
        std::cout << "\n";
        if (i == 0 && first_row_is_header) {
            std::cout.fill('-');
            std::cout << std::setw(int(total_width)) << "";
            std::cout.fill(' ');
            std::cout << "\n";
        }
    }
}
