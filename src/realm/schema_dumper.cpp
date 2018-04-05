/*************************************************************************
 *
 * Copyright 2016 Realm Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **************************************************************************/

// C++ headers
#include <iostream>

// C headers
#include <getopt.h>

// Realm headers
#include <realm.hpp>
#include <realm/lang_bind_helper.hpp>

using realm::util::Optional;

namespace {

#ifdef _WIN32
int setenv(const char* name, const char* value, int overwrite)
{
    int errcode = 0;
    if (!overwrite) {
        size_t envsize = 0;
        errcode = getenv_s(&envsize, NULL, 0, name);
        if (errcode || envsize)
            return errcode;
    }
    return _putenv_s(name, value);
}
#endif

void show_help(const std::string& program_name)
{
    std::cerr << "Usage: " << program_name << " [options] FILE\n"
                                              "\n"
                                              "Arguments:\n"
                                              "\n"
                                              "  FILE    The Realm file that should have its schema dumped.\n"
                                              "\n"
                                              "Options:\n"
                                              "\n"
                                              "  -k,--key     Encryption key to decrypt the Realm\n"
                                              "  -u,--upgrade Perform file format upgrade if required\n"
                                              "  -h,--help    Print this message\n";
}

namespace Logging {

struct Naught {
};

template <typename T>
struct LogData {
    T items;
};

template <typename Begin, typename Value>
constexpr LogData<std::pair<Begin&&, Value&&>> operator<<(LogData<Begin>&& begin, Value&& value) noexcept
{
    return {{std::forward<Begin>(begin.items), std::forward<Value>(value)}};
}

template <typename Begin, size_t n>
constexpr LogData<std::pair<Begin&&, const char*>> operator<<(LogData<Begin>&& begin, const char (&value)[n]) noexcept
{
    return {{std::forward<Begin>(begin.items), value}};
}

typedef std::ostream& (*PfnManipulator)(std::ostream&);

template <typename Begin>
constexpr LogData<std::pair<Begin&&, PfnManipulator>> operator<<(LogData<Begin>&& begin,
                                                                 PfnManipulator value) noexcept
{
    return {{std::forward<Begin>(begin.items), value}};
}

template <typename Begin, typename Last>
void print(std::ostream& os, std::pair<Begin, Last>&& data)
{
    print(os, std::move(data.first));
    os << data.second;
}

inline void print(std::ostream&, Naught)
{
}

template <typename List>
void Log(const char* file, int line, LogData<List>&& data)
{
    std::cout << '[' << file << ':' << line << "] ";
    print(std::cout, std::move(data.items));
    std::cout << std::endl;
}

#define LOG(x) (Logging::Log(__FILE__, __LINE__, Logging::LogData<Logging::Naught>() << x))

} // namespace Logging

struct Configuration {
    std::string path;
    Optional<std::string> key;
    bool upgrade = false;
};

void parse_arguments(int argc, char* argv[], Configuration& configuration)
{
    static struct option long_options[] = {{"key", required_argument, nullptr, 'k'},
                                           {"upgrade", no_argument, nullptr, 'u'},
                                           {"help", no_argument, nullptr, 'h'},
                                           {nullptr, 0, nullptr, 0}};

    static const char* opt_desc = "k:uh";

    int opt_index = 0;
    char opt;

    while ((opt = getopt_long(argc, argv, opt_desc, long_options, &opt_index)) != -1) {
        switch (opt) {
            case 'k':
                configuration.key = std::string(optarg);
                break;
            case 'u':
                configuration.upgrade = true;
                break;
            case 'h':
                show_help(argv[0]);
                std::exit(EXIT_SUCCESS);
            default:
                show_help(argv[0]);
                std::exit(EXIT_FAILURE);
        }
    }

    if (optind == argc) {
        std::cerr << "Error: Missing argument. What should I try to open?\n\n";
        show_help(argv[0]);
        std::exit(EXIT_FAILURE);
    }

    configuration.path = argv[optind++];

    if (optind < argc) {
        std::cerr << "Error: Extraneous argument provided: " << argv[optind] << "\n\n";
        show_help(argv[0]);
        std::exit(EXIT_FAILURE);
    }
}

Configuration build_configuration(int argc, char* argv[])
{
    // Force GNU getopt to behave in a POSIX-y way. This is required so that
    // positional argument detection is handled properly, and the same on all
    // platforms.
    setenv("POSIXLY_CORRECT", "1", 0);

    Configuration c;

    parse_arguments(argc, argv, c);

    return c;
}

using realm::ConstTableRef;
using realm::Group;
using realm::LangBindHelper;
using realm::ReadTransaction;
using realm::DB;
using realm::SharedGroupOptions;

class SchemaDumper {
public:
    // ctors & dtors
    SchemaDumper(const Configuration& config);

    // public methods
    void list_tables(std::ostream& stream);
    void list_columns(std::ostream& stream, const ConstTableRef& table);

private:
    // private methods
    void open();

    // private members
    const Configuration& m_config;
    DB m_sg{DB::unattached_tag{}};
};

SchemaDumper::SchemaDumper(const Configuration& config)
    : m_config(config)
{
    open();
}

void SchemaDumper::list_tables(std::ostream& stream)
{
    ReadTransaction rt(m_sg);
    const Group& group = rt.get_group();
    size_t table_count = group.size();

    for (size_t idx = 0; idx < table_count; ++idx) {
        std::string table_name = group.get_table_name(idx);
        const ConstTableRef table = group.get_table(idx);

        stream << "table " << table_name << " {\n";
        list_columns(stream, table);
        stream << "}\n";

        if (idx + 1 < table_count) {
            stream << '\n';
        }
    }
}

void SchemaDumper::list_columns(std::ostream& stream, const ConstTableRef& table)
{
    size_t column_count = table->get_column_count();

    for (size_t idx = 0; idx < column_count; ++idx) {
        std::string column_name = table->get_column_name(idx);
        realm::DataType column_type = table->get_column_type(idx);
        std::string column_type_name = LangBindHelper::get_data_type_name(column_type);

        stream << "    " << column_type_name << " " << column_name << " (type id: " << column_type << ")";

        if (idx + 1 < column_count) {
            stream << ',';
        }
        stream << '\n';
    }
}

void SchemaDumper::open()
{
    LOG("Opening Realm file `" << m_config.path << '\'');

    bool dont_create = true;
    bool upgrade_file_format = m_config.upgrade;
    const char* encryption_key = nullptr;

    if (m_config.key) {
        encryption_key = (*m_config.key).c_str();
        LOG("Using encryption key `" << *m_config.key << '\'');
    }
    SharedGroupOptions options;
    options.encryption_key = encryption_key;
    options.allow_file_format_upgrade = upgrade_file_format;
    m_sg.open(m_config.path, dont_create, options);
}

} // unnamed namespace

int main(int argc, char* argv[])
{
    Configuration config = build_configuration(argc, argv);
    try {
        SchemaDumper sd(config);
        sd.list_tables(std::cout);
    }
    catch (const realm::util::File::NotFound& e) {
        LOG("Error while opening Realm file: " << e.what());
        std::exit(EXIT_FAILURE);
    }
    catch (const realm::FileFormatUpgradeRequired&) {
        LOG("Error: This Realm file requires a file format upgrade before being usable");
        std::exit(EXIT_FAILURE);
    }
}
