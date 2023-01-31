/*
 * realm_fuzzer.cpp
 *
 *  Created on: Oct 4, 2022
 *      Author: jed
 */

#include "realm/object-store/shared_realm.hpp"
#include "realm/object-store/object.hpp"
#include "realm/object-store/results.hpp"
#include <realm/object-store/util/scheduler.hpp>
#include <realm/unicode.hpp>
#include <external/json/json.hpp>

#include <cstdlib>
#include <iostream>
#include <string>
#include <map>
#include <uv.h>

using namespace realm;

std::map<TableKey, std::vector<std::string>> deleted_objects;

std::vector<std::string> strings = {
    "quick",     "six",       "blind",      "smart",      "out",       "burst",      "perfectly", "on",
    "furniture", "dejection", "determine",  "my",         "depending", "an",         "to.",       "add",
    "short",     "water",     "court",      "fat.",       "her",       "bachelor",   "honoured",  "perceive",
    "securing",  "but",       "desirous",   "ham",        "required.", "questions",  "deficient", "acuteness",
    "to",        "engrossed", "as.",        "entirely",   "led",       "ten",        "humoured",  "greatest",
    "and",       "yourself.", "besides",    "yes",        "country",   "on",         "observe.",  "she",
    "continue",  "appetite",  "endeavor",   "she",        "judgment",  "interest",   "the",       "met.",
    "for",       "she",       "surrounded", "motionless", "fat",       "resolution", "may",       "well"};

std::vector<int64_t> integers = {
    2,      7478,        1402,      21,         895,        52620,    22837,      3558,      33110,
    175585, 95327301,    802973,    726627,     46548,      25157,    912874,     9593074,   621533,
    81754,  91003490383, 652212360, 1351501563, 1141578126, 92854479, 678859800,  279758185, 1204173118,
    -67842, -2897,       -49889,    -2377840,   -353,       -7367,    -281925594, -98875,    -522614,
    -8214,  -64,         -4816173,  -7676,      -2,         -9826,    -649,       -56629,    -22727,
    -7,     -3,          -1,        -93692,     -9,         -704,     -52,        -685524,   -747945183,
};

class Generator {
public:
    virtual bool has_next() = 0;
    virtual int next() = 0;
};

static Mixed from_string(ColKey col_key, std::string_view string_val)
{
    switch (col_key.get_type()) {
        case col_type_Int:
            return int64_t(strtoll(string_val.data(), nullptr, 10));
        case col_type_String:
            return StringData(string_val);
        case col_type_Timestamp:
            return Timestamp(string_val);
        case col_type_Float:
            return strtof(string_val.data(), nullptr);
            break;
        case col_type_Double:
            return strtod(string_val.data(), nullptr);
            break;
        case col_type_Decimal:
            return Decimal128(string_val.data());
            break;
        default:
            break;
    }
    return {};
}

struct CallbackWrapper {
    util::UniqueFunction<void(std::string)> callback;

    void before(const CollectionChangeSet&)
    {
        callback("before");
    }

    void after(const CollectionChangeSet&)
    {
        callback("after");
    }
};

class Fuzzer {
public:
    Fuzzer(SharedRealm r, Generator& g)
        : m_generator(g)
        , m_realm(r)
    {
        const Schema& schema = m_realm->schema();
        for (auto& os : schema) {
            auto& t = m_table_info.emplace_back();
            t.key = os.table_key;
            t.name = os.name;
            std::vector<PropInfo>& props = m_table_info.back().properties;
            for (auto prop : os.persisted_properties) {
                if (!prop.is_primary) {
                    auto& p = props.emplace_back();
                    p.key = prop.column_key;
                    p.name = prop.name;
                }
            }
        }
    }
    bool step()
    {
        auto step = get_instr(11);
        switch (step) {
            case 0:
                break;
            case 1: {
                if (m_object_info.size() < 20) {
                    auto table_index = get_instr(m_table_info.size());
                    auto table_ref = m_realm->read_group().get_table(m_table_info[table_index].key);
                    if (auto sz = table_ref->size()) {
                        auto object_index = get_instr(sz);
                        auto it = std::find_if(m_object_info.begin(), m_object_info.end(), [&](const ObjInfo& o) {
                            return o.index == object_index && o.table == &m_table_info[table_index];
                        });
                        if (it == m_object_info.end()) {
                            m_object_info.emplace_back(m_table_info[table_index], object_index);
                            ObjInfo& i = m_object_info.back();
                            i.object = Object(m_realm, i.table->name, object_index);
                            i.m_token = i.object.add_notification_callback([table_ref](CollectionChangeSet c) {
                                for (auto&& x : c.columns) {
                                    std::cout << "  Prop changed: " << table_ref->get_column_name(ColKey(x.first))
                                              << std::endl;
                                }
                            });
                            std::cout << "Object added: " << table_ref->get_name() << " "
                                      << i.object.obj().get_primary_key() << std::endl;
                        }
                    }
                }
                break;
            }
            case 2: {
                m_realm->async_begin_transaction([this]() {
                    if (!m_object_info.empty()) {
                        ObjInfo& i = m_object_info[get_instr(m_object_info.size())];
                        auto& prop = i.table->properties[get_instr(i.table->properties.size())];
                        auto obj = i.object.obj();
                        if (obj.is_valid() && !prop.key.is_collection()) {
                            auto mixed = mutate(prop.key);
                            obj.set_any(prop.key, mixed);
                            m_realm->async_commit_transaction([&prop](auto) {
                                std::cout << "Prop mutated: " << prop.name << std::endl;
                            });
                        }
                    }
                });
                break;
            }
            case 3: {
                if (!m_object_info.empty()) {
                    auto it = m_object_info.begin() + (get_instr(m_object_info.size()));
                    m_object_info.erase(it);
                    // std::cout << "Object removed" << std::endl;
                }
                break;
            }
            case 4: {
                // std::cout << "Null transaction" << std::endl;
                m_realm->begin_transaction();
                m_realm->commit_transaction();
                break;
            }
            case 5: {
                if (!m_frozen_realm) {
                    // std::cout << "Freeze" << std::endl;
                    m_realm->read_group();
                    m_frozen_realm = m_realm->freeze();
                }
                break;
            }
            case 6: {
                if (m_frozen_realm) {
                    // std::cout << "Delete frozen" << std::endl;
                    m_frozen_realm = nullptr;
                }
                break;
            }
            case 7: {
                break;
            }
            case 8: {
                auto table_index = get_instr(m_table_info.size());
                auto table_ref = m_realm->read_group().get_table(m_table_info[table_index].key);
                m_realm->begin_transaction();
                auto sz = table_ref->size();
                if (sz > 10) {
                    auto object_index = get_instr(sz);
                    auto obj = table_ref->get_object(object_index);
                    std::vector<std::string>& objects = deleted_objects[table_ref->get_key()];
                    objects.push_back(obj.to_string());
                    obj.remove();
                    m_realm->commit_transaction();
                }
                else {
                    m_realm->cancel_transaction();
                }
                break;
            }
            case 9: {
                auto table_index = get_instr(m_table_info.size());
                auto table_ref = m_realm->read_group().get_table(m_table_info[table_index].key);
                std::vector<std::string>& objects = deleted_objects[table_ref->get_key()];
                if (!objects.empty()) {
                    m_realm->begin_transaction();
                    std::string values = objects.back();
                    auto j = nlohmann::json::parse(values);
                    objects.pop_back();
                    auto pk_col = table_ref->get_primary_key_column();
                    Obj obj;
                    if (pk_col) {
                        std::string col_name{table_ref->get_column_name(pk_col)};
                        auto& id = j[col_name];
                        obj = table_ref->create_object_with_primary_key(from_string(pk_col, id.get<std::string>()));
                    }
                    else {
                        obj = table_ref->create_object();
                    }
                    for (auto col_key : table_ref->get_column_keys()) {
                        if (col_key == pk_col) {
                            continue;
                        }
                        std::string col_name{table_ref->get_column_name(col_key)};
                        auto& val = j[col_name];
                        if (val.is_string()) {
                            obj.set_any(col_key, from_string(col_key, val.get<std::string>()));
                        }
                    }
                    m_realm->commit_transaction();
                }
                break;
            }
            case 10: {
                if (!m_secondary_realm) {
                    m_secondary_realm = Realm::get_shared_realm(m_realm->config());
                    auto table_ref = m_secondary_realm->read_group().get_table(m_table_info[0].key);
                    auto q = table_ref->query("fileSize > 150000");
                    m_results = Results(m_secondary_realm, q);
                    CallbackWrapper cb;
                    cb.callback = [](std::string str) {
                        std::cout << str << std::endl;
                    };
                    m_cb_token = m_results.add_notification_callback(std::move(cb));
                }
                break;
            }
            default:
                break;
        }
        // std::cout << "Verify" << std::endl;
        util::StderrLogger logger;
        m_realm->read_group().verify_cluster(logger);
        return done;
    }

private:
    struct PropInfo {
        ColKey key;
        std::string name;
    };
    struct TableInfo {
        TableKey key;
        std::string name;
        std::vector<PropInfo> properties;
    };
    struct ObjInfo {
        ObjInfo(const TableInfo& t, int i)
            : table(&t)
            , index(i)
        {
        }
        const TableInfo* table;
        int index;
        Object object;
        NotificationToken m_token;
    };
    Generator& m_generator;
    SharedRealm m_realm;
    SharedRealm m_secondary_realm;
    SharedRealm m_frozen_realm;
    Results m_results;
    NotificationToken m_cb_token;
    std::vector<TableInfo> m_table_info;
    std::vector<ObjInfo> m_object_info;
    std::string m_buffer;
    bool done = false;

    int get_instr(size_t max)
    {
        if (m_generator.has_next()) {
            return (m_generator.next() + 1) % max;
        }
        done = true;
        return 0;
    }
    Mixed mutate(ColKey col_key)
    {
        switch (col_key.get_type()) {
            case col_type_Int:
                return get_int();
            case col_type_String: {
                size_t nb_words = get_instr(25);
                m_buffer = "";
                std::string sep = "";
                for (size_t i = 0; i < nb_words; i++) {
                    m_buffer += (sep + strings[get_instr(strings.size())]);
                    sep = " ";
                }
                return m_buffer;
            }
            case col_type_Bool:
                return get_instr(2) ? true : false;
            case col_type_Float:
                return get_float();
            case col_type_Double:
                return get_double();
            case col_type_Timestamp:
                return Timestamp(get_positive(), int32_t(get_positive()) % Timestamp::nanoseconds_per_second);
            case col_type_Decimal:
                return Decimal(get_double());
            default:
                break;
        }
        return {};
    }
    int64_t get_int()
    {
        return integers[get_instr(integers.size())];
    }
    double get_double()
    {
        double d = double(get_int());
        return d + d / get_int();
    }
    float get_float()
    {
        float f = float(int(get_int()));
        return f + f / get_int();
    }
    int64_t get_positive()
    {
        auto i = get_int();
        return i >= 0 ? i : -i;
    }
};

static int run(Generator&& generator)
{
    RealmConfig config;
    config.path = "default.realm";

    uv_loop_t* loop = uv_default_loop();
    uv_idle_t idle_handle;
    idle_handle.data = new Fuzzer(Realm::get_shared_realm(config), generator);
    uv_idle_init(loop, &idle_handle);
    uv_idle_start(&idle_handle, [](uv_idle_t* handle) {
        Fuzzer* fuzzer = static_cast<Fuzzer*>(handle->data);
        auto done = fuzzer->step();
        if (done) {
            uv_idle_stop(handle);
            delete fuzzer;
        }
    });
    uv_run(loop, UV_RUN_DEFAULT);
    return 0;
}

#ifdef LIBFUZZER
class FuzzerGenerator : public Generator {
public:
    FuzzerGenerator(const char* data, size_t size)
        : m_fuzzy(data, size)
    {
    }
    bool has_next() override
    {
        return m_step < m_fuzzy.length();
    }
    int next() override
    {
        return int(uint8_t(m_fuzzy[m_step++]));
    }

private:
    std::string m_fuzzy;
    size_t m_step = 0;
};

extern "C" int LLVMFuzzerTestOneInput(const char* data, size_t size);

int LLVMFuzzerTestOneInput(const char* data, size_t size)
{
    return run(FuzzerGenerator(data, size));
}
#else

class RandomGenerator : public Generator {
public:
    RandomGenerator()
    {
        srand(unsigned(time(NULL)));
    }
    bool has_next() override
    {
        return true;
    }
    int next() override
    {
        return rand();
    }
};

int main()
{
    return run(RandomGenerator());
}
#endif
