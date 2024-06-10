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

#include <iostream>
#include <iomanip>
#include <regex>
#include <set>
#include <sstream>
#include <set>

#include <realm.hpp>
#if REALM_ENABLE_GEOSPATIAL
#include <realm/geospatial.hpp>
#endif
#include <realm/string_data.hpp>
#include <realm/util/file.hpp>

#include "../test_types_helper.hpp"
#include "../util/timer.hpp"
#include "../util/random.hpp"
#include "../util/benchmark_results.hpp"
#include "../util/test_path.hpp"
#include "../util/unit_test.hpp"
#if REALM_ENABLE_ENCRYPTION
#include "../util/crypt_key.hpp"
#endif

using namespace realm;
using namespace realm::util;
using namespace realm::test_util;

static std::vector<std::regex> g_bench_filter;

static bool should_filter_benchmark(const std::string& name)
{
    if (g_bench_filter.empty())
        return false;

    return std::all_of(g_bench_filter.begin(), g_bench_filter.end(), [&](const std::regex& regex) {
        return !std::regex_match(name, regex);
    });
}

namespace {
// not smaller than 100.000 or the UID based benchmarks has to be modified!
#define BASE_SIZE 200000

/**
  This bechmark suite represents a number of common use cases,
  from the perspective of the bindings. It does *not* benchmark
  the type-safe C++ API, but only the things that language bindings
  are likely to use internally.

  This has the following implications:
  - All access is done with a DB in transactions.
  - The DB has MemOnly durability (is not backed by a file).
    (but a few benchmarks are also run with Full durability where that is more relevant)
  - Cases have been derived from:
    https://github.com/realm/realm-java/blob/bp-performance-test/realm/src/androidTest/java/io/realm/RealmPerformanceTest.java
  - Other cases has been added to track improvements (e.g. TimeStamp queries)
  - And yet other has been added to reflect change in idiomatic use (e.g. core5->core6)
*/

const size_t min_repetitions = 5;
const size_t max_repetitions = 1000;
const double min_duration_s = 0.5;
const double min_warmup_time_s = 0.1;

const char* to_lead_cstr(DBOptions::Durability level);
const char* to_ident_cstr(DBOptions::Durability level);

struct Benchmark {
    Benchmark() {}
    virtual ~Benchmark() {}
    virtual const char* name() const = 0;
    virtual void before_all(DBRef) {}
    virtual void after_all(DBRef)
    {
        m_keys.clear();
    }
    virtual void before_each(DBRef db)
    {
        m_tr.reset(new WriteTransaction(db));
        m_table = m_tr->get_table(name());
    }
    virtual void after_each(DBRef)
    {
        m_table = nullptr;
        m_tr = nullptr;
    }
    virtual void operator()(DBRef) = 0;
    DBOptions::Durability m_durability = DBOptions::Durability::Full;
    const char* m_encryption_key = nullptr;
    std::vector<ObjKey> m_keys;
    ColKey m_col;
    std::unique_ptr<WriteTransaction> m_tr;
    TableRef m_table;
};

struct BenchmarkUnorderedTableViewClear : Benchmark {
    const char* name() const
    {
        return "UnorderedTableViewClear";
    }
    void before_all(DBRef group)
    {
        const size_t rows = BASE_SIZE;
        WriteTransaction tr(group);
        TableRef tbl = tr.add_table(name());
        m_col = tbl->add_column(type_String, "s", true);
        tbl->create_objects(rows, m_keys);

        for (size_t t = 0; t < rows / 3; t += 3) {
            tbl->get_object(m_keys[t + 0]).set(m_col, StringData("foo"));
            tbl->get_object(m_keys[t + 1]).set(m_col, StringData("bar"));
            tbl->get_object(m_keys[t + 2]).set(m_col, StringData("hello"));
        }
        tr.commit();
    }
    void operator()(DBRef)
    {
        TableRef tbl = m_table;
        TableView tv = (tbl->column<String>(m_col) == "foo").find_all();
        tv.clear();
    }
};

struct BenchmarkUnorderedTableViewClearIndexed : BenchmarkUnorderedTableViewClear {
    const char* name() const
    {
        return "UnorderedTableViewClearIndexed";
    }
    void before_all(DBRef group)
    {
        BenchmarkUnorderedTableViewClear::before_all(group);
        WriteTransaction tr(group);
        TableRef tbl = tr.get_table(name());
        tbl->add_search_index(m_col);
        tr.commit();
    }
};

struct AddTable : Benchmark {
    const char* name() const
    {
        return "AddTable";
    }

    void operator()(DBRef group)
    {
        WriteTransaction tr(group); // FIXME: Includes some transaction management in what's measured.
        TableRef t = tr.add_table(name());
        t->add_column(type_String, "first");
        t->add_column(type_Int, "second");
        t->add_column(type_Float, "third");
        tr.commit();
    }
    void before_each(DBRef) {}
    void after_each(DBRef group)
    {
        WriteTransaction tr(group);
        tr.get_group().remove_table(name());
        tr.commit();
    }
};

struct BenchmarkWithStringsTable : Benchmark {
    void before_all(DBRef group)
    {
        WriteTransaction tr(group);
        TableRef t = tr.add_table(name());
        m_col = t->add_column(type_String, "chars");
        tr.commit();
    }

    void after_all(DBRef group)
    {
        WriteTransaction tr(group);
        tr.get_group().remove_table(name());
        tr.commit();
    }
};

struct BenchmarkWithStrings : BenchmarkWithStringsTable {
    void before_all(DBRef group)
    {
        BenchmarkWithStringsTable::before_all(group);
        WriteTransaction tr(group);
        TableRef t = tr.get_table(name());

        for (size_t i = 0; i < BASE_SIZE; ++i) {
            std::stringstream ss;
            ss << rand();
            auto s = ss.str();
            Obj obj = t->create_object();
            obj.set<StringData>(m_col, s);
            m_keys.push_back(obj.get_key());
        }
        tr.commit();
    }
};

struct BenchmarkWithStringsFewDup : BenchmarkWithStringsTable {
    void before_all(DBRef group)
    {
        BenchmarkWithStringsTable::before_all(group);
        WriteTransaction tr(group);
        TableRef t = tr.get_table(name());

        Random r;
        for (size_t i = 0; i < BASE_SIZE; ++i) {
            std::stringstream ss;
            ss << r.draw_int(0, BASE_SIZE / 2);
            auto s = ss.str();
            Obj obj = t->create_object();
            obj.set<StringData>(m_col, s);
            m_keys.push_back(obj.get_key());
        }
        t->add_search_index(m_col);
        tr.commit();
    }
};

struct BenchmarkWithStringsManyDup : BenchmarkWithStringsTable {
    void before_all(DBRef group)
    {
        BenchmarkWithStringsTable::before_all(group);
        WriteTransaction tr(group);
        TableRef t = tr.get_table(name());
        Random r;
        for (size_t i = 0; i < BASE_SIZE; ++i) {
            std::stringstream ss;
            ss << r.draw_int(0, 100);
            auto s = ss.str();
            Obj obj = t->create_object();
            obj.set<StringData>(m_col, s);
            m_keys.push_back(obj.get_key());
        }
        t->add_search_index(m_col);
        tr.commit();
    }
};

struct BenchmarkLongStringsManyDup : BenchmarkWithStringsTable {
    void before_all(DBRef group)
    {
        const char* strings[] = {
            "An object database (also object-oriented database management system) is a database management system in "
            "which information is represented in the form of objects as used in object-oriented programming. Object "
            "databases are different from relational databases which are table-oriented. Object-relational databases "
            "are a hybrid of both approaches.",
            "Object database management systems grew out of research during the early to mid-1970s into having "
            "intrinsic database management support for graph-structured objects. The term 'object-oriented database "
            "system' first appeared around 1985.[4] Notable research projects included Encore-Ob/Server (Brown "
            "University), EXODUS (University of Wisconsin–Madison), IRIS (Hewlett-Packard), ODE (Bell Labs), ORION "
            "(Microelectronics and Computer Technology Corporation or MCC), Vodak (GMD-IPSI), and Zeitgeist (Texas "
            "Instruments). The ORION project had more published papers than any of the other efforts. Won Kim of MCC "
            "compiled the best of those papers in a book published by The MIT Press.",
            "Early commercial products included Gemstone (Servio Logic, name changed to GemStone Systems), Gbase "
            "(Graphael), and Vbase (Ontologic). The early to mid-1990s saw additional commercial products enter the "
            "market. These included ITASCA (Itasca Systems), Jasmine (Fujitsu, marketed by Computer Associates), "
            "Matisse (Matisse Software), Objectivity/DB (Objectivity, Inc.), ObjectStore (Progress Software, "
            "acquired from eXcelon which was originally Object Design), ONTOS (Ontos, Inc., name changed from "
            "Ontologic), O2[6] (O2 Technology, merged with several companies, acquired by Informix, which was in "
            "turn acquired by IBM), POET (now FastObjects from Versant which acquired Poet Software), Versant Object "
            "Database (Versant Corporation), VOSS (Logic Arts) and JADE (Jade Software Corporation). Some of these "
            "products remain on the market and have been joined by new open source and commercial products such as "
            "InterSystems Caché.",
            "As the usage of web-based technology increases with the implementation of Intranets and extranets, "
            "companies have a vested interest in OODBMSs to display their complex data. Using a DBMS that has been "
            "specifically designed to store data as objects gives an advantage to those companies that are geared "
            "towards multimedia presentation or organizations that utilize computer-aided design (CAD).[3]",
            "Object database management systems added the concept of persistence to object programming languages. "
            "The early commercial products were integrated with various languages: GemStone (Smalltalk), Gbase "
            "(LISP), Vbase (COP) and VOSS (Virtual Object Storage System for Smalltalk). For much of the 1990s, C++ "
            "dominated the commercial object database management market. Vendors added Java in the late 1990s and "
            "more recently, C#.",
            "L’archive ouverte pluridisciplinaire HAL, est destinée au dépôt et à la diffusion de documents "
            "scientifiques de niveau recherche, publiés ou non, émanant des établissements d’enseignement et de "
            "recherche français ou étrangers, des laboratoires publics ou privés.",
            "object object object object object duplicates",
        };

        BenchmarkWithStringsTable::before_all(group);
        WriteTransaction tr(group);
        TableRef t = tr.get_table(name());
        Random r;
        for (size_t i = 0; i < BASE_SIZE; ++i) {
            Obj obj = t->create_object();
            obj.set<StringData>(m_col, strings[i % 7]);
            m_keys.push_back(obj.get_key());
        }
        t->add_fulltext_index(m_col);
        tr.commit();
    }
};

struct BenchmarkFindAllStringFewDupes : BenchmarkWithStringsFewDup {
    const char* name() const
    {
        return "FindAllStringFewDupes";
    }

    void operator()(DBRef)
    {
        ConstTableRef table = m_table;
        TableView view = table->where().equal(m_col, StringData("10", 2)).find_all();
    }
};

struct BenchmarkFindAllStringManyDupes : BenchmarkWithStringsManyDup {
    const char* name() const
    {
        return "FindAllStringManyDupes";
    }

    void operator()(DBRef)
    {
        ConstTableRef table = m_table;
        TableView view = table->where().equal(m_col, StringData("10", 2)).find_all();
    }
};

struct BenchmarkFindAllFulltextStringManyDupes : BenchmarkLongStringsManyDup {
    const char* name() const
    {
        return "FindAllFulltextStringManyDupes";
    }

    void operator()(DBRef)
    {
        ConstTableRef table = m_table;
        TableView view = table->where().fulltext(m_col, "object gemstone").find_all();
    }
};

template <bool index>
struct BenchmarkCountStringManyDupes : BenchmarkWithStringsManyDup {
    const char* name() const
    {
        if constexpr (index) {
            return "CountStringManyDupesIndexed";
        }
        return "CountStringManyDupesNonIndexed";
    }

    void before_all(DBRef group)
    {
        BenchmarkWithStringsManyDup::before_all(group);
        WriteTransaction tr(group);
        TableRef t = tr.get_table(name());
        if constexpr (index) {
            t->add_search_index(m_col);
        }
        else {
            t->remove_search_index(m_col);
        }
        tr.commit();
    }

    void operator()(DBRef)
    {
        ConstTableRef table = m_table;
        std::vector<std::string> strs = {
            "10", "20", "30", "40", "50", "60", "70", "80", "90", "100",
        };
        for (auto s : strs) {
            table->where().equal(m_col, StringData(s)).count();
        }
    }
};


struct BenchmarkFindFirstStringFewDupes : BenchmarkWithStringsFewDup {
    const char* name() const
    {
        return "FindFirstStringFewDupes";
    }

    void operator()(DBRef)
    {
        ConstTableRef table = m_table;
        std::vector<std::string> strs = {
            "10", "20", "30", "40", "50", "60", "70", "80", "90", "100",
        };
        for (auto s : strs) {
            table->where().equal(m_col, StringData(s)).find();
        }
    }
};

struct BenchmarkQueryStringOverLinks : BenchmarkWithStringsFewDup {
    ColKey link_col;
    ColKey id_col;
    void before_all(DBRef group)
    {
        BenchmarkWithStringsFewDup::before_all(group);
        WriteTransaction tr(group);
        TableRef t = tr.add_table("Links");
        id_col = t->add_column(type_Int, "id");
        TableRef strings = tr.get_table(name());
        link_col = t->add_column(*strings, "myLink");
        const size_t num_links = strings->size();

        auto target = strings->begin();
        for (size_t i = 0; i < num_links; ++i) {
            t->create_object().set_all(int64_t(i), target->get_key());
            ++target;
        }
        tr.commit();
    }
    const char* name() const
    {
        return "QueryStringOverLinks";
    }
    virtual void before_each(DBRef group)
    {
        m_tr.reset(new WriteTransaction(group));
        m_table = m_tr->get_table("Links");
    }
    virtual void after_each(DBRef)
    {
        m_table = nullptr;
        m_tr = nullptr;
    }
    void operator()(DBRef)
    {
        TableRef table = m_table;
        std::vector<std::string> strs = {
            "10", "20", "30", "40", "50", "60", "70", "80", "90", "100",
        };

        for (auto s : strs) {
            Query query = table->link(link_col).column<String>(m_col) == StringData(s);
            TableView results = query.find_all();
        }
    }

    void after_all(DBRef group)
    {
        WriteTransaction tr(group);
        tr.get_group().remove_table("Links");
        tr.commit();
        BenchmarkWithStringsFewDup::after_all(group);
    }
};

struct BenchmarkSubQuery : BenchmarkQueryStringOverLinks {
    const char* name() const
    {
        return "SubqueryStrings";
    }
    void operator()(DBRef)
    {
        Query subquery = m_table->get_link_target(link_col)->column<String>(m_col) == "20";
        Query query = m_table->column<Link>(link_col, subquery).count() >= 1;
        TableView results = query.find_all();
    }
};

struct BenchmarkFindFirstStringManyDupes : BenchmarkWithStringsManyDup {
    const char* name() const
    {
        return "FindFirstStringManyDupes";
    }

    void operator()(DBRef)
    {
        ConstTableRef table = m_table;
        std::vector<std::string> strs = {
            "10", "20", "30", "40", "50", "60", "70", "80", "90", "100",
        };
        for (auto s : strs) {
            table->where().equal(m_col, StringData(s)).find();
        }
    }
};

struct BenchmarkWithLongStrings : BenchmarkWithStrings {
    void before_all(DBRef group)
    {
        BenchmarkWithStrings::before_all(group);
        WriteTransaction tr(group);
        TableRef t = tr.get_table(name());

        // This should be enough to upgrade the entire array:
        static std::string really_long_string = "A really long string, longer than 63 bytes at least, I guess......";
        t->get_object(m_keys[0]).set<StringData>(m_col, really_long_string);
        t->get_object(m_keys[BASE_SIZE / 4]).set<StringData>(m_col, really_long_string);
        t->get_object(m_keys[BASE_SIZE * 2 / 4]).set<StringData>(m_col, really_long_string);
        t->get_object(m_keys[BASE_SIZE * 3 / 4]).set<StringData>(m_col, really_long_string);
        tr.commit();
    }
};

// Note: this benchmark is sensitive to changes in test_types_helper.hpp
template <class Type>
struct BenchmarkWithType : Benchmark {
    std::string benchmark_name;
    using underlying_type = typename Type::underlying_type;
    std::vector<OwnedMixed> needles;
    BenchmarkWithType()
        : Benchmark()
    {
        set_name_with_prefix("QueryEqual");
    }
    void set_name_with_prefix(std::string_view prefix)
    {
        benchmark_name =
            util::format("%1<%2><%3><%4>", prefix, get_data_type_name(Type::data_type),
                         Type::is_nullable ? "Nullable" : "NonNullable", Type::is_indexed ? "Indexed" : "NonIndexed");
    }
    void do_add_perf_data(WriteTransaction& tr, bool add_index)
    {
        TestValueGenerator gen;
        TableRef t = tr.get_or_add_table(name());
        m_col = t->add_column(Type::data_type, name(), Type::is_nullable);
        Random r;
        for (size_t i = 0; i < BASE_SIZE / 2; ++i) {
            int64_t randomness = r.draw_int<int64_t>(0, 1000000);
            auto value = gen.convert_for_test<underlying_type>(randomness);
            // a hand full of duplicates
            for (size_t j = 0; j < 2; ++j) {
                t->create_object().set_any(m_col, value);
            }
        }
        while (needles.size() < 50) {
            OwnedMixed needle;
            while (needle.is_null()) {
                needle = t->get_object(r.draw_int<size_t>(0, t->size())).get_any(m_col);
            }
            needles.push_back(std::move(needle));
        }
        if (add_index) {
            t->add_search_index(m_col);
        }
    }
    void before_all(DBRef group)
    {
        WriteTransaction tr(group);
        do_add_perf_data(tr, Type::is_indexed);
        tr.commit();
    }

    const char* name() const
    {
        return benchmark_name.c_str();
    }

    void operator()(DBRef)
    {
        for (Mixed needle : needles) {
            TableView results = m_table->where().equal(m_col, needle.get<underlying_type>()).find_all();
            static_cast<void>(results);
        }
    }
    void after_all(DBRef group)
    {
        WriteTransaction tr(group);
        tr.get_group().remove_table(name());
        tr.commit();
    }
};

template <typename Type>
struct BenchmarkMixedCaseInsensitiveEqual : public BenchmarkWithType<Type> {
    using Base = BenchmarkWithType<Type>;
    using underlying_type = typename Type::underlying_type;
    BenchmarkMixedCaseInsensitiveEqual<Type>()
        : BenchmarkWithType<Type>()
    {
        BenchmarkWithType<Type>::set_name_with_prefix("QueryInsensitiveEqual");
    }

    void operator()(DBRef)
    {
        constexpr bool insensitive_matching = false;
        for (Mixed needle : Base::needles) {
            if constexpr (std::is_same_v<underlying_type, Mixed>) {
                TableView results =
                    Base::m_table->where().equal(Base::m_col, needle, insensitive_matching).find_all();
                static_cast<void>(results);
            }
            else {
                TableView results = Base::m_table->where()
                                        .equal(Base::m_col, needle.get<underlying_type>(), insensitive_matching)
                                        .find_all();
                static_cast<void>(results);
            }
        }
    }
};

template <typename Type>
struct BenchmarkRangeForType : public BenchmarkWithType<Type> {
    using Base = BenchmarkWithType<Type>;
    using underlying_type = typename Type::underlying_type;
    BenchmarkRangeForType<Type>()
        : BenchmarkWithType<Type>()
    {
        BenchmarkWithType<Type>::set_name_with_prefix("QueryRange");
    }

    void before_all(DBRef group) override
    {
        BenchmarkWithType<Type>::before_all(group);
        std::sort(this->needles.begin(), this->needles.end());
    }

    void operator()(DBRef) override
    {
        for (size_t i = 1; i < Base::needles.size(); i++) {
            if constexpr (std::is_same_v<underlying_type, Mixed>) {
                TableView results = Base::m_table->where()
                                        .greater(Base::m_col, Base::needles[i - 1])
                                        .less(Base::m_col, Base::needles[i])
                                        .find_all();
                static_cast<void>(results);
            }
            else {
                TableView results = Base::m_table->where()
                                        .between(Base::m_col, Base::needles[i - 1].template get<underlying_type>(),
                                                 Base::needles[i].template get<underlying_type>())
                                        .find_all();
                static_cast<void>(results);
            }
        }
    }
};

template <typename Type>
struct BenchmarkCreateIndexForType : public BenchmarkWithType<Type> {
    using Base = BenchmarkWithType<Type>;
    using underlying_type = typename Type::underlying_type;
    BenchmarkCreateIndexForType<Type>()
        : BenchmarkWithType<Type>()
    {
        BenchmarkWithType<Type>::set_name_with_prefix("CreateIndexFor");
    }
    void before_all(DBRef group) override
    {
        WriteTransaction tr(group);
        constexpr bool add_index = false;
        BenchmarkWithType<Type>::do_add_perf_data(tr, add_index);
        tr.commit();
    }
    void operator()(DBRef) override
    {
        Benchmark::m_table->add_search_index(Benchmark::m_col);
    }
};

template <typename Type>
struct BenchmarkInsertToIndexForType : public BenchmarkWithType<Type> {
    using Base = BenchmarkWithType<Type>;
    using underlying_type = typename Type::underlying_type;
    BenchmarkInsertToIndexForType<Type>()
        : BenchmarkWithType<Type>()
    {
        BenchmarkWithType<Type>::set_name_with_prefix("InsertWithIndex");
    }
    void before_all(DBRef group) override
    {
        WriteTransaction tr(group);
        TableRef t = tr.get_or_add_table(Base::name());
        Base::m_col = t->add_column(Type::data_type, Base::name(), Type::is_nullable);
        if (Type::is_indexed) {
            t->add_search_index(Base::m_col);
        }
        tr.commit();
        m_random_values.reserve(BASE_SIZE / 2);
        for (size_t i = 0; i < BASE_SIZE / 2; ++i) {
            int64_t randomness = m_random.draw_int<int64_t>();
            m_random_values.push_back(m_test_value_generator.convert_for_test<underlying_type>(randomness));
        }
    }
    void operator()(DBRef) override
    {
        for (auto it = m_random_values.begin(); it != m_random_values.end(); ++it) {
            // a hand full of duplicates
            Base::m_table->create_object().set_any(Base::m_col, *it);
            Base::m_table->create_object().set_any(Base::m_col, *it);
        }
    }
    std::vector<Mixed> m_random_values;
    TestValueGenerator m_test_value_generator;
    Random m_random;
};

template <typename Type>
struct BenchmarkInsertPKToIndexForType : public BenchmarkWithType<Type> {
    using Base = BenchmarkWithType<Type>;
    using underlying_type = typename Type::underlying_type;
    BenchmarkInsertPKToIndexForType<Type>()
        : BenchmarkWithType<Type>()
    {
        BenchmarkWithType<Type>::set_name_with_prefix("InsertPK");
    }
    void before_all(DBRef group) override
    {
        WriteTransaction tr(group);
        TableRef t = tr.get_or_add_table(Base::name());
        Base::m_col = t->add_column(Type::data_type, Base::name(), Type::is_nullable);
        t->set_primary_key_column(Base::m_col);
        REALM_ASSERT(t->has_search_index(Base::m_col));
        tr.commit();
        while (m_unique_randoms.size() < BASE_SIZE) {
            int64_t random_int = m_random.draw_int<int64_t>();
            m_unique_randoms.insert(m_test_value_generator.convert_for_test<underlying_type>(random_int));
        }
    }
    void operator()(DBRef) override
    {
        for (auto it = m_unique_randoms.begin(); it != m_unique_randoms.end(); ++it) {
            Base::m_table->create_object_with_primary_key(*it);
        }
    }
    TestValueGenerator m_test_value_generator;
    Random m_random;
    std::set<underlying_type> m_unique_randoms;
};

template <typename Type>
struct BenchmarkEraseObjectForType : public BenchmarkWithType<Type> {
    using Base = BenchmarkWithType<Type>;
    using underlying_type = typename Type::underlying_type;
    BenchmarkEraseObjectForType<Type>()
        : BenchmarkWithType<Type>()
    {
        BenchmarkWithType<Type>::set_name_with_prefix("EraseObject");
    }
    void before_all(DBRef group) override
    {
        Base::before_all(group);
    }
    void operator()(DBRef) override
    {
        while (!Base::m_table->is_empty()) {
            Base::m_table->begin()->remove();
        }
    }
};

template <typename Type, size_t NUM_CONDITIONS>
struct BenchmarkParsedChainedOrEquality : public BenchmarkWithType<Type> {
    using Base = BenchmarkWithType<Type>;
    using underlying_type = typename Type::underlying_type;
    BenchmarkParsedChainedOrEquality<Type, NUM_CONDITIONS>()
        : BenchmarkWithType<Type>()
    {
        BenchmarkWithType<Type>::set_name_with_prefix(util::format("QueryChainedOrEquality_%1", NUM_CONDITIONS));
    }
    void before_all(DBRef db) override
    {
        Random r;
        Base::before_all(db);
        auto wt = db->start_write();
        TableRef table = wt->get_table(Base::name());
        REALM_ASSERT(Base::needles.size());
        while (Base::needles.size() < NUM_CONDITIONS) {
            OwnedMixed needle;
            while (needle.is_null()) {
                needle = table->get_object(r.draw_int<size_t>(0, table->size())).get_any(Base::m_col);
            }
            Base::needles.push_back(std::move(needle));
        }

        table->rename_column(Base::m_col, "col");
        std::string col_name = table->get_column_name(Base::m_col);

        for (size_t i = 0; i < Base::needles.size(); ++i) {
            m_query_string += util::format("%1%2 == %3", i == 0 ? "" : " or ", col_name, Base::needles[i]);
        }

        wt->commit();
    }

    void operator()(DBRef) override
    {
        TableView tv = Base::m_table->query(m_query_string).find_all();
        tv.clear();
    }

    std::string m_query_string;
};

template <typename Type, size_t NUM_CONDITIONS>
struct BenchmarkParsedIn : public BenchmarkWithType<Type> {
    using Base = BenchmarkWithType<Type>;
    using underlying_type = typename Type::underlying_type;
    BenchmarkParsedIn<Type, NUM_CONDITIONS>()
        : BenchmarkWithType<Type>()
    {
        BenchmarkWithType<Type>::set_name_with_prefix(util::format("QueryParsedIN_%1", NUM_CONDITIONS));
    }
    void before_all(DBRef db) override
    {
        Random r;
        Base::before_all(db);
        auto wt = db->start_write();
        TableRef table = wt->get_table(Base::name());
        REALM_ASSERT(Base::needles.size());
        while (Base::needles.size() < NUM_CONDITIONS) {
            OwnedMixed needle;
            while (needle.is_null()) {
                needle = table->get_object(r.draw_int<size_t>(0, table->size())).get_any(Base::m_col);
            }
            Base::needles.push_back(std::move(needle));
        }

        table->rename_column(Base::m_col, "col");
        std::string col_name = table->get_column_name(Base::m_col);

        m_query_string = "col IN {";
        for (size_t i = 0; i < Base::needles.size(); ++i) {
            m_query_string += util::format("%1%2", i == 0 ? "" : ", ", Base::needles[i]);
        }
        m_query_string += "}";
        wt->commit();
    }

    void operator()(DBRef) override
    {
        TableView tv = Base::m_table->query(m_query_string).find_all();
        tv.clear();
    }

    std::string m_query_string;
};


struct BenchmarkWithTimestamps : Benchmark {
    std::multiset<Timestamp> values;
    Timestamp needle;
    size_t num_results_to_needle;
    size_t num_nulls_added = 0;
    double percent_results_to_needle = 0.5;
    double percent_chance_of_null = 0.0;
    ColKey timestamps_col_ndx;
    void before_all(DBRef group)
    {
        WriteTransaction tr(group);
        TableRef t = tr.add_table(name());
        m_col = t->add_column(type_Timestamp, name(), true);
        Random r;
        for (size_t i = 0; i < BASE_SIZE; ++i) {
            Timestamp time{r.draw_int<int64_t>(0, 1000000), r.draw_int<int32_t>(0, 1000000)};
            if (r.draw_int<int64_t>(0, 100) / 100.0 < percent_chance_of_null) {
                time = Timestamp{};
                ++num_nulls_added;
            }
            else {
                values.insert(time);
            }
            auto obj = t->create_object();
            obj.set<Timestamp>(m_col, time);
        }
        tr.commit();
        // simulate a work load where this percent of random results match
        num_results_to_needle = size_t(values.size() * percent_results_to_needle);
        // this relies on values being stored in sorted order by std::multiset
        auto it = values.begin();
        for (size_t i = 0; i < num_results_to_needle; ++i) {
            ++it;
        }
        needle = *it;
    }

    void after_all(DBRef group)
    {
        WriteTransaction tr(group);
        tr.get_group().remove_table(name());
        tr.commit();
    }
};

struct BenchmarkQueryTimestampGreater : BenchmarkWithTimestamps {
    void before_all(DBRef group)
    {
        percent_chance_of_null = 0.10f;
        percent_results_to_needle = 0.80f;
        BenchmarkWithTimestamps::before_all(group);
    }
    const char* name() const
    {
        return "QueryTimestampGreater";
    }

    void operator()(DBRef)
    {
        ConstTableRef table = m_table;
        Query query = table->where().greater(m_col, needle);
        TableView results = query.find_all();
        REALM_ASSERT_EX(results.size() == values.size() - num_results_to_needle - 1, results.size(),
                        num_results_to_needle, values.size());
        static_cast<void>(results);
    }
};

struct BenchmarkQueryTimestampGreaterOverLinks : BenchmarkQueryTimestampGreater {
    ColKey link_col_ndx;
    ColKey id_col_ndx;
    void before_all(DBRef group)
    {
        BenchmarkQueryTimestampGreater::before_all(group);
        WriteTransaction tr(group);
        TableRef t = tr.add_table("Links");
        id_col_ndx = t->add_column(type_Int, "id");
        TableRef timestamps = tr.get_table(name());
        link_col_ndx = t->add_column(*timestamps, "myLink");
        const size_t num_timestamps = timestamps->size();
        auto target = timestamps->begin();
        for (size_t i = 0; i < num_timestamps; ++i) {
            t->create_object().set<Int>(id_col_ndx, i).set(link_col_ndx, target->get_key());
            ++target;
        }
        tr.commit();
    }
    const char* name() const
    {
        return "QueryTimestampGreaterOverLinks";
    }

    void operator()(DBRef)
    {
        TableRef table = m_tr->get_table("Links");
        Query query = table->link(link_col_ndx).column<Timestamp>(m_col) > needle;
        TableView results = query.find_all();
        REALM_ASSERT_EX(results.size() == values.size() - num_results_to_needle - 1, results.size(),
                        num_results_to_needle, values.size());
        static_cast<void>(results);
    }

    void after_all(DBRef group)
    {
        WriteTransaction tr(group);
        tr.get_group().remove_table("Links");
        tr.commit();
    }
};


struct BenchmarkQueryTimestampGreaterEqual : BenchmarkWithTimestamps {
    void before_all(DBRef group)
    {
        percent_chance_of_null = 0.10f;
        percent_results_to_needle = 0.80f;
        BenchmarkWithTimestamps::before_all(group);
    }
    const char* name() const
    {
        return "QueryTimestampGreaterEqual";
    }

    void operator()(DBRef)
    {
        ConstTableRef table = m_table;
        Query query = table->where().greater_equal(m_col, needle);
        TableView results = query.find_all();
        REALM_ASSERT_EX(results.size() == values.size() - num_results_to_needle, results.size(),
                        num_results_to_needle, values.size());
        static_cast<void>(results);
    }
};


struct BenchmarkQueryTimestampLess : BenchmarkWithTimestamps {
    void before_all(DBRef group)
    {
        percent_chance_of_null = 0.10f;
        percent_results_to_needle = 0.20f;
        BenchmarkWithTimestamps::before_all(group);
    }
    const char* name() const
    {
        return "QueryTimestampLess";
    }

    void operator()(DBRef)
    {
        ConstTableRef table = m_table;
        Query query = table->where().less(m_col, needle);
        TableView results = query.find_all();
        REALM_ASSERT_EX(results.size() == num_results_to_needle, results.size(), num_results_to_needle,
                        values.size());
        static_cast<void>(results);
    }
};

struct BenchmarkQueryTimestampLessEqual : BenchmarkWithTimestamps {
    void before_all(DBRef group)
    {
        percent_chance_of_null = 0.10f;
        percent_results_to_needle = 0.20f;
        BenchmarkWithTimestamps::before_all(group);
    }
    const char* name() const
    {
        return "QueryTimestampLessEqual";
    }

    void operator()(DBRef)
    {
        ConstTableRef table = m_table;
        Query query = table->where().less_equal(m_col, needle);
        TableView results = query.find_all();
        REALM_ASSERT_EX(results.size() == num_results_to_needle + 1, results.size(), num_results_to_needle,
                        values.size());
        static_cast<void>(results);
    }
};


struct BenchmarkQueryTimestampEqual : BenchmarkWithTimestamps {
    void before_all(DBRef group)
    {
        percent_chance_of_null = 0.10f;
        percent_results_to_needle = 0.33f;
        BenchmarkWithTimestamps::before_all(group);
    }
    const char* name() const
    {
        return "QueryTimestampEqual";
    }

    void operator()(DBRef)
    {
        ConstTableRef table = m_table;
        Query query = table->where().equal(m_col, needle);
        TableView results = query.find_all();
        REALM_ASSERT_EX(results.size() == values.count(needle), results.size(), num_results_to_needle,
                        values.count(needle), values.size());
        static_cast<void>(results);
    }
};

struct BenchmarkQueryTimestampNotEqual : BenchmarkWithTimestamps {
    void before_all(DBRef group)
    {
        percent_chance_of_null = 0.60f;
        percent_results_to_needle = 0.10f;
        BenchmarkWithTimestamps::before_all(group);
    }
    const char* name() const
    {
        return "QueryTimestampNotEqual";
    }

    void operator()(DBRef)
    {
        ConstTableRef table = m_table;
        Query query = table->where().not_equal(m_col, needle);
        TableView results = query.find_all();
        REALM_ASSERT_EX(results.size() == values.size() - values.count(needle) + num_nulls_added, results.size(),
                        values.size(), values.count(needle));
        static_cast<void>(results);
    }
};

struct BenchmarkQueryTimestampNotNull : BenchmarkWithTimestamps {
    void before_all(DBRef group)
    {
        percent_chance_of_null = 0.60f;
        percent_results_to_needle = 0.0;
        BenchmarkWithTimestamps::before_all(group);
        needle = Timestamp{};
    }
    const char* name() const
    {
        return "QueryTimestampNotNull";
    }

    void operator()(DBRef)
    {
        ConstTableRef table = m_table;
        Query query = table->where().not_equal(m_col, realm::null());
        TableView results = query.find_all();
        REALM_ASSERT_EX(results.size() == values.size(), results.size(), num_nulls_added, num_results_to_needle,
                        values.size());
        static_cast<void>(results);
    }
};

struct BenchmarkQueryTimestampEqualNull : BenchmarkWithTimestamps {
    void before_all(DBRef group)
    {
        percent_chance_of_null = 0.10;
        percent_results_to_needle = 0.0;
        BenchmarkWithTimestamps::before_all(group);
        needle = Timestamp{};
    }
    const char* name() const
    {
        return "QueryTimestampEqualNull";
    }
    void operator()(DBRef)
    {
        ConstTableRef table = m_table;
        Query query = table->where().equal(m_col, realm::null());
        TableView results = query.find_all();
        REALM_ASSERT_EX(results.size() == num_nulls_added, results.size(), num_nulls_added, values.size());
        static_cast<void>(results);
    }
};

struct BenchmarkWithIntsTable : Benchmark {
    void before_all(DBRef group)
    {
        WriteTransaction tr(group);
        TableRef t = tr.add_table(name());
        m_col = t->add_column(type_Int, "ints");
        tr.commit();
    }

    void after_all(DBRef group)
    {
        WriteTransaction tr(group);
        tr.get_group().remove_table(name());
        tr.commit();
    }
};

struct BenchmarkWithInts : BenchmarkWithIntsTable {
    void before_all(DBRef group)
    {
        BenchmarkWithIntsTable::before_all(group);
        WriteTransaction tr(group);
        TableRef t = tr.get_table(name());

        Random r;
        for (size_t i = 0; i < BASE_SIZE; ++i) {
            int64_t val;
            do {
                val = r.draw_int<int64_t>();
            } while (val < 0);
            Obj obj = t->create_object(ObjKey(val));
            obj.set(m_col, val);
            m_keys.push_back(obj.get_key());
        }
        tr.commit();
    }
};

struct BenchmarkIntVsDoubleColumns : Benchmark {
    ColKey ints_col_ndx;
    ColKey doubles_col_ndx;
    constexpr static size_t num_rows = BASE_SIZE * 4;
    void before_all(DBRef group)
    {
        WriteTransaction tr(group);
        TableRef t = tr.add_table(name());
        ints_col_ndx = t->add_column(type_Int, "ints");
        doubles_col_ndx = t->add_column(type_Double, "doubles");
        for (size_t i = 0; i < num_rows; ++i) {
            t->create_object().set<Int>(ints_col_ndx, i).set(doubles_col_ndx, double(num_rows - i));
        }
        tr.commit();
    }
    const char* name() const
    {
        return "QueryIntsVsDoubleColumns";
    }
    void operator()(DBRef)
    {
        TableRef table = m_table;
        Query q = (table->column<Int>(ints_col_ndx) > table->column<Double>(doubles_col_ndx));
        REALM_ASSERT_3(q.count(), ==, ((num_rows / 2) - 1));
    }

    void after_all(DBRef group)
    {
        WriteTransaction tr(group);
        tr.get_group().remove_table(name());
        tr.commit();
    }
};


struct BenchmarkQueryIntListSize : Benchmark {
    ColKey int_list_col_ndx;
    constexpr static size_t num_rows = BASE_SIZE * 4;
    void before_all(DBRef group)
    {
        WriteTransaction tr(group);
        TableRef t = tr.add_table(name());
        int_list_col_ndx = t->add_column_list(type_Int, "ints");
        for (size_t i = 0; i < num_rows; ++i) {
            t->create_object().set_list_values<Int>(int_list_col_ndx, {0, 1, 2});
        }
        tr.commit();
    }
    const char* name() const
    {
        return "QueryListOfPrimitiveIntsSize";
    }
    void operator()(DBRef)
    {
        TableRef table = m_table;
        Query q = table->where().size_equal(int_list_col_ndx, 3);
        size_t count = q.count();
        REALM_ASSERT_3(count, ==, (num_rows));
    }

    void after_all(DBRef group)
    {
        WriteTransaction tr(group);
        tr.get_group().remove_table(name());
        tr.commit();
    }
};


struct BenchmarkWithIntUIDsRandomOrderSeqAccess : BenchmarkWithIntsTable {
    const char* name() const
    {
        return "IntUIDsRandomOrderSeqAccess";
    }
    void before_all(DBRef group)
    {
        BenchmarkWithIntsTable::before_all(group);
        WriteTransaction tr(group);
        TableRef t = tr.get_table(name());
        Random r;
        for (size_t i = 0; i < BASE_SIZE; ++i) {
            int64_t val;
            while (1) { // make all ints unique
                val = r.draw_int<int64_t>();
                if (val < 0)
                    continue;
                auto search = m_key_set.find(val);
                if (search == m_key_set.end()) {
                    m_key_set.insert(val);
                    break;
                }
            }
            m_keys.push_back(ObjKey(val));
            Obj obj = t->create_object(ObjKey(val));
            obj.set(m_col, val);
        }
        tr.commit();
    }
    void operator()(DBRef)
    {
        ConstTableRef t = m_table;
        volatile uint64_t sum = 0;
        for (size_t i = 0; i < 100000; ++i) {
            auto obj = t->get_object(m_keys[i]);
            sum = sum + obj.get<Int>(m_col);
        }
    }
    std::set<int64_t> m_key_set;
};

struct BenchmarkWithIntUIDsRandomOrderRandomAccess : BenchmarkWithIntUIDsRandomOrderSeqAccess {
    const char* name() const
    {
        return "IntUIDsRandomOrderRandomAccess";
    }
    void before_all(DBRef group)
    {
        BenchmarkWithIntUIDsRandomOrderSeqAccess::before_all(group);
        // randomize key order for later access
        std::random_device rd;
        std::mt19937 g(rd());
        std::shuffle(m_keys.begin(), m_keys.end(), g);
    }
};

struct BenchmarkWithIntUIDsRandomOrderRandomDelete : BenchmarkWithIntUIDsRandomOrderRandomAccess {
    const char* name() const
    {
        return "IntUIDsRandomOrderRandomDelete";
    }
    void before_all(DBRef group)
    {
        BenchmarkWithIntUIDsRandomOrderRandomAccess::before_all(group);
    }
    void operator()(DBRef)
    {
        TableRef t = m_table;
        for (size_t i = 0; i < 10000; ++i) {
            t->remove_object(m_keys[i]);
        }
        // note: abort transaction so next run can start afresh
    }
};
struct BenchmarkWithIntUIDsRandomOrderRandomCreate : BenchmarkWithIntUIDsRandomOrderRandomAccess {
    const char* name() const
    {
        return "IntUIDsRandomOrderRandomCreate";
    }
    void before_all(DBRef group)
    {
        BenchmarkWithIntUIDsRandomOrderRandomAccess::before_all(group);
        int64_t val;
        // produce 10000 more unique keys to drive later object creations
        Random r;
        for (size_t i = 0; i < 10000; ++i) {
            while (1) { // make all ints unique
                val = r.draw_int<int64_t>();
                if (val < 0)
                    continue;
                auto search = m_key_set.find(val);
                if (search == m_key_set.end()) {
                    m_key_set.insert(val);
                    break;
                }
            }
            m_keys.push_back(ObjKey(val));
        }
    }
    void operator()(DBRef)
    {
        TableRef t = m_table;
        for (size_t i = 0; i < 10000; ++i) {
            auto val = m_keys[BASE_SIZE + i];
            Obj obj = t->create_object(val);
            obj.set<Int>(m_col, val.value);
        }
        // abort transaction
    }
};

struct BenchmarkQueryChainedOrInts : BenchmarkWithIntsTable {
    const size_t num_queried_matches = 1000;
    const size_t num_rows = BASE_SIZE;
    std::vector<int64_t> values_to_query;
    const char* name() const
    {
        return "QueryChainedOrInts";
    }

    void before_all(DBRef group)
    {
        BenchmarkWithIntsTable::before_all(group);
        WriteTransaction tr(group);
        TableRef t = tr.get_table(name());
        std::vector<ObjKey> keys;
        t->create_objects(num_rows, keys);
        REALM_ASSERT(num_rows > num_queried_matches);
        Random r;
        size_t i = 0;
        for (auto e : *t) {
            e.set<Int>(m_col, i);
            ++i;
        }
        for (i = 0; i < num_queried_matches; ++i) {
            size_t ndx_to_match = (num_rows / num_queried_matches) * i;
            values_to_query.push_back(t->get_object(ndx_to_match).get<Int>(m_col));
        }
        tr.commit();
    }

    void operator()(DBRef)
    {
        ConstTableRef table = m_table;
        Query query = table->where();
        for (size_t i = 0; i < values_to_query.size(); ++i) {
            query.Or().equal(m_col, values_to_query[i]);
        }
        TableView results = query.find_all();
        REALM_ASSERT_EX(results.size() == num_queried_matches, results.size(), num_queried_matches,
                        values_to_query.size());
        static_cast<void>(results);
    }
};

struct BenchmarkQueryChainedOrIntsCount : BenchmarkQueryChainedOrInts {
    const char* name() const
    {
        return "QueryChainedOrIntsCount";
    }

    void operator()(DBRef)
    {
        ConstTableRef table = m_table;
        Query query = table->where();
        for (size_t i = 0; i < values_to_query.size(); ++i) {
            query.Or().equal(m_col, values_to_query[i]);
        }
        size_t matches = query.count();
        REALM_ASSERT_EX(matches == num_queried_matches, matches, num_queried_matches, values_to_query.size());
    }
};

struct BenchmarkQueryChainedOrIntsIndexed : BenchmarkQueryChainedOrInts {
    const char* name() const
    {
        return "QueryChainedOrIntsIndexed";
    }
    void before_all(DBRef group)
    {
        BenchmarkQueryChainedOrInts::before_all(group);
        WriteTransaction tr(group);
        TableRef t = tr.get_table(name());
        t->add_search_index(m_col);
        tr.commit();
    }
};


struct BenchmarkQueryIntEquality : BenchmarkQueryChainedOrInts {
    const char* name() const
    {
        return "QueryIntEquality";
    }

    void operator()(DBRef)
    {
        ConstTableRef table = m_table;
        for (int k = 0; k < 1000; k++) {
            Query query = table->where().equal(m_col, k);
            TableView results = query.find_all();
            REALM_ASSERT_EX(results.size() == 1, results.size(), 1);
            static_cast<void>(results);
        }
    }
};

struct BenchmarkQueryIntEqualityIndexed : BenchmarkQueryIntEquality {
    const char* name() const
    {
        return "QueryIntEqualityIndexed";
    }
    void before_all(DBRef group)
    {
        BenchmarkQueryIntEquality::before_all(group);
        WriteTransaction tr(group);
        TableRef t = tr.get_table(name());
        t->add_search_index(m_col);
        tr.commit();
    }
};

struct BenchmarkForeignAggAvg : BenchmarkWithIntsTable {
    ColKey m_double_col;
    Random m_rand;
    const size_t num_queried_matches = 1000;
    const size_t num_rows = BASE_SIZE;
    std::vector<int64_t> values_to_query;

    const char* name() const
    {
        return "QueryWithForeignAggAvg";
    }

    void before_all(DBRef group)
    {
        BenchmarkWithIntsTable::before_all(group);
        WriteTransaction tr(group);
        TableRef t = tr.get_table(name());
        m_double_col = t->add_column(type_Double, "double_col");
        std::vector<ObjKey> keys;
        t->create_objects(num_rows, keys);
        REALM_ASSERT(num_rows > num_queried_matches);
        Random r;
        size_t i = 0;
        for (auto e : *t) {
            e.set<Int>(m_col, i).set<Double>(m_double_col, double(i));
            ++i;
        }
        for (i = 0; i < num_queried_matches; ++i) {
            size_t ndx_to_match = (num_rows / num_queried_matches) * i;
            values_to_query.push_back(t->get_object(ndx_to_match).get<Int>(m_col));
        }
        tr.commit();
    }

    void operator()(DBRef)
    {
        ConstTableRef table = m_table;
        for (size_t i = 0; i < 50; ++i) {
            auto result = table->where()
                              .not_equal(m_col, values_to_query[m_rand.draw_int<size_t>(0, values_to_query.size())])
                              .avg(m_double_col);
            REALM_ASSERT(result);
        }
    }
};

struct BenchmarkQuery : BenchmarkWithStrings {
    const char* name() const
    {
        return "Query";
    }

    void operator()(DBRef)
    {
        ConstTableRef table = m_table;
        TableView view = table->find_all_string(m_col, "200");
    }
};

struct BenchmarkWithStringsTableForIn : BenchmarkWithStringsTable {
    const size_t num_queried_matches = 200;
    const size_t num_rows = BASE_SIZE;
    std::vector<std::string> values_to_query;

    void create_table(DBRef group, bool index)
    {
        BenchmarkWithStringsTable::before_all(group);
        WriteTransaction tr(group);
        TableRef t = tr.get_table(name());
        REALM_ASSERT(num_rows > num_queried_matches);
        for (size_t i = 0; i < num_rows; ++i) {
            t->create_object().set(m_col, util::to_string(i));
        }
        if (index)
            t->add_search_index(m_col);
        for (size_t i = 0; i < num_queried_matches; ++i) {
            size_t ndx_to_match = (num_rows / num_queried_matches) * i;
            auto obj = t->get_object(ndx_to_match);
            values_to_query.push_back(obj.get<String>(m_col));
        }
        tr.commit();
    }
};

template <bool index>
struct BenchmarkQueryNotChainedOrStrings : BenchmarkWithStringsTableForIn {
    const char* name() const
    {
        return index ? "QueryNotChainedOrStringsIndexed" : "QueryNotChainedOrStrings";
    }

    void before_all(DBRef group)
    {
        create_table(group, index);
    }

    void operator()(DBRef)
    {
        ConstTableRef table = m_table;
        Query query = table->where();
        query.Not();
        query.group();
        for (size_t i = 0; i < values_to_query.size(); ++i) {
            query.Or().equal(m_col, StringData(values_to_query[i]));
        }
        query.end_group();
        TableView results = query.find_all();
        REALM_ASSERT_EX(results.size() == num_rows - num_queried_matches, results.size(),
                        num_rows - num_queried_matches, values_to_query.size());
        static_cast<void>(results);
    }
};

template <bool index>
struct BenchmarkQueryChainedOrStrings : BenchmarkWithStringsTableForIn {
    const char* name() const
    {
        return index ? "QueryChainedOrStringsIndexed" : "QueryChainedOrStrings";
    }

    void before_all(DBRef group)
    {
        create_table(group, index);
    }

    void operator()(DBRef)
    {
        ConstTableRef table = m_table;
        Query query = table->where();
        for (size_t i = 0; i < values_to_query.size(); ++i) {
            query.Or().equal(m_col, StringData(values_to_query[i]));
        }
        TableView results = query.find_all();
        REALM_ASSERT_EX(results.size() == num_queried_matches, results.size(), num_queried_matches,
                        values_to_query.size());
        static_cast<void>(results);
    }
};

struct BenchmarkQueryChainedOrStringsPredicate : BenchmarkWithStringsTableForIn {
    std::set<std::string> uniq;

    void before_all(DBRef group)
    {
        create_table(group, false);
        uniq = std::set<std::string>(values_to_query.begin(), values_to_query.end());
    }
};

struct BenchmarkQueryChainedOrStringsViewFilterPredicate : BenchmarkQueryChainedOrStringsPredicate {
    const char* name() const
    {
        return "QueryChainedOrStringsViewFilterPredicate";
    }

    void operator()(DBRef)
    {
        ConstTableRef table = m_table;

        auto predicate = [this](const Obj& obj) {
            return uniq.find(obj.get<String>(m_col)) != uniq.end();
        };

        TableView results = table->where().find_all();
        results.filter(FilterDescriptor(predicate));
        REALM_ASSERT_EX(results.size() == num_queried_matches, results.size(), num_queried_matches,
                        values_to_query.size());
        static_cast<void>(results);
    }
};

struct BenchmarkSort : BenchmarkWithStrings {
    const char* name() const
    {
        return "Sort";
    }

    void operator()(DBRef)
    {
        ConstTableRef table = m_table;
        TableView view = table->get_sorted_view(m_col);
    }
};

struct BenchmarkEmptyCommit : Benchmark {
    const char* name() const
    {
        return "EmptyCommit";
    }
    void before_all(DBRef) {}
    void after_all(DBRef) {}
    void before_each(DBRef) {}
    void after_each(DBRef) {}
    void operator()(DBRef group)
    {
        WriteTransaction tr(group);
        tr.commit();
    }
};

struct BenchmarkSortInt : BenchmarkWithInts {
    const char* name() const
    {
        return "SortInt";
    }

    void operator()(DBRef)
    {
        ConstTableRef table = m_table;
        TableView view = table->get_sorted_view(m_col);
    }
};

struct BenchmarkSortIntList : Benchmark {
    const char* name() const
    {
        return "SortIntList";
    }

    void before_all(DBRef group)
    {
        WriteTransaction tr(group);
        TableRef t = tr.add_table(name());
        m_col = t->add_column_list(type_Int, "ints");
        auto obj = t->create_object();
        m_obj = obj.get_key();

        auto list = obj.get_list<int64_t>(m_col);
        Random r;
        for (size_t i = 0; i < BASE_SIZE; ++i) {
            list.add(r.draw_int<int64_t>());
        }
        tr.commit();
        m_indices.reserve(BASE_SIZE);
    }

    void after_all(DBRef db)
    {
        WriteTransaction tr(db);
        tr.get_group().remove_table(name());
        tr.commit();
    }

    void operator()(DBRef db)
    {
        realm::ReadTransaction tr(db);
        auto table = tr.get_group().get_table(name());
        auto list = table->get_object(m_obj).get_list<int64_t>(m_col);
        list.sort(m_indices, true);
        list.sort(m_indices, false);
        m_indices.clear();
    }

    ColKey m_col;
    ObjKey m_obj;
    std::vector<size_t> m_indices;
};

struct BenchmarkSortIntDictionary : Benchmark {
    const char* name() const
    {
        return "SortIntDictionary";
    }

    void before_all(DBRef group)
    {
        WriteTransaction tr(group);
        TableRef t = tr.add_table(name());
        m_col = t->add_column_dictionary(type_Int, "ints");
        auto obj = t->create_object();
        m_obj = obj.get_key();

        Dictionary dict = obj.get_dictionary(m_col);
        Random r;
        for (size_t i = 0; i < BASE_SIZE; ++i) {
            dict.insert(util::to_string(i), r.draw_int<int64_t>());
        }
        tr.commit();
        m_indices.reserve(BASE_SIZE);
    }

    void after_all(DBRef db)
    {
        WriteTransaction tr(db);
        tr.get_group().remove_table(name());
        tr.commit();
    }

    void operator()(DBRef db)
    {
        realm::ReadTransaction tr(db);
        auto table = tr.get_group().get_table(name());
        auto dict = table->get_object(m_obj).get_dictionary(m_col);
        dict.sort(m_indices, true);
        dict.sort(m_indices, false);
        m_indices.clear();
    }

    ColKey m_col;
    ObjKey m_obj;
    std::vector<size_t> m_indices;
};

struct BenchmarkSortThenLimit : Benchmark {
    const char* name() const
    {
        return "SortThenLimit";
    }

    void before_all(DBRef group)
    {
        WriteTransaction tr(group);
        TableRef t = tr.add_table(name());
        m_col = t->add_column(type_Int, "first");

        std::vector<int> values(10000);
        std::iota(values.begin(), values.end(), 0);
        std::shuffle(values.begin(), values.end(), std::mt19937(std::random_device()()));

        for (auto i : values) {
            t->create_object().set(m_col, i);
        }
        tr.commit();
    }

    void after_all(DBRef db)
    {
        WriteTransaction tr(db);
        tr.get_group().remove_table(name());
        tr.commit();
    }

    void operator()(DBRef db)
    {
        realm::ReadTransaction tr(db);
        auto tv = tr.get_group().get_table(name())->where().find_all();
        DescriptorOrdering ordering;
        ordering.append_sort(SortDescriptor({{m_col}}));
        ordering.append_limit(100);

        tv.apply_descriptor_ordering(ordering);
    }

    ColKey m_col;
};

struct BenchmarkInsert : BenchmarkWithStringsTable {
    const char* name() const
    {
        return "Insert";
    }

    void operator()(DBRef)
    {
        TableRef t = m_table;

        for (size_t i = 0; i < 10000; ++i) {
            Obj obj = t->create_object();
            obj.set(m_col, "a");
            m_keys.push_back(obj.get_key());
        }
    }
};

struct BenchmarkGetString : BenchmarkWithStrings {
    const char* name() const
    {
        return "GetString";
    }

    void operator()(DBRef)
    {
        ConstTableRef table = m_table;

        volatile int dummy = 0;
        for (auto obj : *table) {
            StringData str = obj.get<String>(m_col);
            dummy = dummy + str[0]; // to avoid over-optimization
        }
    }
};

struct BenchmarkSetString : BenchmarkWithStrings {
    const char* name() const
    {
        return "SetString";
    }

    void operator()(DBRef)
    {
        TableRef table = m_table;

        for (auto obj : *table) {
            obj.set<String>(m_col, "c");
        }
    }
};

struct BenchmarkCreateIndex : BenchmarkWithStrings {
    const char* name() const
    {
        return "CreateIndex";
    }
    void operator()(DBRef)
    {
        TableRef table = m_table;
        table->add_search_index(m_col);
    }
};

struct BenchmarkGetLongString : BenchmarkWithLongStrings {
    const char* name() const
    {
        return "GetLongString";
    }

    void operator()(DBRef)
    {
        ConstTableRef table = m_table;
        volatile int dummy = 0;
        for (auto obj : *table) {
            StringData str = obj.get<String>(m_col);
            dummy = dummy + str[0]; // to avoid over-optimization
        }
    }
};

struct BenchmarkQueryLongString : BenchmarkWithStrings {
    static constexpr const char* long_string = "This is some other long string, that takes a lot of time to find";
    bool ok;

    const char* name() const
    {
        return "QueryLongString";
    }

    void before_all(DBRef group)
    {
        BenchmarkWithStrings::before_all(group);
        WriteTransaction tr(group);
        TableRef t = tr.get_table(name());
        auto it = t->begin();
        it->set<String>(m_col, "Some random string");
        ++it;
        it->set<String>(m_col, long_string);
        tr.commit();
    }

    void operator()(DBRef)
    {
        ConstTableRef table = m_table;
        StringData str(long_string);
        ok = true;
        auto q = table->where().equal(m_col, str);
        for (size_t ndx = 0; ndx < 1000; ndx++) {
            auto res = q.find();
            if (res != ObjKey(1)) {
                ok = false;
            }
        }
    }
};

struct BenchmarkQueryInsensitiveString : BenchmarkWithStringsTable {
    const char* name() const
    {
        return "QueryInsensitiveString";
    }

    std::string gen_random_case_string(size_t length)
    {
        std::stringstream ss;
        for (size_t c = 0; c < length; ++c) {
            bool lowercase = (rand() % 2) == 0;
            // choose characters from a-z or A-Z
            ss << char((rand() % 26) + (lowercase ? 97 : 65));
        }
        return ss.str();
    }

    std::string shuffle_case(std::string str)
    {
        for (size_t i = 0; i < str.size(); ++i) {
            char c = str[i];
            if ((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z')) {
                bool change_case = (rand() % 2) == 0;
                c ^= change_case ? 0x20 : 0;
            }
            str[i] = c;
        }
        return str;
    }

    size_t rand()
    {
        return seeded_rand.draw_int<size_t>();
    }

    void before_all(DBRef group)
    {
        BenchmarkWithStringsTable::before_all(group);

        // chosen by fair dice roll, guaranteed to be random
        static const unsigned long seed = 4;
        seeded_rand.seed(seed);

        WriteTransaction tr(group);
        TableRef t = tr.get_table(name());

        const size_t max_chars_in_string = 100;

        for (size_t i = 0; i < BASE_SIZE; ++i) {
            size_t num_chars = rand() % max_chars_in_string;
            std::string randomly_cased_string = gen_random_case_string(num_chars);
            Obj obj = t->create_object();
            obj.set<String>(m_col, randomly_cased_string);
            m_keys.push_back(obj.get_key());
        }
        tr.commit();
    }
    std::string needle;
    bool successful = false;
    Random seeded_rand;

    void before_each(DBRef group)
    {
        m_tr.reset(new WriteTransaction(group)); // just go get a nonconst TableRef..
        ConstTableRef table = m_tr->get_table(name());
        size_t target_row = rand() % table->size();
        Obj obj = table->get_object(m_keys[target_row]);
        StringData target_str = obj.get<String>(m_col);
        needle = shuffle_case(target_str.data());
        m_table = m_tr->get_table(name());
    }

    void operator()(DBRef)
    {
        ConstTableRef table = m_table;
        StringData str(needle);
        for (int i = 0; i < 1000; ++i) {
            Query q = table->where().equal(m_col, str, false);
            TableView res = q.find_all();
            successful = res.size() > 0;
        }
    }
};

struct BenchmarkQueryInsensitiveStringIndexed : BenchmarkQueryInsensitiveString {
    const char* name() const
    {
        return "QueryInsensitiveStringIndexed";
    }
    void before_all(DBRef group)
    {
        BenchmarkQueryInsensitiveString::before_all(group);
        WriteTransaction tr(group);
        TableRef t = tr.get_table(name());
        t->add_search_index(m_col);
        tr.commit();
    }
};

struct BenchmarkSetLongString : BenchmarkWithLongStrings {
    const char* name() const
    {
        return "SetLongString";
    }

    void operator()(DBRef)
    {
        TableRef table = m_table;
        size_t len = m_keys.size();
        for (size_t i = 0; i < len; ++i) {
            Obj obj = table->get_object(m_keys[i]);
            obj.set<String>(m_col, "c");
        }
        // don't commit
    }
};

struct BenchmarkQueryNot : Benchmark {
    const char* name() const
    {
        return "QueryNot";
    }

    void before_all(DBRef group)
    {
        WriteTransaction tr(group);
        TableRef table = tr.add_table(name());
        m_col = table->add_column(type_Int, "first");
        for (size_t i = 0; i < BASE_SIZE; ++i) {
            table->create_object().set(m_col, 1);
        }
        tr.commit();
    }

    void operator()(DBRef)
    {
        ConstTableRef table = m_table;
        Query q = table->where();
        q.not_equal(m_col, 2); // never found, = worst case
        TableView results = q.find_all();
        results.size();
    }

    void after_all(DBRef group)
    {
        WriteTransaction tr(group);
        tr.get_group().remove_table(name());
        tr.commit();
    }
};

struct BenchmarkGetLinkList : Benchmark {
    const char* name() const
    {
        return "GetLinkList";
    }
    static const size_t rows = BASE_SIZE;

    void before_all(DBRef group)
    {
        WriteTransaction tr(group);
        std::string n = std::string(name()) + "_Destination";
        TableRef destination_table = tr.add_table(n);
        TableRef table = tr.add_table(name());
        m_col_link = table->add_column_list(*destination_table, "linklist");
        table->create_objects(rows, m_keys);
        tr.commit();
    }

    void operator()(DBRef)
    {
        ConstTableRef table = m_table;
        std::vector<LnkLstPtr> linklists(rows);
        for (size_t i = 0; i < rows; ++i) {
            auto obj = table->get_object(m_keys[i]);
            linklists[i] = obj.get_linklist_ptr(m_col_link);
        }
        for (size_t i = 0; i < rows; ++i) {
            auto obj = table->get_object(m_keys[i]);
            obj.get_linklist_ptr(m_col_link);
        }
        for (size_t i = 0; i < rows; ++i) {
            linklists[i].reset();
        }
    }

    void after_all(DBRef group)
    {
        WriteTransaction tr(group);
        tr.get_group().remove_table(name());
        auto n = std::string(name()) + "_Destination";
        tr.get_group().remove_table(n);
        tr.commit();
    }

    ColKey m_col_link;
};

struct BenchmarkNonInitiatorOpen : Benchmark {
    const char* name() const
    {
        return "NonInitiatorOpen";
    }
    // the shared realm will be removed after the benchmark finishes
    std::unique_ptr<realm::test_util::DBTestPathGuard> path;
    DBRef initiator;

    DBRef do_open()
    {
        return DB::create(*path, DBOptions(m_durability, m_encryption_key));
    }

    void before_all(DBRef)
    {
        // Generate the benchmark result texts:
        std::string ident = util::format("BenchmarkCommonTasks_%1_%2", this->name(), to_ident_cstr(m_durability));

        unit_test::TestDetails test_details;
        test_details.suite_name = "BenchmarkCommonTasks";
        test_details.test_name = ident.c_str();
        test_details.file_name = __FILE__;
        test_details.line_number = __LINE__;

        path = std::make_unique<DBTestPathGuard>(get_test_path(ident, ".realm"));

        // open once - session initiation
        initiator = do_open();
    }
    void before_each(DBRef) {}
    void after_each(DBRef) {}
    void operator()(DBRef)
    {
        // use groups of 10 to get higher times
        for (size_t i = 0; i < 10; ++i) {
            do_open();
            // let it close, otherwise we get error: too many open files
        }
    }
};

struct BenchmarkInitiatorOpen : public BenchmarkNonInitiatorOpen {
    const char* name() const
    {
        return "InitiatorOpen";
    }
    void before_all(DBRef r)
    {
        BenchmarkNonInitiatorOpen::before_all(r); // create file
        initiator.reset();                        // for close.
    }
};

struct IterateTableByIterator : Benchmark {
    const char* name() const override
    {
        return "IterateTableByIterator";
    }

    static const int row_count = 100'000;

    void before_all(DBRef db) override
    {
        WriteTransaction tr(db);
        TableRef t = tr.add_table(name());
        for (int i = 0; i < row_count; ++i)
            t->create_object();
        tr.commit();

        m_tr.reset(new WriteTransaction(db));
        m_table = m_tr->get_table(name());
    }
    void after_all(DBRef) override
    {
        m_tr.reset();
    }
    void before_each(DBRef) override {}
    void after_each(DBRef) override {}

    void operator()(DBRef) override
    {
        for (auto& obj : *m_table)
            static_cast<void>(obj.get_key());
    }
};

struct IterateTableByIteratorIndex : Benchmark {
    const char* name() const override
    {
        return "IterateTableByIteratorIndex";
    }

    static const int row_count = 100'000;

    void before_all(DBRef db) override
    {
        WriteTransaction tr(db);
        TableRef t = tr.add_table(name());
        for (int i = 0; i < row_count; ++i)
            t->create_object();
        tr.commit();

        m_tr.reset(new WriteTransaction(db));
        m_table = m_tr->get_table(name());
    }
    void after_all(DBRef) override
    {
        m_tr.reset();
    }
    void before_each(DBRef) override {}
    void after_each(DBRef) override {}

    void operator()(DBRef) override
    {
        auto it = m_table->begin();
        for (size_t i = 0; i < m_table->size(); ++i) {
            it.go(i);
            static_cast<void>(it->get_key());
        }
    }
};

struct IterateTableByIndexNoPrimaryKey : Benchmark {
    const char* name() const override
    {
        return "IterateTableByIndexNoPrimaryKey";
    }

    static const int row_count = 100'000;

    void before_all(DBRef db) override
    {
        WriteTransaction tr(db);
        TableRef t = tr.add_table(name());
        for (int i = 0; i < row_count; ++i)
            t->create_object();
        tr.commit();

        m_tr.reset(new WriteTransaction(db));
        m_table = m_tr->get_table(name());
    }
    void after_all(DBRef) override
    {
        m_tr.reset();
    }
    void before_each(DBRef) override {}
    void after_each(DBRef) override {}

    void operator()(DBRef) override
    {
        for (size_t i = 0, size = m_table->size(); i < size; ++i) {
            m_table->get_object(i);
        }
    }
};

struct IterateTableByIndexIntPrimaryKey : IterateTableByIndexNoPrimaryKey {
    const char* name() const override
    {
        return "IterateTableByIndexIntPrimaryKey";
    }

    void before_all(DBRef db) override
    {
        WriteTransaction tr(db);
        TableRef t = tr.get_group().add_table_with_primary_key("class_table", type_Int, "pk", false);
        for (int i = 0; i < row_count; ++i)
            t->create_object_with_primary_key(i);
        tr.commit();

        m_tr.reset(new WriteTransaction(db));
        m_table = m_tr->get_table("class_table");
    }
};

struct IterateTableByIndexStringPrimaryKey : IterateTableByIndexNoPrimaryKey {
    const char* name() const override
    {
        return "IterateTableByIndexStringPrimaryKey";
    }

    void before_all(DBRef db) override
    {
        WriteTransaction tr(db);

        TableRef t = tr.get_group().add_table_with_primary_key("class_table", type_String, "pk", false);
        for (int i = 0; i < row_count; ++i)
            t->create_object_with_primary_key(util::to_string(i).c_str());
        tr.commit();

        m_tr.reset(new WriteTransaction(db));
        m_table = m_tr->get_table("class_table");
    }
};

struct TransactionDuplicate : Benchmark {
    const char* name() const
    {
        return "TransactionDuplicate";
    }

    void operator()(DBRef db)
    {
        auto tr = db->start_read();
        for (int i = 0; i < 10'000; ++i)
            tr->duplicate();
    }
    void before_each(DBRef) {}
    void after_each(DBRef) {}
};

#if REALM_ENABLE_GEOSPATIAL

struct BenchmarkWithGeospatial : Benchmark {
    std::string loc_name() const
    {
        return std::string(name()) + "Location";
    }

    void before_all(DBRef group) override
    {
        WriteTransaction tr(group);

        bool was_added = false;
        auto t = tr.get_or_add_table(name(), Table::Type::TopLevel, &was_added);

        if (was_added) {
            auto loc = tr.add_table(loc_name(), Table::Type::Embedded);
            loc->add_column(type_String, "type");
            loc->add_column_list(type_Double, "coordinates");
            m_col = t->add_column(*loc, "location");
        }

        tr.commit();
    }

    void after_all(DBRef group) override
    {
        (void)group;
        WriteTransaction tr(group);
        tr.get_group().remove_table(name());
        tr.get_group().remove_table(loc_name());
        tr.commit();
    }

    void assign_points(TableRef table)
    {
        for (size_t i = 0; i < BASE_SIZE; ++i) {
            double lon = ((double)i / BASE_SIZE) * 360 - 180;
            double lat = (i / 1000.0) / (BASE_SIZE / 1000.0) * 180 - 90;
            table->get_object(i).set(m_col, Geospatial{GeoPoint{lon, lat, 42.24}});
        }
    }

    void add_records(TableRef table, bool assign_value = true)
    {
        for (size_t i = 0; i < BASE_SIZE; ++i)
            table->create_object();

        if (assign_value)
            assign_points(table);
    }
};

struct BenchmarkWithGeoPoints : BenchmarkWithGeospatial {
    void before_all(DBRef group) override
    {
        BenchmarkWithGeospatial::before_all(group);
        WriteTransaction tr(group);
        add_records(tr.get_table(name()));
        tr.commit();
    }
};

struct BenchmarkAssignGeoPoints : BenchmarkWithGeoPoints {
    const char* name() const override
    {
        return "AssignGeoPoints";
    }

    void operator()(DBRef) override
    {
        assign_points(m_table);
    }
};

struct BenchmarkAssignGeoPointsFromNull : BenchmarkWithGeoPoints {
    const char* name() const override
    {
        return "AssignGeoPointsFromNull";
    }

    void before_all(DBRef group) override
    {
        BenchmarkWithGeospatial::before_all(group);
        WriteTransaction tr(group);
        add_records(tr.get_table(name()), false);
        tr.commit();
    }

    void operator()(DBRef) override
    {
        assign_points(m_table);
    }
};

struct BenchmarkFetchGeoPoints : BenchmarkWithGeoPoints {
    const char* name() const override
    {
        return "FetchGeoPoints";
    }

    void operator()(DBRef) override
    {
        for (size_t i = 0; i < BASE_SIZE; ++i) {
            auto g = m_table->get_object(i).get<Geospatial>(m_col);
            if (!g.is_valid().is_ok() || g.get_type() != Geospatial::Type::Point)
                throw std::logic_error("Invalid GeoPoint");
        }
    }
};

struct BenchmarkGeoPointsWithinBox : BenchmarkWithGeoPoints {
    const char* name() const override
    {
        return "GeoPointsWithinBox";
    }

    void operator()(DBRef) override
    {
        auto geometry = GeoBox{{-34.0, -34.0}, {42.0, 42.0}};
        m_table->column<Link>(m_col).geo_within(geometry).count();
    }
};

struct BenchmarkGeoPointsWithinBoxRQL : BenchmarkWithGeoPoints {
    const char* name() const override
    {
        return "GeoPointsWithinBoxRQL";
    }

    void operator()(DBRef) override
    {
        m_table->query("location geoWithin geoBox([-34.0, -34.0], [42.0, 42.0])").count();
    }
};

struct BenchmarkGeoPointsWithinCircle : BenchmarkWithGeoPoints {
    const char* name() const override
    {
        return "GeoPointsWithinCircle";
    }

    void operator()(DBRef) override
    {
        auto geometry = GeoCircle::from_kms(5000, {42.0, 42.0});
        m_table->column<Link>(m_col).geo_within(geometry).count();
    }
};

struct BenchmarkGeoPointsWithinCircleRQL : BenchmarkWithGeoPoints {
    const char* name() const override
    {
        return "GeoPointsWithinCircleRQL";
    }

    void operator()(DBRef) override
    {
        m_table->query("location geoWithin geoCircle([42.0, 42.0], 0.78393252)").count();
    }
};

struct BenchmarkGeoPointsWithinPolygon : BenchmarkWithGeoPoints {
    const char* name() const override
    {
        return "GeoPointsWithinPolygon";
    }

    void operator()(DBRef) override
    {
        GeoPolygon geometry{{{{-24, -24}, {-34, 34}, {44, 44}, {-55, 55}, {-24, -24}}}};
        m_table->column<Link>(m_col).geo_within(geometry).count();
    }
};

struct BenchmarkGeoPointsWithinPolygonRQL : BenchmarkWithGeoPoints {
    const char* name() const override
    {
        return "GeoPointsWithinPolygonRQL";
    }

    void operator()(DBRef) override
    {
        m_table
            ->query("location geoWithin geoPolygon({[-24.0, -24.0], [-34.0, 34.0], [44.0, 44.0], [-55.0, 55], "
                    "[-24.0, -24.0]})")
            .count();
    }
};

#endif

const char* to_lead_cstr(DBOptions::Durability level)
{
    switch (level) {
        case DBOptions::Durability::Full:
            return "Full   ";
        case DBOptions::Durability::MemOnly:
            return "MemOnly";
        case DBOptions::Durability::Unsafe:
            return "Unsafe ";
    }
    return nullptr;
}

const char* to_ident_cstr(DBOptions::Durability level)
{
    switch (level) {
        case DBOptions::Durability::Full:
            return "Full";
        case DBOptions::Durability::MemOnly:
            return "MemOnly";
        case realm::DBOptions::Durability::Unsafe:
            return "Unsafe";
    }
    return nullptr;
}

void run_benchmark_once(Benchmark& benchmark, DBRef sg, Timer& timer)
{
    timer.pause();
    benchmark.before_each(sg);
    timer.unpause();

    benchmark(sg);

    timer.pause();
    benchmark.after_each(sg);
    timer.unpause();
}

/// This little piece of likely over-engineering runs the benchmark a number of times,
/// with each durability setting, and reports the results for each run.
template <typename B>
void run_benchmark(BenchmarkResults& results, bool force_full = false)
{
    typedef std::pair<DBOptions::Durability, const char*> config_pair;
    std::vector<config_pair> configs;

    if (force_full) {
        configs.push_back(config_pair(DBOptions::Durability::Full, nullptr));
#if REALM_ENABLE_ENCRYPTION
        configs.push_back(config_pair(DBOptions::Durability::Full, crypt_key(true)));
#endif
    }
    else {
        configs.push_back(config_pair(DBOptions::Durability::MemOnly, nullptr));
    }

    Timer timer(Timer::type_UserTime);

    for (auto it = configs.begin(); it != configs.end(); ++it) {
        DBOptions::Durability level = it->first;
        const char* key = it->second;

        B benchmark;
        if (should_filter_benchmark(benchmark.name()))
            return;

        benchmark.m_durability = level;
        benchmark.m_encryption_key = key;

        // Generate the benchmark result texts:
        std::stringstream lead_text_ss;
        std::stringstream ident_ss;
        lead_text_ss << benchmark.name() << " (" << to_lead_cstr(level) << ", "
                     << (key == nullptr ? "EncryptionOff" : "EncryptionOn") << ")";
        ident_ss << benchmark.name() << "_" << to_ident_cstr(level)
                 << (key == nullptr ? "_EncryptionOff" : "_EncryptionOn");
        std::string ident = ident_ss.str();

        realm::test_util::unit_test::TestDetails test_details;
        test_details.suite_name = "BenchmarkCommonTasks";
        test_details.test_name = ident.c_str();
        test_details.file_name = __FILE__;
        test_details.line_number = __LINE__;

        // Open a SharedGroup:
        realm::test_util::DBTestPathGuard realm_path(
            test_util::get_test_path("benchmark_common_tasks_" + ident, ".realm"));
        DBRef group;
        group = DB::create(realm_path, DBOptions(level, key));
        benchmark.before_all(group);

        // Warm-up and initial measuring:
        size_t num_warmup_reps = 1;
        double time_to_execute_warmup_reps = 0;
        while (time_to_execute_warmup_reps < min_warmup_time_s && num_warmup_reps < max_repetitions) {
            num_warmup_reps *= 3;
            Timer t(Timer::type_UserTime);
            for (size_t i = 0; i < num_warmup_reps; ++i) {
                run_benchmark_once(benchmark, group, t);
            }
            time_to_execute_warmup_reps = t.get_elapsed_time();
        }
        double time_to_execute_one_rep = time_to_execute_warmup_reps / num_warmup_reps;
        size_t required_reps = size_t(min_duration_s / time_to_execute_one_rep);
        if (required_reps < min_repetitions) {
            required_reps = min_repetitions;
        }
        if (required_reps > max_repetitions) {
            required_reps = max_repetitions;
        }
        std::cout << "Req runs: " << std::setw(4) << required_reps << "  ";
        for (size_t rep = 0; rep < required_reps; ++rep) {
            Timer t;
            run_benchmark_once(benchmark, group, t);
            double s = t.get_elapsed_time();
            results.submit(ident.c_str(), s);
        }

        benchmark.after_all(group);

        results.finish(ident, lead_text_ss.str(), "runtime_secs");
    }
    std::cout << std::endl;
}

} // anonymous namespace

extern "C" int benchmark_common_tasks_main();

int benchmark_common_tasks_main()
{
    std::string results_file_stem = realm::test_util::get_test_path_prefix();
    std::cout << "Results path: " << results_file_stem << std::endl;
    results_file_stem += "results";
    BenchmarkResults results(40, "benchmark-common-tasks", results_file_stem.c_str());

#define BENCH(...) run_benchmark<__VA_ARGS__>(results)
#define BENCH2(B, mode) run_benchmark<B>(results, mode)
    BENCH2(BenchmarkEmptyCommit, true);
    BENCH2(BenchmarkEmptyCommit, false);
    BENCH2(BenchmarkNonInitiatorOpen, true);
    BENCH2(BenchmarkInitiatorOpen, true);
    BENCH2(AddTable, true);
    BENCH2(AddTable, false);

    BENCH(IterateTableByIndexNoPrimaryKey);
    BENCH(IterateTableByIndexIntPrimaryKey);
    BENCH(IterateTableByIndexStringPrimaryKey);
    BENCH(IterateTableByIterator);
    BENCH(IterateTableByIteratorIndex);

    BENCH(BenchmarkSort);
    BENCH(BenchmarkSortInt);
    BENCH(BenchmarkSortIntList);
    BENCH(BenchmarkSortIntDictionary);
    BENCH(BenchmarkSortThenLimit);

    BENCH(BenchmarkUnorderedTableViewClear);
    BENCH(BenchmarkUnorderedTableViewClearIndexed);

    // getting/setting - tableview or not
    BENCH(BenchmarkGetString);
    BENCH(BenchmarkSetString);
    BENCH(BenchmarkGetLinkList);
    BENCH(BenchmarkInsert);
    BENCH2(BenchmarkCreateIndex, true);
    BENCH2(BenchmarkCreateIndex, false);
    BENCH(BenchmarkGetLongString);
    BENCH(BenchmarkSetLongString);

    // queries / searching
    BENCH(BenchmarkFindAllStringFewDupes);
    BENCH(BenchmarkFindAllStringManyDupes);
    BENCH(BenchmarkFindAllFulltextStringManyDupes);
    BENCH(BenchmarkFindFirstStringFewDupes);
    BENCH(BenchmarkFindFirstStringManyDupes);
    BENCH(BenchmarkCountStringManyDupes<false>);
    BENCH(BenchmarkCountStringManyDupes<true>);
    BENCH(BenchmarkQuery);
    BENCH(BenchmarkQueryNot);
    BENCH(BenchmarkQueryLongString);

    BENCH(BenchmarkQueryInsensitiveString);
    BENCH(BenchmarkQueryInsensitiveStringIndexed);
    BENCH(BenchmarkQueryChainedOrStrings<false>);
    BENCH(BenchmarkQueryChainedOrStrings<true>);
    BENCH(BenchmarkQueryChainedOrStringsViewFilterPredicate);
    BENCH(BenchmarkQueryNotChainedOrStrings<false>);
    BENCH(BenchmarkQueryNotChainedOrStrings<true>);
    BENCH(BenchmarkQueryChainedOrInts);
    BENCH(BenchmarkQueryChainedOrIntsIndexed);
    BENCH(BenchmarkQueryChainedOrIntsCount);
    BENCH(BenchmarkQueryIntEquality);
    BENCH(BenchmarkQueryIntEqualityIndexed);
    BENCH(BenchmarkForeignAggAvg);
    BENCH(BenchmarkIntVsDoubleColumns);
    BENCH(BenchmarkQueryStringOverLinks);
    BENCH(BenchmarkSubQuery);
    BENCH(BenchmarkWithType<Indexed<Mixed>>);
    BENCH(BenchmarkWithType<Prop<Mixed>>);
    BENCH(BenchmarkWithType<Indexed<UUID>>);
    BENCH(BenchmarkWithType<Prop<UUID>>);
    BENCH(BenchmarkWithType<Indexed<ObjectId>>);
    BENCH(BenchmarkWithType<Prop<ObjectId>>);
    BENCH(BenchmarkWithType<Indexed<Timestamp>>);
    BENCH(BenchmarkWithType<Prop<Timestamp>>);
    BENCH(BenchmarkWithType<Indexed<Int>>);
    BENCH(BenchmarkWithType<Indexed<String>>);
    BENCH(BenchmarkWithType<Indexed<Bool>>);
    BENCH(BenchmarkWithType<Prop<Bool>>);
    BENCH(BenchmarkMixedCaseInsensitiveEqual<Prop<Mixed>>);
    BENCH(BenchmarkMixedCaseInsensitiveEqual<Indexed<Mixed>>);
    BENCH(BenchmarkMixedCaseInsensitiveEqual<Prop<String>>);
    BENCH(BenchmarkMixedCaseInsensitiveEqual<Indexed<String>>);

    BENCH(BenchmarkParsedChainedOrEquality<Indexed<UUID>, 5000>);
    BENCH(BenchmarkParsedChainedOrEquality<Indexed<ObjectId>, 5000>);
    BENCH(BenchmarkParsedChainedOrEquality<Prop<UUID>, 5000>);
    BENCH(BenchmarkParsedChainedOrEquality<Prop<ObjectId>, 5000>);
    BENCH(BenchmarkParsedIn<Indexed<UUID>, 5000>);
    BENCH(BenchmarkParsedIn<Indexed<ObjectId>, 5000>);
    BENCH(BenchmarkParsedIn<Prop<UUID>, 5000>);
    BENCH(BenchmarkParsedIn<Prop<ObjectId>, 5000>);
    BENCH(BenchmarkParsedIn<Indexed<UUID>, 5>);
    BENCH(BenchmarkParsedIn<Indexed<ObjectId>, 5>);
    BENCH(BenchmarkParsedIn<Prop<UUID>, 5>);
    BENCH(BenchmarkParsedIn<Prop<ObjectId>, 5>);

    BENCH(BenchmarkRangeForType<Prop<Int>>);
    BENCH(BenchmarkRangeForType<Prop<Timestamp>>);
    BENCH(BenchmarkCreateIndexForType<NullableIndexed<String>>);
    BENCH(BenchmarkCreateIndexForType<NullableIndexed<Int>>);
    BENCH(BenchmarkCreateIndexForType<NullableIndexed<Timestamp>>);

    BENCH(BenchmarkInsertToIndexForType<NullableIndexed<String>>);
    BENCH(BenchmarkInsertToIndexForType<NullableIndexed<Int>>);
    BENCH(BenchmarkInsertToIndexForType<NullableIndexed<Timestamp>>);

    BENCH(BenchmarkInsertPKToIndexForType<NullableIndexed<String>>);
    BENCH(BenchmarkInsertPKToIndexForType<NullableIndexed<Int>>);

    BENCH(BenchmarkEraseObjectForType<Prop<String>>);
    BENCH(BenchmarkEraseObjectForType<Prop<Int>>);
    BENCH(BenchmarkEraseObjectForType<Prop<Timestamp>>);
    BENCH(BenchmarkEraseObjectForType<NullableIndexed<String>>);
    BENCH(BenchmarkEraseObjectForType<NullableIndexed<Int>>);
    BENCH(BenchmarkEraseObjectForType<NullableIndexed<Timestamp>>);

    BENCH(BenchmarkQueryTimestampGreaterOverLinks);
    BENCH(BenchmarkQueryTimestampGreater);
    BENCH(BenchmarkQueryTimestampGreaterEqual);
    BENCH(BenchmarkQueryTimestampLess);
    BENCH(BenchmarkQueryTimestampLessEqual);
    BENCH(BenchmarkQueryTimestampEqual);
    BENCH(BenchmarkQueryTimestampNotEqual);
    BENCH(BenchmarkQueryTimestampNotNull);
    BENCH(BenchmarkQueryTimestampEqualNull);
    BENCH(BenchmarkQueryIntListSize);

    BENCH(BenchmarkWithIntUIDsRandomOrderSeqAccess);
    BENCH(BenchmarkWithIntUIDsRandomOrderRandomAccess);
    BENCH(BenchmarkWithIntUIDsRandomOrderRandomDelete);
    BENCH(BenchmarkWithIntUIDsRandomOrderRandomCreate);

    BENCH(TransactionDuplicate);

#if REALM_ENABLE_GEOSPATIAL
    BENCH(BenchmarkAssignGeoPoints);
    BENCH(BenchmarkAssignGeoPointsFromNull);
    BENCH(BenchmarkFetchGeoPoints);
    BENCH(BenchmarkGeoPointsWithinBox);
    BENCH(BenchmarkGeoPointsWithinBoxRQL);
    BENCH(BenchmarkGeoPointsWithinCircle);
    BENCH(BenchmarkGeoPointsWithinCircleRQL);
    BENCH(BenchmarkGeoPointsWithinPolygon);
    BENCH(BenchmarkGeoPointsWithinPolygonRQL);
#endif

#undef BENCH
#undef BENCH2
    return 0;
}

int main(int argc, const char** argv)
{
    if (argc > 1) {
        std::string arg_path = argv[1];
        if (arg_path == "-h" || arg_path == "--help") {
            std::cout << "Usage: " << argv[0] << " [-h|--help] [PATH] [NAMES]" << std::endl
                      << "Run the common tasks benchmark test application." << std::endl
                      << "Results are placed in the executable directory by default." << std::endl
                      << std::endl
                      << "Arguments:" << std::endl
                      << "  -h, --help      display this help" << std::endl
                      << "  PATH            alternate path to store the results files;" << std::endl
                      << "                  this path should end with a slash." << std::endl
                      << "  NAMES           benchmark names to run (':' separated)" << std::endl
                      << "                  Full regex is supported as a filter for a name" << std::endl
                      << std::endl;
            return 1;
        }
    }

    if (!initialize_test_path(argc, argv))
        return 1;

    if (argc > 2) {
        std::string filter = argv[2];
        for (size_t i = 0, j = 0, len = filter.size(); i <= len; ++i) {
            if (i == len || filter[i] == ':') {
                if (j < i)
                    g_bench_filter.emplace_back(filter.substr(j, i - j));
                j = i + 1;
            }
        }
    }

    return benchmark_common_tasks_main();
}
