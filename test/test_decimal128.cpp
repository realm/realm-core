/*************************************************************************
 *
 * Copyright 2019 Realm Inc.
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

#include <realm.hpp>
#include <realm/array_decimal128.hpp>

#include "test.hpp"


using namespace realm;

TEST(Decimal_Basics)
{
    auto test_str_nan = [&](const std::string& str) {
        Decimal128 d = Decimal128(str);
        CHECK_EQUAL(d.to_string(), "NaN");
    };
    auto test_str = [&](const std::string& str, const std::string& ref) {
        Decimal128 d = Decimal128(str);
        CHECK_EQUAL(d.to_string(), ref);
    };
    test_str("0", "0");
    test_str("0.000", "0E-3");
    test_str("0E-3", "0E-3");
    test_str("3.1416", "3.1416");
    test_str("3.1416e-4", "3.1416E-4");
    test_str("-3.1416e-4", "-3.1416E-4");
    test_str("10e2", "1.0E3");
    test_str("10e+2", "1.0E3");
    test_str("1e-00021", "1E-21");
    test_str("10.100e2", "1010.0");
    test_str(".00000001", "1E-8");
    test_str(".00000001000000000", "1.000000000E-8");
    test_str("1.14142E27", "1.14142E27");
    test_str("+Infinity", "Inf");
    test_str("-INF", "-Inf");
    test_str("  0", "0");
    test_str_nan(":");
    test_str_nan("0.0.0");
    test_str("9.99e6144", "+9990000000000000000000000000000000E+6111"); // largest decimal128
    test_str("1.701e38", "1.701E38");                                   // largest float
    test_str("1.797e308", "1.797E308");                                 // largest double
    test_str_nan("0.0Q1");
    test_str_nan("0.0Eq");
    Decimal128 pi = Decimal128("3.141592653589793238"); // 19 significant digits
    CHECK_EQUAL(pi.to_string(), "3.141592653589793238");
    Decimal128::Bid128 bid;
    int exp;
    bool sign;
    pi.unpack(bid, exp, sign);
    Decimal128 pi2(bid, exp, sign);
    CHECK_EQUAL(pi, pi2);
    Decimal128 d = Decimal128("-10.5");
    Decimal128 d1 = Decimal128("20.25");
    CHECK(d < d1);
    Decimal128 d2 = Decimal128("100");
    CHECK(d1 < d2);
    Decimal128 d3 = Decimal128("-1000.5");
    CHECK(d3 < d1);
    CHECK(d3 < d2);
    CHECK(d1 > d3);
    CHECK(d2 > d3);
    CHECK(d3 + d3 < d3);

    Decimal128 y;
    CHECK(!y.is_null());
    y = d1;

    Decimal128 d10(10);
    CHECK(d10 < d2);
    CHECK(d10 >= d);
}

TEST(Decimal_Arithmetics)
{
    Decimal128 d(10);
    Decimal128 q;

    q = d + Decimal128(20);
    CHECK_EQUAL(q.to_string(), "30");
    q = d + Decimal128(-20);
    CHECK_EQUAL(q.to_string(), "-10");
    q = Decimal128(20);
    q += d;
    CHECK_EQUAL(q.to_string(), "30");

    q = d - Decimal128(15);
    CHECK_EQUAL(q.to_string(), "-5");
    q = d - Decimal128(-15);
    CHECK_EQUAL(q.to_string(), "25");
    q = Decimal128(20);
    q -= d;
    CHECK_EQUAL(q.to_string(), "10");

    q = d / int(4);
    CHECK_EQUAL(q.to_string(), "2.5");
    q = d / size_t(4);
    CHECK_EQUAL(q.to_string(), "2.5");
    q = d / int64_t(4);
    CHECK_EQUAL(q.to_string(), "2.5");
    q = d / int(-4);
    CHECK_EQUAL(q.to_string(), "-2.5");
    q = d / int64_t(-4);
    CHECK_EQUAL(q.to_string(), "-2.5");
    q = Decimal128(20);
    q /= d;
    CHECK_EQUAL(q.to_string(), "2");

    q = d * int(4);
    CHECK_EQUAL(q.to_string(), "40");
    q = d * size_t(5);
    CHECK_EQUAL(q.to_string(), "50");
    q = d * int64_t(6);
    CHECK_EQUAL(q.to_string(), "60");
    q = d * int(-4);
    CHECK_EQUAL(q.to_string(), "-40");
    q = d * int64_t(-6);
    CHECK_EQUAL(q.to_string(), "-60");
    q = Decimal128(20);
    q *= d;
    CHECK_EQUAL(q.to_string(), "200");
}

TEST(Decimal_Array)
{
    const char str0[] = "12345.67";
    const char str1[] = "1000.00";
    const char str2[] = "-45";

    ArrayDecimal128 arr(Allocator::get_default());
    arr.create();

    arr.add(Decimal128(str0));
    arr.add(Decimal128(str1));
    arr.insert(1, Decimal128(str2));

    Decimal128 id2(str2);
    CHECK_EQUAL(arr.get(0), Decimal128(str0));
    CHECK_EQUAL(arr.get(1), id2);
    CHECK_EQUAL(arr.get(2), Decimal128(str1));
    CHECK_EQUAL(arr.find_first(id2), 1);

    arr.erase(1);
    CHECK_EQUAL(arr.get(1), Decimal128(str1));

    ArrayDecimal128 arr1(Allocator::get_default());
    arr1.create();
    arr.move(arr1, 1);

    CHECK_EQUAL(arr.size(), 1);
    CHECK_EQUAL(arr1.size(), 1);
    CHECK_EQUAL(arr1.get(0), Decimal128(str1));

    arr.destroy();
    arr1.destroy();
}

TEST(Decimal_Table)
{
    const char str0[] = "12345.67";
    const char str1[] = "1000.00";

    Table t;
    auto col_price = t.add_column(type_Decimal, "id");
    auto obj0 = t.create_object().set(col_price, Decimal128(str0));
    auto obj1 = t.create_object().set(col_price, Decimal128(str1));
    CHECK_EQUAL(obj0.get<Decimal128>(col_price), Decimal128(str0));
    CHECK_EQUAL(obj1.get<Decimal128>(col_price), Decimal128(str1));
    auto key = t.find_first(col_price, Decimal128(str1));
    CHECK_EQUAL(key, obj1.get_key());
    auto d = obj1.get_any(col_price);
    CHECK_EQUAL(d.get<Decimal128>().to_string(), "1000.00");
}


TEST(Decimal_Query)
{
    SHARED_GROUP_TEST_PATH(path);
    DBRef db = DB::create(path);

    {
        auto wt = db->start_write();
        auto table = wt->add_table("Foo");
        auto col_dec = table->add_column(type_Decimal, "price", true);
        auto col_int = table->add_column(type_Int, "size");
        auto col_str = table->add_column(type_String, "description");
        for (int i = 1; i < 100; i++) {
            auto obj = table->create_object().set(col_dec, Decimal128(i)).set(col_int, i % 10);
            if ((i % 19) == 0) {
                obj.set(col_str, "Nice");
            }
        }
        table->create_object(); // Contains null

        auto bar = wt->add_table("Bar");
        bar->add_column(type_Decimal, "dummy", true);
        ObjKeys keys;
        bar->create_objects(10, keys); // All nulls

        wt->commit();
    }
    {
        auto rt = db->start_read();
        auto table = rt->get_table("Foo");
        auto col = table->get_column_key("price");
        auto col_int = table->get_column_key("size");
        auto col_str = table->get_column_key("description");
        Query q = table->column<Decimal>(col) > Decimal128(0);
        CHECK_EQUAL(q.count(), 99);
        q = table->where().greater(col, Decimal128(0));
        CHECK_EQUAL(q.count(), 99);
        Query q1 = table->column<Decimal>(col) < Decimal128(25);
        CHECK_EQUAL(q1.count(), 24);
        q1 = table->where().less(col, Decimal128(25));
        CHECK_EQUAL(q1.count(), 24);
        q1 = table->where().less_equal(col, Decimal128(25));
        CHECK_EQUAL(q1.count(), 25);
        Query q2 = table->column<Decimal>(col) == realm::null();
        CHECK_EQUAL(q2.count(), 1);
        q2 = table->where().equal(col, realm::null());
        CHECK_EQUAL(q2.count(), 1);
        q2 = table->where().between(col, Decimal128(25), Decimal128(60));
        CHECK_EQUAL(q2.count(), 36);
        Decimal128 sum;
        Decimal128 max;
        Decimal128 min(100);
        size_t cnt = 0;
        for (auto o : *table) {
            if (o.get<Int>(col_int) == 3) {
                auto val = o.get<Decimal128>(col);
                sum += val;
                cnt++;
                if (val > max)
                    max = val;
                if (val < min)
                    min = val;
            }
        }
        size_t actual;
        CHECK_EQUAL(table->where().equal(col_int, 3).sum_decimal128(col), sum);
        CHECK_EQUAL(table->where().equal(col_int, 3).average_decimal128(col, &actual), sum / cnt);
        CHECK_EQUAL(actual, cnt);
        CHECK_EQUAL(table->where().equal(col_int, 3).maximum_decimal128(col), max);
        CHECK_EQUAL(table->where().equal(col_int, 3).minimum_decimal128(col), min);
        CHECK_EQUAL(table->where().equal(col_str, "Nice").sum_decimal128(col), Decimal128(285));
        CHECK_EQUAL(table->where().equal(col_str, "Nice").average_decimal128(col), Decimal128(57));
        CHECK_EQUAL(table->where().equal(col_str, "Nice").maximum_decimal128(col), Decimal128(95));
        CHECK_EQUAL(table->where().equal(col_str, "Nice").minimum_decimal128(col), Decimal128(19));
        CHECK_EQUAL(table->where().average_decimal128(col), Decimal128(50));

        table = rt->get_table("Bar");
        col = table->get_column_key("dummy");
        CHECK_EQUAL(table->where().average_decimal128(col, &actual), Decimal128(0));
        CHECK_EQUAL(actual, 0);
        CHECK_EQUAL(table->where().sum_decimal128(col), Decimal128(0));
        ObjKey k;
        CHECK_EQUAL(table->where().maximum_decimal128(col, &k), Decimal128(0));
        CHECK_NOT(k);
        CHECK_EQUAL(table->where().minimum_decimal128(col, &k), Decimal128(0));
        CHECK_NOT(k);
    }
}

TEST(Decimal_Distinct)
{
    SHARED_GROUP_TEST_PATH(path);
    DBRef db = DB::create(path);

    {
        auto wt = db->start_write();
        auto table = wt->add_table("Foo");
        auto col_dec = table->add_column(type_Decimal, "price", true);
        for (int i = 1; i < 100; i++) {
            auto obj = table->create_object().set(col_dec, Decimal128(i % 10));
        }

        wt->commit();
    }
    {
        auto rt = db->start_read();
        auto table = rt->get_table("Foo");
        ColKey col = table->get_column_key("price");
        DescriptorOrdering order;
        order.append_distinct(DistinctDescriptor({{col}}));
        auto tv = table->where().find_all(order);
        CHECK_EQUAL(tv.size(), 10);
    }
}

TEST(Decimal_Aggregates)
{
    SHARED_GROUP_TEST_PATH(path);
    DBRef db = DB::create(path);
    int sum = 0;
    size_t count = 0;
    {
        auto wt = db->start_write();
        auto table = wt->add_table("Foo");
        auto col_dec = table->add_column(type_Decimal, "price", true);
        for (int i = 0; i < 100; i++) {
            Obj obj = table->create_object();
            if (i % 10) {
                int val = i % 60;
                obj.set(col_dec, Decimal128(val));
                sum += val;
                count++;
            }
            else {
                CHECK(obj.get<Decimal128>(col_dec).is_null());
            }
        }
        wt->commit();
    }
    {
        auto rt = db->start_read();
        // rt->to_json(std::cout);
        auto table = rt->get_table("Foo");
        auto col = table->get_column_key("price");
        CHECK_EQUAL(table->count_decimal(col, Decimal128(51)), 1);
        CHECK_EQUAL(table->count_decimal(col, Decimal128(31)), 2);
        CHECK_EQUAL(table->sum_decimal(col), Decimal128(sum));
        CHECK_EQUAL(table->average_decimal(col), Decimal128(sum) / count);
        CHECK_EQUAL(table->maximum_decimal(col), Decimal128(59));
        CHECK_EQUAL(table->minimum_decimal(col), Decimal128(1));
    }
}
