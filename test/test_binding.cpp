#include <realm/binding.hpp>
#include <realm/group_shared.hpp>

#include <set>

#include "test.hpp"

using namespace realm;
using namespace realm::binding;

namespace {

struct Foo {
    REALM_OBJECT(reflect);
    static void reflect(Reflect&);

    Property<int> m_number; 
    Property<std::string> m_string;
};

void Foo::reflect(Reflect& r)
{
    r.name("Foo");
    r.bind_property(&Foo::m_number, "number");
    r.bind_property(&Foo::m_string, "string");
}

TEST(Binding_GetClassName)
{
    CHECK_EQUAL(get_class_info<Foo>().name, "Foo");
}

TEST(Binding_EnumerateProperties)
{
    auto& class_info = get_class_info<Foo>();
    std::set<std::string> property_names;
    for (auto& property: class_info.properties) {
        property_names.insert(property.second.name);
    }

    CHECK_EQUAL(property_names.size(), 2);
    CHECK(property_names.count("number"));
    CHECK(property_names.count("string"));
}

TEST(Binding_CreateObject)
{
    SHARED_GROUP_TEST_PATH(path);

    Schema schema;
    schema.add<Foo>();

    SharedGroup sg{path};

    {
        WriteTransaction tr{sg};
        schema.auto_migrate(tr);
        Foo foo = create_object<Foo>(tr);
        foo.m_number = 123;
        foo.m_string = "Hello, World!";
        tr.commit();
    }

    {
        ReadTransaction tr{sg};
        ConstTableRef table = tr.get_table("class_Foo");
        CHECK_EQUAL(table->get_int(0, 0), 123);
        CHECK_EQUAL(table->get_string(1, 0), "Hello, World!");
    }
}

} // anonymous namespace
