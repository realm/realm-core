#include <cstring>
#include <string>
#include <iostream>

#include <tightdb/group.hpp>

using namespace std;
using namespace tightdb;

namespace {

template<class A> struct Wrap {
    A m_array;
    bool m_must_destroy;
    Wrap(Allocator& alloc): m_array(alloc), m_must_destroy(false) {}
    ~Wrap() { if (m_must_destroy) m_array.Destroy(); }
    size_t size() const { return m_array.size(); }
    size_t get_as_ref(size_t i) const { return m_array.GetAsRef(i); }
};


struct Converter {
    Converter(SlabAlloc& a, int v): m_alloc(a), m_version(v) {}

    void convert()
    {
        Group new_group;
        size_t top_ref = m_alloc.GetTopRef();
        if (top_ref) convert_group(top_ref, new_group);
    }

private:
    SlabAlloc& m_alloc;
    int m_version;

    void convert_group(size_t ref, Group& new_group)
    {
        Wrap<Array> top(m_alloc);
        init(top, ref);
        Wrap<ArrayString> table_names(m_alloc);
        Wrap<Array>       table_refs(m_alloc);
        init(table_names, top.get_as_ref(0));
        init(table_refs,  top.get_as_ref(1));
        size_t n = table_refs.size();
        for (size_t i=0; i<n; ++i) {
            string name = table_names.m_array.Get(i); // FIXME: Explicit string length
            cout << "Converting table: '" << name << "'\n";
            TableRef new_table = new_group.get_table(name.c_str()); // FIXME: Explicit string length
            convert_table_and_spec(table_refs.m_array.Get(i), *new_table);
        }
    }

    void convert_table_and_spec(size_t ref, Table& new_table)
    {
        Wrap<Array> top(m_alloc);
        init(top, ref);
        convert_spec(top.get_as_ref(0), new_table.get_spec());
        new_table.update_from_spec();
        convert_columns(top.get_as_ref(0), top.get_as_ref(1), new_table);
    }

    void convert_spec(size_t ref, Spec& new_spec)
    {
        Wrap<Array> top(m_alloc);
        init(top, ref);
        if (top.size() != 2 && top.size() != 3)
            throw runtime_error("Unexpected size of spec top array");
        Wrap<Array>       column_types(m_alloc);
        Wrap<ArrayString> column_names(m_alloc);
        Wrap<Array>       column_subspecs(m_alloc);
        init(column_types, top.get_as_ref(0));
        init(column_names, top.get_as_ref(1));
        if (2 < top.size()) init(column_subspecs, top.get_as_ref(2));
        size_t name_ndx    = 0;
        size_t subspec_ndx = 0;
        size_t n = column_types.size();
        for (size_t i=0; i<n; ++i) {
            ColumnType type = ColumnType(column_types.m_array.Get(i));
            DataType new_type;
            switch (type) {
                case col_type_Int:
                case col_type_Bool:
                case col_type_Date:
                case col_type_Float:
                case col_type_Double:
                case col_type_String:
                case col_type_Binary:
                case col_type_Table:
                case col_type_Mixed:
                    new_type = DataType(type);
                    break;
                case col_type_StringEnum:
                    new_type = type_String;
                    break;
                case col_attr_Indexed:
                    continue;
                case col_type_Reserved1:
                case col_type_Reserved4:
                case col_attr_Unique:
                case col_attr_Sorted:
                case col_attr_None:
                    throw runtime_error("Unexpected column type");
            }
            string name = column_names.m_array.Get(name_ndx); // FIXME: Explicit string length
            ++name_ndx;
            cout << "col name: " << name << "\n";
            if (new_type == type_Table) {
                Spec subspec = new_spec.add_subtable_column(name.c_str()); // FIXME: Explicit string length
                convert_spec(column_subspecs.get_as_ref(subspec_ndx), subspec);
                ++subspec_ndx;
            }
            else {
                new_spec.add_column(new_type, name.c_str());
            }
        }
    }

    void convert_columns(size_t spec_ref, size_t columns_ref, Table& new_table)
    {
        Wrap<Array>       column_types(m_alloc);
        Wrap<ArrayString> column_names(m_alloc);
        Wrap<Array>       column_subspecs(m_alloc);
        Wrap<Array>       column_refs(m_alloc);
        {
            Wrap<Array> spec(m_alloc);
            init(spec, spec_ref);
            init(column_types, spec.get_as_ref(0));
            init(column_names, spec.get_as_ref(1));
            if (2 < spec.size()) init(column_subspecs, spec.get_as_ref(2));
        }
        init(column_refs, columns_ref);

        size_t column_ref_ndx     = 0;
        size_t column_type_ndx    = 0;
        size_t column_subspec_ndx = 0;
        size_t n = new_table.get_column_count();
        for (size_t i=0; i<n; ++i) {
            size_t column_ref = column_refs.m_array.Get(column_ref_ndx);
            ++column_ref_ndx;
            ColumnType type;
            DataType new_type;
            bool indexed = false;
          again:
            type = ColumnType(column_types.m_array.Get(column_type_ndx));
            switch (type) {
                case col_type_Int:
                    convert_column<int64_t>(column_ref, new_table, i);
                    break;
                case col_type_Bool:
                    convert_column<bool>(column_ref, new_table, i);
                    break;
                case col_type_Date:
                    convert_column<Date>(column_ref, new_table, i);
                    break;
                case col_type_Float:
                    convert_column<float>(column_ref, new_table, i);
                    break;
                case col_type_Double:
                    convert_column<double>(column_ref, new_table, i);
                    break;
                case col_type_String:
                    convert_string_column(column_ref, new_table, i);
                    break;
                case col_type_StringEnum: {
                    size_t strings_ref = column_ref;
                    size_t refs_ref    = column_refs.m_array.Get(column_ref_ndx);;
                    ++column_ref_ndx;
                    convert_string_enum_column(strings_ref, refs_ref, new_table, i);
                    break;
                }
                case col_type_Binary:
                    convert_binary_column(column_ref, new_table, i);
                    break;
                case col_type_Table: {
                    size_t subspec_ref = column_subspecs.get_as_ref(column_subspec_ndx);
                    ++column_subspec_ndx;
                    convert_subtable_column(subspec_ref, column_ref, new_table, i);
                    break;
                }
                case col_type_Mixed:
                    convert_mixed_column(column_ref, new_table, i);
                    break;
                case col_attr_Indexed:
                    indexed = true;
                    ++column_type_ndx;
                    goto again;
                case col_type_Reserved1:
                case col_type_Reserved4:
                case col_attr_Unique:
                case col_attr_Sorted:
                case col_attr_None:
                    throw runtime_error("Unexpected column type");
            }
            if (indexed) new_table.set_index(i);
            ++column_type_ndx;
        }
    }

    template<class T> void convert_column(size_t ref, Table& new_table, size_t col_ndx)
    {
        cout << "column_ref  = " << ref << "\n";
    }

    void convert_string_column(size_t ref, Table& new_table, size_t col_ndx)
    {
        cout << "sring_column_ref = " << ref << "\n";
    }

    void convert_string_enum_column(size_t strings_ref, size_t refs_ref, Table& new_table, size_t col_ndx)
    {
        cout << "sring_enum_column_strings_ref = " << strings_ref << "\n";
        cout << "sring_enum_column_refs_ref    = " << refs_ref << "\n";
    }

    void convert_binary_column(size_t ref, Table& new_table, size_t col_ndx)
    {
        cout << "binary_column_ref = " << ref << "\n";
    }

    void convert_subtable_column(size_t subspec_ref, size_t column_ref, Table& new_table, size_t col_ndx)
    {
        cout << "subtable_column_subspec_ref = " << subspec_ref << "\n";
        cout << "subtable_column_column_ref  = " << column_ref << "\n";
    }

    void convert_mixed_column(size_t ref, Table& new_table, size_t col_ndx)
    {
        cout << "mixed_column_ref = " << ref << "\n";
    }

    template<class A> void init(Wrap<A>& array, size_t ref)
    {
        if (init(array.m_array, ref)) array.m_must_destroy = true;
    }

    bool init(Array& array, size_t ref)
    {
        // If conversion of the array is needed (a decision which may
        // be based on m_version) then that conversion should be done
        // here. When converting, allocate space for a new array, and
        // return true.
        array.UpdateRef(ref);
        return false;
    }
};

} // Anonymous namespace

int main(int argc, char* argv[])
{
    // Process command line
    {
        const char* prog = argv[0];
        --argc;
        ++argv;
        bool error = false;
        bool help  = false;
        int argc2 = 0;
        for (int i=0; i<argc; ++i) {
            char* arg = argv[i];
            if (arg[0] != '-') {
                argv[argc2++] = arg;
                continue;
            }

            if (strcmp(arg, "-h") == 0 || strcmp(arg, "--help") == 0) {
                help = true;
                continue;
            }
            error = true;
            break;
        }
        argc = argc2;

        if (argc != 1) error = true;

        if (error || help) {
            if (error) cerr << "ERROR: Bad command line.\n\n";
            cerr <<
                "Synopsis: "<<prog<<"  [DATABASE]\n\n"
                "Options:\n"
                "  -h, --help          Display command-line synopsis followed by the list of\n"
                "                      available options.\n";
            return error ? 1 : 0;
        }
    }

    string database_file = argv[0];

    SlabAlloc alloc;

    bool is_shared = false;
    bool read_only = true;
    bool no_create = true;
    int version;
    alloc.attach_file(database_file, is_shared, read_only, no_create, &version);

    cout << "Detected version = " << version << "\n";

    if (version == 1) {
        cout << "No conversion needed\n";
    }
    else if (version < 1) {
        cout << "Converting to version 1\n";
        Converter cvt(alloc, version);
        cvt.convert();
    }
}
