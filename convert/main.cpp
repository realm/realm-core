#include <cstring>
#include <string>
#include <iostream>

#include <tightdb/column_basic.hpp>
#include <tightdb/column_mixed.hpp>
#include <tightdb/group.hpp>

using namespace std;
using namespace tightdb;

// FIXME: Command line switch to optimize output group

namespace {

template<class A> struct Wrap {
    A m_array;
    bool m_must_destroy;
    Wrap(Allocator& alloc): m_array(alloc), m_must_destroy(false) {}
    ~Wrap() { if (m_must_destroy) m_array.Destroy(); }
    bool empty() const { return m_array.is_empty(); }
    size_t size() const { return m_array.size(); }
    size_t get_as_ref(size_t i) const { return m_array.GetAsRef(i); }
};


struct Converter {
    Converter(SlabAlloc& a, int v, Group& g): m_alloc(a), m_version(v), m_new_group(g) {}

    void convert()
    {
        size_t top_ref = m_alloc.GetTopRef();
        if (top_ref) convert_group(top_ref, m_new_group);
    }

private:
    SlabAlloc& m_alloc;
    int m_version;
    Group &m_new_group;

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
            DataType new_type = DataType();
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

        size_t num_cols = new_table.get_column_count();

        // Determine number of rows
        size_t num_rows = 0;
        if (0 < num_cols) {
            Wrap<Array> column_root(m_alloc);
            init(column_root, column_refs.m_array.Get(0));
            if (column_root.m_array.is_leaf()) {
                num_rows = column_root.size();
            }
            else {
                Wrap<Array> root_offsets(m_alloc);
                init(root_offsets, column_root.get_as_ref(0));
                if (root_offsets.empty()) throw runtime_error("Unexpected empty non-leaf node");
                num_rows = size_t(root_offsets.m_array.back());
            }
        }

        new_table.add_empty_row(num_rows);

        size_t column_ref_ndx     = 0;
        size_t column_type_ndx    = 0;
        size_t column_subspec_ndx = 0;
        for (size_t i=0; i<num_cols; ++i) {
            size_t column_ref = column_refs.m_array.Get(column_ref_ndx);
            ++column_ref_ndx;
            ColumnType type;
            bool indexed = false;
          again:
            type = ColumnType(column_types.m_array.Get(column_type_ndx));
            switch (type) {
                case col_type_Int:
                    convert_int_column(column_ref, new_table, i);
                    break;
                case col_type_Bool:
                    convert_bool_column(column_ref, new_table, i);
                    break;
                case col_type_Date:
                    convert_date_column(column_ref, new_table, i);
                    break;
                case col_type_Float:
                    convert_float_column(column_ref, new_table, i);
                    break;
                case col_type_Double:
                    convert_double_column(column_ref, new_table, i);
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

    void convert_int_column(size_t ref, Table& new_table, size_t col_ndx)
    {
        cout << "column_ref  = " << ref << "\n";
        Column col(ref, 0, 0, m_alloc);
        size_t n = col.Size();
        if (n != new_table.size()) throw runtime_error("Unexpected column size");
        for (size_t i=0; i<n; ++i) {
            new_table.set_int(col_ndx, i, col.Get(i));
        }
    }

    void convert_bool_column(size_t ref, Table& new_table, size_t col_ndx)
    {
        cout << "column_ref  = " << ref << "\n";
        Column col(ref, 0, 0, m_alloc);
        size_t n = col.Size();
        if (n != new_table.size()) throw runtime_error("Unexpected column size");
        for (size_t i=0; i<n; ++i) {
            new_table.set_bool(col_ndx, i, bool(col.Get(i)));
        }
    }

    void convert_date_column(size_t ref, Table& new_table, size_t col_ndx)
    {
        cout << "column_ref  = " << ref << "\n";
        Column col(ref, 0, 0, m_alloc);
        size_t n = col.Size();
        if (n != new_table.size()) throw runtime_error("Unexpected column size");
        for (size_t i=0; i<n; ++i) {
            new_table.set_date(col_ndx, i, time_t(col.Get(i)));
        }
    }

    void convert_float_column(size_t ref, Table& new_table, size_t col_ndx)
    {
        cout << "column_ref  = " << ref << "\n";
        ColumnFloat col(ref, 0, 0, m_alloc);
        size_t n = col.Size();
        if (n != new_table.size()) throw runtime_error("Unexpected column size");
        for (size_t i=0; i<n; ++i) {
            new_table.set_float(col_ndx, i, col.Get(i));
        }
    }

    void convert_double_column(size_t ref, Table& new_table, size_t col_ndx)
    {
        cout << "column_ref  = " << ref << "\n";
        ColumnDouble col(ref, 0, 0, m_alloc);
        size_t n = col.Size();
        if (n != new_table.size()) throw runtime_error("Unexpected column size");
        for (size_t i=0; i<n; ++i) {
            new_table.set_double(col_ndx, i, col.Get(i));
        }
    }


    struct HandleStringsLeaf {
        HandleStringsLeaf(Table& t, size_t c): m_new_table(t), m_col_ndx(c) {}
        void operator()(size_t ref) const
        {
        }
    private:
        Table& m_new_table;
        size_t m_col_ndx;
    };

    void convert_string_column(size_t ref, Table& new_table, size_t col_ndx)
    {
        cout << "string_column_ref = " << ref << "\n";
        for_each_leaf(ref, HandleStringsLeaf(new_table, col_ndx));
    }


    struct HandleEnumStringsLeaf {
        HandleEnumStringsLeaf(vector<string>& s): m_strings(s) {}
        void operator()(size_t ref) const
        {
        }
    private:
        vector<string>& m_strings;
    };

    struct HandleEnumRefsLeaf {
        HandleEnumRefsLeaf(const vector<string>& s, Table& t, size_t c): m_strings(s), m_new_table(t), m_col_ndx(c) {}
        void operator()(size_t ref) const
        {
        }
    private:
        const vector<string>& m_strings;
        Table& m_new_table;
        size_t m_col_ndx;

    };

    void convert_string_enum_column(size_t strings_ref, size_t refs_ref, Table& new_table, size_t col_ndx)
    {
        cout << "string_enum_column_strings_ref = " << strings_ref << "\n";
        cout << "string_enum_column_refs_ref    = " << refs_ref << "\n";
        vector<string> strings;
        for_each_leaf(strings_ref, HandleEnumStringsLeaf(strings));
        for_each_leaf(refs_ref, HandleEnumRefsLeaf(strings, new_table, col_ndx));
    }


    void convert_binary_column(size_t ref, Table& new_table, size_t col_ndx)
    {
        cout << "binary_column_ref = " << ref << "\n";
        ColumnBinary col(ref, 0, 0, m_alloc);
        size_t n = col.Size();
        if (n != new_table.size()) throw runtime_error("Unexpected column size");
        for (size_t i=0; i<n; ++i) {
            new_table.set_binary(col_ndx, i, col.Get(i));
        }
    }

    void convert_subtable_column(size_t subspec_ref, size_t column_ref, Table& new_table, size_t col_ndx)
    {
        cout << "subtable_column_subspec_ref = " << subspec_ref << "\n";
        cout << "subtable_column_column_ref  = " << column_ref << "\n";
        Column col(column_ref, 0, 0, m_alloc);
        size_t n = col.Size();
        if (n != new_table.size()) throw runtime_error("Unexpected column size");
        for (size_t i=0; i<n; ++i) {
            size_t subtable_ref = col.GetAsRef(i);
            if (!subtable_ref) continue;
            TableRef subtable = new_table.get_subtable(col_ndx, i);
            convert_columns(subspec_ref, col.Get(i), *subtable);
        }
    }

    void convert_mixed_column(size_t ref, Table& new_table, size_t col_ndx)
    {
        cout << "mixed_column_ref = " << ref << "\n";
        ColumnMixed col(m_alloc, 0, 0, 0, 0, ref);
        size_t n = col.Size();
        if (n != new_table.size()) throw runtime_error("Unexpected column size");
        for (size_t i=0; i<n; ++i) {
            Mixed m;
            switch (col.get_type(i)) {
                case type_Int:    m.set_int(col.get_int(i));       break;
                case type_Bool:   m.set_bool(col.get_bool(i));     break;
                case type_Date:   m.set_date(col.get_date(i));     break;
                case type_Float:  m.set_float(col.get_float(i));   break;
                case type_Double: m.set_double(col.get_double(i)); break;
                case type_String: m.set_string(col.get_string(i)); break;
                case type_Binary: m.set_binary(col.get_binary(i)); break;
                case type_Table: {
                    new_table.clear_subtable(col_ndx, i);
                    size_t subtable_ref = col.get_subtable_ref(i);
                    TableRef subtable = new_table.get_subtable(col_ndx, i);
                    convert_table_and_spec(subtable_ref, *subtable);
                    continue;
                }
                case type_Mixed:
                    throw runtime_error("Unexpected mixed type");
            }
            new_table.set_mixed(col_ndx, i, m);
        }
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

    template<class H> void for_each_leaf(size_t ref, H handler)
    {
        Wrap<Array> node(m_alloc);
        init(node, ref);
        if (node.m_array.is_leaf()) {
            handler(ref);
            return;
        }

        if (node.size() < 2) throw runtime_error("Too few elements in non-leaf node array");
        Wrap<Array> children(m_alloc);
        init(children, node.get_as_ref(1));
        size_t n = children.size();
        for (size_t i=0; i<n; ++i) {
            for_each_leaf(children.get_as_ref(i), handler);
        }
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
        Group new_group;
        Converter cvt(alloc, version, new_group);
        cvt.convert();
        new_group.write(database_file+".new");
    }
}
