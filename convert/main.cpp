#include <cstring>
#include <string>
#include <iostream>

#include <tightdb/column_mixed.hpp>
#include <tightdb.hpp>

using namespace std;
using namespace tightdb;

/*

NB: Currently, no conversion is done! This code just shows how to go
about handling conversion when it becomes necessary.

The main idea is to allow the incoming file format older version to be
older than the one supported by the current version of the core
library.

This is handled by accessing the incoming database in a low-level way,
where version differences can be incorporated as alternative
branches. The new copy is built using the high-level API which will
ensure that the new copy uses the current format.

Testing:

To be able to test this, we need a repository of datbase files using
older file format versions. Each file must contain data that expresses
all important variations of the file format: Tables of various size
such that there at least a 0, 1, and 2 level B+-tree. Tables with all
column types, including string enumerations. Strings and binrary data
of various sizes to trigger each leaf type.

*/


// FIXME: Command line switch to optimize output group

namespace {

template<class A> class Wrap {
public:
    A m_array;
    bool m_must_destroy;
    Wrap(Allocator& alloc): m_array(alloc), m_must_destroy(false) {}
    ~Wrap() { if (m_must_destroy) m_array.destroy(); }
    bool empty() const { return m_array.is_empty(); }
    size_t size() const { return m_array.size(); }
    size_t get_as_ref(size_t i) const { return m_array.get_as_ref(i); }
};


class Converter {
public:
    Converter(SlabAlloc& alloc, ref_type top_ref, int version, Group& group):
        m_alloc(alloc), m_top_ref(top_ref), m_version(version), m_new_group(group) {}

    void convert()
    {
        convert_group(m_top_ref, m_new_group);
    }

private:
    SlabAlloc& m_alloc;
    ref_type m_top_ref;
    int m_version;
    Group &m_new_group;

    void convert_group(ref_type ref, Group& new_group)
    {
        Wrap<Array> top(m_alloc);
        init(top, ref);
        Wrap<ArrayString> table_names(m_alloc);
        Wrap<Array>       table_refs(m_alloc);
        init(table_names, top.get_as_ref(0));
        init(table_refs,  top.get_as_ref(1));
        size_t n = table_refs.size();
        for (size_t i = 0; i != n; ++i) {
            StringData name = table_names.m_array.get(i);
            cout << "Converting table: '" << name << "'\n";
            TableRef new_table = new_group.get_table(name);
            convert_table_and_spec(table_refs.m_array.get(i), *new_table);
        }
    }

    void convert_table_and_spec(ref_type ref, Table& new_table)
    {
        Wrap<Array> top(m_alloc);
        init(top, ref);
        DescriptorRef new_desc = new_table.get_descriptor();
        convert_spec(top.get_as_ref(0), *new_desc);
        convert_columns(top.get_as_ref(0), top.get_as_ref(1), new_table);
    }

    void convert_spec(ref_type ref, Descriptor& new_desc)
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
        if (2 < top.size())
            init(column_subspecs, top.get_as_ref(3));
        size_t name_ndx    = 0;
        size_t subspec_ndx = 0;
        size_t n = column_types.size();
        for (size_t i = 0; i != n; ++i) {
            ColumnType type = ColumnType(column_types.m_array.get(i));
            DataType new_type = DataType();
            switch (type) {
                case col_type_Int:
                case col_type_Bool:
                case col_type_DateTime:
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
                case col_type_Reserved1:
                case col_type_Reserved4:
                    throw runtime_error("Unexpected column type");
            }
            StringData name = column_names.m_array.get(name_ndx);
            ++name_ndx;
            cout << "col name: " << name << "\n";
            DescriptorRef subdesc;
            new_desc.add_column(new_type, name, &subdesc);
            if (new_type == type_Table) {
                convert_spec(column_subspecs.get_as_ref(subspec_ndx), *subdesc);
                ++subspec_ndx;
            }
        }
    }

    void convert_columns(ref_type spec_ref, ref_type columns_ref, Table& new_table)
    {
        Wrap<Array>       column_types(m_alloc);
        Wrap<ArrayString> column_names(m_alloc);
        Wrap<Array>       column_attribs(m_alloc);
        Wrap<Array>       column_subspecs(m_alloc);
        Wrap<Array>       column_enumkeys(m_alloc);
        Wrap<Array>       column_refs(m_alloc);
        {
            Wrap<Array> spec(m_alloc);
            init(spec, spec_ref);
            init(column_types,   spec.get_as_ref(0));
            init(column_names,   spec.get_as_ref(1));
            init(column_attribs, spec.get_as_ref(2));
            if (3 < spec.size())
                init(column_subspecs, spec.get_as_ref(3));
            if (4 < spec.size())
                init(column_enumkeys, spec.get_as_ref(4));
        }
        init(column_refs, columns_ref);

        size_t num_cols = new_table.get_column_count();

        // Determine number of rows
        size_t num_rows = 0;
        if (0 < num_cols) {
            ref_type ref = column_refs.m_array.front();
            ColumnType type = ColumnType(column_types.m_array.front());
            if (type == col_type_Mixed) {
                Array top(m_alloc);
                top.init_from_ref(ref);
                ref = top.get_as_ref(0);
                type = col_type_Int;
            }
            MemRef mem(ref, m_alloc);
            bool is_inner_node = Array::get_hasrefs_from_header(mem.m_addr);
            if (is_inner_node) {
                Wrap<Array> inner_node(m_alloc);
                init(inner_node, mem);
                if (inner_node.size() < 3)
                    throw runtime_error("Too few elements in inner B+-tree node");
                int_fast64_t v = inner_node.m_array.back();
                if (v % 2 == 0)
                    throw runtime_error("Unexpected ref at back of inner B+-tree node");
                num_rows = to_ref(v / 2);
            }
            else {
                switch (type) {
                    case col_type_Int:
                    case col_type_Bool:
                    case col_type_DateTime:
                    case col_type_StringEnum:
                    case col_type_Table: {
                        Array leaf(m_alloc);
                        leaf.init_from_mem(mem);
                        num_rows = leaf.size();
                        break;
                    }
                    case col_type_Float: {
                        ArrayFloat leaf(m_alloc);
                        leaf.init_from_mem(mem);
                        num_rows = leaf.size();
                        break;
                    }
                    case col_type_Double: {
                        ArrayDouble leaf(m_alloc);
                        leaf.init_from_mem(mem);
                        num_rows = leaf.size();
                        break;
                    }
                    case col_type_String: {
                        bool long_strings = Array::get_hasrefs_from_header(mem.m_addr);
                        if (!long_strings) {
                            // Small strings
                            ArrayString leaf(m_alloc);
                            leaf.init_from_mem(mem);
                            num_rows = leaf.size();
                            break;
                        }
                        bool is_big = Array::get_context_flag_from_header(mem.m_addr);
                        if (!is_big) {
                            // Medium strings
                            ArrayStringLong leaf(m_alloc);
                            leaf.init_from_mem(mem);
                            num_rows = leaf.size();
                            break;
                        }
                        // Big strings
                        ArrayBigBlobs leaf(m_alloc);
                        leaf.init_from_mem(mem);
                        num_rows = leaf.size();
                        break;
                    }
                    case col_type_Binary: {
                        bool is_big = Array::get_context_flag_from_header(mem.m_addr);
                        if (!is_big) {
                            // Small blobs
                            ArrayBinary leaf(m_alloc);
                            leaf.init_from_mem(mem);
                            num_rows = leaf.size();
                            break;
                        }
                        // Big blobs
                        ArrayBigBlobs leaf(m_alloc);
                        leaf.init_from_mem(mem);
                        num_rows = leaf.size();
                        break;
                    }
                    case col_type_Mixed:
                    case col_type_Reserved1:
                    case col_type_Reserved4:
                        throw runtime_error("Unexpected column type");
                }
            }
        }

        new_table.add_empty_row(num_rows);

        size_t column_ref_ndx      = 0;
        size_t column_subspec_ndx  = 0;
        size_t column_enumkeys_ndx = 0;
        for (size_t i = 0; i != num_cols; ++i) {
            ref_type column_ref = column_refs.m_array.get(column_ref_ndx);
            ++column_ref_ndx;
            ColumnType type = ColumnType(column_types.m_array.get(i));
            switch (type) {
                case col_type_Int:
                    convert_int_column(column_ref, new_table, i);
                    break;
                case col_type_Bool:
                    convert_bool_column(column_ref, new_table, i);
                    break;
                case col_type_DateTime:
                    convert_datetime_column(column_ref, new_table, i);
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
                    ref_type strings_ref = column_enumkeys.m_array.get(column_enumkeys_ndx);
                    ++column_enumkeys_ndx;
                    convert_string_enum_column(strings_ref, column_ref, new_table, i);
                    break;
                }
                case col_type_Binary:
                    convert_binary_column(column_ref, new_table, i);
                    break;
                case col_type_Table: {
                    ref_type subspec_ref = column_subspecs.get_as_ref(column_subspec_ndx);
                    ++column_subspec_ndx;
                    convert_subtable_column(subspec_ref, column_ref, new_table, i);
                    break;
                }
                case col_type_Mixed:
                    convert_mixed_column(column_ref, new_table, i);
                    break;
                case col_type_Reserved1:
                case col_type_Reserved4:
                    throw runtime_error("Unexpected column type");
            }
            ColumnAttr attr = ColumnAttr(column_attribs.m_array.get(i));
            switch (attr) {
                case col_attr_None:
                    break;
                case col_attr_Indexed:
                    ++column_ref_ndx;
                    new_table.set_index(i);
                    break;
                case col_attr_Unique:
                case col_attr_Sorted:
                    throw runtime_error("Unexpected column attribute");
            }
        }
    }

    void convert_int_column(ref_type ref, Table& new_table, size_t col_ndx)
    {
        cout << "column_ref  = " << ref << "\n";
        Column col(ref, 0, 0, m_alloc);
        size_t n = col.size();
        if (n != new_table.size())
            throw runtime_error("Unexpected column size");
        for (size_t i=0; i<n; ++i)
            new_table.set_int(col_ndx, i, col.get(i));
    }

    void convert_bool_column(ref_type ref, Table& new_table, size_t col_ndx)
    {
        cout << "column_ref  = " << ref << "\n";
        Column col(ref, 0, 0, m_alloc);
        size_t n = col.size();
        if (n != new_table.size())
            throw runtime_error("Unexpected column size");
        for (size_t i=0; i<n; ++i)
            new_table.set_bool(col_ndx, i, bool(col.get(i)));
    }

    void convert_datetime_column(ref_type ref, Table& new_table, size_t col_ndx)
    {
        cout << "column_ref  = " << ref << "\n";
        Column col(ref, 0, 0, m_alloc);
        size_t n = col.size();
        if (n != new_table.size())
            throw runtime_error("Unexpected column size");
        for (size_t i=0; i<n; ++i)
            new_table.set_datetime(col_ndx, i, time_t(col.get(i)));
    }

    void convert_float_column(ref_type ref, Table& new_table, size_t col_ndx)
    {
        cout << "column_ref  = " << ref << "\n";
        ColumnFloat col(ref, 0, 0, m_alloc);
        size_t n = col.size();
        if (n != new_table.size())
            throw runtime_error("Unexpected column size");
        for (size_t i=0; i<n; ++i)
            new_table.set_float(col_ndx, i, col.get(i));
    }

    void convert_double_column(ref_type ref, Table& new_table, size_t col_ndx)
    {
        cout << "column_ref  = " << ref << "\n";
        ColumnDouble col(ref, 0, 0, m_alloc);
        size_t n = col.size();
        if (n != new_table.size())
            throw runtime_error("Unexpected column size");
        for (size_t i=0; i<n; ++i)
            new_table.set_double(col_ndx, i, col.get(i));
    }


    template<class ElemHandler> class IntegerLeafHandler {
    public:
        IntegerLeafHandler(const Converter& conv, ElemHandler& elem_handler):
            m_conv(conv),
            m_elem_handler(elem_handler)
        {
        }
        void operator()(MemRef mem)
        {
            Array leaf(m_conv.m_alloc);
            leaf.init_from_mem(mem);
            size_t n = leaf.size();
            for (size_t i = 0; i != n; ++i) {
                int_fast64_t value = leaf.get(i);
                m_elem_handler(value);
            }
        }
    private:
        const Converter& m_conv;
        ElemHandler& m_elem_handler;
    };


    template<class ElemHandler> class StringLeafHandler {
    public:
        StringLeafHandler(const Converter& conv, ElemHandler& elem_handler):
            m_conv(conv),
            m_elem_handler(elem_handler)
        {
        }
        void operator()(MemRef mem)
        {
            bool long_strings = Array::get_hasrefs_from_header(mem.m_addr);
            if (!long_strings) {
                // Small strings
                ArrayString leaf(m_conv.m_alloc);
                leaf.init_from_mem(mem);
                size_t n = leaf.size();
                for (size_t i = 0; i != n; ++i) {
                    StringData str = leaf.get(i);
                    m_elem_handler(str);
                }
                return;
            }
            bool is_big = Array::get_context_flag_from_header(mem.m_addr);
            if (!is_big) {
                // Medium strings
                ArrayStringLong leaf(m_conv.m_alloc);
                leaf.init_from_mem(mem);
                size_t n = leaf.size();
                for (size_t i = 0; i != n; ++i) {
                    StringData str = leaf.get(i);
                    m_elem_handler(str);
                }
                return;
            }
            // Big strings
            ArrayBigBlobs leaf(m_conv.m_alloc);
            leaf.init_from_mem(mem);
            size_t n = leaf.size();
            for (size_t i = 0; i != n; ++i) {
                StringData str = leaf.get_string(i);
                m_elem_handler(str);
            }
        }
    private:
        const Converter& m_conv;
        ElemHandler& m_elem_handler;
    };


    class StringSetter {
    public:
        StringSetter(Table& table, size_t col_ndx):
            m_table(table),
            m_col_ndx(col_ndx),
            m_row_ndx(0)
        {
        }
        void operator()(StringData str)
        {
            m_table.set_string(m_col_ndx, m_row_ndx, str);
            ++m_row_ndx;
        }
    private:
        Table& m_table;
        const size_t m_col_ndx;
        size_t m_row_ndx;
    };

    void convert_string_column(ref_type ref, Table& new_table, size_t col_ndx)
    {
        cout << "string_column_ref = " << ref << "\n";
        StringSetter elem_handler(new_table, col_ndx);
        StringLeafHandler<StringSetter> leaf_handler(*this, elem_handler);
        foreach_bptree_leaf(ref, leaf_handler);
    }


    class StringCollector {
    public:
        StringCollector(vector<StringData>& strings):
            m_strings(strings)
        {
        }
        void operator()(StringData str)
        {
            m_strings.push_back(str);
        }
    private:
        vector<StringData>& m_strings;
    };

    class StringEnumSetter: StringSetter {
    public:
        StringEnumSetter(Table& table, size_t col_ndx, const vector<StringData>& strings):
            StringSetter(table, col_ndx),
            m_strings(strings)
        {
        }
        void operator()(int_fast64_t index)
        {
            StringData str = m_strings[index];
            StringSetter::operator()(str);
        }
    private:
        const vector<StringData>& m_strings;
    };

    void convert_string_enum_column(ref_type strings_ref, ref_type indexes_ref, Table& new_table, size_t col_ndx)
    {
        cout << "string_enum_column_strings_ref = " << strings_ref << "\n";
        cout << "string_enum_column_indexes_ref = " << indexes_ref << "\n";
        vector<StringData> strings;
        {
            StringCollector elem_handler(strings);
            StringLeafHandler<StringCollector> leaf_handler(*this, elem_handler);
            foreach_bptree_leaf(strings_ref, leaf_handler);
        }
        {
            StringEnumSetter elem_handler(new_table, col_ndx, strings);
            IntegerLeafHandler<StringEnumSetter> leaf_handler(*this, elem_handler);
            foreach_bptree_leaf(indexes_ref, leaf_handler);
        }
    }


    void convert_binary_column(ref_type ref, Table& new_table, size_t col_ndx)
    {
        cout << "binary_column_ref = " << ref << "\n";
        ColumnBinary col(ref, 0, 0, m_alloc);
        size_t n = col.size();
        if (n != new_table.size())
            throw runtime_error("Unexpected column size");
        for (size_t i=0; i<n; ++i)
            new_table.set_binary(col_ndx, i, col.get(i));
    }

    void convert_subtable_column(ref_type subspec_ref, size_t column_ref, Table& new_table, size_t col_ndx)
    {
        cout << "subtable_column_subspec_ref = " << subspec_ref << "\n";
        cout << "subtable_column_column_ref  = " << column_ref << "\n";
        Column col(column_ref, 0, 0, m_alloc);
        size_t n = col.size();
        if (n != new_table.size())
            throw runtime_error("Unexpected column size");
        for (size_t i=0; i<n; ++i) {
            ref_type subtable_ref = col.get_as_ref(i);
            if (!subtable_ref)
                continue;
            TableRef subtable = new_table.get_subtable(col_ndx, i);
            convert_columns(subspec_ref, col.get(i), *subtable);
        }
    }

    void convert_mixed_column(ref_type ref, Table& new_table, size_t col_ndx)
    {
        cout << "mixed_column_ref = " << ref << "\n";
        ColumnMixed col(m_alloc, 0, 0, 0, 0, ref);
        size_t n = col.size();
        if (n != new_table.size())
            throw runtime_error("Unexpected column size");
        for (size_t i=0; i<n; ++i) {
            Mixed m;
            switch (col.get_type(i)) {
                case type_Int:      m.set_int(col.get_int(i));           break;
                case type_Bool:     m.set_bool(col.get_bool(i));         break;
                case type_DateTime: m.set_datetime(col.get_datetime(i)); break;
                case type_Float:    m.set_float(col.get_float(i));       break;
                case type_Double:   m.set_double(col.get_double(i));     break;
                case type_String:   m.set_string(col.get_string(i));     break;
                case type_Binary:   m.set_binary(col.get_binary(i));     break;
                case type_Table: {
                    new_table.clear_subtable(col_ndx, i);
                    ref_type subtable_ref = col.get_subtable_ref(i);
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

    template<class A> void init(Wrap<A>& array, MemRef mem)
    {
        if (init(array.m_array, mem))
            array.m_must_destroy = true;
    }

    template<class A> void init(Wrap<A>& array, ref_type ref)
    {
        MemRef mem(ref, m_alloc);
        init(array, mem);
    }

    bool init(Array& array, MemRef mem)
    {
        // If conversion of the array is needed (a decision which may
        // be based on m_version) then that conversion should be done
        // here. When converting, allocate space for a new array, and
        // return true.
        array.init_from_mem(mem);
        return false;
    }

    template<class H> void foreach_bptree_leaf(ref_type ref, H& handler)
    {
        MemRef mem(ref, m_alloc);
        if (!Array::get_is_inner_bptree_node_from_header(mem.m_addr)) {
            handler(mem);
            return;
        }

        Wrap<Array> inner_node(m_alloc);
        init(inner_node, mem);
        if (inner_node.size() < 3)
            throw runtime_error("Too few elements in inner B+-tree node");
        size_t n = inner_node.size() - 2;
        for (size_t i = 0; i != n; ++i)
            foreach_bptree_leaf(inner_node.get_as_ref(1 + i), handler);
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

        if (argc != 1)
            error = true;

        if (error || help) {
            if (error)
                cerr << "ERROR: Bad command line.\n\n";
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

    bool is_shared     = false;
    bool read_only     = true;
    bool no_create     = true;
    bool skip_validate = false;
    int version;
    ref_type top_ref =
        alloc.attach_file(database_file, is_shared, read_only, no_create, skip_validate, &version);

    cout << "Detected version = " << version << "\n";

    if (version == 1) {
        cout << "No conversion needed\n";
    }
    else if (version < 1) {
        cout << "Converting to version 1\n";
        Group new_group;
        if (top_ref) {
            Converter cvt(alloc, top_ref, version, new_group);
            cvt.convert();
        }
        new_group.write(database_file+".new");
    }
}
