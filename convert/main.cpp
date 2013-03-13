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
        size_t top_ref = m_alloc.GetTopRef();
        if (top_ref) convert_group(top_ref);
    }

private:
    SlabAlloc& m_alloc;
    int m_version;
    Group m_new_group;

    void convert_group(size_t ref)
    {
        Wrap<Array> top(m_alloc);
        init(top, ref);
        Wrap<ArrayString> table_names(m_alloc);
        Wrap<Array> table_refs(m_alloc);
        init(table_names, top.get_as_ref(0));
        init(table_refs,  top.get_as_ref(1));
        size_t n = table_refs.size();
        for (size_t i=0; i<n; ++i) {
            cout << "Converting table: '" << table_names.m_array.Get(i) << "'\n";
            convert_table(table_refs.m_array.Get(i));
        }
    }

    void convert_table(size_t ref)
    {
        cerr << "table_ref = " << ref << "\n";
    }

    void convert_column()
    {
    }

    template<class A> void init(Wrap<A>& array, size_t ref)
    {
        // If conversion of the array is needed (a decision which may
        // be based on m_version) then that conversion should be done
        // here. When converting, allocate space for a new array, and
        // set array.m_must_destroy = true.
        array.m_array.UpdateRef(ref);
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
