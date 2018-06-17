/*
 * Usage: realm-trawler <realm-file-name>
 *
 * This tool will dump the structure of a realm file and print out any inconsistencies it finds.
 *
 * First it will print out information found in the top group. If there are inconsistencies in the
 * free list, this will be reported.
 *
 * Next, it will go through all tables and print the name, type and primary structure of the columns
 * found in the table. The user data found in the tables will not be interpreted.
 *
 * Generally all references will be checked in the sense that they should point to something that has
 * a valid header, meaning that the header must have a valid signature. Also, references that point
 * to areas included in the free list will be considered invalid. References that are not valid
 * will not be followed. It is checked that an area is only referenced once.
 *
 * Lastly it is checked that all space is accounted for. The combination of the free list and the
 * table tree should cover the whole file. Any leaked areas are reported.
 */

#include <iostream>
#include <fstream>
#include <vector>
#include <set>
#include <map>
#include <memory>
#include <algorithm>
#include <cassert>
#include <cstring>
#include "array_direct.hpp"

constexpr const int signature = 0x41414141;
constexpr const int alt_signature = 41414141;
constexpr const char* err_txt = "*** Error: ";

struct Header {
    uint64_t m_top_ref[2]; // 2 * 8 bytes
    // Info-block 8-bytes
    uint8_t m_mnemonic[4];    // "T-DB"
    uint8_t m_file_format[2]; // See `library_file_format`
    uint8_t m_reserved;
    // bit 0 of m_flags is used to select between the two top refs.
    uint8_t m_flags;
};

struct StreamingFooter {
    uint64_t m_top_ref;
    uint64_t m_magic_cookie;
};

static const uint_fast64_t footer_magic_cookie = 0x3034125237E526C8ULL;

#ifdef REALM_DEBUG
namespace realm {
namespace util {
void terminate(const char* message, const char* file, long line, std::initializer_list<Printable>&&) noexcept
{
    std::cerr << file << ":" << line << ": " << message << std::endl;
    exit(1);
}
}
}
#endif

template <class T>
void consolidate_list(std::vector<T>& list)
{
    if (list.size() > 1) {
        std::sort(begin(list), end(list), [](T& a, T& b) { return a.start < b.start; });

        // Combine any adjacent chunks in the freelist, except for when the chunks
        // are on the edge of an allocation slab
        auto prev = list.begin();
        for (auto it = list.begin() + 1; it != list.end(); ++it) {
            if (prev->start + prev->length != it->start) {
                REALM_ASSERT(prev->start + prev->length < it->start);
                prev = it;
                continue;
            }

            prev->length += it->length;
            it->length = 0;
        }

        // Remove all of the now zero-size chunks from the free list
        list.erase(std::remove_if(begin(list), end(list), [](T& chunk) { return chunk.length == 0; }), end(list));
    }
}

struct Entry {
    Entry()
        : Entry(0, 0)
    {
    }
    Entry(uint64_t s, uint64_t l)
        : start(s)
        , length(l)
    {
    }
    bool operator<(const Entry& rhs) const
    {
        return start < rhs.start;
    }
    uint64_t start;
    uint64_t length;
};

struct FreeListEntry : public Entry {
    FreeListEntry()
        : FreeListEntry(0, 0)
    {
    }
    FreeListEntry(uint64_t s, uint64_t l, uint64_t v = 0)
        : Entry(s, l)
        , version(v)
    {
    }
    uint64_t version;
};

class Node {
public:
    Node()
    {
    }
    Node(std::ifstream& is, uint64_t ref)
    {
        init(is, ref);
    }
    void init(std::ifstream& is, uint64_t ref);
    bool valid() const
    {
        return m_valid;
    }
    bool has_refs() const
    {
        return (m_header[4] & 0x40) != 0;
    }
    unsigned width() const
    {
        return (1 << (unsigned(m_header[4]) & 0x07)) >> 1;
    }
    unsigned size() const
    {
        return unsigned(m_size);
    }
    unsigned length() const
    {
        unsigned width_type = (unsigned(m_header[4]) & 0x18) >> 3;
        return calc_byte_size(width_type, m_size, width());
    }
    uint64_t size_in_bytes() const
    {
        return sizeof(m_header) + length();
    }

protected:
    unsigned char m_header[8];
    unsigned m_size = 0;
    bool m_valid = false;

    static unsigned calc_byte_size(unsigned wtype, unsigned size, unsigned width)
    {
        unsigned num_bytes = 0;
        switch (wtype) {
            case 0: {
                unsigned num_bits = size * width;
                num_bytes = (num_bits + 7) >> 3;
                break;
            }
            case 1: {
                num_bytes = size * width;
                break;
            }
            case 2:
                num_bytes = size;
                break;
        }

        // Ensure 8-byte alignment
        return (num_bytes + 7) & ~size_t(7);
    }
};

class Array : public Node {
public:
    Array()
    {
    }
    Array(std::ifstream& is, uint64_t ref)
        : Node(is, ref)
    {
        init(is, ref);
    }
    void init(std::ifstream& is, uint64_t ref)
    {
        Node::init(is, ref);
        unsigned len = length();
        m_data.reset(new char[len]);
        is.read(m_data.get(), len);
        m_has_refs = has_refs();
    }
    int64_t get_val(unsigned ndx) const
    {
        int64_t val = realm::get_direct(m_data.get(), width(), ndx);

        if (m_has_refs) {
            if (val & 1) {
                val >>= 1;
            }
        }
        return val;
    }
    uint64_t get_ref(unsigned ndx) const
    {
        REALM_ASSERT(m_has_refs);
        int64_t val = realm::get_direct(m_data.get(), width(), ndx);

        if (val & 1)
            val = 0;

        return uint64_t(val);
    }
    std::string get_string(unsigned ndx) const
    {
        std::string str;
        if (valid()) {
            auto sz = size_in_bytes();
            auto w = width();
            REALM_ASSERT(ndx * w < sz);
            str = m_data.get() + ndx * w;
        }
        return str;
    }

    static void get_nodes(std::ifstream& is, uint64_t ref, std::vector<Entry>&);

private:
    std::unique_ptr<char> m_data;
    bool m_has_refs = false;
};

class Group : public Array {
public:
    Group(std::ifstream& is, uint64_t ref)
        : Array(is, ref)
        , m_is(is)
    {
        if (valid()) {
            m_table_names.init(is, get_ref(0));
            m_file_size = get_val(2);
            m_free_list_positions.init(is, get_ref(3));
            m_free_list_sizes.init(is, get_ref(4));
            m_free_list_versions.init(is, get_ref(5));
        }
    }
    uint64_t get_file_size() const
    {
        return m_file_size;
    }
    int get_current_version() const
    {
        return int();
    }
    int get_nb_tables() const
    {
        return m_table_names.size();
    }
    void get_table_nodes(size_t ndx, std::vector<Entry>& nodes) const;
    std::vector<Entry> get_allocated_nodes() const;
    std::vector<FreeListEntry> get_free_list() const;

private:
    friend std::ostream& operator<<(std::ostream& ostr, const Group& g);
    std::ifstream& m_is;
    uint64_t m_file_size;
    Array m_table_names;
    Array m_free_list_positions;
    Array m_free_list_sizes;
    Array m_free_list_versions;
};

std::ostream& operator<<(std::ostream& ostr, const Group& g)
{
    if (g.valid()) {
        ostr << "File size: " << g.get_file_size() << std::endl;
        ostr << "Current version:" << g.get_current_version() << std::endl;
        ostr << "Free list size: " << g.m_free_list_positions.size() << std::endl;
        ostr << "Tables: " << std::endl;

        for (int i = 0; i < g.get_nb_tables(); i++) {
            std::cout << "    " << g.m_table_names.get_string(i) << std::endl;
        }
    }
    else {
        ostr << "Invalid group" << std::endl;
    }
    return ostr;
}

class RealmFile {
public:
    RealmFile(std::ifstream& is);
    // Walk the file and check that it consists of valid nodes
    void node_scan();
    void memory_leaks();
    void free_list_info() const;

private:
    std::ifstream& m_is;
    uint64_t m_top_ref;
    uint64_t m_start_pos;
    int m_file_format_version;
    std::unique_ptr<Group> m_group;
};

void Node::init(std::ifstream& is, uint64_t ref)
{
    unsigned char header[8];
    is.seekg(ref, is.beg);
    is.read(reinterpret_cast<char*>(&m_header), sizeof(header));

    if (memcmp(m_header, &signature, 4)) {
    }
    else {
        m_size = (m_header[5] << 16) + (m_header[6] << 8) + m_header[7];
        m_valid = true;
    }
}

void Array::get_nodes(std::ifstream& is, uint64_t ref, std::vector<Entry>& nodes)
{
    Array arr(is, ref);
    nodes.emplace_back(ref, arr.size_in_bytes());
    if (arr.has_refs()) {
        auto sz = arr.size();
        for (size_t i = 0; i < sz; i++) {
            uint64_t r = arr.get_ref(i);
            if (r)
                get_nodes(is, r, nodes);
        }
    }
}

void Group::get_table_nodes(size_t ndx, std::vector<Entry>& nodes) const
{
    Array table_refs(m_is, get_ref(1));
    auto ref = table_refs.get_val(ndx);

    Array::get_nodes(m_is, ref, nodes);

    consolidate_list(nodes);
}

std::vector<Entry> Group::get_allocated_nodes() const
{
    std::vector<Entry> all_nodes;

    Array::get_nodes(m_is, get_ref(0), all_nodes); // Table names

    for (int i = 0; i < get_nb_tables(); i++) {
        get_table_nodes(i, all_nodes);
    }
    if (size() > 8) {
        Array::get_nodes(m_is, get_ref(8), all_nodes);
    }

    consolidate_list(all_nodes);
    return all_nodes;
}

std::vector<FreeListEntry> Group::get_free_list() const
{
    std::vector<FreeListEntry> list;
    size_t sz = m_free_list_positions.size();
    REALM_ASSERT(sz == m_free_list_sizes.size());
    REALM_ASSERT(sz == m_free_list_versions.size());
    for (size_t i = 0; i < sz; i++) {
        int64_t pos = m_free_list_positions.get_val(i);
        int64_t size = m_free_list_sizes.get_val(i);
        int64_t version = m_free_list_versions.get_val(i);
        list.emplace_back(pos, size, version);
    }
    consolidate_list(list);
    return list;
}

RealmFile::RealmFile(std::ifstream& is)
    : m_is(is)
{
    m_top_ref = 0;
    union Buffer {
        Header file_header;
        StreamingFooter file_footer;
        char plain[sizeof(Header)];
    } buffer;

    is.seekg (0);
    is.read(buffer.plain, sizeof(Header));
    if (is) {
        if (std::memcmp(buffer.file_header.m_mnemonic, "T-DB", 4)) {
            std::cerr << err_txt << "Not a realm file ?" << std::endl;
            return;
        }
        else {
            m_top_ref = buffer.file_header.m_top_ref[buffer.file_header.m_flags];
            m_start_pos = sizeof(Header);
            if (m_top_ref == 0xFFFFFFFFFFFFFFFFULL && buffer.file_header.m_flags == 0) {
                // Streaming format
                m_start_pos = 0;
                is.seekg(0 - sizeof(StreamingFooter), is.end);
                is.read(buffer.plain, sizeof(StreamingFooter));
                if (buffer.file_footer.m_magic_cookie == footer_magic_cookie) {
                    m_top_ref = unsigned(buffer.file_footer.m_top_ref);
                }
                else {
                    std::cerr << err_txt << "Top ref not found" << std::endl;
                    return;
                }
            }
        }
    }
    m_group = std::make_unique<Group>(is, m_top_ref);
    m_file_format_version = buffer.file_header.m_file_format[buffer.file_header.m_flags];
    std::cout << "Top ref: 0x" << std::hex << m_top_ref << std::dec << std::endl;
    std::cout << "File format version: " << m_file_format_version << std::endl;
    std::cout << *m_group << std::endl;
}

void RealmFile::node_scan()
{
    if (m_group->valid()) {
        std::map<uint64_t, unsigned> sizes;
        uint64_t ref = m_start_pos;
        auto free_list = m_group->get_free_list();
        auto free_entry = free_list.begin();
        bool searching = false;
        while (ref < m_group->get_file_size()) {
            while (ref == free_entry->start) {
                ref += free_entry->length;
                ++free_entry;
            }
            Node n(m_is, ref);
            if (n.valid()) {
                if (searching) {
                    std::cerr << "Resuming from ref: " << ref << std::endl;
                    searching = false;
                }
                auto size_in_bytes = n.size_in_bytes();
                sizes[size_in_bytes]++;
                ref += size_in_bytes;
            }
            else {
                if (!searching) {
                    std::cerr << "Invalid ref: " << ref << std::endl;
                    searching = true;
                }
                ref += 8;
            }
        }
        std::cout << "Allocated space:" << std::endl;
        for (auto s : sizes) {
            std::cout << "    Size: " << s.first << " count: " << s.second << std::endl;
        }
    }
}
void RealmFile::memory_leaks()
{
    if (m_group->valid()) {
        auto nodes = m_group->get_allocated_nodes();
        auto free_list = m_group->get_free_list();
        for (auto& entry : free_list) {
            nodes.emplace_back(entry.start, entry.length);
        }
        consolidate_list(nodes);
    }
}

void RealmFile::free_list_info() const
{
    std::map<uint64_t, unsigned> free_sizes;
    std::map<uint64_t, unsigned> pinned_sizes;
    std::cout << "Free space:" << std::endl;
    auto free_list = m_group->get_free_list();
    size_t pinned_free_list_size = 0;
    size_t total_free_list_size = 0;
    auto it = free_list.begin();
    auto end = free_list.end();
    while (it != end) {
        // std::cout << it->start << ", " << it->length << ", " << it->version << std::endl;
        total_free_list_size += it->length;
        if (it->version != 0) {
            pinned_free_list_size += it->length;
            pinned_sizes[it->length]++;
        }
        else {
            free_sizes[it->length]++;
        }

        ++it;
    }
    std::cout << "Free space sizes:" << std::endl;
    for (auto s : free_sizes) {
        std::cout << "    Size: " << s.first << " count: " << s.second << std::endl;
    }
    std::cout << "Pinned sizes:" << std::endl;
    for (auto s : pinned_sizes) {
        std::cout << "    Size: " << s.first << " count: " << s.second << std::endl;
    }
    std::cout << "Total free space size:  " << total_free_list_size << std::endl;
    std::cout << "Pinned free space size: " << pinned_free_list_size << std::endl;
}

int main (int argc, const char* argv[])
{
    if (argc > 1) {
        std::ifstream is(argv[1], std::ifstream::binary);
        if (is) {
            RealmFile rf(is);
            if (argc > 2) {
                for (const char* command = argv[2]; *command != '\0'; command++) {
                    switch (*command) {
                        case 'f':
                            rf.free_list_info();
                            break;
                        case 'm':
                            rf.memory_leaks();
                            break;
                        case 'w':
                            rf.node_scan();
                            break;
                    }
                }
            }
        }
        else {
            std::cerr << err_txt << "Could not open file '" << argv[1] << "'" << std::endl;
        }
    }
    else {
        std::cerr << "Usage: realm-trawler <realmfile>" << std::endl;
    }

    return 0;
}
