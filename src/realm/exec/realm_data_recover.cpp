#include <iostream>
#include <string>
#include <fstream>
#include <memory>
#include <vector>
#include <realm.hpp>
#include <nlohmann/json.hpp>

using json = nlohmann::json;

// dunno what this magic number is (TODO document this)
constexpr const int signature = 0x41414141;
uint64_t current_logical_file_size;

// slab allocator config
struct realm_config {
    std::string encryption_key;
    std::string path;
};

// copied and adapted from realm-trawler (TODO: worth to be moved into shared util)

struct slab_entry {
    slab_entry()
        : slab_entry(0, 0)
    {
    }
    slab_entry(uint64_t s, uint64_t l)
        : start(s)
        , length(l)
    {
    }
    bool operator<(const slab_entry& rhs) const
    {
        return start < rhs.start;
    }
    uint64_t start;
    uint64_t length;
};

struct free_list_entry : public slab_entry {
    free_list_entry()
        : free_list_entry(0, 0)
    {
    }
    free_list_entry(uint64_t s, uint64_t l, uint64_t v = 0)
        : slab_entry(s, l)
        , version(v)
    {
    }
    uint64_t version;
};

// represent a node in the database
struct realm_node {
    realm_node() = default;
    realm_node(realm::Allocator& alloc, uint64_t ref);
    void init(realm::Allocator& alloc, uint64_t ref);
    bool valid() const;
    bool has_refs() const;
    unsigned width() const;
    unsigned size() const;
    unsigned length() const;
    uint64_t ref() const;
    uint64_t size_in_bytes() const;
    char* data();
    static unsigned calc_byte_size(unsigned wtype, unsigned size, unsigned width);

    uint64_t m_ref = 0;
    char* m_header;
    unsigned m_size = 0;
    bool m_valid = false;
};

struct realm_array : realm_node {
    realm_array() = default;
    realm_array(realm::Allocator& alloc, uint64_t ref);
    bool is_inner_bptree_node() const;
    void init(realm::Allocator& alloc, uint64_t ref);
    int64_t get_val(size_t ndx) const;
    uint64_t get_ref(size_t ndx) const;
    std::string get_string(size_t ndx) const;
    size_t mem_usage(realm::Allocator& alloc) const;
    void _mem_usage(realm::Allocator& alloc, size_t& mem) const;

    char* m_data;
    bool m_has_refs = false;
};

struct realm_table;
struct realm_group : public realm_array {
    realm_group(realm::SlabAlloc& allocator, uint64_t ref);


    //     : Array(alloc, ref)
    //     , m_alloc(alloc)
    // {
    //     m_valid &= (size() <= 12);
    //     if (valid()) {
    //         m_file_size = get_val(2);
    //         current_logical_file_size = m_file_size;
    //         m_table_names.init(alloc, get_ref(0));
    //         m_table_refs.init(alloc, get_ref(1));
    //         if (size() > 3) {
    //             m_free_list_positions.init(alloc, get_ref(3));
    //             m_free_list_sizes.init(alloc, get_ref(4));
    //             m_free_list_versions.init(alloc, get_ref(5));
    //         }
    //         if (size() > 8) {
    //             m_history.init(alloc, get_ref(8));
    //         }
    //         if (size() > 11) {
    //             auto ref = get_ref(11);
    //             if (ref)
    //                 m_evacuation_info.init(alloc, ref);
    //         }
    //     }
    // }
    uint64_t get_file_size() const;
    // {
    //     return m_file_size;
    // }
    uint64_t get_free_space_size() const;
    // {
    //     uint64_t sz = 0;
    //     for (size_t i = 0; i < m_free_list_sizes.size(); i++) {
    //         sz += m_free_list_sizes.get_val(i);
    //     }
    //     return sz;
    // }
    int get_current_version() const;
    // {
    //     return int(get_val(6));
    // }
    std::string get_history_type() const;
    // {
    //     switch (int(get_val(7))) {
    //         case 0:
    //             return "None";
    //         case 1:
    //             return "OutOfRealm";
    //         case 2:
    //             return "InRealm";
    //         case 3:
    //             return "SyncClient";
    //         case 4:
    //             return "SyncServer";
    //     }
    //     return "Unknown";
    // }
    std::vector<realm::BinaryData> get_changesets();
    // {
    //     std::vector<realm::BinaryData> ret;
    //     if (int(get_val(7)) == 2) {
    //         for (size_t n = 0; n < m_history.size(); n++) {
    //             auto ref = m_history.get_ref(n);
    //             Node node(m_alloc, ref);
    //             ret.emplace_back(node.data(), node.size());
    //         }
    //     }
    //     if (int(get_val(7)) == 3) {
    //         auto ref = m_history.get_ref(0); // ct history
    //         m_history.init(m_alloc, ref);
    //         for (size_t n = 0; n < m_history.size(); n++) {
    //             ref = m_history.get_ref(n);
    //             Node node(m_alloc, ref);
    //             ret.emplace_back(node.data(), node.size());
    //         }
    //     }
    //     return ret;
    // }
    int get_history_schema_version() const;
    // {
    //     return int(get_val(9));
    // }
    int get_file_ident() const;
    // {
    //     return int(get_val(10));
    // }
    void print_evacuation_info(std::ostream& ostr) const;
    // {
    //     if (m_evacuation_info.valid()) {
    //         ostr << "Evacuation limit: " << size_t(m_evacuation_info.get_val(0));
    //         if (m_evacuation_info.get_val(1)) {
    //             ostr << " Scan done" << std::endl;
    //         }
    //         else {
    //             ostr << " Progress: [";
    //             for (size_t i = 2; i < m_evacuation_info.size(); i++) {
    //                 if (i > 2)
    //                     ostr << ',';
    //                 ostr << m_evacuation_info.get_val(i);
    //             }
    //             ostr << "]" << std::endl;
    //         }
    //     }
    // }
    unsigned get_nb_tables() const;
    // {
    //     return m_table_names.size();
    // }
    std::string get_table_name(size_t i) const;
    // {
    //     return m_table_names.get_string(i);
    // }
    realm_table* get_table(size_t i) const;
    // {
    //     auto& ret = m_tables[i];
    //     if (!ret) {
    //         ret = new Table(m_alloc, m_table_refs.get_ref(i));
    //     }
    //     return ret;
    // }
    std::vector<slab_entry> get_allocated_nodes() const;
    std::vector<free_list_entry> get_free_list() const;
    void print_schema() const;

private:
    // friend std::ostream& operator<<(std::ostream& ostr, const realm_group& g);
    realm::Allocator& m_alloc;
    uint64_t m_file_size;
    realm_array m_table_names;
    realm_array m_table_refs;
    realm_array m_free_list_positions;
    realm_array m_free_list_sizes;
    realm_array m_free_list_versions;
    realm_array m_evacuation_info;
    realm_array m_history;
    mutable std::map<size_t, realm_table*> m_tables;
};

struct realm_handle {
    realm_handle(const realm_config& cnf);

    uint64_t m_top_ref;
    uint64_t m_start_pos;
    int m_file_format_version;
    std::unique_ptr<realm_group> m_group;
    realm::SlabAlloc m_alloc;
};

// implementation

realm_node::realm_node(realm::Allocator& alloc, uint64_t ref)
{
    init(alloc, ref);
}
void realm_node::init(realm::Allocator& alloc, uint64_t ref)
{
    m_ref = ref;
    m_header = alloc.translate(ref);

    auto res = memcmp(m_header, &signature, 4);
    if (!res) {
        unsigned char* u = reinterpret_cast<unsigned char*>(m_header);
        m_size = (u[5] << 16) + (u[6] << 8) + u[7];
        m_valid = true;
    }
}
bool realm_node::valid() const
{
    return m_valid;
}
bool realm_node::has_refs() const
{
    return (m_header[4] & 0x40) != 0;
}
unsigned realm_node::width() const
{
    return (1 << (unsigned(m_header[4]) & 0x07)) >> 1;
}
unsigned realm_node::size() const
{
    return unsigned(m_size);
}
unsigned realm_node::length() const
{
    unsigned width_type = (unsigned(m_header[4]) & 0x18) >> 3;
    return calc_byte_size(width_type, m_size, width());
}
uint64_t realm_node::ref() const
{
    return m_ref;
}
uint64_t realm_node::size_in_bytes() const
{
    return 8 + length();
}
char* realm_node::data()
{
    return realm::Array::get_data_from_header(m_header);
}
unsigned realm_node::calc_byte_size(unsigned wtype, unsigned size, unsigned width)
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

realm_array::realm_array(realm::Allocator& alloc, uint64_t ref)
    : realm_node(alloc, ref)
{
    init(alloc, ref);
}
bool realm_array::is_inner_bptree_node() const
{
    return realm::NodeHeader::get_is_inner_bptree_node_from_header(m_header);
}
void realm_array::init(realm::Allocator& alloc, uint64_t ref)
{
    realm_node::init(alloc, ref);
    m_data = data();
    m_has_refs = has_refs();
}
int64_t realm_array::get_val(size_t ndx) const
{
    int64_t val = realm::get_direct(m_data, width(), ndx);
    if (m_has_refs) {
        if (val & 1) {
            val >>= 1;
        }
    }
    return val;
}
uint64_t realm_array::get_ref(size_t ndx) const
{
    REALM_ASSERT(m_has_refs);
    int64_t val = realm::get_direct(m_data, width(), ndx);

    if (val & 1)
        return 0;

    uint64_t ref = uint64_t(val);
    if (ref > current_logical_file_size || (ref & 7)) {
        std::cout << "*** Invalid ref: 0x" << std::hex << ref << std::dec << std::endl;
        return 0;
    }

    return ref;
}
std::string realm_array::get_string(size_t ndx) const
{
    std::string str;
    if (valid()) {
        auto sz = size_in_bytes();
        auto w = width();
        REALM_ASSERT(ndx * w < sz);
        str = m_data + ndx * w;
    }
    return str;
}
size_t realm_array::mem_usage(realm::Allocator& alloc) const
{
    size_t mem = 0;
    _mem_usage(alloc, mem);
    return mem;
}

void realm_array::_mem_usage(realm::Allocator& alloc, size_t& mem) const
{
    if (m_has_refs) {
        for (size_t i = 0; i < m_size; ++i) {
            if (uint64_t ref = get_ref(i)) {
                realm_array subarray(alloc, ref);
                subarray._mem_usage(alloc, mem);
            }
        }
    }
    mem += size_in_bytes();
}

realm_handle::realm_handle(const realm_config& cnf)
{
    realm::SlabAlloc::Config slab_config;
    if (!cnf.encryption_key.empty())
        slab_config.encryption_key = cnf.encryption_key.c_str();
    slab_config.read_only = true;
    slab_config.no_create = true;
    m_top_ref = m_alloc.attach_file(cnf.path, slab_config);
    m_start_pos = 24;
    m_group = std::make_unique<realm_group>(m_alloc, m_top_ref);
    m_file_format_version = m_alloc.get_committed_file_format_version();
}

realm_group::realm_group(realm::SlabAlloc& alloc, uint64_t ref)
    : realm_array(alloc, ref)
    , m_alloc(alloc)
{
    m_valid &= (size() <= 12);
    if (valid()) {
        m_file_size = get_val(2);
        current_logical_file_size = m_file_size;
        m_table_names.init(alloc, get_ref(0));
        m_table_refs.init(alloc, get_ref(1));
        if (size() > 3) {
            m_free_list_positions.init(alloc, get_ref(3));
            m_free_list_sizes.init(alloc, get_ref(4));
            m_free_list_versions.init(alloc, get_ref(5));
        }
        if (size() > 8) {
            m_history.init(alloc, get_ref(8));
        }
        if (size() > 11) {
            auto ref = get_ref(11);
            if (ref)
                m_evacuation_info.init(alloc, ref);
        }
    }
}

uint64_t realm_group::get_file_size() const
{
    return m_file_size;
}
uint64_t realm_group::get_free_space_size() const
{
    uint64_t sz = 0;
    for (size_t i = 0; i < m_free_list_sizes.size(); i++) {
        sz += m_free_list_sizes.get_val(i);
    }
    return sz;
}
int realm_group::get_current_version() const
{
    return int(get_val(6));
}
std::string realm_group::get_history_type() const
{
    switch (int(get_val(7))) {
        case 0:
            return "None";
        case 1:
            return "OutOfRealm";
        case 2:
            return "InRealm";
        case 3:
            return "SyncClient";
        case 4:
            return "SyncServer";
    }
    return "Unknown";
}
std::vector<realm::BinaryData> realm_group::get_changesets()
{
    std::vector<realm::BinaryData> ret;
    if (int(get_val(7)) == 2) { // TODO replace this magic numbers with something meaningful
        for (size_t n = 0; n < m_history.size(); n++) {
            auto ref = m_history.get_ref(n);
            realm_node node(m_alloc, ref);
            ret.emplace_back(node.data(), node.size());
        }
    }
    if (int(get_val(7)) == 3) {          // TODO replace this magic numbers with something meaningful
        auto ref = m_history.get_ref(0); // ct history
        m_history.init(m_alloc, ref);
        for (size_t n = 0; n < m_history.size(); n++) {
            ref = m_history.get_ref(n);
            realm_node node(m_alloc, ref);
            ret.emplace_back(node.data(), node.size());
        }
    }
    return ret;
}
int realm_group::get_history_schema_version() const
{
    return int(get_val(9));
}
int realm_group::get_file_ident() const
{
    return int(get_val(10));
}
void realm_group::print_evacuation_info(std::ostream& ostr) const
{
    if (m_evacuation_info.valid()) {
        ostr << "Evacuation limit: " << size_t(m_evacuation_info.get_val(0));
        if (m_evacuation_info.get_val(1)) {
            ostr << " Scan done" << std::endl;
        }
        else {
            ostr << " Progress: [";
            for (size_t i = 2; i < m_evacuation_info.size(); i++) {
                if (i > 2)
                    ostr << ',';
                ostr << m_evacuation_info.get_val(i);
            }
            ostr << "]" << std::endl;
        }
    }
}
unsigned realm_group::get_nb_tables() const
{
    return m_table_names.size();
}
std::string realm_group::get_table_name(size_t i) const
{
    return m_table_names.get_string(i);
}
realm_table* realm_group::get_table(size_t i) const
{
    auto& ret = m_tables[i];
    if (!ret) {
        ret = nullptr; // TOOD: represent a table ==> new realm_table(m_alloc, m_table_refs.get_ref(i));
    }
    return ret;
}
std::vector<slab_entry> realm_group::get_allocated_nodes() const
{
    return {};
}
std::vector<free_list_entry> realm_group::get_free_list() const
{
    // TODO
    return {};
}
void realm_group::print_schema() const
{
    // TODO
}

// utility functions

static realm_config setup_config(int argc, char** argv)
{
    realm_config cnf;

    if (argc > 1) {
        int curr_arg = 1;
        if (strcmp(argv[curr_arg], "--keyfile") == 0) {
            const auto& file_path = argv[++curr_arg];
            std::ifstream in{file_path};
            char s[64];
            in.read(s, sizeof(s));
            cnf.encryption_key = s;
        }
        else if (strcmp(argv[curr_arg], "--hexkey") == 0) {
            const auto chars = argv[++curr_arg];
            if (strlen(chars) == 128) {
                for (int i = 0; i < 64; ++i) {
                    cnf.encryption_key.push_back(chars[i * 2] << 4 | chars[i * 2 + 1]);
                }
            }
            else {
                throw std::invalid_argument{"Encryption string must be 128 bytes long"};
            }
        }
        else {
            cnf.path = argv[curr_arg];
        }
    }

    if (cnf.path.empty())
        throw std::invalid_argument{"No path specified where to load the database"};

    return cnf;
}

static void dump_realm(const realm_handle& handle)
{
    std::cout << "File format: " << handle.m_file_format_version << std::endl;
}

// main program

int main(int argc, char** argv)
{

    // realm-recovery-data --keyfile <file>
    // realm-recovery-data --hexkey <....>
    // dump database
    try {
        std::string encryption_key;
        auto config = setup_config(argc, argv);
        dump_realm(realm_handle{config});
    }
    catch (const std::exception& e) {
        std::cout << "Something went wrong: " << e.what() << std::endl;
    }
    return 0;
}