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

    uint64_t get_file_size() const;
    uint64_t get_free_space_size() const;
    int get_current_version() const;
    std::string get_history_type() const;
    std::vector<realm::BinaryData> get_changesets();
    int get_history_schema_version() const;
    int get_file_ident() const;
    void print_evacuation_info(std::ostream& ostr) const;
    unsigned get_nb_tables() const;
    std::string get_table_name(size_t i) const;
    std::shared_ptr<realm_table> get_table(size_t i) const;

    std::vector<slab_entry> get_allocated_nodes() const;
    std::vector<free_list_entry> get_free_list() const;
    void print_schema() const;

    std::string human_readable(uint64_t val) const;
    std::vector<slab_entry> get_nodes(realm::Allocator& alloc, uint64_t ref) const;
    void consolidate_lists(std::vector<slab_entry>& list, std::vector<slab_entry>& list2) const;
    std::string print_path() const;

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
    mutable std::map<size_t, std::shared_ptr<realm_table>> m_tables;
    mutable std::vector<unsigned> m_path;
};

struct realm_table : public realm_array {
public:
    realm_table() = default;
    realm_table(realm::Allocator& alloc, uint64_t ref);
    std::string get_column_name(realm::ColKey col_key) const;
    void print_columns(const realm_group&) const;
    size_t get_size(realm::Allocator& alloc) const;
    size_t get_subspec_ndx_after(size_t column_ndx) const noexcept;

    realm_array m_column_types;
    realm_array m_column_names;
    realm_array m_column_attributes;
    realm_array m_enum_keys;
    realm_array m_column_subspecs;
    realm_array m_column_colkeys;
    realm_array m_opposite_table;
    realm_array m_clusters;
    realm::ColKey m_pk_col;
    realm::Table::Type m_table_type = realm::Table::Type::TopLevel;
    std::vector<size_t> m_leaf_ndx2spec_ndx;
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
std::shared_ptr<realm_table> realm_group::get_table(size_t i) const
{
    auto& table = m_tables[i];
    if (!table) {
        table = std::make_shared<realm_table>(m_alloc, m_table_refs.get_ref(i));
    }
    return table;
}
std::vector<slab_entry> realm_group::get_allocated_nodes() const
{
    std::vector<slab_entry> all_nodes;
    all_nodes.emplace_back(0, 24);                  // Header area
    all_nodes.emplace_back(m_ref, size_in_bytes()); // Top array itself

    m_path.push_back(0);
    auto table_name_nodes = get_nodes(m_alloc, get_ref(0)); // Table names
    consolidate_lists(all_nodes, table_name_nodes);
    m_path.back() = 1;
    auto table_nodes = get_nodes(m_alloc, get_ref(1)); // Tables
    consolidate_lists(all_nodes, table_nodes);

    auto get_size = [](const std::vector<slab_entry>& list) {
        uint64_t sz = 0;
        std::for_each(list.begin(), list.end(), [&](const auto& e) {
            sz += e.length;
        });
        return sz;
    };

    std::cout << "State size: " << human_readable(get_size(all_nodes)) << std::endl;

    if (size() > 3) {
        std::vector<slab_entry> free_lists;
        free_lists.emplace_back(m_free_list_positions.ref(), m_free_list_positions.size_in_bytes());
        free_lists.emplace_back(m_free_list_sizes.ref(), m_free_list_sizes.size_in_bytes());
        free_lists.emplace_back(m_free_list_versions.ref(), m_free_list_versions.size_in_bytes());
        consolidate_lists(all_nodes, free_lists);
    }

    if (size() > 8) {
        std::vector<slab_entry> history;
        history = get_nodes(m_alloc, get_ref(8));
        std::cout << "History size: " << human_readable(get_size(history)) << std::endl;
        consolidate_lists(all_nodes, history);
    }

    if (size() > 11) {
        std::vector<slab_entry> evac_info;
        evac_info = get_nodes(m_alloc, get_ref(11));
        consolidate_lists(all_nodes, evac_info);
    }

    return all_nodes;
}
std::vector<free_list_entry> realm_group::get_free_list() const
{
    std::vector<free_list_entry> list;
    if (valid()) {
        unsigned sz = m_free_list_positions.size();
        if (sz != m_free_list_sizes.size()) {
            std::cout << "FreeList positions size: " << sz << " FreeList sizes size: " << m_free_list_sizes.size()
                      << std::endl;
            return list;
        }
        if (sz != m_free_list_versions.size()) {
            std::cout << "FreeList positions size: " << sz
                      << " FreeList versions size: " << m_free_list_versions.size() << std::endl;
            return list;
        }
        for (unsigned i = 0; i < sz; i++) {
            int64_t pos = m_free_list_positions.get_val(i);
            int64_t size = m_free_list_sizes.get_val(i);
            int64_t version = m_free_list_versions.get_val(i);
            list.emplace_back(pos, size, version);
        }
    }
    return list;
}
void realm_group::print_schema() const
{
    if (valid()) {
        std::cout << "Tables: " << std::endl;

        for (unsigned i = 0; i < get_nb_tables(); i++) {
            auto table = get_table(i);
            std::cout << "    " << i << ": " << get_table_name(i) << " - size: " << table->get_size(m_alloc)
                      << " datasize: " << human_readable(table->mem_usage(m_alloc)) << std::endl;
            table->print_columns(*this);
        }
    }
}

std::string realm_group::human_readable(uint64_t val) const
{
    std::ostringstream out;
    out.precision(3);
    if (val < 1024) {
        out << val;
    }
    else if (val < 1024 * 1024) {
        out << (double(val) / 1024) << "K";
    }
    else if (val < 1024 * 1024 * 1024) {
        out << (double(val) / (1024 * 1024)) << "M";
    }
    else {
        out << (double(val) / (1024 * 1024 * 1024)) << "G";
    }
    return out.str();
}

std::vector<slab_entry> realm_group::get_nodes(realm::Allocator& alloc, uint64_t ref) const
{
    std::vector<slab_entry> nodes;
    if (ref != 0) {
        realm_array arr(alloc, ref);
        if (!arr.valid()) {
            std::cout << "Not and array: 0x" << std::hex << ref << std::dec << ", path: " << print_path()
                      << std::endl;
            return {};
        }
        nodes.emplace_back(ref, arr.size_in_bytes());
        if (arr.has_refs()) {
            auto sz = arr.size();
            m_path.push_back(0);
            for (unsigned i = 0; i < sz; i++) {
                uint64_t r = arr.get_ref(i);
                if (r) {
                    m_path.back() = i;
                    auto sub_nodes = get_nodes(alloc, r);
                    consolidate_lists(nodes, sub_nodes);
                }
            }
            m_path.pop_back();
        }
    }
    return nodes;
}

void realm_group::consolidate_lists(std::vector<slab_entry>& list, std::vector<slab_entry>& list2) const
{
    list.insert(list.end(), list2.begin(), list2.end());
    list2.clear();
    if (list.size() > 1) {
        std::sort(begin(list), end(list), [](auto& a, auto& b) {
            return a.start < b.start;
        });

        auto prev = list.begin();
        for (auto it = list.begin() + 1; it != list.end(); ++it) {
            if (prev->start + prev->length != it->start) {
                if (prev->start + prev->length > it->start) {
                    std::cout << "*** Overlapping entries:" << std::endl;
                    std::cout << std::hex;
                    std::cout << "    0x" << prev->start << "..0x" << prev->start + prev->length << std::endl;
                    std::cout << "    0x" << it->start << "..0x" << it->start + it->length << std::endl;
                    std::cout << std::dec;
                    // Remove new entry from list
                    it->length = 0;
                }
                prev = it;
                continue;
            }

            prev->length += it->length;
            it->length = 0;
        }

        // Remove all of the now zero-size chunks from the free list
        list.erase(std::remove_if(begin(list), end(list),
                                  [](auto& chunk) {
                                      return chunk.length == 0;
                                  }),
                   end(list));
    }
}

std::string realm_group::print_path() const
{
    std::string ret = "[" + std::to_string(m_path[0]);
    for (auto it = m_path.begin() + 1; it != m_path.end(); ++it) {
        ret += ", ";
        ret += std::to_string(*it);
    }
    return ret + "]";
}

realm_table::realm_table(realm::Allocator& alloc, uint64_t ref)
    : realm_array(alloc, ref)
{
    if (valid()) {
        realm_array spec(alloc, get_ref(0));
        m_column_types.init(alloc, spec.get_ref(0));
        m_column_names.init(alloc, spec.get_ref(1));
        m_column_attributes.init(alloc, spec.get_ref(2));
        if (spec.size() > 5) {
            // Must be a Core-6 file.
            m_enum_keys.init(alloc, spec.get_ref(4));
            m_column_colkeys.init(alloc, spec.get_ref(5));

            size_t num_spec_cols = m_column_types.size();

            for (size_t spec_ndx = 0; spec_ndx < num_spec_cols; ++spec_ndx) {
                realm::ColKey col_key{m_column_colkeys.get_val(spec_ndx)};
                unsigned leaf_ndx = col_key.get_index().val;
                if (leaf_ndx >= m_leaf_ndx2spec_ndx.size()) {
                    m_leaf_ndx2spec_ndx.resize(leaf_ndx + 1, -1);
                }
                m_leaf_ndx2spec_ndx[leaf_ndx] = spec_ndx;
            }
        }
        // TODO: more magic numbers, this stuff needs to be documented.
        else if (spec.size() > 3) {
            // In pre Core-6, the subspecs array is optional
            // Not used in Core-6
            m_column_subspecs.init(alloc, spec.get_ref(3));
        }
        if (size() > 7) {
            // Must be a Core-6 file.
            m_clusters.init(alloc, get_ref(2));
            m_opposite_table.init(alloc, get_ref(7));
        }
        if (size() > 11) {
            auto pk_col = get_val(11);
            if (pk_col)
                m_pk_col = realm::ColKey(pk_col);
        }
        if (size() > 12) {
            auto flags = get_val(12);
            m_table_type = static_cast<realm::Table::Type>(flags & 0x3);
        }
    }
}
std::string realm_table::get_column_name(realm::ColKey col_key) const
{
    return m_column_names.get_string(m_leaf_ndx2spec_ndx[col_key.get_index().val]);
}
void realm_table::print_columns(const realm_group& group) const
{
    // TODO add JSON sturcture.
    std::cout << "        <" << m_table_type << ">" << std::endl;
    for (unsigned i = 0; i < m_column_names.size(); i++) {
        auto type = realm::ColumnType(m_column_types.get_val(i) & 0xFFFF);
        auto attr = realm::ColumnAttr(m_column_attributes.get_val(i));
        std::string type_str;
        realm::ColKey col_key;
        if (this->m_column_colkeys.valid()) {
            // core6
            col_key = realm::ColKey(m_column_colkeys.get_val(i));
        }

        if (type == realm::col_type_Link || type == realm::col_type_LinkList) {
            size_t target_table_ndx;
            if (col_key) {
                // core6
                realm::TableKey opposite_table_key(uint32_t(m_opposite_table.get_val(col_key.get_index().val)));
                target_table_ndx = opposite_table_key.value & 0xFFFF;
            }
            else {
                target_table_ndx = size_t(m_column_subspecs.get_val(get_subspec_ndx_after(i)));
            }
            type_str += group.get_table_name(target_table_ndx);
            if (!col_key && type == realm::col_type_LinkList) {
                type_str += "[]";
            }
        }
        else {
            type_str = get_data_type_name(realm::DataType(type));
        }
        if (col_key) {
            if (col_key.is_list())
                type_str += "[]";
            if (col_key.is_set())
                type_str += "{}";
            if (col_key.is_dictionary()) {
                auto key_type = realm::DataType(static_cast<int>(m_column_types.get_val(i) >> 16));
                type_str = std::string("{") + get_data_type_name(key_type) + ", " + type_str + "}";
            }
        }
        if (attr & realm::col_attr_Nullable)
            type_str += "?";
        if (attr & realm::col_attr_Indexed)
            type_str += " (indexed)";
        if (m_enum_keys.valid() && m_enum_keys.get_val(i)) {
            type_str += " (enumerated)";
        }
        std::string star = (m_pk_col && (m_pk_col == col_key)) ? "*" : "";
        std::cout << "        " << i << ": " << star << m_column_names.get_string(i) << ": " << type_str << std::endl;
    }
}

size_t realm_table::get_size(realm::Allocator& alloc) const
{
    size_t ret = 0;
    if (m_clusters.valid()) {
        if (m_clusters.is_inner_bptree_node()) {
            ret = size_t(m_clusters.get_val(2));
        }
        else {
            if (uint64_t key_ref = m_clusters.get_ref(0)) {
                auto header = alloc.translate(key_ref);
                ret = realm::NodeHeader::get_size_from_header(header);
            }
            else {
                ret = m_clusters.get_val(0);
            }
        }
    }
    return ret;
}

size_t realm_table::get_subspec_ndx_after(size_t column_ndx) const noexcept
{
    REALM_ASSERT(column_ndx <= m_column_names.size());
    // The m_subspecs array only keep info for subtables so we need to
    // count up to it's position
    size_t subspec_ndx = 0;
    for (size_t i = 0; i != column_ndx; ++i) {
        auto type = realm::ColumnType(m_column_types.get_val(i));
        if (type == realm::col_type_Link || type == realm::col_type_LinkList) {
            subspec_ndx += 1; // index of dest column
        }
        else if (type == realm::col_type_BackLink) {
            subspec_ndx += 2; // index of table and index of linked column
        }
    }
    return subspec_ndx;
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
    handle.m_group->print_schema();
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