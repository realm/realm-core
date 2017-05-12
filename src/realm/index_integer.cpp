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

#include <realm/index_integer.hpp>
#include <realm/column.hpp>

using namespace realm;

namespace {

constexpr uint64_t null_value = 0xdeadbeef;

#define ROTATE(x, k) ((x << k) | (x >> (64 - k)))
#define STEP(h1, h2, k) h1 = (h1 ^ h2) + ROTATE(h2, k);

struct Digest {
    uint8_t idx;
    int8_t quick_key;
};

void hash128(uint64_t key, uint64_t& hash1, uint64_t& hash2, uint64_t mask)
{
    register uint64_t a = 0;
    register uint64_t b = 0;
    register uint64_t c = 0xdeadbeefdeadbeefULL + key;
    register uint64_t d = 0xdeadbeefdeadbeefULL + (8ULL << 56);

    STEP(d, c, 15);
    STEP(a, d, 52);
    STEP(b, a, 26);
    STEP(c, b, 51);
    STEP(d, c, 28);
    STEP(a, d, 9);
    STEP(b, a, 47);
    STEP(c, b, 54);
    STEP(d, c, 32);
    STEP(a, d, 25);
    STEP(b, a, 63);

    hash1 = a & mask;
    hash2 = b & mask;
}

ref_type lookup(Allocator& alloc, ref_type ref, uint64_t index, int levels)
{
    if (ref) {
        levels--;
        if (levels > 0) {
            int shifts = levels << 3;
            unsigned char c = index >> shifts;

            MemRef mem(ref, alloc);
            char* header = mem.get_addr();
            size_t width = Array::get_width_from_header(header);
            char* data = Array::get_data_from_header(header);
            ref = to_ref(get_direct(data, width, c));
            return lookup(alloc, ref, index, levels);
        }
    }
    return ref;
}

int depth(Array& arr)
{
    if (arr.get_context_flag()) {
        size_t sz = arr.size();
        for (size_t i = 0; i < sz; i++) {
            ref_type ref = to_ref(arr.get(i));
            if (ref) {
                Array subarr(arr.get_alloc());
                subarr.init_from_ref(ref);
                return depth(subarr) + 1;
            }
        }
    }
    return 1;
}
}

/*****************************************************************************/
/*                           IntegerIndex::TreeLeaf                           */
/*****************************************************************************/

inline void IntegerIndex::TreeLeaf::ensure_writeable(Treetop* treeTop, uint64_t hash)
{
    REALM_ASSERT(!has_parent());
    if (is_read_only()) {
        copy_on_write();
        treeTop->cow_path(hash, get_ref());
    }
}

IntegerIndex::TreeLeaf::TreeLeaf(Allocator& alloc, IntegerIndex* index)
    : Array(alloc)
    , m_index(index)
    , m_condenser(alloc)
    , m_values(alloc)
{
}

ref_type IntegerIndex::TreeLeaf::create(Allocator& alloc)
{
    Array arr(alloc);

    arr.create(Array::type_HasRefs); // Throws

    Array condenser(alloc);
    condenser.create(Array::type_Normal, false, 256, 0); // Throws
    condenser.ensure_minimum_width(0x7fff);
    arr.add(from_ref(condenser.get_ref()));

    MemRef mem = Array::create_empty_array(Array::type_HasRefs, false, alloc); // Throws
    arr.add(from_ref(mem.get_ref()));

    return arr.get_ref();
}

void IntegerIndex::TreeLeaf::init(ref_type ref)
{
    if (!is_attached() || ref != get_ref()) {
        init_from_ref(ref);
        m_condenser.set_parent(this, 0);
        m_condenser.init_from_parent();
        m_values.set_parent(this, 1);
        m_values.init_from_parent();
    }
}

void IntegerIndex::TreeLeaf::clear()
{
    memset(m_condenser.m_data, 0, 256 * sizeof(uint16_t));
}

int IntegerIndex::TreeLeaf::find(uint64_t hash, int64_t key) const
{
    int subhash = hash & 0xFF; // cut off all above one byte
    int subhash_limit = subhash + 4;
    int8_t quick_key = int8_t(key);
    Digest* digest_arr = reinterpret_cast<Digest*>(m_condenser.m_data);
    while (subhash != subhash_limit) {
        Digest& digest = digest_arr[subhash & 0xFF];
        subhash++;
        if (digest.idx == 0)
            continue;
        if (digest.quick_key != quick_key)
            continue;
        if (!m_index || m_index->get_key_value(get_first_value(digest.idx - 1)) == key)
            return digest.idx - 1;
    }
    return -1;
}

int IntegerIndex::TreeLeaf::find_empty_or_equal(uint64_t hash, int64_t key) const
{
    int subhash = hash & 0xFF; // cut off all above one byte
    int subhash_limit = subhash + 4;
    int8_t quick_key = int8_t(key);
    Digest* digest_arr = reinterpret_cast<Digest*>(m_condenser.m_data);
    while (subhash != subhash_limit) {
        Digest& digest = digest_arr[subhash & 0xFF];
        if (digest.idx == 0 && m_values.size() < 255)
            return subhash & 0xFF;
        subhash++;
        if (m_index && (digest.quick_key == quick_key) &&
            (m_index->get_key_value(get_first_value(digest.idx - 1)) == key))
            throw(unsigned(digest.idx - 1));
    }
    return -1;
}

int IntegerIndex::TreeLeaf::find_empty(uint64_t hash) const
{
    int subhash = hash & 0xFF; // cut off all above one byte
    int subhash_limit = subhash + 4;
    uint16_t* data = reinterpret_cast<uint16_t*>(m_condenser.m_data);
    while (subhash != subhash_limit) {
        if (data[subhash & 0xFF] == 0 && m_values.size() < 255)
            return subhash & 0xFF;
        subhash++;
    }
    return -1;
}

bool IntegerIndex::TreeLeaf::insert_1(Treetop* treeTop, uint64_t hash, int64_t key, int64_t value)
{
    try {
        int subhash = find_empty_or_equal(hash, key);
        if (subhash < 0) {
            return false;
        }

        // we're adding a new key:

        REALM_ASSERT(!has_parent());
        uint16_t* data;
        if (is_read_only()) {
            data = m_condenser.get_writable_data(subhash);
            treeTop->cow_path(hash, get_ref());
        }
        else {
            data = reinterpret_cast<uint16_t*>(m_condenser.m_data) + subhash;
        }

        uint8_t idx = m_values.size();
        uint16_t quick_key = (key << 8) & 0xFF00;
        *data = quick_key + idx + 1;
        m_values.add(value);
    }
    catch (const unsigned& found) {
        // key already at position 'found'
        int_fast64_t slot_value = m_values.get(found);

        // Single match (lowest bit set indicates literal row_ndx)
        if ((slot_value & 1) != 0) {
            size_t row_ndx1 = to_size_t(value >> 1);
            size_t row_ndx2 = to_size_t(slot_value >> 1);

            Array row_list(get_alloc());
            row_list.create(Array::type_Normal); // Throws
            row_list.add(std::min(row_ndx1, row_ndx2));
            row_list.add(std::max(row_ndx1, row_ndx2));
            m_values.set(found, from_ref(row_list.get_ref()));
        }
        else {
            size_t row_ndx = to_size_t(value >> 1);
            Array sub(get_alloc()); // Throws
            sub.set_parent(&m_values, found);
            sub.init_from_parent();
            size_t lower = sub.lower_bound_int(row_ndx);
            sub.insert(lower, row_ndx);
        }
    }

    treeTop->incr_count();

    return true;
}

bool IntegerIndex::TreeLeaf::insert_2(Treetop* treeTop, uint64_t hash, int64_t key, int64_t& value)
{
    bool conflict = false;
    uint16_t quick_key = (key << 8) & 0xFF00;
    int subhash = find_empty(hash);
    if (subhash < 0) {
        conflict = true;
        subhash = hash & 0xffULL;
    }

    REALM_ASSERT(!has_parent());
    uint16_t* data;
    if (is_read_only()) {
        data = m_condenser.get_writable_data(subhash);
        treeTop->cow_path(hash, get_ref());
    }
    else {
        data = reinterpret_cast<uint16_t*>(m_condenser.m_data) + subhash;
    }

    if (conflict) { // we're reusing the spot of an old key:
        uint16_t digest = *data;
        uint8_t idx = digest & 0xFF;
        digest = quick_key + idx;
        --idx;
        int64_t old_value = m_values.get(idx);
        *data = digest;
        m_values.set(idx, value);
        value = old_value;

        return false;
    }
    else { // we're adding a new key:
        uint8_t idx = m_values.size();
        *data = quick_key + idx + 1;
        m_values.add(value);
        treeTop->incr_count();

        return true;
    }
}

void IntegerIndex::TreeLeaf::erase(Treetop* treeTop, uint64_t hash, unsigned index, int64_t value)
{
    ensure_writeable(treeTop, hash);

    int_fast64_t slot_value = m_values.get(index);

    // Single match (lowest bit set indicates literal row_ndx)
    if ((slot_value & 1) != 0) {
        REALM_ASSERT(slot_value == value);
        // Move last over
        unsigned last_index = m_values.size() - 1;
        REALM_ASSERT(index <= last_index);
        int steps = 1;
        if (index < last_index) {
            int64_t last_value = m_values.get(last_index);
            m_values.set(index, last_value);
            steps = 2;
        }
        m_values.erase(last_index);

        uint16_t* data = m_condenser.get_writable_data(0);
        unsigned i = 0;
        do {
            while (data[i] == 0) {
                i++;
                REALM_ASSERT_DEBUG(i < 256);
            }
            uint16_t digest = data[i];
            uint8_t idx = digest - 1;
            if (idx == index) {
                data[i] = 0;
                steps--;
            }
            else if (idx == last_index) {
                data[i] = (digest & 0xFF00) + index + 1;
                steps--;
            }
            i++;
        } while (steps > 0);
    }
    else {
        size_t row_ndx = to_size_t(value >> 1);
        Array sub(get_alloc()); // Throws
        sub.set_parent(&m_values, index);
        sub.init_from_parent();
        if (sub.size() > 2) {
            size_t lower = sub.lower_bound_int(row_ndx);
            sub.erase(lower);
        }
        else {
            size_t row_to_keep = to_size_t(sub.get(0));
            // Size must be 2
            if (row_to_keep == row_ndx) {
                row_to_keep = to_size_t(sub.get(1));
            }
            sub.destroy();
            int64_t shifted = int64_t((uint64_t(row_to_keep) << 1) + 1); // shift to indicate literal
            m_values.set(index, shifted);
        }
    }
    treeTop->decr_count();
}

size_t IntegerIndex::TreeLeaf::count(int in_leaf_idx) const
{
    REALM_ASSERT(in_leaf_idx >= 0);
    int64_t slot_value = m_values.get(in_leaf_idx);

    // Single match (lowest bit set indicates literal row_ndx)
    if ((slot_value & 1) != 0) {
        return 1;
    }
    else {
        MemRef mem(to_ref(slot_value), get_alloc()); // Throws
        return Array::get_size_from_header(mem.get_addr());
    }
}

void IntegerIndex::TreeLeaf::update_ref(Treetop* treeTop, uint64_t hash, int in_leaf_idx, size_t old_row_ndx,
                                        size_t new_row_ndx)
{
    ensure_writeable(treeTop, hash);

    int64_t slot_value = m_values.get(in_leaf_idx);

    // Single match (lowest bit set indicates literal row_ndx)
    if ((slot_value & 1) != 0) {
        int64_t shifted = int64_t((uint64_t(new_row_ndx) << 1) + 1); // shift to indicate literal
        REALM_ASSERT(to_size_t(slot_value >> 1) == old_row_ndx);
        m_values.set(in_leaf_idx, shifted);
    }
    else {
        Array sub(get_alloc()); // Throws
        sub.init_from_ref(to_ref(slot_value));
        size_t lower = sub.lower_bound_int(old_row_ndx);
        sub.erase(lower);
        lower = sub.lower_bound_int(new_row_ndx);
        sub.insert(lower, new_row_ndx);
    }
}

int64_t IntegerIndex::TreeLeaf::get_first_value(int in_leaf_idx) const
{
    REALM_ASSERT(in_leaf_idx >= 0);
    int64_t slot_value = m_values.get(in_leaf_idx);

    // Single match (lowest bit set indicates literal row_ndx)
    if ((slot_value & 1) != 0) {
        return slot_value >> 1;
    }
    else {
        Array sub(get_alloc()); // Throws
        sub.init_from_ref(to_ref(slot_value));
        return sub.get(0);
    }
}

void IntegerIndex::TreeLeaf::get_all_values(int in_leaf_idx, std::vector<int64_t>& values) const
{
    REALM_ASSERT(in_leaf_idx >= 0);
    int64_t slot_value = m_values.get(in_leaf_idx);

    // Single match (lowest bit set indicates literal row_ndx)
    if ((slot_value & 1) != 0) {
        size_t row_ndx = to_size_t(slot_value >> 1);
        values.resize(1);
        values[0] = row_ndx;
    }
    else {
        Array sub(get_alloc()); // Throws
        sub.init_from_ref(to_ref(slot_value));
        size_t sz = sub.size();
        values.resize(sz);
        for (unsigned i = 0; i < sz; i++) {
            values[i] = sub.get(i);
        }
    }
}

void IntegerIndex::TreeLeaf::adjust_row_indexes(size_t min_row_ndx, int diff)
{
    REALM_ASSERT(diff > 0 || diff == -1); // only used by insert and delete

    const size_t array_size = m_values.size();

    for (size_t i = 0; i < array_size; ++i) {
        int64_t ref = m_values.get(i);

        // low bit set indicate literal ref (shifted)
        if (ref & 1) {
            size_t r = size_t(uint64_t(ref) >> 1);
            if (r >= min_row_ndx) {
                size_t adjusted_ref = ((r + diff) << 1) + 1;
                m_values.set(i, adjusted_ref);
            }
        }
        else {
            Array sub(get_alloc()); // Throws
            sub.set_parent(&m_values, i);
            sub.init_from_parent();
            sub.adjust_ge(min_row_ndx, diff);
        }
    }
}

/*****************************************************************************/
/*                           IntegerIndex::Treetop                            */
/*****************************************************************************/

IntegerIndex::Treetop::Treetop(Allocator& alloc)
    : Array(alloc)
{
    init(256);
}

IntegerIndex::Treetop::Treetop(ref_type ref, Allocator& alloc)
    : Array(alloc)
{
    init_from_ref(ref);
    init();
}

IntegerIndex::Treetop::Treetop(Treetop&& other)
    : Array(other.get_alloc())
    , m_count(other.m_count)
    , m_mask(other.m_mask)
    , m_levels(other.m_levels)
{
    if (other.is_attached()) {
        init_from_ref(other.get_ref());
        other.detach();
    }
}

void IntegerIndex::Treetop::init()
{
    REALM_ASSERT(is_attached());
    m_levels = ::depth(*this);
    if (m_levels == 1) {
        m_mask = 0xff;
    }
    else {
        m_mask = size() - 1;
        int l = m_levels;
        while (--l) {
            m_mask = (m_mask << 8) | 0xff;
        }
    }
    size_t count = 0;
    for_each([&count](TreeLeaf* leaf) { count += leaf->size(); });
    m_count = count;
}

void IntegerIndex::Treetop::init(size_t capacity)
{
    REALM_ASSERT(!is_attached());
    m_count = 0;
    int bits = 4; // minimal size of tree is 16
    while ((1ULL << bits) < capacity)
        ++bits;
    size_t real_capacity = 1ULL << bits;
    m_mask = real_capacity - 1;
    m_levels = ((bits - 1) >> 3) + 1;
    if (m_levels == 1) {
        init_from_ref(TreeLeaf::create(get_alloc()));
    }
    else {
        // top_level_size = real_capacity / 256 ^ (levels - 1)
        int top_level_size = (real_capacity >> ((m_levels - 1) << 3));
        create(Array::type_HasRefs, true, top_level_size, 0);
    }
}

void IntegerIndex::Treetop::clear(IntegerIndex::TreeLeaf& leaf)
{
    if (m_levels == 1) {
        leaf.init(get_ref());
        leaf.clear();
    }
    else {
        destroy_children();
        set_all_to_zero();
    }
    m_count = 0;
}

size_t IntegerIndex::Treetop::get_count() const noexcept
{
    return m_count;
}

void IntegerIndex::Treetop::incr_count()
{
    m_count++;
}

void IntegerIndex::Treetop::decr_count()
{
    REALM_ASSERT(m_count > 0);
    m_count--;
}

bool IntegerIndex::Treetop::ready_to_grow() const noexcept
{
    return (m_count + (m_count >> 1)) > m_mask;
}

void IntegerIndex::Treetop::cow(Array& arr, uint64_t masked_index, int levels, ref_type leaf_ref)
{
    levels--;
    int shifts = levels << 3;
    unsigned char c = masked_index >> shifts;
    if (levels == 1) {
        arr.set(c, leaf_ref);
    }
    else {
        Array subarr(arr.get_alloc());
        subarr.set_parent(&arr, c);
        ref_type ref = arr.get(c);
        if (ref == 0) {
            subarr.create(Array::type_HasRefs, true, 256, 0);
            subarr.update_parent();
        }
        else {
            subarr.init_from_ref(ref);
        }
        cow(subarr, masked_index, levels, leaf_ref);
    }
}

void IntegerIndex::Treetop::cow_path(uint64_t hash, ref_type leaf_ref)
{
    switch (m_levels) {
        case 1:
            init_from_ref(leaf_ref);
            break;
        case 2:
            set(hash >> 8, from_ref(leaf_ref));
            break;
        default:
            REALM_ASSERT(m_levels < 8);
            cow(*this, hash, m_levels, leaf_ref);
            break;
    }
}

bool IntegerIndex::Treetop::lookup(uint64_t index, TreeLeaf& leaf)
{
    ref_type ref = ::lookup(get_alloc(), get_ref(), index, m_levels);
    if (ref == 0) {
        return false;
    }
    leaf.init(ref);
    return true;
}

void IntegerIndex::Treetop::lookup_or_create(uint64_t hash, TreeLeaf& leaf)
{
    ref_type ref = ::lookup(get_alloc(), get_ref(), hash, m_levels);
    if (ref == 0) {
        ref = TreeLeaf::create(get_alloc());
        cow_path(hash, ref);
    }
    leaf.init(ref);
}

void IntegerIndex::Treetop::adjust_row_indexes(size_t min_row_ndx, int diff)
{
    auto cb = [=](TreeLeaf* leaf) { leaf->adjust_row_indexes(min_row_ndx, diff); };
    for_each(cb);
}

void IntegerIndex::Treetop::for_each(std::function<void(TreeLeaf*)> func)
{
    for_each(*this, func);
}

void IntegerIndex::Treetop::for_each(Array& arr, std::function<void(TreeLeaf*)> func)
{
    if (arr.get_context_flag()) {
        size_t sz = arr.size();
        for (size_t i = 0; i < sz; i++) {
            ref_type ref = to_ref(arr.get(i));
            if (ref) {
                Array sub_arr(arr.get_alloc());
                sub_arr.set_parent(&arr, i);
                sub_arr.init_from_ref(ref);
                for_each(sub_arr, func);
            }
        }
    }
    else {
        IntegerIndex::TreeLeaf leaf(arr.get_alloc());
        leaf.set_parent(arr.get_parent(), arr.get_ndx_in_parent());
        leaf.init(arr.get_ref());
        func(&leaf);
    }
}

/*****************************************************************************/
/*                                IntegerIndex                                */
/*****************************************************************************/

IntegerIndex::IntegerIndex(ColumnBase* target_column, Allocator& alloc)
    : m_target_column(target_column)
    , m_top(alloc)
    , m_current_leaf(alloc, this)
{
    set_top(&m_top);
}

IntegerIndex::IntegerIndex(ref_type ref, ArrayParent* parent, size_t ndx_in_parent, ColumnBase* target_column,
                           Allocator& alloc)
    : m_target_column(target_column)
    , m_top(ref, alloc)
    , m_current_leaf(alloc, this)
{
    m_top.set_parent(parent, ndx_in_parent);

    set_top(&m_top);
}


IntegerIndex::~IntegerIndex() noexcept
{
}

void IntegerIndex::update_from_parent(size_t old_baseline) noexcept
{
    if (m_top.update_from_parent(old_baseline)) {
        m_top.init();
    }
}

void IntegerIndex::refresh_accessor_tree(size_t, const Spec&)
{
    m_top.init_from_parent();
    m_top.init();
}

void IntegerIndex::clear()
{
    m_top.clear(this->m_current_leaf);
}

constexpr int max_collisions = 20;

void IntegerIndex::insert(size_t row_ndx, int64_t key, size_t num_rows, bool is_append)
{
    if (!is_append) {
        m_top.adjust_row_indexes(row_ndx, num_rows); // Throws
    }

    while (num_rows--) {
        uint64_t hash = m_top.m_mask + 1;
        int64_t shifted = int64_t((uint64_t(row_ndx) << 1) + 1); // shift to indicate literal
        uint64_t h_1;
        uint64_t h_2;
        hash128(key, h_1, h_2, m_top.m_mask);
        hash = h_1;
        m_top.lookup_or_create(hash, m_current_leaf);
        if (!m_current_leaf.insert_1(&m_top, hash, key, shifted)) {
            int collision_count = 0;
            while (collision_count < max_collisions) {
                hash = (hash != h_1) ? h_1 : h_2;

                m_top.lookup_or_create(hash, m_current_leaf);
                if (m_current_leaf.insert_2(&m_top, hash, key, shifted)) {
                    break;
                }
                // `shifted` is now updated
                if (shifted & 1) {
                    key = get_key_value(shifted >> 1);
                }
                else {
                    Array arr(m_top.get_alloc());
                    arr.init_from_ref(to_ref(shifted));
                    key = get_key_value(arr.get(0));
                }

                hash128(key, h_1, h_2, m_top.m_mask);

                ++collision_count;
                if (collision_count == max_collisions) {
                    grow_tree();
                    collision_count = 0;
                }
            }
        }

        if (m_top.ready_to_grow()) {
            grow_tree();
        }
        row_ndx++;
    }
}

void IntegerIndex::insert(size_t row_ndx, util::Optional<int64_t> value, size_t num_rows, bool is_append)
{
    if (value) {
        insert(row_ndx, *value, num_rows, is_append);
    }
    else {
        insert_null(row_ndx, num_rows, is_append);
    }
}

void IntegerIndex::insert_null(size_t row_ndx, size_t num_rows, bool is_append)
{
    insert(row_ndx, null_value, num_rows, is_append);
}

void IntegerIndex::update_ref(int64_t value, size_t old_row_ndx, size_t new_row_ndx)
{
    uint64_t hash;
    int in_leaf_idx = get_leaf_index(value, &hash);
    REALM_ASSERT(in_leaf_idx >= 0);
    m_current_leaf.update_ref(&m_top, hash, in_leaf_idx, old_row_ndx, new_row_ndx);
}

void IntegerIndex::update_ref(util::Optional<int64_t> value, size_t old_row_ndx, size_t new_row_ndx)
{
    if (value) {
        update_ref(*value, old_row_ndx, new_row_ndx);
    }
    else {
        update_ref(null_value, old_row_ndx, new_row_ndx);
    }
}

void IntegerIndex::erase(size_t row_ndx, bool is_last)
{
    int64_t old_key = get_key_value(row_ndx);
    do_delete(row_ndx, old_key); // Throws

    if (!is_last) {
        m_top.adjust_row_indexes(row_ndx, -1);
    }
}

size_t IntegerIndex::count(int64_t value) const
{
    int in_leaf_idx = get_leaf_index(value);
    if (in_leaf_idx >= 0) {
        return m_current_leaf.count(in_leaf_idx);
    }
    return 0;
}

size_t IntegerIndex::find_first(int64_t value) const
{
    int in_leaf_idx = get_leaf_index(value);
    if (in_leaf_idx >= 0) {
        return m_current_leaf.get_first_value(in_leaf_idx);
    }
    return realm::npos;
}

size_t IntegerIndex::find_first(util::Optional<int64_t> value) const
{
    if (value) {
        return find_first(*value);
    }
    else {
        return find_first(null_value);
    }
}

void IntegerIndex::find_all(IntegerColumn& result, int64_t value) const
{
    int in_leaf_idx = get_leaf_index(value);
    if (in_leaf_idx >= 0) {
        std::vector<int64_t> values;
        m_current_leaf.get_all_values(in_leaf_idx, values);
        for (auto val : values)
            result.add(val);
    }
}

void IntegerIndex::find_all(IntegerColumn& result, util::Optional<int64_t> value) const
{
    if (value) {
        find_all(result, *value);
    }
    else {
        find_all(result, null_value);
    }
}

void IntegerIndex::distinct(IntegerColumn& result) const
{
    auto func = [&](TreeLeaf* leaf) {
        size_t sz = leaf->size();
        for (size_t i = 0; i < sz; i++) {
            result.add(leaf->get_first_value(i));
        }
    };
    m_top.for_each(func);
}

int64_t IntegerIndex::get_key_value(size_t row)
{
    StringConversionBuffer buffer;
    StringData value = m_target_column->get_index_data(row, buffer);
    if (value.is_null())
        return null_value;
    else
        return *reinterpret_cast<const int64_t*>(value.data());
}

int IntegerIndex::get_leaf_index(int64_t key, uint64_t* hash) const
{
    uint64_t h_1;
    uint64_t h_2;
    hash128(key, h_1, h_2, m_top.m_mask);
    int in_leaf_idx = -1;
    if (m_top.lookup(h_1, m_current_leaf)) {
        in_leaf_idx = m_current_leaf.find(h_1, key);
        if (hash)
            *hash = h_1;
    }
    if (in_leaf_idx < 0) {
        if (m_top.lookup(h_2, m_current_leaf)) {
            in_leaf_idx = m_current_leaf.find(h_2, key);
            if (hash)
                *hash = h_2;
        }
    }
    return in_leaf_idx;
}

void IntegerIndex::do_delete(size_t row_ndx, int64_t key)
{
    int64_t shifted = int64_t((uint64_t(row_ndx) << 1) + 1); // shift to indicate literal
    uint64_t hash = 0;
    int in_leaf_idx = get_leaf_index(key, &hash);
    REALM_ASSERT(in_leaf_idx >= 0);
    m_current_leaf.erase(&m_top, hash, unsigned(in_leaf_idx), shifted);
}

void IntegerIndex::grow_tree()
{
    // make a backup and set up new tree:
    uint64_t new_size = 4 * (m_top.m_mask + 1) - 1;
    Treetop old_top(std::move(m_top));

    m_top.init(new_size);

    auto do_insert = [this](TreeLeaf* leaf) {
        std::vector<int64_t> values(1);
        size_t sz = leaf->size();
        for (unsigned i = 0; i < sz; i++) {
            leaf->get_all_values(i, values);
            uint64_t key = get_key_value(values[0]);

            for (auto row : values) {
                insert(row, key, 1, true);
            }
        }
    };

    old_top.for_each(do_insert);
    old_top.destroy_deep();

    m_top.update_parent();
}
