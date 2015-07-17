#include <vector>

#include <realm/array_integer.hpp>
#include <realm/column.hpp>
#include <realm/impl/destroy_guard.hpp>

using namespace realm;

// Find max and min value, but break search if difference exceeds 'maxdiff' (in which case *min and *max is set to 0)
// Useful for counting-sort functions
template <size_t w>
bool ArrayInteger::minmax(size_t from, size_t to, uint64_t maxdiff, int64_t *min, int64_t *max) const
{
    int64_t min2;
    int64_t max2;
    size_t t;

    max2 = Array::get<w>(from);
    min2 = max2;

    for (t = from + 1; t < to; t++) {
        int64_t v = Array::get<w>(t);
        // Utilizes that range test is only needed if max2 or min2 were changed
        if (v < min2) {
            min2 = v;
            if (uint64_t(max2 - min2) > maxdiff)
                break;
        }
        else if (v > max2) {
            max2 = v;
            if (uint64_t(max2 - min2) > maxdiff)
                break;
        }
    }

    if (t < to) {
        *max = 0;
        *min = 0;
        return false;
    }
    else {
        *max = max2;
        *min = min2;
        return true;
    }
}


std::vector<int64_t> ArrayInteger::ToVector() const
{
    std::vector<int64_t> v;
    const size_t count = size();
    for (size_t t = 0; t < count; ++t)
        v.push_back(Array::get(t));
    return v;
}

MemRef ArrayIntNull::create_array(Type type, bool context_flag, std::size_t size, int_fast64_t value, Allocator& alloc)
{
    MemRef r = Array::create(type, context_flag, wtype_Bits, size + 1, value, alloc); // Throws
    ArrayIntNull arr(alloc);
    _impl::DestroyGuard<ArrayIntNull> dg(&arr);
    arr.Array::init_from_mem(r);
    if (arr.m_width == 64) {
        int_fast64_t null_value = value ^ 1; // Just anything different from value.
        arr.Array::set(0, null_value); // Throws
    }
    else {
        arr.Array::set(0, arr.m_ubound); // Throws
    }
    dg.release();
    return r;
}

void ArrayIntNull::init_from_ref(ref_type ref) REALM_NOEXCEPT
{
    REALM_ASSERT_DEBUG(ref);
    char* header = m_alloc.translate(ref);
    init_from_mem(MemRef{header, ref});
}

void ArrayIntNull::init_from_mem(MemRef mem) REALM_NOEXCEPT
{
    Array::init_from_mem(mem);
    
    if (m_size == 0) {
        // This can only happen when mem is being reused from another
        // array (which happens when shrinking the B+tree), so we need
        // to add the "magic" null value to the beginning.
        
        // Since init_* functions are noexcept, but insert() isn't, we
        // need to ensure that insert() will not allocate.
        REALM_ASSERT(m_capacity != 0);
        Array::insert(0, m_ubound);
    }
}

void ArrayIntNull::init_from_parent() REALM_NOEXCEPT
{
    init_from_ref(get_ref_from_parent());
}

namespace {
    int64_t next_null_candidate(int64_t previous_candidate) {
        uint64_t x = static_cast<uint64_t>(previous_candidate);
        // Increment by a prime number. This guarantees that we will
        // eventually hit every possible integer in the 2^64 range.
        x += 0xfffffffbULL;
        return static_cast<int64_t>(x);
    }
}

int_fast64_t ArrayIntNull::choose_random_null(int64_t incoming)
{
    // We just need any number -- it could have been `rand()`, but
    // random numbers are hard, and we don't want to risk locking mutices
    // or saving state. The top of the stack should be "random enough".
    int64_t candidate = reinterpret_cast<int64_t>(&candidate);

    while (true) {
        candidate = next_null_candidate(candidate);
        if (candidate == incoming) {
            continue;
        }
        if (can_use_as_null(candidate)) {
            return candidate;
        }
    }
}

bool ArrayIntNull::can_use_as_null(int64_t candidate)
{
    return find_first(candidate) == npos;
}

void ArrayIntNull::replace_nulls_with(int64_t new_null)
{
    int64_t old_null = Array::get(0);
    Array::set(0, new_null);
    std::size_t i = 1;
    while (true) {
        std::size_t found = Array::find_first(old_null, i);
        if (found < Array::size()) {
            Array::set(found, new_null);
            i = found + 1;
        }
        else {
            break;
        }
    }
}


void ArrayIntNull::avoid_null_collision(int64_t value)
{
    if (m_width == 64) {
        if (value == null_value()) {
            int_fast64_t new_null = choose_random_null(value);
            replace_nulls_with(new_null);
        }
    }
    else {
        if (value < m_lbound || value >= m_ubound) {
            size_t new_width = bit_width(value);
            int64_t new_upper_bound = Array::ubound_for_width(new_width);

            // We're using upper bound as magic NULL value, so we have to check
            // explicitly that the incoming value doesn't happen to be the new
            // NULL value. If it is, we upgrade one step further.
            if (new_width < 64 && value == new_upper_bound) {
                new_width = (new_width == 0 ? 1 : new_width * 2);
                new_upper_bound = Array::ubound_for_width(new_width);
            }

            int64_t new_null;
            if (new_width == 64) {
                // Width will be upgraded to 64, so we need to pick a random NULL.
                new_null = choose_random_null(value);
            }
            else {
                new_null = new_upper_bound;
            }

            replace_nulls_with(new_null); // Expands array
        }
    }
}

void ArrayIntNull::find_all(Column* result, int64_t value, std::size_t col_offset, std::size_t begin, std::size_t end) const
{
    // FIXME: We can't use the fast Array::find_all here, because it would put the wrong indices
    // in the result column. Since find_all may be invoked many times for different leaves in the
    // B+tree with the same result column, we also can't simply adjust indices after finding them
    // (because then the first indices would be adjusted multiple times for each subsequent leaf)
    
    if (end == npos) {
        end = size();
    }
    
    for (size_t i = begin; i < end; ++i) {
        if (get(i) == value) {
            result->add(col_offset + i);
        }
    }
}

namespace {
    
// FIXME: Move this logic to BpTree.
struct ArrayIntNullLeafInserter {
    template <class T>
    static ref_type leaf_insert(Allocator& alloc, ArrayIntNull& self, std::size_t ndx, T value, Array::TreeInsertBase& state)
    {
        size_t leaf_size = self.size();
        REALM_ASSERT_DEBUG(leaf_size <= REALM_MAX_BPNODE_SIZE);
        if (leaf_size < ndx)
            ndx = leaf_size;
        if (REALM_LIKELY(leaf_size < REALM_MAX_BPNODE_SIZE)) {
            self.insert(ndx, value); // Throws
            return 0; // Leaf was not split
        }

        // Split leaf node
        ArrayIntNull new_leaf(alloc);
        new_leaf.create(Array::type_Normal); // Throws
        if (ndx == leaf_size) {
            new_leaf.add(value); // Throws
            state.m_split_offset = ndx;
        }
        else {
            for (size_t i = ndx; i < leaf_size; ++i) {
                if (self.is_null(i)) {
                    new_leaf.add(null{}); // Throws
                }
                else {
                    new_leaf.add(self.get(i)); // Throws
                }
            }
            self.truncate(ndx); // Throws
            self.add(value); // Throws
            state.m_split_offset = ndx + 1;
        }
        state.m_split_size = leaf_size + 1;
        return new_leaf.get_ref();
    }
};

} // anonymous namespace

ref_type ArrayIntNull::bptree_leaf_insert(std::size_t ndx, int64_t value, Array::TreeInsertBase& state)
{
    return ArrayIntNullLeafInserter::leaf_insert(get_alloc(), *this, ndx, value, state);
}

ref_type ArrayIntNull::bptree_leaf_insert(std::size_t ndx, null, Array::TreeInsertBase& state)
{
    return ArrayIntNullLeafInserter::leaf_insert(get_alloc(), *this, ndx, null{}, state);
}

MemRef ArrayIntNull::slice(std::size_t offset, std::size_t size, Allocator& target_alloc) const
{
    // NOTE: It would be nice to consolidate this with Array::slice somehow.

    REALM_ASSERT(is_attached());

    Array slice(target_alloc);
    _impl::DeepArrayDestroyGuard dg(&slice);
    Type type = get_type();
    slice.create(type, m_context_flag); // Throws
    slice.add(null_value());

    size_t begin = offset + 1;
    size_t end   = offset + size + 1;
    for (size_t i = begin; i != end; ++i) {
        int_fast64_t value = Array::get(i);
        slice.add(value); // Throws
    }
    dg.release();
    return slice.get_mem();
}

MemRef ArrayIntNull::slice_and_clone_children(size_t offset, size_t size, Allocator& target_alloc) const
{
    // NOTE: It would be nice to consolidate this with Array::slice_and_clone_children somehow.

    REALM_ASSERT(is_attached());
    REALM_ASSERT(!has_refs());
    return slice(offset, size, target_alloc);
}
