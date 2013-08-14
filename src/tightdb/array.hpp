/*************************************************************************
 *
 * TIGHTDB CONFIDENTIAL
 * __________________
 *
 *  [2011] - [2012] TightDB Inc
 *  All Rights Reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of TightDB Incorporated and its suppliers,
 * if any.  The intellectual and technical concepts contained
 * herein are proprietary to TightDB Incorporated
 * and its suppliers and may be covered by U.S. and Foreign Patents,
 * patents in process, and are protected by trade secret or copyright law.
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from TightDB Incorporated.
 *
 **************************************************************************/

/*
Searching: The main finding function is:
    template<class cond, Action action, size_t bitwidth, class Callback> void find(int64_t value, size_t start, size_t end, size_t baseindex, QueryState *state, Callback callback) const

    cond:       One of Equal, NotEqual, Greater, etc. classes
    Action:     One of act_ReturnFirst, act_FindAll, act_Max, act_CallbackIdx, etc, constants
    Callback:   Optional function to call for each search result. Will be called if action == act_CallbackIdx

    find() will call find_action_pattern() or find_action() that again calls match() for each search result which optionally calls callback():

        find() -> find_action() -------> bool match() -> bool callback()
             |                            ^
             +-> find_action_pattern()----+

    If callback() returns false, find() will exit, otherwise it will keep searching remaining items in array.
*/

#ifndef TIGHTDB_ARRAY_HPP
#define TIGHTDB_ARRAY_HPP

#include <cmath>
#include <cstdlib> // std::size_t
#include <cstring> // memmove
#include <utility>
#include <vector>
#include <ostream>

#include <stdint.h> // unint8_t etc

#include <tightdb/meta.hpp>
#include <tightdb/assert.hpp>
#include <tightdb/alloc.hpp>
#include <tightdb/utilities.hpp>
#include <tightdb/string_data.hpp>
#include <tightdb/query_conditions.hpp>

/*
    MMX: mmintrin.h
    SSE: xmmintrin.h
    SSE2: emmintrin.h
    SSE3: pmmintrin.h
    SSSE3: tmmintrin.h
    SSE4A: ammintrin.h
    SSE4.1: smmintrin.h
    SSE4.2: nmmintrin.h
*/
#ifdef TIGHTDB_COMPILER_SSE
#  include <emmintrin.h> // SSE2
#  include <tightdb/tightdb_nmmintrin.h> // SSE42
#endif

#ifdef TIGHTDB_DEBUG
#  include <stdio.h>
#endif

namespace tightdb {

enum Action {act_ReturnFirst, act_Sum, act_Max, act_Min, act_Count, act_FindAll, act_CallIdx, act_CallbackIdx,
             act_CallbackVal, act_CallbackNone, act_CallbackBoth};

template<class T> inline T no0(T v) { return v == 0 ? 1 : v; }

/// Special index value. It has various meanings depending on
/// context. It is returned by some search functions to indicate 'not
/// found'. It is similar in function to std::string::npos.
const std::size_t npos = std::size_t(-1);

/// Alias for tightdb::npos.
const std::size_t not_found = npos;

 /* wid == 16/32 likely when accessing offsets in B tree */
#define TIGHTDB_TEMPEX(fun, wid, arg) \
    if (wid == 16) {fun<16> arg;} \
    else if (wid == 32) {fun<32> arg;} \
    else if (wid == 0) {fun<0> arg;} \
    else if (wid == 1) {fun<1> arg;} \
    else if (wid == 2) {fun<2> arg;} \
    else if (wid == 4) {fun<4> arg;} \
    else if (wid == 8) {fun<8> arg;} \
    else if (wid == 64) {fun<64> arg;} \
    else {TIGHTDB_ASSERT(false); fun<0> arg;}

#define TIGHTDB_TEMPEX2(fun, targ, wid, arg) \
    if (wid == 16) {fun<targ, 16> arg;} \
    else if (wid == 32) {fun<targ, 32> arg;} \
    else if (wid == 0) {fun<targ, 0> arg;} \
    else if (wid == 1) {fun<targ, 1> arg;} \
    else if (wid == 2) {fun<targ, 2> arg;} \
    else if (wid == 4) {fun<targ, 4> arg;} \
    else if (wid == 8) {fun<targ, 8> arg;} \
    else if (wid == 64) {fun<targ, 64> arg;} \
    else {TIGHTDB_ASSERT(false); fun<targ, 0> arg;}

#define TIGHTDB_TEMPEX3(fun, targ1, targ2, wid, arg) \
    if (wid == 16) {fun<targ1, targ2, 16> arg;} \
    else if (wid == 32) {fun<targ1, targ2, 32> arg;} \
    else if (wid == 0) {fun<targ1, targ2, 0> arg;} \
    else if (wid == 1) {fun<targ1, targ2, 1> arg;} \
    else if (wid == 2) {fun<targ1, targ2, 2> arg;} \
    else if (wid == 4) {fun<targ1, targ2, 4> arg;} \
    else if (wid == 8) {fun<targ1, targ2, 8> arg;} \
    else if (wid == 64) {fun<targ1, targ2, 64> arg;} \
    else {TIGHTDB_ASSERT(false); fun<targ1, targ2, 0> arg;}

#define TIGHTDB_TEMPEX4(fun, targ1, targ2, wid, targ3, arg) \
    if (wid == 16) {fun<targ1, targ2, 16, targ3> arg;} \
    else if (wid == 32) {fun<targ1, targ2, 32, targ3> arg;} \
    else if (wid == 0) {fun<targ1, targ2, 0, targ3> arg;} \
    else if (wid == 1) {fun<targ1, targ2, 1, targ3> arg;} \
    else if (wid == 2) {fun<targ1, targ2, 2, targ3> arg;} \
    else if (wid == 4) {fun<targ1, targ2, 4, targ3> arg;} \
    else if (wid == 8) {fun<targ1, targ2, 8, targ3> arg;} \
    else if (wid == 64) {fun<targ1, targ2, 64, targ3> arg;} \
    else {TIGHTDB_ASSERT(false); fun<targ1, targ2, 0, targ3> arg;}

#define TIGHTDB_TEMPEX5(fun, targ1, targ2, targ3, targ4, wid, arg) \
    if (wid == 16) {fun<targ1, targ2, targ3, targ4, 16> arg;} \
    else if (wid == 32) {fun<targ1, targ2, targ3, targ4, 32> arg;} \
    else if (wid == 0) {fun<targ1, targ2, targ3, targ4, 0> arg;} \
    else if (wid == 1) {fun<targ1, targ2, targ3, targ4, 1> arg;} \
    else if (wid == 2) {fun<targ1, targ2, targ3, targ4, 2> arg;} \
    else if (wid == 4) {fun<targ1, targ2, targ3, targ4, 4> arg;} \
    else if (wid == 8) {fun<targ1, targ2, targ3, targ4, 8> arg;} \
    else if (wid == 64) {fun<targ1, targ2, targ3, targ4, 64> arg;} \
    else {TIGHTDB_ASSERT(false); fun<targ1, targ2, targ3, targ4, 0> arg;}


// Pre-definitions
class Array;
class AdaptiveStringColumn;
class GroupWriter;
class Column;
template<class T> class QueryState;


#ifdef TIGHTDB_DEBUG
class MemStats {
public:
    MemStats() : allocated(0), used(0), array_count(0) {}
    MemStats(std::size_t allocated, std::size_t used, std::size_t array_count):
        allocated(allocated), used(used), array_count(array_count) {}
    MemStats(const MemStats& m)
    {
        allocated = m.allocated;
        used = m.used;
        array_count = m.array_count;
    }
    void add(const MemStats& m)
    {
        allocated += m.allocated;
        used += m.used;
        array_count += m.array_count;
    }
    std::size_t allocated;
    std::size_t used;
    std::size_t array_count;
};
#endif


class ArrayParent
{
public:
    virtual ~ArrayParent() {}

    // FIXME: Must be protected. Solve problem by having the Array constructor, that creates a new array, call it.
    virtual void update_child_ref(std::size_t child_ndx, ref_type new_ref) = 0;

protected:
    friend class Array;

    virtual ref_type get_child_ref(std::size_t child_ndx) const TIGHTDB_NOEXCEPT = 0;
};


/// Provides access to individual array nodes of the database.
///
/// This class serves purely as an accessor, it assumes no ownership
/// of the referenced memory.
///
/// An array accessor can be in one of two states: attached or
/// unattached. It is in the attached state if, and only if
/// is_attached() returns true. Most non-static member functions of
/// this class have undefined behaviour if the accessor is in the
/// unattached state. The exceptions are: is_attached(), detach(),
/// create(), init_from_ref(), init_from_mem(), has_parent(),
/// get_parent(), set_parent(), get_ndx_in_parent(),
/// adjust_ndx_in_parent().
///
/// An array accessor contains information about the parent of the
/// referenced array node. This 'reverse' reference is not explicitely
/// present in the underlying node hierarchy, but it is needed when
/// modifying an array. A modification may lead to relocation of the
/// undeerlying array node, and the parent must be updated
/// accordingly. Since this applies recursivly all the way to the root
/// node, it is essential that the entire chain of parent accessors is
/// constructed and propperly maintained when a particular array is
/// modified.
///
/// The parent reference (`pointer to parent`, `index in parent`) is
/// updated independently from the state of attachment to an
/// underlying node. In particular, the parent reference remains valid
/// and is unannfected by changes in attachment. These two aspects of
/// the state of the accessor is updated independently, and it is
/// entirely the responsibility of the caller to update them such that
/// they are consistent with the underlying node hierarchy before
/// calling any method that modifies the underlying array node.
///
/// FIXME: This class currently has a copy constructor, but it does
/// not provide valid copy semantics in any sense. It violates
/// constness by detaching the original. It attempts to do what a
/// moving constructor could do in C++11, but as it currently is, it
/// is completely defunct. There are three options for a fix of which
/// the third is most attractive but hardest to implement: (1) Keep
/// the copy constructor but stop detaching the original. For this to
/// work, it is important that the constness of the accessor has
/// nothing to do with the constness of the underlying memory,
/// otherwise constness can be violated simply by copying the
/// accessor. (2) Disallov copying but associate the constness of the
/// accessor with the constness of the underlying memory. (3) Provide
/// full ownership semantics like is done for Table accessors, and
/// provide a proper copy constructor that really produces a copy of
/// the array. For this to work, the class should assume ownership if,
/// and only if there is no parent. A copy produced by a copy
/// constructor will not have a parent. Even if the original was part
/// of a database, the copy will be free-standing, that is, not be
/// part of any database. For intra, or inter database copying, one
/// would have to also specify the target allocator.
class Array: public ArrayParent {
public:

//    void state_init(int action, QueryState *state);
//    bool match(int action, std::size_t index, int64_t value, QueryState *state);

    enum Type {
        type_Normal,
        type_InnerColumnNode, ///< Inner node of B+-tree
        type_HasRefs
    };

    /// Create a new array, and if \a parent and \a ndx_in_parent are
    /// specified, update the parent to point to this new array.
    ///
    /// Note that if no parent is specified, the caller assumes
    /// ownership of the allocated underlying node. It is not owned by
    /// the accessor.
    ///
    /// FIXME: If the Array class is to continue to function as an
    /// accessor class and have no ownership of the underlying memory,
    /// then this constructor must be removed. The problem is that
    /// memory will be leaked when it is used to construct members of
    /// a bigger class (such as Group) and something fails before the
    /// constructor of the bigger class completes. Roughly speaking, a
    /// resource must be allocated in the constructor when, and only
    /// when it is released in the destructor (RAII). Anything else
    /// constitutes a "disaster waiting to happen".
    explicit Array(Type type = type_Normal, ArrayParent* = 0, std::size_t ndx_in_parent = 0,
                   Allocator& = Allocator::get_default());

    /// Initialize an array wrapper from the specified memory
    /// reference.
    Array(MemRef, ArrayParent*, std::size_t ndx_in_parent, Allocator&) TIGHTDB_NOEXCEPT;

    /// Initialize an array wrapper from the specified memory
    /// reference. Note that the version taking a MemRef argument is
    /// slightly faster, because it does not need to map the 'ref' to
    /// a memory pointer.
    explicit Array(ref_type, ArrayParent* = 0, std::size_t ndx_in_parent = 0,
                   Allocator& = Allocator::get_default()) TIGHTDB_NOEXCEPT;

    /// Create an array in the unattached state.
    explicit Array(Allocator&) TIGHTDB_NOEXCEPT;

    /// FIXME: This is an attempt at a moving constructor - but it violates constness in an unfortunate way.
    Array(const Array&) TIGHTDB_NOEXCEPT;

    /// Create a new array as a copy of the specified array using the
    /// specified allocator.
    Array(const Array&, Allocator&);

    // Fastest way to instantiate an array, if you just want to utilize its methods
    struct no_prealloc_tag {};
    explicit Array(no_prealloc_tag) TIGHTDB_NOEXCEPT;

    virtual ~Array() TIGHTDB_NOEXCEPT {}

    /// Create a new empty array of the specified type and attach to
    /// it. This does not modify the parent reference information.
    ///
    /// Note that the caller assumes ownership of the allocated
    /// underlying node. It is not owned by the accessor.
    void create(Type);

    /// Reinitialize this array accessor to point to the specified new
    /// underlying array. This does not modify the parent reference
    /// information.
    void init_from_ref(ref_type) TIGHTDB_NOEXCEPT;

    /// Same as init_from_ref(ref_type) but avoid the mapping of 'ref'
    /// to memory pointer.
    void init_from_mem(MemRef) TIGHTDB_NOEXCEPT;

    /// Update the parents reference to this child. The requires, of
    /// course, that the parent information stored in this child is up
    /// to date. If the parent pointer is set to null, this function
    /// does nothing.
    void update_parent();

    /// When there is a chance that this array node has been modified
    /// and possibly relocated, this function will update the accessor
    /// such that it becomes valid again. Of course, this is allowed
    /// only when the parent continues to exist and it continues to
    /// have some child at the same index as the child that this
    /// accessosr was originally attached to. Even then, one must be
    /// carefull, because the new child at that index, may be a
    /// completely different one in a logical sense.
    ///
    /// FIXME: What is the point of this one? When can it ever be used safely?
    bool update_from_parent() TIGHTDB_NOEXCEPT;

    /// Change the type of an already attached array node.
    ///
    /// The effect of calling this function on an unattached accessor
    /// is undefined.
    void set_type(Type);

    /// Construct a complete copy of this array (including its
    /// subarrays) using the specified allocator and return just the
    /// ref to the new array.
    ref_type clone(Allocator&) const;

    void move_assign(Array&); // Move semantics for assignment

    /// Construct an empty array of the specified type and return just
    /// the reference to the underlying memory.
    static ref_type create_empty_array(Type, Allocator&);

    // Parent tracking
    bool has_parent() const TIGHTDB_NOEXCEPT { return m_parent != 0; }
    ArrayParent* get_parent() const TIGHTDB_NOEXCEPT { return m_parent; }

    /// Setting a new parent affects ownership of the attached array
    /// node, if any. If a non-null parent is specified, and there was
    /// no parent originally, then the caller passes ownership to the
    /// parent, and vice versa. This assumes, of course, that the
    /// change in parentship reflects a corresponding change in the
    /// list of children in the affected parents.
    void set_parent(ArrayParent* parent, std::size_t ndx_in_parent) TIGHTDB_NOEXCEPT;

    std::size_t get_ndx_in_parent() const TIGHTDB_NOEXCEPT { return m_ndx_in_parent; }
    void adjust_ndx_in_parent(int diff) TIGHTDB_NOEXCEPT { m_ndx_in_parent += diff; }

    bool is_attached() const TIGHTDB_NOEXCEPT { return m_data != 0; }

    /// Detach from the underlying array node. This method has no
    /// effect if the accessor is currently unattached (idempotency).
    void detach() TIGHTDB_NOEXCEPT { m_data = 0; }

    std::size_t size() const TIGHTDB_NOEXCEPT { return m_size; }
    bool is_empty() const TIGHTDB_NOEXCEPT { return m_size == 0; }

    void insert(std::size_t ndx, int64_t value);
    void add(int64_t value);
    void set(std::size_t ndx, int64_t value);
    template<std::size_t w> void Set(std::size_t ndx, int64_t value);

    int64_t get(std::size_t ndx) const TIGHTDB_NOEXCEPT;
    template<std::size_t w> int64_t Get(std::size_t ndx) const TIGHTDB_NOEXCEPT;

    ref_type get_as_ref(std::size_t ndx) const TIGHTDB_NOEXCEPT;

    int64_t operator[](std::size_t ndx) const TIGHTDB_NOEXCEPT { return get(ndx); }
    int64_t back() const TIGHTDB_NOEXCEPT;
    void erase(std::size_t ndx);
    void clear();

    /// If neccessary, expand the representation so that it can store
    /// the specified value.
    void ensure_minimum_width(int64_t value);

    // Direct access methods
    const Array* GetBlock(std::size_t ndx, Array& arr, std::size_t& off,
                          bool use_retval = false) const TIGHTDB_NOEXCEPT; // FIXME: Constness is not propagated to the sub-array
    std::size_t ColumnFind(int64_t target, ref_type ref, Array& cache) const;

    typedef StringData (*StringGetter)(void*, std::size_t); // Pre-declare getter function from string index
    size_t IndexStringFindFirst(StringData value, void* column, StringGetter get_func) const;
    void   IndexStringFindAll(Array& result, StringData value, void* column, StringGetter get_func) const;
    size_t IndexStringCount(StringData value, void* column, StringGetter get_func) const;
    FindRes IndexStringFindAllNoCopy(StringData value, size_t& res_ref, void* column, StringGetter get_func) const;

    void SetAllToZero();
    void Increment(int64_t value, std::size_t start=0, std::size_t end=std::size_t(-1));
    void IncrementIf(int64_t limit, int64_t value);
    void adjust(std::size_t start, int64_t diff);

    //@{
    /// Find the lower/upper bound of the specified value in a
    /// sequence of integers which must already be sorted ascendingly.
    ///
    /// For an integer value '`v`', lower_bound_int(v) returns the
    /// index '`l`' of the first element such that `get(l) &ge; v`,
    /// and upper_bound_int(v) returns the index '`u`' of the first
    /// element such that `get(u) &gt; v`. In both cases, if no such
    /// element is found, the returned value is the number of elements
    /// in the array.
    ///
    ///     3 3 3 4 4 4 5 6 7 9 9 9
    ///     ^     ^     ^     ^     ^
    ///     |     |     |     |     |
    ///     |     |     |     |      -- Lower and upper bound of 15
    ///     |     |     |     |
    ///     |     |     |      -- Lower and upper bound of 8
    ///     |     |     |
    ///     |     |      -- Upper bound of 4
    ///     |     |
    ///     |      -- Lower bound of 4
    ///     |
    ///      -- Lower and upper bound of 1
    ///
    /// These functions are similar to std::lower_bound() and
    /// std::upper_bound().
    ///
    /// We currently use binary search. See for example
    /// http://www.tbray.org/ongoing/When/200x/2003/03/22/Binary.
    ///
    /// FIXME: It may be worth considering if overall efficiency can
    /// be improved by doing a linear search for short sequences.
    std::size_t lower_bound_int(int64_t value) const TIGHTDB_NOEXCEPT;
    std::size_t upper_bound_int(int64_t value) const TIGHTDB_NOEXCEPT;
    //@}

    std::size_t FindGTE(int64_t target, std::size_t start) const;
    void Preset(int64_t min, int64_t max, std::size_t count);
    void Preset(std::size_t bitwidth, std::size_t count);

    int64_t sum(std::size_t start = 0, std::size_t end = std::size_t(-1)) const;
    std::size_t count(int64_t value) const;
    bool maximum(int64_t& result, std::size_t start = 0, std::size_t end = std::size_t(-1)) const;
    bool minimum(int64_t& result, std::size_t start = 0, std::size_t end = std::size_t(-1)) const;
    void sort();
    void ReferenceSort(Array& ref);

    // FIXME: Carefull with this one. It handles only shortening
    // operations. Either rename to truncate() or implement expanding
    // case.
    void resize(std::size_t count);

    /// Returns true if type is not type_InnerColumnNode
    bool is_leaf() const TIGHTDB_NOEXCEPT { return !m_isNode; }

    /// Returns true if type is either type_HasRefs or type_InnerColumnNode
    bool has_refs() const TIGHTDB_NOEXCEPT { return m_hasRefs; }

    bool is_index_node() const  TIGHTDB_NOEXCEPT { return get_indexflag_from_header(); }
    void set_is_index_node(bool value) { set_header_indexflag(value); }

    // FIXME: Constness is not propagated to the sub-array. This
    // constitutes a real problem, because modifying the returned
    // array may cause the parent to be modified too.
    Array GetSubArray(std::size_t ndx) const TIGHTDB_NOEXCEPT;

    ref_type get_ref() const TIGHTDB_NOEXCEPT { return m_ref; }
    void destroy();
    static void destroy(ref_type, Allocator&);

    Allocator& get_alloc() const TIGHTDB_NOEXCEPT { return m_alloc; }

    // Serialization

    /// Returns the position in the target where the first byte of
    /// this array was written.
    template<class S> std::size_t write(S& target, bool recurse = true, bool persist = false) const;

    template<class S> void write_at(std::size_t pos, S& out) const;
    std::size_t GetByteSize(bool align = false) const;
    std::vector<int64_t> ToVector() const;

    /// Compare two arrays for equality.
    bool compare_int(const Array&) const;

    // Main finding function - used for find_first, find_all, sum, max, min, etc.
    bool find(int cond, Action action, int64_t value, size_t start, size_t end, size_t baseindex,
              QueryState<int64_t>* state) const;

    template<class cond, Action action, size_t bitwidth, class Callback>
    bool find(int64_t value, size_t start, size_t end, size_t baseindex, QueryState<int64_t>* state,
              Callback callback) const;

    template<class cond, Action action, size_t bitwidth>
    bool find(int64_t value, size_t start, size_t end, size_t baseindex, QueryState<int64_t>* state) const;

    template<class cond, Action action, class Callback>
    bool find(int64_t value, size_t start, size_t end, size_t baseindex, QueryState<int64_t>* state,
              Callback callback) const;

    // Optimized implementation for release mode
    template<class cond2, Action action, size_t bitwidth, class Callback>
    bool find_optimized(int64_t value, size_t start, size_t end, size_t baseindex, QueryState<int64_t>* state,
                        Callback callback) const;

    // Called for each search result
    template<Action action, class Callback>
    bool find_action(size_t index, int64_t value, QueryState<int64_t>* state, Callback callback) const;

    template<Action action, class Callback>
    bool find_action_pattern(size_t index, uint64_t pattern, QueryState<int64_t>* state,
                             Callback callback) const;

    // Wrappers for backwards compatibility and for simple use without setting up state initialization etc
    template<class cond> std::size_t find_first(int64_t value, std::size_t start = 0,
                                                std::size_t end = std::size_t(-1)) const;
    void find_all(Array& result, int64_t value, std::size_t col_offset = 0, std::size_t start = 0,
                  std::size_t end = std::size_t(-1)) const;
    std::size_t find_first(int64_t value, std::size_t start = 0,
                           std::size_t end = size_t(-1)) const;

    // Non-SSE find for the four functions Equal/NotEqual/Less/Greater
    template<class cond2, Action action, size_t bitwidth, class Callback>
    bool Compare(int64_t value, size_t start, size_t end, size_t baseindex, QueryState<int64_t>* state,
                 Callback callback) const;

    // Non-SSE find for Equal/NotEqual
    template<bool eq, Action action, size_t width, class Callback>
    inline bool CompareEquality(int64_t value, size_t start, size_t end, size_t baseindex,
                                QueryState<int64_t>* state, Callback callback) const;

    // Non-SSE find for Less/Greater
    template<bool gt, Action action, size_t bitwidth, class Callback>
    bool CompareRelation(int64_t value, size_t start, size_t end, size_t baseindex, QueryState<int64_t>* state,
                         Callback callback) const;

    template<class cond, Action action, size_t foreign_width, class Callback, size_t width>
    bool CompareLeafs4(const Array* foreign, size_t start, size_t end, size_t baseindex, QueryState<int64_t>* state,
                       Callback callback) const;

    template<class cond, Action action, class Callback, size_t bitwidth, size_t foreign_bitwidth>
    bool CompareLeafs(const Array* foreign, size_t start, size_t end, size_t baseindex, QueryState<int64_t>* state,
                      Callback callback) const;

    template<class cond, Action action, class Callback>
    bool CompareLeafs(const Array* foreign, size_t start, size_t end, size_t baseindex, QueryState<int64_t>* state,
                      Callback callback) const;

    template<class cond, Action action, size_t width, class Callback>
    bool CompareLeafs(const Array* foreign, size_t start, size_t end, size_t baseindex, QueryState<int64_t>* state,
                      Callback callback) const;

    // SSE find for the four functions Equal/NotEqual/Less/Greater
#ifdef TIGHTDB_COMPILER_SSE
    template<class cond2, Action action, size_t width, class Callback>
    bool FindSSE(int64_t value, __m128i *data, size_t items, QueryState<int64_t>* state, size_t baseindex,
                 Callback callback) const;

    template<class cond2, Action action, size_t width, class Callback>
    TIGHTDB_FORCEINLINE bool FindSSE_intern(__m128i* action_data, __m128i* data, size_t items,
                                            QueryState<int64_t>* state, size_t baseindex, Callback callback) const;

#endif

    template<size_t width> inline bool TestZero(uint64_t value) const;         // Tests value for 0-elements
    template<bool eq, size_t width>size_t FindZero(uint64_t v) const;          // Finds position of 0/non-zero element
    template<size_t width> uint64_t cascade(uint64_t a) const;                 // Sets uppermost bits of non-zero elements
    template<bool gt, size_t width>int64_t FindGTLT_Magic(int64_t v) const;    // Compute magic constant needed for searching for value 'v' using bit hacks
    template<size_t width> inline int64_t LowerBits() const;                   // Return chunk with lower bit set in each element
    std::size_t FirstSetBit(unsigned int v) const;
    std::size_t FirstSetBit64(int64_t v) const;
    template<std::size_t w> int64_t GetUniversal(const char* const data, const std::size_t ndx) const;

    // Find value greater/less in 64-bit chunk - only works for positive values
    template<bool gt, Action action, std::size_t width, class Callback>
    bool FindGTLT_Fast(uint64_t chunk, uint64_t magic, QueryState<int64_t>* state, std::size_t baseindex,
                       Callback callback) const;

    // Find value greater/less in 64-bit chunk - no constraints
    template<bool gt, Action action, std::size_t width, class Callback>
    bool FindGTLT(int64_t v, uint64_t chunk, QueryState<int64_t>* state, std::size_t baseindex,
                  Callback callback) const;


    /// Find the leaf node corresponding to the specified tree-level
    /// index. This array instance must be the root node of a B+-tree,
    /// and that root node must not itself be a leaf. This implies, of
    /// course, that the tree must not be empty.
    ///
    /// Despite the fact that the returned MemRef object appear to
    /// allow modification of the referenced memory, the caller must
    /// consider the memory reference as being const-qualified.
    ///
    /// \return (leaf_header, leaf_ndx) where 'leaf_header' points to
    /// the the header of the located leaf, and 'leaf_ndx' is the
    /// local index within that leaf corresponding to the specified
    /// tree-level index.
    std::pair<MemRef, std::size_t> find_btree_leaf(std::size_t elem_ndx) const TIGHTDB_NOEXCEPT;

    struct TreeInsertBase {
        std::size_t m_split_offset;
        std::size_t m_split_size;
    };

    template<class TreeTraits> struct TreeInsert: TreeInsertBase {
        typename TreeTraits::value_type m_value;
    };

    /// Insert a value into a B+-tree at the specified tree-level
    /// index. This array instance must be the root node of a B+-tree,
    /// and that root node must not itself be a leaf. This implies, of
    /// course, that the tree must not be empty.
    template<class TreeTraits>
    ref_type btree_insert(std::size_t elem_ndx, TreeInsert<TreeTraits>& state);

    ref_type btree_leaf_insert(std::size_t ndx, int64_t, TreeInsertBase& state);


    /// Get the specified element without the cost of constructing an
    /// array instance. If an array instance is already available, or
    /// you need to get multiple values, then this method will be
    /// slower.
    static int64_t get(const char* header, std::size_t ndx) TIGHTDB_NOEXCEPT;

    /// The meaning of 'width' depends on the context in which this
    /// array is used.
    std::size_t get_width() const TIGHTDB_NOEXCEPT { return m_width; }

    // FIXME: Should not be mutable
    // FIXME: Should not be public
    mutable char* m_data; // Points to first byte after header

    static bool is_index_node(ref_type, const Allocator&);

    static char* get_data_from_header(char*) TIGHTDB_NOEXCEPT;
    static char* get_header_from_data(char*) TIGHTDB_NOEXCEPT;
    static const char* get_data_from_header(const char*) TIGHTDB_NOEXCEPT;

    enum WidthType {
        wtype_Bits     = 0,
        wtype_Multiply = 1,
        wtype_Ignore   = 2
    };

    static bool get_isleaf_from_header(const char*) TIGHTDB_NOEXCEPT;
    static bool get_hasrefs_from_header(const char*) TIGHTDB_NOEXCEPT;
    static bool get_indexflag_from_header(const char*) TIGHTDB_NOEXCEPT;
    static WidthType get_wtype_from_header(const char*) TIGHTDB_NOEXCEPT;
    static int get_width_from_header(const char*) TIGHTDB_NOEXCEPT;
    static std::size_t get_size_from_header(const char*) TIGHTDB_NOEXCEPT;
    static std::size_t get_capacity_from_header(const char*) TIGHTDB_NOEXCEPT;

    static Type get_type_from_header(const char*) TIGHTDB_NOEXCEPT;

    /// Get the number of bytes currently in use by the specified
    /// array. This includes the array header, but it does not include
    /// allocated bytes corresponding to excess capacity. The result
    /// is guaranteed to be a multiple of 8 (i.e., 64-bit aligned).
    static std::size_t get_byte_size_from_header(const char*) TIGHTDB_NOEXCEPT;

#ifdef TIGHTDB_DEBUG
    void print() const;
    void Verify() const;
    void to_dot(std::ostream&, StringData title = StringData()) const;
    void stats(MemStats& stats) const;
#endif

protected:
    static const int header_size = 8; // Number of bytes used by header

private:
    typedef bool (*CallbackDummy)(int64_t);

    template<size_t w> bool MinMax(size_t from, size_t to, uint64_t maxdiff,
                                   int64_t* min, int64_t* max);
    Array& operator=(const Array&) {return *this;} // not allowed
    template<size_t w> void QuickSort(size_t lo, size_t hi);
    void QuickSort(size_t lo, size_t hi);
    void ReferenceQuickSort(Array& ref);
    template<size_t w> void ReferenceQuickSort(size_t lo, size_t hi, Array& ref);

    template<size_t w> void sort();
    template<size_t w> void ReferenceSort(Array& ref);

    template<size_t w> int64_t sum(size_t start, size_t end) const;

    /// Insert new child after original. If the parent has to be
    /// split, this function returns the \c ref to the new parent
    /// node.
    static ref_type insert_btree_child(Array& offsets, Array& refs, std::size_t orig_child_ndx,
                                       ref_type new_sibling_ref, TreeInsertBase& state);

protected:
//    void AddPositiveLocal(int64_t value);

    void CreateFromHeaderDirect(char* header, ref_type = 0) TIGHTDB_NOEXCEPT;

    virtual std::size_t CalcByteLen(std::size_t count, std::size_t width) const;
    virtual std::size_t CalcItemCount(std::size_t bytes, std::size_t width) const TIGHTDB_NOEXCEPT;
    virtual WidthType GetWidthType() const { return wtype_Bits; }

    bool get_isleaf_from_header() const TIGHTDB_NOEXCEPT;
    bool get_hasrefs_from_header() const TIGHTDB_NOEXCEPT;
    bool get_indexflag_from_header() const TIGHTDB_NOEXCEPT;
    WidthType get_wtype_from_header() const TIGHTDB_NOEXCEPT;
    int get_width_from_header() const TIGHTDB_NOEXCEPT;
    std::size_t get_size_from_header() const TIGHTDB_NOEXCEPT;
    std::size_t get_capacity_from_header() const TIGHTDB_NOEXCEPT;

    void set_header_isleaf(bool value) TIGHTDB_NOEXCEPT;
    void set_header_hasrefs(bool value) TIGHTDB_NOEXCEPT;
    void set_header_indexflag(bool value) TIGHTDB_NOEXCEPT;
    void set_header_wtype(WidthType value) TIGHTDB_NOEXCEPT;
    void set_header_width(int value) TIGHTDB_NOEXCEPT;
    void set_header_size(std::size_t value) TIGHTDB_NOEXCEPT;
    void set_header_capacity(std::size_t value) TIGHTDB_NOEXCEPT;

    static void set_header_isleaf(bool value, char* header) TIGHTDB_NOEXCEPT;
    static void set_header_hasrefs(bool value, char* header) TIGHTDB_NOEXCEPT;
    static void set_header_indexflag(bool value, char* header) TIGHTDB_NOEXCEPT;
    static void set_header_wtype(WidthType value, char* header) TIGHTDB_NOEXCEPT;
    static void set_header_width(int value, char* header) TIGHTDB_NOEXCEPT;
    static void set_header_size(std::size_t value, char* header) TIGHTDB_NOEXCEPT;
    static void set_header_capacity(std::size_t value, char* header) TIGHTDB_NOEXCEPT;

    static void init_header(char* header, bool is_leaf, bool has_refs, WidthType width_type,
                            int width, std::size_t size, std::size_t capacity) TIGHTDB_NOEXCEPT;

    template<std::size_t width> void set_width() TIGHTDB_NOEXCEPT;
    void set_width(std::size_t) TIGHTDB_NOEXCEPT;
    void alloc(std::size_t count, std::size_t width);
    void copy_on_write();

    static std::pair<std::size_t, std::size_t> get_size_pair(const char* header,
                                                             std::size_t ndx) TIGHTDB_NOEXCEPT;

private:
    std::size_t m_ref;
    template<bool max, std::size_t w> bool minmax(int64_t& result, std::size_t start,
                                                  std::size_t end) const;

protected:
    std::size_t m_size;     // Number of elements currently stored.
    std::size_t m_capacity; // Number of elements that fit inside the allocated memory.
// FIXME: m_width Should be an 'int'
    std::size_t m_width;    // Size of an element (meaning depend on type of array).
    bool m_isNode;          // This array is an inner node of B+-tree.
    bool m_hasRefs;         // Elements whose first bit is zero are refs to subarrays.

private:
    ArrayParent* m_parent;
    std::size_t m_ndx_in_parent; // Ignored if m_parent is null.

    Allocator& m_alloc;

protected:
    static const std::size_t initial_capacity = 128;
    static ref_type create_empty_array(Type, WidthType, Allocator&);
    static ref_type clone(const char* header, Allocator& alloc, Allocator& clone_alloc);

    void update_child_ref(std::size_t child_ndx, ref_type new_ref) TIGHTDB_OVERRIDE;
    ref_type get_child_ref(std::size_t child_ndx) const TIGHTDB_NOEXCEPT TIGHTDB_OVERRIDE;

// FIXME: below should be moved to a specific IntegerArray class
protected:
    // Getters and Setters for adaptive-packed arrays
    typedef int64_t (Array::*Getter)(std::size_t) const; // Note: getters must not throw
    typedef void (Array::*Setter)(std::size_t, int64_t);
    typedef bool (Array::*Finder)(int64_t, std::size_t, std::size_t, std::size_t,
                                  QueryState<int64_t>*) const;

    Getter m_getter;
    Setter m_setter;
    Finder m_finder[cond_Count]; // one for each COND_XXX enum

    int64_t m_lbound;       // min number that can be stored with current m_width
    int64_t m_ubound;       // max number that can be stored with current m_width

    friend class GroupWriter;
    friend class AdaptiveStringColumn;
};





// Implementation:

class QueryStateBase { virtual void dyncast(){} };

template<> class QueryState<int64_t>: public QueryStateBase {
public:
    int64_t m_state;
    size_t m_match_count;
    size_t m_limit;

    template<Action action> bool uses_val()
    {
        if (action == act_Max || action == act_Min || action == act_Sum)
            return true;
        else
            return false;
    }

    void init(Action action, Array* akku, size_t limit)
    {
        m_match_count = 0;
        m_limit = limit;

        if (action == act_Max)
            m_state = -0x7fffffffffffffffLL - 1LL;
        else if (action == act_Min)
            m_state = 0x7fffffffffffffffLL;
        else if (action == act_ReturnFirst)
            m_state = not_found;
        else if (action == act_Sum)
            m_state = 0;
        else if (action == act_Count)
            m_state = 0;
        else if (action == act_FindAll)
            m_state = reinterpret_cast<int64_t>(akku);
        else
            TIGHTDB_ASSERT(false);
    }

    template <Action action, bool pattern, class Callback>
    inline bool match(size_t index, uint64_t indexpattern, int64_t value, Callback callback)
    {
        if (pattern) {
            if (action == act_Count) {
                // If we are close to 'limit' argument in query, we cannot count-up a complete chunk. Count up single
                // elements instead
                if(m_match_count + 64 >= m_limit)
                    return false;

                m_state += fast_popcount64(indexpattern);
                m_match_count = size_t(m_state);
                return true;
            }
            // Other aggregates cannot (yet) use bit pattern for anything. Make Array-finder call with pattern = false instead
            return false;
        }

        ++m_match_count;

        if (action == act_CallbackIdx)
            return callback(index);
        else if (action == act_Max) {
            if (value > m_state)
                m_state = value;
        }
        else if (action == act_Min) {
            if (value < m_state)
                m_state = value;
        }
        else if (action == act_Sum)
            m_state += value;
        else if (action == act_Count) {
            m_state++;
            m_match_count = size_t(m_state);
        }
        else if (action == act_FindAll)
            (reinterpret_cast<Array*>(m_state))->add(index);
        else if (action == act_ReturnFirst) {
            m_state = index;
            return false;
        }
        else
            TIGHTDB_ASSERT(false);

        return (m_limit > m_match_count);
    }
};

// Used only for Basic-types: currently float and double
template<class R> class QueryState : public QueryStateBase {
public:
    R m_state;
    size_t m_match_count;
    size_t m_limit;

    template<Action action> bool uses_val()
    {
        return (action == act_Max || action == act_Min || action == act_Sum);
    }

    void init(Action action, Array*, size_t limit)
    {
        TIGHTDB_STATIC_ASSERT((SameType<R, float>::value || SameType<R, double>::value), "");
        m_match_count = 0;
        m_limit = limit;

        if (action == act_Max)
            m_state = -std::numeric_limits<R>::infinity();
        else if (action == act_Min)
            m_state = std::numeric_limits<R>::infinity();
        else if (action == act_Sum)
            m_state = 0.0;
        else
            TIGHTDB_ASSERT(false);
    }

    template<Action action, bool pattern, class Callback, typename resulttype>
    inline bool match(size_t /*index*/, uint64_t /*indexpattern*/, resulttype value, Callback /*callback*/)
    {
        if (pattern)
            return false;

        TIGHTDB_STATIC_ASSERT(action == act_Sum || action == act_Max || action == act_Min, "");
        ++m_match_count;

        if (action == act_Max) {
            if (value > m_state)
                m_state = value;
        }
        else if (action == act_Min) {
            if (value < m_state)
                m_state = value;
        }
        else if (action == act_Sum)
            m_state += value;
        else
            TIGHTDB_ASSERT(false);

        return (m_limit > m_match_count);
    }
};



inline Array::Array(Type type, ArrayParent* parent, std::size_t pndx, Allocator& alloc):
    m_data(0), m_size(0), m_capacity(0), m_width(0), m_isNode(false), m_hasRefs(false),
    m_parent(parent), m_ndx_in_parent(pndx), m_alloc(alloc), m_lbound(0), m_ubound(0)
{
    create(type); // Throws
    update_parent(); // Throws
}

inline Array::Array(MemRef mem, ArrayParent* parent, std::size_t ndx_in_parent,
                    Allocator& alloc) TIGHTDB_NOEXCEPT:
    m_data(0), m_size(0), m_capacity(0), m_width(0), m_isNode(false), m_hasRefs(false),
    m_parent(parent), m_ndx_in_parent(ndx_in_parent), m_alloc(alloc), m_lbound(0), m_ubound(0)
{
    init_from_mem(mem);
}

inline Array::Array(ref_type ref, ArrayParent* parent, std::size_t pndx,
                    Allocator& alloc) TIGHTDB_NOEXCEPT:
    m_data(0), m_size(0), m_capacity(0), m_width(0), m_isNode(false), m_hasRefs(false),
    m_parent(parent), m_ndx_in_parent(pndx), m_alloc(alloc), m_lbound(0), m_ubound(0)
{
    init_from_ref(ref);
}

// Creates new unattached accessor (call create() or init_from_ref() to
// attach).
inline Array::Array(Allocator& alloc) TIGHTDB_NOEXCEPT:
    m_data(0), m_ref(0), m_size(0), m_capacity(0), m_width(std::size_t(-1)), m_isNode(false),
    m_parent(0), m_ndx_in_parent(0), m_alloc(alloc) {}

// Copy-constructor
// Note that this array now own the ref. Should only be used when
// the source array goes away right after (like return values from functions)
inline Array::Array(const Array& src) TIGHTDB_NOEXCEPT:
    ArrayParent(), m_parent(src.m_parent), m_ndx_in_parent(src.m_ndx_in_parent),
    m_alloc(src.m_alloc)
{
    ref_type ref = src.get_ref();
    init_from_ref(ref);
    const_cast<Array&>(src).detach(); // FIXME: Const-violation here!!!
}

inline Array::Array(const Array& array, Allocator& alloc):
    m_data(0), m_size(0), m_capacity(0), m_width(0), m_isNode(false), m_hasRefs(false),
    m_parent(0), m_ndx_in_parent(0), m_alloc(alloc), m_lbound(0), m_ubound(0)
{
    ref_type ref = array.clone(alloc); // Throws
    init_from_ref(ref);
}

// Fastest way to instantiate an Array. For use with GetDirect() that only fills out m_width, m_data
// and a few other basic things needed for read-only access. Or for use if you just want a way to call
// some methods written in Array.*
inline Array::Array(no_prealloc_tag) TIGHTDB_NOEXCEPT: m_alloc(*static_cast<Allocator*>(0)) {}


inline void Array::create(Type type)
{
    ref_type ref = create_empty_array(type, m_alloc); // Throws
    init_from_ref(ref);
}


inline int64_t Array::back() const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(m_size);
    return get(m_size - 1);
}

inline int64_t Array::get(std::size_t ndx) const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(ndx < m_size);
    return (this->*m_getter)(ndx);

// Two ideas that are not efficient but may be worth looking into again:
/*
    // Assume correct width is found early in TIGHTDB_TEMPEX, which is the case for B tree offsets that
    // are probably either 2^16 long. Turns out to be 25% faster if found immediately, but 50-300% slower
    // if found later
    TIGHTDB_TEMPEX(return Get, (ndx));
*/
/*
    // Slightly slower in both of the if-cases. Also needs an matchcount m_size check too, to avoid
    // reading beyond array.
    if (m_width >= 8 && m_size > ndx + 7)
        return Get<64>(ndx >> m_shift) & m_widthmask;
    else
        return (this->*m_getter)(ndx);
*/
}

inline ref_type Array::get_as_ref(std::size_t ndx) const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(m_hasRefs);
    int64_t v = get(ndx);
    return to_ref(v);
}

inline Array Array::GetSubArray(std::size_t ndx) const TIGHTDB_NOEXCEPT
{
    ref_type ref = get_as_ref(ndx);
    TIGHTDB_ASSERT(ref);

    // FIXME: Constness is not propagated to the sub-array. This
    // constitutes a real problem, because modifying the returned
    // array genrally causes the parent to be modified too.
    return Array(ref, const_cast<Array*>(this), ndx, m_alloc);
}


inline bool Array::is_index_node(ref_type ref, const Allocator& alloc)
{
    TIGHTDB_ASSERT(ref);
    return get_indexflag_from_header(alloc.translate(ref));
}


inline void Array::destroy(ref_type ref, Allocator& alloc)
{
    Array array(alloc);
    array.init_from_ref(ref);
    array.destroy();
}



//-------------------------------------------------

inline bool Array::get_isleaf_from_header(const char* header) TIGHTDB_NOEXCEPT
{
    const unsigned char* h = reinterpret_cast<const unsigned char*>(header);
    return (h[0] & 0x80) == 0;
}
inline bool Array::get_hasrefs_from_header(const char* header) TIGHTDB_NOEXCEPT
{
    const unsigned char* h = reinterpret_cast<const unsigned char*>(header);
    return (h[0] & 0x40) != 0;
}
inline bool Array::get_indexflag_from_header(const char* header) TIGHTDB_NOEXCEPT
{
    const unsigned char* h = reinterpret_cast<const unsigned char*>(header);
    return (h[0] & 0x20) != 0;
}
inline Array::WidthType Array::get_wtype_from_header(const char* header) TIGHTDB_NOEXCEPT
{
    const unsigned char* h = reinterpret_cast<const unsigned char*>(header);
    return WidthType((h[0] & 0x18) >> 3);
}
inline int Array::get_width_from_header(const char* header) TIGHTDB_NOEXCEPT
{
    const unsigned char* h = reinterpret_cast<const unsigned char*>(header);
    return (1 << (h[0] & 0x07)) >> 1;
}
inline std::size_t Array::get_size_from_header(const char* header) TIGHTDB_NOEXCEPT
{
    const unsigned char* h = reinterpret_cast<const unsigned char*>(header);
    return (std::size_t(h[1]) << 16) + (std::size_t(h[2]) << 8) + h[3];
}
inline std::size_t Array::get_capacity_from_header(const char* header) TIGHTDB_NOEXCEPT
{
    const unsigned char* h = reinterpret_cast<const unsigned char*>(header);
    return (std::size_t(h[4]) << 16) + (std::size_t(h[5]) << 8) + h[6];
}


inline char* Array::get_data_from_header(char* header) TIGHTDB_NOEXCEPT
{
    return header + header_size;
}
inline char* Array::get_header_from_data(char* data) TIGHTDB_NOEXCEPT
{
    return data - header_size;
}
inline const char* Array::get_data_from_header(const char* header) TIGHTDB_NOEXCEPT
{
    return get_data_from_header(const_cast<char*>(header));
}


inline bool Array::get_isleaf_from_header() const TIGHTDB_NOEXCEPT
{
    return get_isleaf_from_header(get_header_from_data(m_data));
}
inline bool Array::get_hasrefs_from_header() const TIGHTDB_NOEXCEPT
{
    return get_hasrefs_from_header(get_header_from_data(m_data));
}
inline bool Array::get_indexflag_from_header() const TIGHTDB_NOEXCEPT
{
    return get_indexflag_from_header(get_header_from_data(m_data));
}
inline Array::WidthType Array::get_wtype_from_header() const TIGHTDB_NOEXCEPT
{
    return get_wtype_from_header(get_header_from_data(m_data));
}
inline int Array::get_width_from_header() const TIGHTDB_NOEXCEPT
{
    return get_width_from_header(get_header_from_data(m_data));
}
inline std::size_t Array::get_size_from_header() const TIGHTDB_NOEXCEPT
{
    return get_size_from_header(get_header_from_data(m_data));
}
inline std::size_t Array::get_capacity_from_header() const TIGHTDB_NOEXCEPT
{
    return get_capacity_from_header(get_header_from_data(m_data));
}


inline void Array::set_header_isleaf(bool value, char* header) TIGHTDB_NOEXCEPT
{
    uint8_t* h = reinterpret_cast<uint8_t*>(header);
    h[0] = (h[0] & ~0x80) | uint8_t(!value << 7);
}

inline void Array::set_header_hasrefs(bool value, char* header) TIGHTDB_NOEXCEPT
{
    uint8_t* h = reinterpret_cast<uint8_t*>(header);
    h[0] = (h[0] & ~0x40) | uint8_t(value << 6);
}

inline void Array::set_header_indexflag(bool value, char* header) TIGHTDB_NOEXCEPT
{
    uint8_t* h = reinterpret_cast<uint8_t*>(header);
    h[0] = (h[0] & ~0x20) | uint8_t(value << 5);
}

inline void Array::set_header_wtype(WidthType value, char* header) TIGHTDB_NOEXCEPT
{
    // Indicates how to calculate size in bytes based on width
    // 0: bits      (width/8) * size
    // 1: multiply  width * size
    // 2: ignore    1 * size
    uint8_t* h = reinterpret_cast<uint8_t*>(header);
    h[0] = (h[0] & ~0x18) | uint8_t(value << 3);
}

inline void Array::set_header_width(int value, char* header) TIGHTDB_NOEXCEPT
{
    // Pack width in 3 bits (log2)
    int w = 0;
    while (value) { ++w; value >>= 1; }
    TIGHTDB_ASSERT(w < 8);

    uint8_t* h = reinterpret_cast<uint8_t*>(header);
    h[0] = (h[0] & ~0x7) | uint8_t(w);
}

inline void Array::set_header_size(std::size_t value, char* header) TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(value <= 0xFFFFFF);
    uint8_t* h = reinterpret_cast<uint8_t*>(header);
    h[1] = (value >> 16) & 0x000000FF;
    h[2] = (value >>  8) & 0x000000FF;
    h[3] =  value        & 0x000000FF;
}

inline void Array::set_header_capacity(std::size_t value, char* header) TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(value <= 0xFFFFFF);
    uint8_t* h = reinterpret_cast<uint8_t*>(header);
    h[4] = (value >> 16) & 0x000000FF;
    h[5] = (value >>  8) & 0x000000FF;
    h[6] =  value        & 0x000000FF;
}



inline void Array::set_header_isleaf(bool value) TIGHTDB_NOEXCEPT
{
    set_header_isleaf(value, get_header_from_data(m_data));
}
inline void Array::set_header_hasrefs(bool value) TIGHTDB_NOEXCEPT
{
    set_header_hasrefs(value, get_header_from_data(m_data));
}
inline void Array::set_header_indexflag(bool value) TIGHTDB_NOEXCEPT
{
    set_header_indexflag(value, get_header_from_data(m_data));
}
inline void Array::set_header_wtype(WidthType value) TIGHTDB_NOEXCEPT
{
    set_header_wtype(value, get_header_from_data(m_data));
}
inline void Array::set_header_width(int value) TIGHTDB_NOEXCEPT
{
    set_header_width(value, get_header_from_data(m_data));
}
inline void Array::set_header_size(std::size_t value) TIGHTDB_NOEXCEPT
{
    set_header_size(value, get_header_from_data(m_data));
}
inline void Array::set_header_capacity(std::size_t value) TIGHTDB_NOEXCEPT
{
    set_header_capacity(value, get_header_from_data(m_data));
}


inline Array::Type Array::get_type_from_header(const char* header) TIGHTDB_NOEXCEPT
{
    if (!get_isleaf_from_header(header))
        return type_InnerColumnNode;
    if (get_hasrefs_from_header(header))
        return type_HasRefs;
    return type_Normal;
}


inline std::size_t Array::get_byte_size_from_header(const char* header) TIGHTDB_NOEXCEPT
{
    // Calculate full size of array in bytes, including padding
    // for 64bit alignment (that may be composed of random bits)
    std::size_t size = get_size_from_header(header);

    // Adjust size to number of bytes
    switch (get_wtype_from_header(header)) {
        case wtype_Bits: {
            int width = get_width_from_header(header);
            std::size_t bits = (size * width);
            size = bits / 8;
            if (bits & 0x7) ++size;
            break;
        }
        case wtype_Multiply: {
            int width = get_width_from_header(header);
            size *= width;
            break;
        }
        case wtype_Ignore:
            break;
    }

    // Add bytes used for padding
    const std::size_t rest = (~size & 0x7) + 1;
    if (rest < 8) size += rest; // 64bit blocks
    size += get_data_from_header(header) - header; // include header in total

    TIGHTDB_ASSERT(size <= get_capacity_from_header(header));

    return size;
}


inline void Array::init_header(char* header, bool is_leaf, bool has_refs, WidthType width_type,
                               int width, std::size_t size, std::size_t capacity) TIGHTDB_NOEXCEPT
{
    // Note: Since the header layout contains unallocated bit and/or
    // bytes, it is important that we put the entire header into a
    // well defined state initially. Note also: The C++11 standard
    // does not guarantee that int64_t is available on all platforms.
    *reinterpret_cast<int64_t*>(header) = 0;
    set_header_isleaf(is_leaf, header);
    set_header_hasrefs(has_refs, header);
    set_header_wtype(width_type, header);
    set_header_width(width, header);
    set_header_size(size, header);
    set_header_capacity(capacity, header);
}


//-------------------------------------------------

template<class S> std::size_t Array::write(S& out, bool recurse, bool persist) const
{
    TIGHTDB_ASSERT(is_attached());

    // Ignore un-changed arrays when persisting
    if (persist && m_alloc.is_read_only(m_ref))
        return m_ref;

    if (recurse && m_hasRefs) {
        // Temp array for updated refs
        Array new_refs(m_isNode ? type_InnerColumnNode : type_HasRefs);

        // Make sure that all flags are retained
        if (is_index_node())
            new_refs.set_is_index_node(true);

        // First write out all sub-arrays
        std::size_t n = size();
        for (std::size_t i = 0; i < n; ++i) {
            int64_t ref = get(i);
            if (ref == 0 || ref & 0x1) {
                // zero-refs and refs that are not 64-aligned do not point to sub-trees
                new_refs.add(ref);
            }
            else if (persist && m_alloc.is_read_only(to_ref(ref))) {
                // Ignore un-changed arrays when persisting
                new_refs.add(ref);
            }
            else {
                Array sub(to_ref(ref), 0, 0, get_alloc());
                std::size_t sub_pos = sub.write(out, true, persist);
                TIGHTDB_ASSERT((sub_pos & 0x7) == 0); // 64bit alignment
                new_refs.add(sub_pos);
            }
        }

        // Write out the replacement array
        // (but don't write sub-tree as it has alredy been written)
        std::size_t refs_pos = new_refs.write(out, false, persist);

        // Clean-up
        new_refs.set_type(type_Normal); // avoid recursive del
        new_refs.destroy();

        return refs_pos; // Return position
    }

    // TODO: replace capacity with checksum

    // Write array
    const char* header = get_header_from_data(m_data);
    std::size_t size = get_byte_size_from_header(header);
    std::size_t array_pos = out.write(header, size);
    TIGHTDB_ASSERT((array_pos & 0x7) == 0); /// 64-bit alignment

    return array_pos;
}

template<class S> void Array::write_at(std::size_t pos, S& out) const
{
    TIGHTDB_ASSERT(is_attached());

    // TODO: replace capacity with checksum

    // Write array
    const char* header = get_header_from_data(m_data);
    std::size_t size = get_byte_size_from_header(header);
    out.write_at(pos, header, size);
}

inline ref_type Array::clone(Allocator& clone_alloc) const
{
    const char* header = get_header_from_data(m_data);
    return clone(header, m_alloc, clone_alloc); // Throws
}

inline void Array::move_assign(Array& a)
{
    // FIXME: Be carefull with the old parent info here. Should it be copied?

    // FIXME: It will likely be a lot better for the optimizer if we
    // did a member-wise copy, rather than recreating the state from
    // the referenced data. This is important because TableView efficiency, for
    // example, relies on long chains of moves to be optimized away
    // completely. This change should be a 'no-brainer'.
    destroy();
    init_from_ref(a.get_ref());
    a.detach();
}

inline ref_type Array::create_empty_array(Type type, Allocator& alloc)
{
    return create_empty_array(type, wtype_Bits, alloc); // Throws
}

inline void Array::update_parent()
{
    if (m_parent)
        m_parent->update_child_ref(m_ndx_in_parent, m_ref);
}


inline void Array::update_child_ref(size_t child_ndx, ref_type new_ref)
{
    set(child_ndx, new_ref);
}

inline ref_type Array::get_child_ref(size_t child_ndx) const TIGHTDB_NOEXCEPT
{
    return get_as_ref(child_ndx);
}


template<class TreeTraits>
ref_type Array::btree_insert(std::size_t elem_ndx, TreeInsert<TreeTraits>& state)
{
    Allocator& alloc = get_alloc();
    ref_type offsets_ref = get_as_ref(0);
    Array offsets(offsets_ref, this, 0, alloc);
    ref_type refs_ref = get_as_ref(1);
    Array refs(refs_ref, this, 1, alloc);
    TIGHTDB_ASSERT(0 < refs.size());
    TIGHTDB_ASSERT(offsets.size() == refs.size());
    TIGHTDB_ASSERT(elem_ndx == npos || elem_ndx <= to_size_t(offsets.get(refs.size()-1)));
    std::size_t child_ndx, child_elem_ndx;
    if (elem_ndx == npos) {
        // Optimization for append
        child_ndx = refs.size() - 1;
        child_elem_ndx = npos;
    }
    else if (elem_ndx == 0) {
        // Optimization for prepend
        child_ndx = 0;
        child_elem_ndx = 0;
    }
    else {
        // There is a choise to be made when the element is to be
        // inserted between two subtrees. It can either be appended to
        // the first subtree, or it can be prepended to the second
        // one. We currently always append to the first subtree. It is
        // essentially a matter of using the lower vs. the upper bound
        // when searching in in the offsets array.
        child_ndx = offsets.lower_bound_int(elem_ndx);
        TIGHTDB_ASSERT(child_ndx < refs.size());
        std::size_t elem_ndx_offset = child_ndx == 0 ? 0 : to_size_t(offsets.get(child_ndx-1));
        child_elem_ndx = elem_ndx - elem_ndx_offset;
    }
    ref_type child_ref = refs.get_as_ref(child_ndx), new_sibling_ref;
    char* child_header = static_cast<char*>(alloc.translate(child_ref));
    bool child_is_leaf = get_isleaf_from_header(child_header);
    if (child_is_leaf) {
        TIGHTDB_ASSERT(child_elem_ndx == npos || child_elem_ndx <= TIGHTDB_MAX_LIST_SIZE);
        new_sibling_ref = TreeTraits::leaf_insert(MemRef(child_header, child_ref), refs,
                                                  child_ndx, alloc, child_elem_ndx, state);
    }
    else {
        Array child(MemRef(child_header, child_ref), &refs, child_ndx, alloc);
        new_sibling_ref = child.btree_insert(child_elem_ndx, state);
    }

    if (TIGHTDB_LIKELY(!new_sibling_ref)) {
        offsets.adjust(child_ndx, 1);
        return 0;
    }

    return insert_btree_child(offsets, refs, child_ndx, new_sibling_ref, state);
}



//*************************************************************************************
// Finding code                                                                       *
//*************************************************************************************

template<std::size_t w> int64_t Array::GetUniversal(const char* data, std::size_t ndx) const
{
    if (w == 0) {
        return 0;
    }
    else if (w == 1) {
        std::size_t offset = ndx >> 3;
        return (data[offset] >> (ndx & 7)) & 0x01;
    }
    else if (w == 2) {
        std::size_t offset = ndx >> 2;
        return (data[offset] >> ((ndx & 3) << 1)) & 0x03;
    }
    else if (w == 4) {
        std::size_t offset = ndx >> 1;
        return (data[offset] >> ((ndx & 1) << 2)) & 0x0F;
    }
    else if (w == 8) {
        return *reinterpret_cast<const signed char*>(data + ndx);
    }
    else if (w == 16) {
        std::size_t offset = ndx * 2;
        return *reinterpret_cast<const int16_t*>(data + offset);
    }
    else if (w == 32) {
        std::size_t offset = ndx * 4;
        return *reinterpret_cast<const int32_t*>(data + offset);
    }
    else if (w == 64) {
        std::size_t offset = ndx * 8;
        return *reinterpret_cast<const int64_t*>(data + offset);
    }
    else {
        TIGHTDB_ASSERT(false);
        return int64_t(-1);
    }
}

/*
find() (calls find_optimized()) will call match() for each search result.

If pattern == true:
    'indexpattern' contains a 64-bit chunk of elements, each of 'width' bits in size where each element indicates a match if its lower bit is set, otherwise
    it indicates a non-match. 'index' tells the database row index of the first element. You must return true if you chose to 'consume' the chunk or false
    if not. If not, then Array-finder will afterwards call match() successive times with pattern == false.

If pattern == false:
    'index' tells the row index of a single match and 'value' tells its value. Return false to make Array-finder break its search or return true to let it continue until
    'end' or 'limit'.

Array-finder decides itself if - and when - it wants to pass you an indexpattern. It depends on array bit width, match frequency, and wether the arithemetic and
computations for the given search criteria makes it feasible to construct such a pattern.
*/

// These wrapper functions only exist to enable a possibility to make the compiler see that 'value' and/or 'index' are unused, such that caller's
// computation of these values will not be made. Only works if find_action() and find_action_pattern() rewritten as macros. Note: This problem has been fixed in
// next upcoming array.hpp version
template<Action action, class Callback>
bool Array::find_action(size_t index, int64_t value, QueryState<int64_t>* state, Callback callback) const
{
    return state->match<action, false, Callback>(index, 0, value, callback);
}
template<Action action, class Callback>
bool Array::find_action_pattern(size_t index, uint64_t pattern, QueryState<int64_t>* state, Callback callback) const
{
    return state->match<action, true, Callback>(index, pattern, 0, callback);
}


template<size_t width> uint64_t Array::cascade(uint64_t a) const
{
    // Takes a chunk of values as argument and sets the uppermost bit for each element which is 0. Example:
    // width == 4 and v = 01000000 00001000 10000001 00001000 00000000 10100100 00001100 00111110 01110100 00010000 00000000 00000001 10000000 01111110
    // will return:       00001000 00010000 00010000 00010000 00010001 00000000 00010000 00000000 00000000 00000001 00010001 00010000 00000001 00000000

    // static values needed for fast population count
    const uint64_t m1  = 0x5555555555555555ULL;

    if (width == 1) {
        return ~a;
    }
    else if (width == 2) {
        // Masks to avoid spillover between segments in cascades
        const uint64_t c1 = ~0ULL/0x3 * 0x1;

        a |= (a >> 1) & c1; // cascade ones in non-zeroed segments
        a &= m1;     // isolate single bit in each segment
        a ^= m1;     // reverse isolated bits

        return a;
    }
    else if (width == 4) {
        const uint64_t m  = ~0ULL/0xF * 0x1;

        // Masks to avoid spillover between segments in cascades
        const uint64_t c1 = ~0ULL/0xF * 0x7;
        const uint64_t c2 = ~0ULL/0xF * 0x3;

        a |= (a >> 1) & c1; // cascade ones in non-zeroed segments
        a |= (a >> 2) & c2;
        a &= m;     // isolate single bit in each segment
        a ^= m;     // reverse isolated bits

        return a;
    }
    else if (width == 8) {
        const uint64_t m  = ~0ULL/0xFF * 0x1;

        // Masks to avoid spillover between segments in cascades
        const uint64_t c1 = ~0ULL/0xFF * 0x7F;
        const uint64_t c2 = ~0ULL/0xFF * 0x3F;
        const uint64_t c3 = ~0ULL/0xFF * 0x0F;

        a |= (a >> 1) & c1; // cascade ones in non-zeroed segments
        a |= (a >> 2) & c2;
        a |= (a >> 4) & c3;
        a &= m;     // isolate single bit in each segment
        a ^= m;     // reverse isolated bits

        return a;
    }
    else if (width == 16) {
        const uint64_t m  = ~0ULL/0xFFFF * 0x1;

        // Masks to avoid spillover between segments in cascades
        const uint64_t c1 = ~0ULL/0xFFFF * 0x7FFF;
        const uint64_t c2 = ~0ULL/0xFFFF * 0x3FFF;
        const uint64_t c3 = ~0ULL/0xFFFF * 0x0FFF;
        const uint64_t c4 = ~0ULL/0xFFFF * 0x00FF;

        a |= (a >> 1) & c1; // cascade ones in non-zeroed segments
        a |= (a >> 2) & c2;
        a |= (a >> 4) & c3;
        a |= (a >> 8) & c4;
        a &= m;     // isolate single bit in each segment
        a ^= m;     // reverse isolated bits

        return a;
    }

    else if (width == 32) {
        const uint64_t m  = ~0ULL/0xFFFFFFFF * 0x1;

        // Masks to avoid spillover between segments in cascades
        const uint64_t c1 = ~0ULL/0xFFFFFFFF * 0x7FFFFFFF;
        const uint64_t c2 = ~0ULL/0xFFFFFFFF * 0x3FFFFFFF;
        const uint64_t c3 = ~0ULL/0xFFFFFFFF * 0x0FFFFFFF;
        const uint64_t c4 = ~0ULL/0xFFFFFFFF * 0x00FFFFFF;
        const uint64_t c5 = ~0ULL/0xFFFFFFFF * 0x0000FFFF;

        a |= (a >> 1) & c1; // cascade ones in non-zeroed segments
        a |= (a >> 2) & c2;
        a |= (a >> 4) & c3;
        a |= (a >> 8) & c4;
        a |= (a >> 16) & c5;
        a &= m;     // isolate single bit in each segment
        a ^= m;     // reverse isolated bits

        return a;
    }
    else if (width == 64) {
        return a == 0 ? 1 : 0;
    }
    else {
        TIGHTDB_ASSERT(false);
        return uint64_t(-1);
    }
}

// This is the main finding function for Array. Other finding functions are just wrappers around this one.
// Search for 'value' using condition cond2 (Equal, NotEqual, Less, etc) and call find_action() or find_action_pattern() for each match. Break and return if find_action() returns false or 'end' is reached.
template<class cond2, Action action, size_t bitwidth, class Callback> bool Array::find_optimized(int64_t value, size_t start, size_t end, size_t baseindex, QueryState<int64_t>* state, Callback callback) const
{
    cond2 c;
    TIGHTDB_ASSERT(start <= m_size && (end <= m_size || end == std::size_t(-1)) && start <= end);

    // Test first few items with no initial time overhead
    if (start > 0) {
        if (m_size > start && c(Get<bitwidth>(start), value) && start < end) {
            if (!find_action<action, Callback>(start + baseindex, Get<bitwidth>(start), state, callback)) return false;
        }

        ++start;

        if (m_size > start && c(Get<bitwidth>(start), value) && start < end) {
            if (!find_action<action, Callback>(start + baseindex, Get<bitwidth>(start), state, callback)) return false;
        }

        ++start;

        if (m_size > start && c(Get<bitwidth>(start), value) && start < end) {
            if (!find_action<action, Callback>(start + baseindex, Get<bitwidth>(start), state, callback)) return false;
        }

        ++start;

        if (m_size > start && c(Get<bitwidth>(start), value) && start < end) {
            if (!find_action<action, Callback>(start + baseindex, Get<bitwidth>(start), state, callback)) return false;
        }

        ++start;
    }

    if (!(m_size > start && start < end))
        return true;

    if (end == std::size_t(-1)) end = m_size;

    // Return immediately if no items in array can match (such as if cond2 == Greater and value == 100 and m_ubound == 15).
    if (!c.can_match(value, m_lbound, m_ubound))
        return true;

    // call find_action() on all items in array if all items are guaranteed to match (such as cond2 == NotEqual and
    // value == 100 and m_ubound == 15)
    if (c.will_match(value, m_lbound, m_ubound)) {
        if (action == act_Sum || action == act_Max || action == act_Min) {
            int64_t res;
            if (action == act_Sum)
                res = Array::sum(start, end);
            if (action == act_Max)
                Array::maximum(res, start, end);
            if (action == act_Min)
                Array::minimum(res, start, end);

            find_action<action, Callback>(start + baseindex, res, state, callback);
        }
        else if (action == act_Count) {
            state->m_state += end - start;
        }
        else {
            for (; start < end; start++)
                if (!find_action<action, Callback>(start + baseindex, Get<bitwidth>(start), state, callback))
                    return false;
        }
        return true;
    }

    // finder cannot handle this bitwidth
    TIGHTDB_ASSERT(m_width != 0);

#if defined(TIGHTDB_COMPILER_SSE)
    if ((cpuid_sse<42>() &&                                  (end - start >= sizeof (__m128i) && m_width >= 8))
    ||  (cpuid_sse<30>() && (SameType<cond2, Equal>::value && end - start >= sizeof (__m128i) && m_width >= 8 && m_width < 64))) {

        // FindSSE() must start at 16-byte boundary, so search area before that using CompareEquality()
        __m128i* const a = reinterpret_cast<__m128i*>(round_up(m_data + start * bitwidth / 8, sizeof (__m128i)));
        __m128i* const b = reinterpret_cast<__m128i*>(round_down(m_data + end * bitwidth / 8, sizeof (__m128i)));

        if (!Compare<cond2, action, bitwidth, Callback>(value, start, (reinterpret_cast<char*>(a) - m_data) * 8 / no0(bitwidth), baseindex, state, callback))
            return false;

        // Search aligned area with SSE
        if (b > a) {
            if (cpuid_sse<42>()) {
                if (!FindSSE<cond2, action, bitwidth, Callback>(value, a, b - a, state, baseindex + ((reinterpret_cast<char*>(a) - m_data) * 8 / no0(bitwidth)), callback))
                    return false;
                }
                else if (cpuid_sse<30>()) {

                if (!FindSSE<Equal, action, bitwidth, Callback>(value, a, b - a, state, baseindex + ((reinterpret_cast<char*>(a) - m_data) * 8 / no0(bitwidth)), callback))
                    return false;
                }
        }

        // Search remainder with CompareEquality()
        if (!Compare<cond2, action, bitwidth, Callback>(value, (reinterpret_cast<char*>(b) - m_data) * 8 / no0(bitwidth), end, baseindex, state, callback))
            return false;

        return true;
    }
    else {
        return Compare<cond2, action, bitwidth, Callback>(value, start, end, baseindex, state, callback);
    }
#else
return Compare<cond2, action, bitwidth, Callback>(value, start, end, baseindex, state, callback);
#endif
}

template<size_t width> inline int64_t Array::LowerBits() const
{
    if (width == 1)
        return 0xFFFFFFFFFFFFFFFFULL;
    else if (width == 2)
        return 0x5555555555555555ULL;
    else if (width == 4)
        return 0x1111111111111111ULL;
    else if (width == 8)
        return 0x0101010101010101ULL;
    else if (width == 16)
        return 0x0001000100010001ULL;
    else if (width == 32)
        return 0x0000000100000001ULL;
    else if (width == 64)
        return 0x0000000000000001ULL;
    else {
        TIGHTDB_ASSERT(false);
        return int64_t(-1);
    }
}

// Tests if any chunk in 'value' is 0
template<size_t width> inline bool Array::TestZero(uint64_t value) const
{
    uint64_t hasZeroByte;
    uint64_t lower = LowerBits<width>();
    uint64_t upper = LowerBits<width>() * 1ULL << (width == 0 ? 0 : (width - 1ULL));
    hasZeroByte = (value - lower) & ~value & upper;
    return hasZeroByte != 0;
}

// Finds first zero (if eq == true) or non-zero (if eq == false) element in v and returns its position.
// IMPORTANT: This function assumes that at least 1 item matches (test this with TestZero() or other means first)!
template<bool eq, size_t width>size_t Array::FindZero(uint64_t v) const
{
    size_t start = 0;
    uint64_t hasZeroByte;
    // Warning free way of computing (1ULL << width) - 1
    uint64_t mask = (width == 64 ? ~0ULL : ((1ULL << (width == 64 ? 0 : width)) - 1ULL));

    if (eq == (((v >> (width * start)) & mask) == 0)) {
        return 0;
    }

    // Bisection optimization, speeds up small bitwidths with high match frequency. More partions than 2 do NOT pay
    // off because the work done by TestZero() is wasted for the cases where the value exists in first half, but
    // useful if it exists in last half. Sweet spot turns out to be the widths and partitions below.
    if (width <= 8) {
        hasZeroByte = TestZero<width>(v | 0xffffffff00000000ULL);
        if (eq ? !hasZeroByte : (v & 0x00000000ffffffffULL) == 0) {
            // 00?? -> increasing
            start += 64 / no0(width) / 2;
            if (width <= 4) {
                hasZeroByte = TestZero<width>(v | 0xffff000000000000ULL);
                if (eq ? !hasZeroByte : (v & 0x0000ffffffffffffULL) == 0) {
                    // 000?
                    start += 64 / no0(width) / 4;
                }
            }
        }
        else {
            if (width <= 4) {
                // ??00
                hasZeroByte = TestZero<width>(v | 0xffffffffffff0000ULL);
                if (eq ? !hasZeroByte : (v & 0x000000000000ffffULL) == 0) {
                    // 0?00
                    start += 64 / no0(width) / 4;
                }
            }
        }
    }

    while (eq == (((v >> (width * start)) & mask) != 0)) {
        // You must only call FindZero() if you are sure that at least 1 item matches
        TIGHTDB_ASSERT(start <= 8 * sizeof(v));
        start++;
    }

    return start;
}

// Generate a magic constant used for later bithacks
template<bool gt, size_t width>int64_t Array::FindGTLT_Magic(int64_t v) const
{
    uint64_t mask1 = (width == 64 ? ~0ULL : ((1ULL << (width == 64 ? 0 : width)) - 1ULL)); // Warning free way of computing (1ULL << width) - 1
    uint64_t mask2 = mask1 >> 1;
    uint64_t magic = gt ? (~0ULL / no0(mask1) * (mask2 - v)) : (~0ULL / no0(mask1) * v);
    return magic;
}

template<bool gt, Action action, size_t width, class Callback> bool Array::FindGTLT_Fast(uint64_t chunk, uint64_t magic, QueryState<int64_t>* state, size_t baseindex, Callback callback) const
{
    // Tests if a a chunk of values contains values that are greater (if gt == true) or less (if gt == false) than v.
    // Fast, but limited to work when all values in the chunk are positive.

    uint64_t mask1 = (width == 64 ? ~0ULL : ((1ULL << (width == 64 ? 0 : width)) - 1ULL)); // Warning free way of computing (1ULL << width) - 1
    uint64_t mask2 = mask1 >> 1;
    uint64_t m = gt ? (((chunk + magic) | chunk) & ~0ULL / no0(mask1) * (mask2 + 1)) : ((chunk - magic) & ~chunk&~0ULL/no0(mask1)*(mask2+1));
    size_t p = 0;
    while (m) {
        if (find_action_pattern<action, Callback>(baseindex, m >> (no0(width) - 1), state, callback))
            break; // consumed, so do not call find_action()

        size_t t = FirstSetBit64(m) / no0(width);
        p += t;
        if (!find_action<action, Callback>(p + baseindex, (chunk >> (p * width)) & mask1, state, callback))
            return false;

        if ((t + 1) * width == 64)
            m = 0;
        else
            m >>= (t + 1) * width;
        p++;
    }

    return true;
}


template<bool gt, Action action, size_t width, class Callback> bool Array::FindGTLT(int64_t v, uint64_t chunk, QueryState<int64_t>* state, size_t baseindex, Callback callback) const
{
    // Find items in 'chunk' that are greater (if gt == true) or smaller (if gt == false) than 'v'. Fixme, __forceinline can make it crash in vS2010 - find out why
    if (width == 1) {
        for (size_t t = 0; t < 64; t++) {
            if (gt ? static_cast<int64_t>(chunk & 0x1) > v : static_cast<int64_t>(chunk & 0x1) < v) {if (!find_action<action, Callback>( t + baseindex, static_cast<int64_t>(chunk & 0x1), state, callback)) return false;} chunk >>= 1;
        }
    }
    else if (width == 2) {
        // Alot (50% +) faster than loop/compiler-unrolled loop
        if (gt ? static_cast<int64_t>(chunk & 0x3) > v : static_cast<int64_t>(chunk & 0x3) < v) {if (!find_action<action, Callback>( 0 + baseindex, static_cast<int64_t>(chunk & 0x3), state, callback)) return false;} chunk >>= 2;
        if (gt ? static_cast<int64_t>(chunk & 0x3) > v : static_cast<int64_t>(chunk & 0x3) < v) {if (!find_action<action, Callback>( 1 + baseindex, static_cast<int64_t>(chunk & 0x3), state, callback)) return false;} chunk >>= 2;
        if (gt ? static_cast<int64_t>(chunk & 0x3) > v : static_cast<int64_t>(chunk & 0x3) < v) {if (!find_action<action, Callback>( 2 + baseindex, static_cast<int64_t>(chunk & 0x3), state, callback)) return false;} chunk >>= 2;
        if (gt ? static_cast<int64_t>(chunk & 0x3) > v : static_cast<int64_t>(chunk & 0x3) < v) {if (!find_action<action, Callback>( 3 + baseindex, static_cast<int64_t>(chunk & 0x3), state, callback)) return false;} chunk >>= 2;
        if (gt ? static_cast<int64_t>(chunk & 0x3) > v : static_cast<int64_t>(chunk & 0x3) < v) {if (!find_action<action, Callback>( 4 + baseindex, static_cast<int64_t>(chunk & 0x3), state, callback)) return false;} chunk >>= 2;
        if (gt ? static_cast<int64_t>(chunk & 0x3) > v : static_cast<int64_t>(chunk & 0x3) < v) {if (!find_action<action, Callback>( 5 + baseindex, static_cast<int64_t>(chunk & 0x3), state, callback)) return false;} chunk >>= 2;
        if (gt ? static_cast<int64_t>(chunk & 0x3) > v : static_cast<int64_t>(chunk & 0x3) < v) {if (!find_action<action, Callback>( 6 + baseindex, static_cast<int64_t>(chunk & 0x3), state, callback)) return false;} chunk >>= 2;
        if (gt ? static_cast<int64_t>(chunk & 0x3) > v : static_cast<int64_t>(chunk & 0x3) < v) {if (!find_action<action, Callback>( 7 + baseindex, static_cast<int64_t>(chunk & 0x3), state, callback)) return false;} chunk >>= 2;

        if (gt ? static_cast<int64_t>(chunk & 0x3) > v : static_cast<int64_t>(chunk & 0x3) < v) {if (!find_action<action, Callback>( 8 + baseindex, static_cast<int64_t>(chunk & 0x3), state, callback)) return false;} chunk >>= 2;
        if (gt ? static_cast<int64_t>(chunk & 0x3) > v : static_cast<int64_t>(chunk & 0x3) < v) {if (!find_action<action, Callback>( 9 + baseindex, static_cast<int64_t>(chunk & 0x3), state, callback)) return false;} chunk >>= 2;
        if (gt ? static_cast<int64_t>(chunk & 0x3) > v : static_cast<int64_t>(chunk & 0x3) < v) {if (!find_action<action, Callback>( 10 + baseindex, static_cast<int64_t>(chunk & 0x3), state, callback)) return false;} chunk >>= 2;
        if (gt ? static_cast<int64_t>(chunk & 0x3) > v : static_cast<int64_t>(chunk & 0x3) < v) {if (!find_action<action, Callback>( 11 + baseindex, static_cast<int64_t>(chunk & 0x3), state, callback)) return false;} chunk >>= 2;
        if (gt ? static_cast<int64_t>(chunk & 0x3) > v : static_cast<int64_t>(chunk & 0x3) < v) {if (!find_action<action, Callback>( 12 + baseindex, static_cast<int64_t>(chunk & 0x3), state, callback)) return false;} chunk >>= 2;
        if (gt ? static_cast<int64_t>(chunk & 0x3) > v : static_cast<int64_t>(chunk & 0x3) < v) {if (!find_action<action, Callback>( 13 + baseindex, static_cast<int64_t>(chunk & 0x3), state, callback)) return false;} chunk >>= 2;
        if (gt ? static_cast<int64_t>(chunk & 0x3) > v : static_cast<int64_t>(chunk & 0x3) < v) {if (!find_action<action, Callback>( 14 + baseindex, static_cast<int64_t>(chunk & 0x3), state, callback)) return false;} chunk >>= 2;
        if (gt ? static_cast<int64_t>(chunk & 0x3) > v : static_cast<int64_t>(chunk & 0x3) < v) {if (!find_action<action, Callback>( 15 + baseindex, static_cast<int64_t>(chunk & 0x3), state, callback)) return false;} chunk >>= 2;

        if (gt ? static_cast<int64_t>(chunk & 0x3) > v : static_cast<int64_t>(chunk & 0x3) < v) {if (!find_action<action, Callback>( 16 + baseindex, static_cast<int64_t>(chunk & 0x3), state, callback)) return false;} chunk >>= 2;
        if (gt ? static_cast<int64_t>(chunk & 0x3) > v : static_cast<int64_t>(chunk & 0x3) < v) {if (!find_action<action, Callback>( 17 + baseindex, static_cast<int64_t>(chunk & 0x3), state, callback)) return false;} chunk >>= 2;
        if (gt ? static_cast<int64_t>(chunk & 0x3) > v : static_cast<int64_t>(chunk & 0x3) < v) {if (!find_action<action, Callback>( 18 + baseindex, static_cast<int64_t>(chunk & 0x3), state, callback)) return false;} chunk >>= 2;
        if (gt ? static_cast<int64_t>(chunk & 0x3) > v : static_cast<int64_t>(chunk & 0x3) < v) {if (!find_action<action, Callback>( 19 + baseindex, static_cast<int64_t>(chunk & 0x3), state, callback)) return false;} chunk >>= 2;
        if (gt ? static_cast<int64_t>(chunk & 0x3) > v : static_cast<int64_t>(chunk & 0x3) < v) {if (!find_action<action, Callback>( 20 + baseindex, static_cast<int64_t>(chunk & 0x3), state, callback)) return false;} chunk >>= 2;
        if (gt ? static_cast<int64_t>(chunk & 0x3) > v : static_cast<int64_t>(chunk & 0x3) < v) {if (!find_action<action, Callback>( 21 + baseindex, static_cast<int64_t>(chunk & 0x3), state, callback)) return false;} chunk >>= 2;
        if (gt ? static_cast<int64_t>(chunk & 0x3) > v : static_cast<int64_t>(chunk & 0x3) < v) {if (!find_action<action, Callback>( 22 + baseindex, static_cast<int64_t>(chunk & 0x3), state, callback)) return false;} chunk >>= 2;
        if (gt ? static_cast<int64_t>(chunk & 0x3) > v : static_cast<int64_t>(chunk & 0x3) < v) {if (!find_action<action, Callback>( 23 + baseindex, static_cast<int64_t>(chunk & 0x3), state, callback)) return false;} chunk >>= 2;

        if (gt ? static_cast<int64_t>(chunk & 0x3) > v : static_cast<int64_t>(chunk & 0x3) < v) {if (!find_action<action, Callback>( 24 + baseindex, static_cast<int64_t>(chunk & 0x3), state, callback)) return false;} chunk >>= 2;
        if (gt ? static_cast<int64_t>(chunk & 0x3) > v : static_cast<int64_t>(chunk & 0x3) < v) {if (!find_action<action, Callback>( 25 + baseindex, static_cast<int64_t>(chunk & 0x3), state, callback)) return false;} chunk >>= 2;
        if (gt ? static_cast<int64_t>(chunk & 0x3) > v : static_cast<int64_t>(chunk & 0x3) < v) {if (!find_action<action, Callback>( 26 + baseindex, static_cast<int64_t>(chunk & 0x3), state, callback)) return false;} chunk >>= 2;
        if (gt ? static_cast<int64_t>(chunk & 0x3) > v : static_cast<int64_t>(chunk & 0x3) < v) {if (!find_action<action, Callback>( 27 + baseindex, static_cast<int64_t>(chunk & 0x3), state, callback)) return false;} chunk >>= 2;
        if (gt ? static_cast<int64_t>(chunk & 0x3) > v : static_cast<int64_t>(chunk & 0x3) < v) {if (!find_action<action, Callback>( 28 + baseindex, static_cast<int64_t>(chunk & 0x3), state, callback)) return false;} chunk >>= 2;
        if (gt ? static_cast<int64_t>(chunk & 0x3) > v : static_cast<int64_t>(chunk & 0x3) < v) {if (!find_action<action, Callback>( 29 + baseindex, static_cast<int64_t>(chunk & 0x3), state, callback)) return false;} chunk >>= 2;
        if (gt ? static_cast<int64_t>(chunk & 0x3) > v : static_cast<int64_t>(chunk & 0x3) < v) {if (!find_action<action, Callback>( 30 + baseindex, static_cast<int64_t>(chunk & 0x3), state, callback)) return false;} chunk >>= 2;
        if (gt ? static_cast<int64_t>(chunk & 0x3) > v : static_cast<int64_t>(chunk & 0x3) < v) {if (!find_action<action, Callback>( 31 + baseindex, static_cast<int64_t>(chunk & 0x3), state, callback)) return false;} chunk >>= 2;
    }
    else if (width == 4) {
        // 128 ms:
        if (gt ? static_cast<int64_t>(chunk & 0xf) > v : static_cast<int64_t>(chunk & 0xf) < v) {if (!find_action<action, Callback>( 0 + baseindex, static_cast<int64_t>(chunk & 0xf), state, callback)) return false;} chunk >>= 4;
        if (gt ? static_cast<int64_t>(chunk & 0xf) > v : static_cast<int64_t>(chunk & 0xf) < v) {if (!find_action<action, Callback>( 1 + baseindex, static_cast<int64_t>(chunk & 0xf), state, callback)) return false;} chunk >>= 4;
        if (gt ? static_cast<int64_t>(chunk & 0xf) > v : static_cast<int64_t>(chunk & 0xf) < v) {if (!find_action<action, Callback>( 2 + baseindex, static_cast<int64_t>(chunk & 0xf), state, callback)) return false;} chunk >>= 4;
        if (gt ? static_cast<int64_t>(chunk & 0xf) > v : static_cast<int64_t>(chunk & 0xf) < v) {if (!find_action<action, Callback>( 3 + baseindex, static_cast<int64_t>(chunk & 0xf), state, callback)) return false;} chunk >>= 4;
        if (gt ? static_cast<int64_t>(chunk & 0xf) > v : static_cast<int64_t>(chunk & 0xf) < v) {if (!find_action<action, Callback>( 4 + baseindex, static_cast<int64_t>(chunk & 0xf), state, callback)) return false;} chunk >>= 4;
        if (gt ? static_cast<int64_t>(chunk & 0xf) > v : static_cast<int64_t>(chunk & 0xf) < v) {if (!find_action<action, Callback>( 5 + baseindex, static_cast<int64_t>(chunk & 0xf), state, callback)) return false;} chunk >>= 4;
        if (gt ? static_cast<int64_t>(chunk & 0xf) > v : static_cast<int64_t>(chunk & 0xf) < v) {if (!find_action<action, Callback>( 6 + baseindex, static_cast<int64_t>(chunk & 0xf), state, callback)) return false;} chunk >>= 4;
        if (gt ? static_cast<int64_t>(chunk & 0xf) > v : static_cast<int64_t>(chunk & 0xf) < v) {if (!find_action<action, Callback>( 7 + baseindex, static_cast<int64_t>(chunk & 0xf), state, callback)) return false;} chunk >>= 4;

        if (gt ? static_cast<int64_t>(chunk & 0xf) > v : static_cast<int64_t>(chunk & 0xf) < v) {if (!find_action<action, Callback>( 8 + baseindex, static_cast<int64_t>(chunk & 0xf), state, callback)) return false;} chunk >>= 4;
        if (gt ? static_cast<int64_t>(chunk & 0xf) > v : static_cast<int64_t>(chunk & 0xf) < v) {if (!find_action<action, Callback>( 9 + baseindex, static_cast<int64_t>(chunk & 0xf), state, callback)) return false;} chunk >>= 4;
        if (gt ? static_cast<int64_t>(chunk & 0xf) > v : static_cast<int64_t>(chunk & 0xf) < v) {if (!find_action<action, Callback>( 10 + baseindex, static_cast<int64_t>(chunk & 0xf), state, callback)) return false;} chunk >>= 4;
        if (gt ? static_cast<int64_t>(chunk & 0xf) > v : static_cast<int64_t>(chunk & 0xf) < v) {if (!find_action<action, Callback>( 11 + baseindex, static_cast<int64_t>(chunk & 0xf), state, callback)) return false;} chunk >>= 4;
        if (gt ? static_cast<int64_t>(chunk & 0xf) > v : static_cast<int64_t>(chunk & 0xf) < v) {if (!find_action<action, Callback>( 12 + baseindex, static_cast<int64_t>(chunk & 0xf), state, callback)) return false;} chunk >>= 4;
        if (gt ? static_cast<int64_t>(chunk & 0xf) > v : static_cast<int64_t>(chunk & 0xf) < v) {if (!find_action<action, Callback>( 13 + baseindex, static_cast<int64_t>(chunk & 0xf), state, callback)) return false;} chunk >>= 4;
        if (gt ? static_cast<int64_t>(chunk & 0xf) > v : static_cast<int64_t>(chunk & 0xf) < v) {if (!find_action<action, Callback>( 14 + baseindex, static_cast<int64_t>(chunk & 0xf), state, callback)) return false;} chunk >>= 4;
        if (gt ? static_cast<int64_t>(chunk & 0xf) > v : static_cast<int64_t>(chunk & 0xf) < v) {if (!find_action<action, Callback>( 15 + baseindex, static_cast<int64_t>(chunk & 0xf), state, callback)) return false;} chunk >>= 4;

        // 187 ms:
        // if (gt ? static_cast<int64_t>(chunk >> 0*4) & 0xf > v : static_cast<int64_t>(chunk >> 0*4) & 0xf < v) return 0;
    }
    else if (width == 8) {
        // 88 ms:
        if (gt ? static_cast<char>(chunk) > v : static_cast<char>(chunk) < v) {if (!find_action<action, Callback>( 0 + baseindex, static_cast<char>(chunk), state, callback)) return false;} chunk >>= 8;
        if (gt ? static_cast<char>(chunk) > v : static_cast<char>(chunk) < v) {if (!find_action<action, Callback>( 1 + baseindex, static_cast<char>(chunk), state, callback)) return false;} chunk >>= 8;
        if (gt ? static_cast<char>(chunk) > v : static_cast<char>(chunk) < v) {if (!find_action<action, Callback>( 2 + baseindex, static_cast<char>(chunk), state, callback)) return false;} chunk >>= 8;
        if (gt ? static_cast<char>(chunk) > v : static_cast<char>(chunk) < v) {if (!find_action<action, Callback>( 3 + baseindex, static_cast<char>(chunk), state, callback)) return false;} chunk >>= 8;
        if (gt ? static_cast<char>(chunk) > v : static_cast<char>(chunk) < v) {if (!find_action<action, Callback>( 4 + baseindex, static_cast<char>(chunk), state, callback)) return false;} chunk >>= 8;
        if (gt ? static_cast<char>(chunk) > v : static_cast<char>(chunk) < v) {if (!find_action<action, Callback>( 5 + baseindex, static_cast<char>(chunk), state, callback)) return false;} chunk >>= 8;
        if (gt ? static_cast<char>(chunk) > v : static_cast<char>(chunk) < v) {if (!find_action<action, Callback>( 6 + baseindex, static_cast<char>(chunk), state, callback)) return false;} chunk >>= 8;
        if (gt ? static_cast<char>(chunk) > v : static_cast<char>(chunk) < v) {if (!find_action<action, Callback>( 7 + baseindex, static_cast<char>(chunk), state, callback)) return false;} chunk >>= 8;

        //97 ms ms:
        // if (gt ? static_cast<char>(chunk >> 0*8) > v : static_cast<char>(chunk >> 0*8) < v) return 0;
    }
    else if (width == 16) {

        if (gt ? static_cast<short int>(chunk >> 0*16) > v : static_cast<short int>(chunk >> 0*16) < v) {if (!find_action<action, Callback>( 0 + baseindex, static_cast<short int>(chunk >> 0*16), state, callback)) return false;};
        if (gt ? static_cast<short int>(chunk >> 1*16) > v : static_cast<short int>(chunk >> 1*16) < v) {if (!find_action<action, Callback>( 1 + baseindex, static_cast<short int>(chunk >> 1*16), state, callback)) return false;};
        if (gt ? static_cast<short int>(chunk >> 2*16) > v : static_cast<short int>(chunk >> 2*16) < v) {if (!find_action<action, Callback>( 2 + baseindex, static_cast<short int>(chunk >> 2*16), state, callback)) return false;};
        if (gt ? static_cast<short int>(chunk >> 3*16) > v : static_cast<short int>(chunk >> 3*16) < v) {if (!find_action<action, Callback>( 3 + baseindex, static_cast<short int>(chunk >> 3*16), state, callback)) return false;};

        /*
        // Faster but disabled due to bug in VC2010 compiler (fixed in 2012 toolchain) where last 'if' is errorneously optimized away
        if (gt ? static_cast<short int>chunk > v : static_cast<short int>chunk < v) {if (!state->AddPositiveLocal(0 + baseindex); else return 0;} chunk >>= 16;
        if (gt ? static_cast<short int>chunk > v : static_cast<short int>chunk < v) {if (!state->AddPositiveLocal(1 + baseindex); else return 1;} chunk >>= 16;
        if (gt ? static_cast<short int>chunk > v : static_cast<short int>chunk < v) {if (!state->AddPositiveLocal(2 + baseindex); else return 2;} chunk >>= 16;
        if (gt ? static_cast<short int>chunk > v : static_cast<short int>chunk < v) {if (!state->AddPositiveLocal(3 + baseindex); else return 3;} chunk >>= 16;

        // Following illustrates it:
        #include <stdint.h>
        #include <stdio.h>
        #include <stdlib.h>

        size_t bug(int64_t v, uint64_t chunk)
        {
            bool gt = true;

            if (gt ? static_cast<short int>chunk > v : static_cast<short int>chunk < v) {return 0;} chunk >>= 16;
            if (gt ? static_cast<short int>chunk > v : static_cast<short int>chunk < v) {return 1;} chunk >>= 16;
            if (gt ? static_cast<short int>chunk > v : static_cast<short int>chunk < v) {return 2;} chunk >>= 16;
            if (gt ? static_cast<short int>chunk > v : static_cast<short int>chunk < v) {return 3;} chunk >>= 16;

            return -1;
        }

        int main(int argc, char const *const argv[])
        {
            int64_t v;
            if (rand()*rand() == 3) {
                v = rand()*rand()*rand()*rand()*rand();
                printf("Change '3' to something else and run test again\n");
            }
            else {
                v = 0x2222000000000000ULL;
            }

            size_t idx;

            idx = bug(200, v);
            if (idx != 3)
                printf("Compiler failed: idx == %d (expected idx == 3)\n", idx);

            v = 0x2222000000000000ULL;
            idx = bug(200, v);
            if (idx == 3)
                printf("Touching v made it work\n", idx);
        }
        */
    }
    else if (width == 32) {
        if (gt ? static_cast<int>(chunk) > v : static_cast<int>(chunk) < v) {if (!find_action<action, Callback>( 0 + baseindex, static_cast<int>(chunk), state, callback)) return false;} chunk >>= 32;
        if (gt ? static_cast<int>(chunk) > v : static_cast<int>(chunk) < v) {if (!find_action<action, Callback>( 1 + baseindex, static_cast<int>(chunk), state, callback)) return false;} chunk >>= 32;
    }
    else if (width == 64) {
        if (gt ? static_cast<int64_t>(v) > v : static_cast<int64_t>(v) < v) {if (!find_action<action, Callback>( 0 + baseindex, static_cast<int64_t>(v), state, callback)) return false;};
    }

    return true;
}


template<bool eq, Action action, size_t width, class Callback> inline bool Array::CompareEquality(int64_t value, size_t start, size_t end, size_t baseindex, QueryState<int64_t>* state, Callback callback) const
{
    // Find items in this Array that are equal (eq == true) or different (eq = false) from 'value'

    TIGHTDB_ASSERT(start <= m_size && (end <= m_size || end == std::size_t(-1)) && start <= end);

    size_t ee = round_up(start, 64 / no0(width));
    ee = ee > end ? end : ee;
    for (; start < ee; ++start)
        if (eq ? (Get<width>(start) == value) : (Get<width>(start) != value)) {
            if (!find_action<action, Callback>(start + baseindex, Get<width>(start), state, callback))
                return false;
        }

    if (start >= end)
        return true;

    if (width != 32 && width != 64) {
        const int64_t* p = reinterpret_cast<const int64_t*>(m_data + (start * width / 8));
        const int64_t* const e = reinterpret_cast<int64_t*>(m_data + (end * width / 8)) - 1;
        const uint64_t mask = (width == 64 ? ~0ULL : ((1ULL << (width == 64 ? 0 : width)) - 1ULL)); // Warning free way of computing (1ULL << width) - 1
        const uint64_t valuemask = ~0ULL / no0(mask) * (value & mask); // the "== ? :" is to avoid division by 0 compiler error

        while (p < e) {
            uint64_t chunk = *p;
            uint64_t v2 = chunk ^ valuemask;
            start = (p - reinterpret_cast<int64_t*>(m_data)) * 8 * 8 / no0(width);
            size_t a = 0;

            while (eq ? TestZero<width>(v2) : v2) {

                if (find_action_pattern<action, Callback>(start + baseindex, cascade<width>(eq ? v2 : ~v2), state, callback))
                    break; // consumed

                size_t t = FindZero<eq, width>(v2);
                a += t;

                if (a >= 64 / no0(width))
                    break;

                if (!find_action<action, Callback>(a + start + baseindex, Get<width>(start + t), state, callback))
                    return false;
                v2 >>= (t + 1) * width;
                a += 1;
            }

            ++p;
        }

        // Loop ended because we are near end or end of array. No need to optimize search in remainder in this case because end of array means that
        // lots of search work has taken place prior to ending here. So time spent searching remainder is relatively tiny
        start = (p - reinterpret_cast<int64_t*>(m_data)) * 8 * 8 / no0(width);
    }

    while (start < end) {
        if (eq ? Get<width>(start) == value : Get<width>(start) != value) {
            if (!find_action<action, Callback>( start + baseindex, Get<width>(start), state, callback))
                return false;
        }
        ++start;
    }

        return true;
}

// There exists a couple of find() functions that take more or less template arguments. Always call the one that
// takes as most as possible to get best performance.

template<class cond, Action action, size_t bitwidth>
bool Array::find(int64_t value, size_t start, size_t end, size_t baseindex, QueryState<int64_t>* state) const
{
    return find<cond, action, bitwidth>(value, start, end, baseindex, state, CallbackDummy());
}

template<class cond, Action action, class Callback>
bool Array::find(int64_t value, size_t start, size_t end, size_t baseindex, QueryState<int64_t>* state,
                 Callback callback) const
{
    TIGHTDB_TEMPEX4(return find, cond, action, m_width, Callback, (value, start, end, baseindex, state, callback));
}

template<class cond, Action action, size_t bitwidth, class Callback>
bool Array::find(int64_t value, size_t start, size_t end, size_t baseindex, QueryState<int64_t>* state,
                 Callback callback) const
{
    return find_optimized<cond, action, bitwidth, Callback>(value, start, end, baseindex, state, callback);
}

#ifdef TIGHTDB_COMPILER_SSE
// 'items' is the number of 16-byte SSE chunks. Returns index of packed element relative to first integer of first chunk
template<class cond2, Action action, size_t width, class Callback>
bool Array::FindSSE(int64_t value, __m128i *data, size_t items, QueryState<int64_t>* state, size_t baseindex,
                    Callback callback) const
{
    __m128i search = {0};

    // FIXME: Lasse, should these casts not be to int8_t, int16_t, int32_t respecitvely?
    if (width == 8)
        search = _mm_set1_epi8(static_cast<char>(value)); // FIXME: Lasse, Should this not be a cast to 'signed char'?
    else if (width == 16)
        search = _mm_set1_epi16(static_cast<short int>(value));
    else if (width == 32)
        search = _mm_set1_epi32(static_cast<int>(value));
    else if (width == 64) {
        search = _mm_set_epi64x(value, value);
    }

    return FindSSE_intern<cond2, action, width, Callback>(data, &search, items, state, baseindex, callback);
}

// Compares packed action_data with packed data (equal, less, etc) and performs aggregate action (max, min, sum,
// find_all, etc) on value inside action_data for first match, if any
template<class cond2, Action action, size_t width, class Callback>
TIGHTDB_FORCEINLINE bool Array::FindSSE_intern(__m128i* action_data, __m128i* data, size_t items,
                                               QueryState<int64_t>* state, size_t baseindex, Callback callback) const
{
    cond2 c;
    int cond = c.condition();
    size_t i = 0;
    __m128i compare = {0};
    unsigned int resmask;

    // Search loop. Unrolling it has been tested to NOT increase performance (apparently mem bound)
    for (i = 0; i < items; ++i) {
        // equal / not-equal
        if (cond == cond_Equal || cond == cond_NotEqual) {
            if (width == 8)
                compare = _mm_cmpeq_epi8(action_data[i], *data);
            if (width == 16)
                compare = _mm_cmpeq_epi16(action_data[i], *data);
            if (width == 32)
                compare = _mm_cmpeq_epi32(action_data[i], *data);
            if (width == 64) {
                compare = _mm_cmpeq_epi64(action_data[i], *data); // SSE 4.2 only
            }
        }

        // greater
        else if (cond == cond_Greater) {
            if (width == 8)
                compare = _mm_cmpgt_epi8(action_data[i], *data);
            if (width == 16)
                compare = _mm_cmpgt_epi16(action_data[i], *data);
            if (width == 32)
                compare = _mm_cmpgt_epi32(action_data[i], *data);
            if (width == 64)
                compare = _mm_cmpgt_epi64(action_data[i], *data);
        }
        // less
        else if (cond == cond_Less) {
            if (width == 8)
                compare = _mm_cmplt_epi8(action_data[i], *data);
            if (width == 16)
                compare = _mm_cmplt_epi16(action_data[i], *data);
            if (width == 32)
                compare = _mm_cmplt_epi32(action_data[i], *data);
            if (width == 64){
                // There exists no _mm_cmplt_epi64 instruction, so emulate it. _mm_set1_epi8(0xff) is pre-calculated by compiler.
                compare = _mm_cmpeq_epi64(action_data[i], *data);
                compare = _mm_andnot_si128(compare, _mm_set1_epi32(0xffffffff));
            }
        }

        resmask = _mm_movemask_epi8(compare);

        if (cond == cond_NotEqual)
            resmask = ~resmask & 0x0000ffff;

//        if (resmask != 0)
//            printf("resmask=%d\n", resmask);

        size_t s = i * sizeof (__m128i) * 8 / no0(width);

        while (resmask != 0) {

            uint64_t upper = LowerBits<width / 8>() << (no0(width / 8) - 1);
            uint64_t pattern = resmask & upper; // fixme, bits at wrong offsets. Only OK because we only use them in 'count' aggregate
            if (find_action_pattern<action, Callback>(s + baseindex, pattern, state, callback))
                break;

            size_t idx = FirstSetBit(resmask) * 8 / no0(width);
            s += idx;
            if (!find_action<action, Callback>( s + baseindex, GetUniversal<width>(reinterpret_cast<char*>(data), s), state, callback))
                return false;
            resmask >>= (idx + 1) * no0(width) / 8;
            ++s;
        }
    }

    return true;
}
#endif //TIGHTDB_COMPILER_SSE

template<class cond, Action action, class Callback>
bool Array::CompareLeafs(const Array* foreign, size_t start, size_t end, size_t baseindex, QueryState<int64_t>* state,
                         Callback callback) const
{
    cond c;
    TIGHTDB_ASSERT(start < end);


    int64_t v;

    // We can compare first element without checking for out-of-range
    v = get(start);
    if (c(v, foreign->get(start))) {
        if (!find_action<action, Callback>(start + baseindex, v, state, callback))
            return false;
    }

    start++;

    if (start + 3 < end) {
        v = get(start);
        if (c(v, foreign->get(start)))
            if (!find_action<action, Callback>(start + baseindex, v, state, callback))
                return false;

        v = get(start + 1);
        if (c(v, foreign->get(start + 1)))
            if (!find_action<action, Callback>(start + 1 + baseindex, v, state, callback))
                return false;

        v = get(start + 2);
        if (c(v, foreign->get(start + 2)))
            if (!find_action<action, Callback>(start + 2 + baseindex, v, state, callback))
                return false;

        start += 3;
    }
    else if (start == end) {
        return true;
    }

    bool r;
    TIGHTDB_TEMPEX4(r = CompareLeafs, cond, action, m_width, Callback, (foreign, start, end, baseindex, state, callback))
    return r;
}


template<class cond, Action action, size_t width, class Callback> bool Array::CompareLeafs(const Array* foreign, size_t start, size_t end, size_t baseindex, QueryState<int64_t>* state, Callback callback) const
{
    size_t fw = foreign->m_width;
    bool r;
    TIGHTDB_TEMPEX5(r = CompareLeafs4, cond, action, width, Callback, fw, (foreign, start, end, baseindex, state, callback))
    return r;
}


template<class cond, Action action, size_t width, class Callback, size_t foreign_width>
bool Array::CompareLeafs4(const Array* foreign, size_t start, size_t end, size_t baseindex, QueryState<int64_t>* state,
                          Callback callback) const
{
    cond c;
    char* foreign_m_data = foreign->m_data;

    if (width == 0 && foreign_width == 0) {
        if (c(0, 0)) {
            while (start < end) {
                if (!find_action<action, Callback>(start + baseindex, 0, state, callback))
                    return false;
                start++;
            }
        }
        else {
            return true;
        }
    }


#if defined(TIGHTDB_COMPILER_SSE)
    if (cpuid_sse<42>() && width == foreign_width && (width == 8 || width == 16 || width == 32)) {
        // We can only use SSE if both bitwidths are equal and above 8 bits and all values are signed
        while (start < end && (((reinterpret_cast<size_t>(m_data) & 0xf) * 8 + start * width) % (128) != 0)) {
            int64_t v = GetUniversal<width>(m_data, start);
            int64_t fv = GetUniversal<foreign_width>(foreign_m_data, start);
            if (c(v, fv)) {
                if (!find_action<action, Callback>(start + baseindex, v, state, callback))
                    return false;
            }
            start++;
        }
        if (start == end)
            return true;


        size_t sse_items = (end - start) * width / 128;
        size_t sse_end = start + sse_items * 128 / no0(width);

        while (start < sse_end) {
            __m128i* a = reinterpret_cast<__m128i*>(m_data + start * width / 8);
            __m128i* b = reinterpret_cast<__m128i*>(foreign_m_data + start * width / 8);

            bool continue_search = FindSSE_intern<cond, action, width, Callback>(a, b, 1, state, baseindex + start, callback);

            if (!continue_search)
                return false;

            start += 128 / no0(width);
        }
    }
#endif


#if 0 // this method turned out to be 33% slower than a naive loop. Find out why

    // index from which both arrays are 64-bit aligned
    size_t a = round_up(start, 8 * sizeof (int64_t) / (width < foreign_width ? width : foreign_width));

    while (start < end && start < a) {
        int64_t v = GetUniversal<width>(m_data, start);
        int64_t fv = GetUniversal<foreign_width>(foreign_m_data, start);

        if (v == fv)
            r++;

        start++;
    }

    if (start >= end)
        return r;

    uint64_t chunk;
    uint64_t fchunk;

    size_t unroll_outer = (foreign_width > width ? foreign_width : width) / (foreign_width < width ? foreign_width : width);
    size_t unroll_inner = 64 / (foreign_width > width ? foreign_width : width);

    while (start + unroll_outer * unroll_inner < end) {

        // fetch new most narrow chunk
        if (foreign_width <= width)
            fchunk = *reinterpret_cast<int64_t*>(foreign_m_data + start * foreign_width / 8);
        else
            chunk = *reinterpret_cast<int64_t*>(m_data + start * width / 8);

        for (size_t uo = 0; uo < unroll_outer; uo++) {

            // fetch new widest chunk
            if (foreign_width > width)
                fchunk = *reinterpret_cast<int64_t*>(foreign_m_data + start * foreign_width / 8);
            else
                chunk = *reinterpret_cast<int64_t*>(m_data + start * width / 8);

            size_t newstart = start + unroll_inner;
            while (start < newstart) {

                // Isolate first value from chunk
                int64_t v = (chunk << (64 - width)) >> (64 - width);
                int64_t fv = (fchunk << (64 - foreign_width)) >> (64 - foreign_width);
                chunk >>= width;
                fchunk >>= foreign_width;

                // Sign extend if required
                v = (width <= 4) ? v : (width == 8) ? int8_t(v) : (width == 16) ? int16_t(v) : (width == 32) ? int32_t(v) : int64_t(v);
                fv = (foreign_width <= 4) ? fv : (foreign_width == 8) ? int8_t(fv) : (foreign_width == 16) ? int16_t(fv) : (foreign_width == 32) ? int32_t(fv) : int64_t(fv);

                if (v == fv)
                    r++;

                start++;

            }


        }
    }
#endif



/*
    // Unrolling helped less than 2% (non-frequent matches). Todo, investigate further
    while (start + 1 < end) {
        int64_t v = GetUniversal<width>(m_data, start);
        int64_t v2 = GetUniversal<width>(m_data, start + 1);

        int64_t fv = GetUniversal<foreign_width>(foreign_m_data, start);
        int64_t fv2 = GetUniversal<foreign_width>(foreign_m_data, start + 1);

        if (c(v, fv)) {
            if (!find_action<action, Callback>(start + baseindex, v, state, callback))
                return false;
        }

        if (c(v2, fv2)) {
            if (!find_action<action, Callback>(start + 1 + baseindex, v2, state, callback))
                return false;
        }

        start += 2;
    }
 */

    while (start < end) {
        int64_t v = GetUniversal<width>(m_data, start);
        int64_t fv = GetUniversal<foreign_width>(foreign_m_data, start);

        if (c(v, fv)) {
            if (!find_action<action, Callback>(start + baseindex, v, state, callback))
                return false;
        }

        start++;
    }

    return true;
}


template<class cond2, Action action, size_t bitwidth, class Callback>
bool Array::Compare(int64_t value, size_t start, size_t end, size_t baseindex, QueryState<int64_t>* state,
                    Callback callback) const
{
    cond2 c;
    int cond = c.condition();
    bool ret = false;

    if (cond == cond_Equal)
        ret = CompareEquality<true, action, bitwidth, Callback>(value, start, end, baseindex, state, callback);
    else if (cond == cond_NotEqual)
        ret = CompareEquality<false, action, bitwidth, Callback>(value, start, end, baseindex, state, callback);
    else if (cond == cond_Greater)
        ret = CompareRelation<true, action, bitwidth, Callback>(value, start, end, baseindex, state, callback);
    else if (cond == cond_Less)
        ret = CompareRelation<false, action, bitwidth, Callback>(value, start, end, baseindex, state, callback);
    else
        TIGHTDB_ASSERT(false);

    return ret;
}

template<bool gt, Action action, size_t bitwidth, class Callback>
bool Array::CompareRelation(int64_t value, size_t start, size_t end, size_t baseindex, QueryState<int64_t>* state,
                            Callback callback) const
{
    TIGHTDB_ASSERT(start <= m_size && (end <= m_size || end == std::size_t(-1)) && start <= end);
    uint64_t mask = (bitwidth == 64 ? ~0ULL : ((1ULL << (bitwidth == 64 ? 0 : bitwidth)) - 1ULL)); // Warning free way of computing (1ULL << width) - 1

    size_t ee = round_up(start, 64 / no0(bitwidth));
    ee = ee > end ? end : ee;
    for (; start < ee; start++) {
        if (gt ? (Get<bitwidth>(start) > value) : (Get<bitwidth>(start) < value)) {
            if (!find_action<action, Callback>(start + baseindex, Get<bitwidth>(start), state, callback))
                return false;
        }
    }

    if (start >= end)
        return true; // none found, continue (return true) regardless what find_action() would have returned on match

    const int64_t* p = reinterpret_cast<const int64_t*>(m_data + (start * bitwidth / 8));
    const int64_t* const e = reinterpret_cast<int64_t*>(m_data + (end * bitwidth / 8)) - 1;

    // Matches are rare enough to setup fast linear search for remaining items. We use
    // bit hacks from http://graphics.stanford.edu/~seander/bithacks.html#HasLessInWord

    if (bitwidth == 1 || bitwidth == 2 || bitwidth == 4 || bitwidth == 8 || bitwidth == 16) {
        uint64_t magic = FindGTLT_Magic<gt, bitwidth>(value);

        // Bit hacks only work if searched item <= 127 for 'greater than' and item <= 128 for 'less than'
        if (value != int64_t((magic & mask)) && value >= 0 && bitwidth >= 2 && value <= static_cast<int64_t>((mask >> 1) - (gt ? 1 : 0))) {
            // 15 ms
            while (p < e) {
                uint64_t upper = LowerBits<bitwidth>() << (no0(bitwidth) - 1);

                const int64_t v = *p;
                size_t idx;

                // Bit hacks only works for positive items in chunk, so test their sign bits
                upper = upper & v;

                if ((bitwidth > 4 ? !upper : true)) {
                    // Assert that all values in chunk are positive.
                    TIGHTDB_ASSERT(bitwidth <= 4 || ((LowerBits<bitwidth>() << (no0(bitwidth) - 1)) & value) == 0);
                    idx = FindGTLT_Fast<gt, action, bitwidth, Callback>(v, magic, state, (p - reinterpret_cast<int64_t*>(m_data)) * 8 * 8 / no0(bitwidth) + baseindex, callback);
                }
                else
                    idx = FindGTLT<gt, action, bitwidth, Callback>(value, v, state, (p - reinterpret_cast<int64_t*>(m_data)) * 8 * 8 / no0(bitwidth) + baseindex, callback);

                if (!idx)
                    return false;
                ++p;
            }
        }
        else {
            // 24 ms
            while (p < e) {
                int64_t v = *p;
                if (!FindGTLT<gt, action, bitwidth, Callback>(value, v, state, (p - reinterpret_cast<int64_t*>(m_data)) * 8 * 8 / no0(bitwidth) + baseindex, callback))
                    return false;
                ++p;
            }
        }
        start = (p - reinterpret_cast<int64_t *>(m_data)) * 8 * 8 / no0(bitwidth);
    }

    // matchcount logic in SIMD no longer pays off for 32/64 bit ints because we have just 4/2 elements

    // Test unaligned end and/or values of width > 16 manually
    while (start < end) {
        if (gt ? Get<bitwidth>(start) > value : Get<bitwidth>(start) < value) {
            if (!find_action<action, Callback>( start + baseindex, Get<bitwidth>(start), state, callback))
                return false;
        }
        ++start;
    }
    return true;

}

template<class cond> size_t Array::find_first(int64_t value, size_t start, size_t end) const
{
    cond c;
    TIGHTDB_ASSERT(start <= m_size && (end <= m_size || end == std::size_t(-1)) && start <= end);
    QueryState<int64_t> state;
    state.m_state = not_found;
    Finder finder = m_finder[c.condition()];
    (this->*finder)(value, start, end, 0, &state);

    return static_cast<size_t>(state.m_state);
}

//*************************************************************************************
// Finding code ends                                                                  *
//*************************************************************************************




} // namespace tightdb

#endif // TIGHTDB_ARRAY_HPP
