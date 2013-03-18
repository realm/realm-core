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
    template <class cond, Action action, size_t bitwidth, class Callback> void find(int64_t value, size_t start, size_t end, size_t baseindex, QueryState *state, Callback callback) const

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
#include <stdint.h> // unint8_t etc
#include <cstdlib> // size_t
#include <cstring> // memmove
#include <utility>
#include <vector>
#include <ostream>

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

enum Action {act_ReturnFirst, act_Sum, act_Max, act_Min, act_Count, act_FindAll, act_CallIdx, act_CallbackIdx, act_CallbackVal, act_CallbackNone, act_CallbackBoth};

template<class T> inline T no0(T v) { return v == 0 ? 1 : v; }

const size_t not_found = size_t(-1);

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


// Pre-definitions
class Array;
class AdaptiveStringColumn;
class GroupWriter;
class Column;
template <class T> class QueryState;


#ifdef TIGHTDB_DEBUG
class MemStats {
public:
    MemStats() : allocated(0), used(0), array_count(0) {}
    MemStats(size_t allocated, size_t used, size_t array_count)
    : allocated(allocated), used(used), array_count(array_count) {}
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
    size_t allocated;
    size_t used;
    size_t array_count;
};
#endif


class ArrayParent
{
public:
    virtual ~ArrayParent() {}

    // FIXME: Must be protected. Solve problem by having the Array constructor, that creates a new array, call it.
    virtual void update_child_ref(size_t child_ndx, size_t new_ref) = 0;

protected:
    friend class Array;

    virtual size_t get_child_ref(size_t child_ndx) const TIGHTDB_NOEXCEPT = 0;
};


/**
 * An Array can be copied, but it will leave the source in a truncated
 * (and therfore unusable) state.
 *
 * \note The parent information in an array ('pointer to parent' and
 * 'index in parent') may be valid even when the array is not valid,
 * that is IsValid() returns false.
 *
 * FIXME: Array should be endowed with proper copy and move semantics
 * like TableView is.
 */
class Array: public ArrayParent {
public:

//    void state_init(int action, QueryState *state);
//    bool match(int action, size_t index, int64_t value, QueryState *state);

    enum ColumnDef {
        coldef_Normal,
        coldef_InnerNode, ///< Inner node of B-tree
        coldef_HasRefs
    };

    /// Create a new array, and if \a parent and \a ndx_in_parent are
    /// specified, update the parent to point to this new array.
    explicit Array(ColumnDef type = coldef_Normal, ArrayParent* = 0, size_t ndx_in_parent = 0,
                   Allocator& = Allocator::get_default());

    /// Initialize an array wrapper from the specified array.
    explicit Array(size_t ref, ArrayParent* = 0, size_t ndx_in_parent = 0,
                   Allocator& = Allocator::get_default()) TIGHTDB_NOEXCEPT;

    /// Create an array in the invalid state (a null array).
    explicit Array(Allocator&) TIGHTDB_NOEXCEPT;

    /// FIXME: This is an attempt at a moving constructor - but it violates constness in an onfortunate way.
    Array(const Array&) TIGHTDB_NOEXCEPT;

    // Fastest way to instantiate an array, if you just want to utilize its methods
    struct no_prealloc_tag {};
    explicit Array(no_prealloc_tag) TIGHTDB_NOEXCEPT;

    virtual ~Array() TIGHTDB_NOEXCEPT {}

    // FIXME: This operator does not compare the arrays, it compares
    // just the data pointers. This is hugely counterintuitive. If
    // this kind of comparison is needed, it should be provided as
    // an ordinary function. Proper (deep) array comparison is
    // probably something that we want too, and that is what we should
    // use operator==() for. For example, proper array comparison
    // would be usefull for checking whether two tables have the same
    // sequence of column types.
    bool operator==(const Array& a) const;

    void SetType(ColumnDef type);
    void UpdateRef(size_t ref);
    void Copy(const Array&); // Copy semantics for assignment
    void move_assign(Array&); // Move semantics for assignment

    /// Construct an empty array of the specified type and return just
    /// the reference to the underlying memory.
    static size_t create_empty_array(ColumnDef, Allocator&);

    // Parent tracking
    bool HasParent() const TIGHTDB_NOEXCEPT {return m_parent != NULL;}
    void SetParent(ArrayParent *parent, size_t ndx_in_parent) TIGHTDB_NOEXCEPT;
    void UpdateParentNdx(int diff) {m_parentNdx += diff;}
    ArrayParent *GetParent() const TIGHTDB_NOEXCEPT {return m_parent;}
    size_t GetParentNdx() const TIGHTDB_NOEXCEPT {return m_parentNdx;}
    bool UpdateFromParent() TIGHTDB_NOEXCEPT;

    bool IsValid() const TIGHTDB_NOEXCEPT {return m_data != NULL;}
    void Invalidate() const TIGHTDB_NOEXCEPT {m_data = NULL;}

    size_t size() const TIGHTDB_NOEXCEPT {return m_len;}
    bool is_empty() const TIGHTDB_NOEXCEPT {return m_len == 0;}

    void Insert(size_t ndx, int64_t value);
    void add(int64_t value);
    void Set(size_t ndx, int64_t value);
    template<size_t w> void Set(size_t ndx, int64_t value);

    int64_t Get(size_t ndx) const TIGHTDB_NOEXCEPT;
    template<size_t w> int64_t Get(size_t ndx) const TIGHTDB_NOEXCEPT;

    size_t GetAsRef(size_t ndx) const TIGHTDB_NOEXCEPT;
    size_t GetAsSizeT(size_t ndx) const TIGHTDB_NOEXCEPT;

    int64_t operator[](size_t ndx) const TIGHTDB_NOEXCEPT {return Get(ndx);}
    int64_t back() const TIGHTDB_NOEXCEPT;
    void Delete(size_t ndx);
    void Clear();

    // Direct access methods
    const Array* GetBlock(size_t ndx, Array& arr, size_t& off,
                          bool use_retval = false) const TIGHTDB_NOEXCEPT; // FIXME: Constness is not propagated to the sub-array
    int64_t column_get(std::size_t ndx) const TIGHTDB_NOEXCEPT;
    StringData string_column_get(std::size_t ndx) const TIGHTDB_NOEXCEPT;
    size_t ColumnFind(int64_t target, size_t ref, Array& cache) const;
    typedef StringData (*StringGetter)(void*, size_t); // Pre-declare getter function from string index
    size_t IndexStringFindFirst(StringData value, void* column, StringGetter get_func) const;
    void   IndexStringFindAll(Array& result, StringData value, void* column, StringGetter get_func) const;
    size_t IndexStringCount(StringData value, void* column, StringGetter get_func) const;

    void SetAllToZero();
    void Increment(int64_t value, size_t start=0, size_t end=(size_t)-1);
    void IncrementIf(int64_t limit, int64_t value);
    template <size_t w> void Adjust(size_t start, int64_t diff);
    void Adjust(size_t start, int64_t diff);
    template <size_t w> bool Increment(int64_t value, size_t start, size_t end);

    size_t FindPos(int64_t value) const TIGHTDB_NOEXCEPT;
    size_t FindPos2(int64_t value) const;
    size_t FindGTE(int64_t target, size_t start) const;
    void Preset(int64_t min, int64_t max, size_t count);
    void Preset(size_t bitwidth, size_t count);

    void FindAllHamming(Array& result, uint64_t value, size_t maxdist, size_t offset=0) const;
    int64_t sum(size_t start = 0, size_t end = (size_t)-1) const;
    size_t count(int64_t value) const;
    bool maximum(int64_t& result, size_t start = 0, size_t end = (size_t)-1) const;
    bool minimum(int64_t& result, size_t start = 0, size_t end = (size_t)-1) const;
    void sort(void);
    void ReferenceSort(Array &ref);
    void Resize(size_t count);

    /// Returns true if type is not coldef_InnerNode
    bool is_leaf() const TIGHTDB_NOEXCEPT { return !m_isNode; }

    /// Returns true if type is either coldef_HasRefs or coldef_InnerNode
    bool HasRefs() const TIGHTDB_NOEXCEPT { return m_hasRefs; }

    // FIXME: Remove this, wrong terminology
    bool IsNode() const TIGHTDB_NOEXCEPT { return m_isNode; }

    bool IsIndexNode() const  TIGHTDB_NOEXCEPT { return get_indexflag_from_header(); }
    void SetIsIndexNode(bool value) { set_header_indexflag(value); }
    Array GetSubArray(size_t ndx) const TIGHTDB_NOEXCEPT; // FIXME: Constness is not propagated to the sub-array. This constitutes a real problem, because modifying the returned array may cause the parent to be modified too.
    size_t GetRef() const TIGHTDB_NOEXCEPT { return m_ref; }
    void Destroy();

    Allocator& GetAllocator() const TIGHTDB_NOEXCEPT {return m_alloc;}

    // Serialization
    template<class S> size_t Write(S& target, bool recurse=true, bool persist=false) const;
    template<class S> void WriteAt(size_t pos, S& out) const;
    size_t GetByteSize(bool align=false) const;
    std::vector<int64_t> ToVector(void) const;

    /// Compare two arrays for equality.
    bool Compare(const Array&) const;

    // Main finding function - used for find_first, find_all, sum, max, min, etc.
    void find(int cond, Action action, int64_t value, size_t start, size_t end, size_t baseindex, QueryState<int64_t>* state) const;

    template <class cond, Action action, size_t bitwidth, class Callback>
    void find(int64_t value, size_t start, size_t end, size_t baseindex, QueryState<int64_t>* state, Callback callback) const;

    template <class cond, Action action, size_t bitwidth>
    void find(int64_t value, size_t start, size_t end, size_t baseindex, QueryState<int64_t>* state) const;

    template <class cond, Action action, class Callback>
    void find(int64_t value, size_t start, size_t end, size_t baseindex, QueryState<int64_t>* state, Callback callback) const;

    // Optimized implementation for release mode
    template <class cond2, Action action, size_t bitwidth, class Callback>
    void find_optimized(int64_t value, size_t start, size_t end, size_t baseindex, QueryState<int64_t>* state, Callback callback) const;

    // Reference implementation of find() - verifies result from optimized version if debug mode
    template <class cond2, Action action, size_t bitwidth, class Callback>
    int64_t find_reference(int64_t value, size_t start, size_t end, size_t baseindex, QueryState<int64_t>* state, Callback callback) const;

    // Called for each search result
    template <Action action, class Callback> bool find_action(size_t index, int64_t value, QueryState<int64_t>* state, Callback callback) const;
    template <Action action, class Callback> bool find_action_pattern(size_t index, uint64_t pattern, QueryState<int64_t>* state, Callback callback) const;

    // Wrappers for backwards compatibility and for simple use without setting up state initialization etc
    template <class cond> size_t find_first(int64_t value, size_t start = 0, size_t end = size_t(-1)) const;
    void find_all(Array& result, int64_t value, size_t colOffset = 0, size_t start = 0, size_t end = (size_t)-1) const;
    size_t find_first(int64_t value, size_t start = 0, size_t end = size_t(-1)) const;

    // Non-SSE find for the four functions Equal/NotEqual/Less/Greater
    template <class cond2, Action action, size_t bitwidth, class Callback>
    bool Compare(int64_t value, size_t start, size_t end, size_t baseindex, QueryState<int64_t>* state, Callback callback) const;

    // Non-SSE find for Equal/NotEqual
    template <bool eq, Action action, size_t width, class Callback>
    inline bool CompareEquality(int64_t value, size_t start, size_t end, size_t baseindex, QueryState<int64_t>* state, Callback callback) const;

    // Non-SSE find for Less/Greater
    template <bool gt, Action action, size_t bitwidth, class Callback>
    bool CompareRelation(int64_t value, size_t start, size_t end, size_t baseindex, QueryState<int64_t>* state, Callback callback) const;

    // SSE find for the four functions Equal/NotEqual/Less/Greater
#ifdef TIGHTDB_COMPILER_SSE
    template <class cond2, Action action, size_t width, class Callback>
    size_t FindSSE(int64_t value, __m128i *data, size_t items, QueryState<int64_t>* state, size_t baseindex, Callback callback) const;
#endif

    template <size_t width> inline bool TestZero(uint64_t value) const;         // Tests value for 0-elements
    template <bool eq, size_t width>size_t FindZero(uint64_t v) const;          // Finds position of 0/non-zero element
    template<size_t width> uint64_t cascade(uint64_t a) const;                  // Sets uppermost bits of non-zero elements
    template <bool gt, size_t width>int64_t FindGTLT_Magic(int64_t v) const;    // Compute magic constant needed for searching for value 'v' using bit hacks
    template <size_t width> inline int64_t LowerBits(void) const;               // Return chunk with lower bit set in each element
    size_t FirstSetBit(unsigned int v) const;
    size_t FirstSetBit64(int64_t v) const;
    template <size_t w> int64_t GetUniversal(const char* const data, const size_t ndx) const;

    // Find value greater/less in 64-bit chunk - only works for positive values
    template <bool gt, Action action, size_t width, class Callback>
    bool FindGTLT_Fast(uint64_t chunk, uint64_t magic, QueryState<int64_t>* state, size_t baseindex, Callback callback) const;

    // Find value greater/less in 64-bit chunk - no constraints
    template <bool gt, Action action, size_t width, class Callback>
    bool FindGTLT(int64_t v, uint64_t chunk, QueryState<int64_t>* state, size_t baseindex, Callback callback) const;

    /// The meaning of 'width' depends on the context in which this
    /// array is used.
    std::size_t get_width() const TIGHTDB_NOEXCEPT { return m_width; }

#ifdef TIGHTDB_DEBUG
    void Print() const;
    void Verify() const;
    void ToDot(std::ostream& out, const char* title=NULL) const;
    void Stats(MemStats& stats) const;
#endif // TIGHTDB_DEBUG

    // FIXME: Should not be mutable
    // FIXME: Should not be public
    mutable char* m_data;

    static bool is_index_node(std::size_t ref, const Allocator&);

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
    static std::size_t get_len_from_header(const char*) TIGHTDB_NOEXCEPT;
    static std::size_t get_capacity_from_header(const char*) TIGHTDB_NOEXCEPT;
    static bool get_isnode_from_header(const char*) TIGHTDB_NOEXCEPT; // DEPRECATED!

    static ColumnDef get_coldef_from_header(const char*) TIGHTDB_NOEXCEPT;

    static std::size_t get_alloc_size_from_header(const char*) TIGHTDB_NOEXCEPT;

private:
    typedef bool (*CallbackDummy)(int64_t);

    template<size_t w> bool MinMax(size_t from, size_t to, uint64_t maxdiff, int64_t* min, int64_t* max);
    Array& operator=(const Array&) {return *this;} // not allowed
    template<size_t w> void QuickSort(size_t lo, size_t hi);
    void QuickSort(size_t lo, size_t hi);
    void ReferenceQuickSort(Array& ref);
    template<size_t w> void ReferenceQuickSort(size_t lo, size_t hi, Array& ref);

    template<size_t w> void sort();
    template<size_t w> void ReferenceSort(Array& ref);

    template<size_t w> int64_t sum(size_t start, size_t end) const;
    template<size_t w> size_t FindPos(int64_t target) const TIGHTDB_NOEXCEPT;

protected:
    friend class GroupWriter;

//    void AddPositiveLocal(int64_t value);

    void init_from_ref(size_t ref) TIGHTDB_NOEXCEPT;
    void CreateFromHeader(char* header, size_t ref=0) TIGHTDB_NOEXCEPT;
    void CreateFromHeaderDirect(char* header, size_t ref=0) TIGHTDB_NOEXCEPT;
    void update_ref_in_parent();

    virtual size_t CalcByteLen(size_t count, size_t width) const;
    virtual size_t CalcItemCount(size_t bytes, size_t width) const TIGHTDB_NOEXCEPT;
    virtual WidthType GetWidthType() const { return wtype_Bits; }

    bool get_isleaf_from_header() const TIGHTDB_NOEXCEPT;
    bool get_hasrefs_from_header() const TIGHTDB_NOEXCEPT;
    bool get_indexflag_from_header() const TIGHTDB_NOEXCEPT;
    WidthType get_wtype_from_header() const TIGHTDB_NOEXCEPT;
    int get_width_from_header() const TIGHTDB_NOEXCEPT;
    std::size_t get_len_from_header() const TIGHTDB_NOEXCEPT;
    std::size_t get_capacity_from_header() const TIGHTDB_NOEXCEPT;
    bool get_isnode_from_header() const TIGHTDB_NOEXCEPT; // DEPRECATED!

    void set_header_isleaf(bool value) TIGHTDB_NOEXCEPT;
    void set_header_hasrefs(bool value) TIGHTDB_NOEXCEPT;
    void set_header_indexflag(bool value) TIGHTDB_NOEXCEPT;
    void set_header_wtype(WidthType value) TIGHTDB_NOEXCEPT;
    void set_header_width(int value) TIGHTDB_NOEXCEPT;
    void set_header_len(std::size_t value) TIGHTDB_NOEXCEPT;
    void set_header_capacity(std::size_t value) TIGHTDB_NOEXCEPT;
    void set_header_isnode(bool value) TIGHTDB_NOEXCEPT; // DEPRECATED!

    static void set_header_isleaf(bool value, char* header) TIGHTDB_NOEXCEPT;
    static void set_header_hasrefs(bool value, char* header) TIGHTDB_NOEXCEPT;
    static void set_header_indexflag(bool value, char* header) TIGHTDB_NOEXCEPT;
    static void set_header_wtype(WidthType value, char* header) TIGHTDB_NOEXCEPT;
    static void set_header_width(int value, char* header) TIGHTDB_NOEXCEPT;
    static void set_header_len(std::size_t value, char* header) TIGHTDB_NOEXCEPT;
    static void set_header_capacity(std::size_t value, char* header) TIGHTDB_NOEXCEPT;
    static void set_header_isnode(bool value, char* header) TIGHTDB_NOEXCEPT; // DEPRECATED!

    static void init_header(char* header, bool is_node, bool has_refs, WidthType width_type,
                            int width, std::size_t length, std::size_t capacity) TIGHTDB_NOEXCEPT;

    template<size_t width> void SetWidth() TIGHTDB_NOEXCEPT;
    void SetWidth(size_t) TIGHTDB_NOEXCEPT;
    void Alloc(size_t count, size_t width);
    void CopyOnWrite();

    // Find the leaf node corresponding to the specified column-level
    // index. The specified array must be the root node of a B+ tree,
    // and that root node must not itself be a leaf. This implies, of
    // course, that the column must not be empty.
    //
    // \return (leaf_header, leaf_index) where 'leaf_header' is a
    // pointer to the header of the located leaf, and 'leaf_index' is
    // the local index within that leaf corresponding to the specified
    // column-level index.
    static std::pair<const char*, std::size_t> find_leaf(const Array* root, std::size_t i) TIGHTDB_NOEXCEPT;

    static std::size_t get_as_size(const char* header, std::size_t ndx) TIGHTDB_NOEXCEPT;

    static std::pair<std::size_t, std::size_t> get_two_as_size(const char* header,
                                                               std::size_t ndx) TIGHTDB_NOEXCEPT;

private:
    size_t m_ref;
    template <bool max, size_t w> bool minmax(int64_t& result, size_t start, size_t end) const;

protected:
    size_t m_len;           // items currently stored
    size_t m_capacity;      // max item capacity
// FIXME: m_width Should be an 'int'
    size_t m_width;         // size of an item in bits or bytes depending on
    bool m_isNode;          // is it a Node or Leaf array
    bool m_hasRefs;         //

private:
    ArrayParent *m_parent;
    size_t m_parentNdx;

    Allocator& m_alloc;

protected:
    static const size_t initial_capacity = 128;
    static size_t create_empty_array(ColumnDef, WidthType, Allocator&);

    void update_child_ref(size_t child_ndx, size_t new_ref) TIGHTDB_OVERRIDE;
    size_t get_child_ref(size_t child_ndx) const TIGHTDB_NOEXCEPT TIGHTDB_OVERRIDE;

// FIXME: below should be moved to a specific ArrayNumber class
protected:
    // Getters and Setters for adaptive-packed arrays
    typedef int64_t (Array::*Getter)(size_t) const; // Note: getters must not throw
    typedef void (Array::*Setter)(size_t, int64_t);
    typedef void (Array::*Finder)(int64_t, size_t, size_t, size_t, QueryState<int64_t>*) const;

    Getter m_getter;
    Setter m_setter;
    Finder m_finder[cond_Count]; // one for each COND_XXX enum

    int64_t m_lbound;       // min number that can be stored with current m_width
    int64_t m_ubound;       // max number that can be stored with current m_width
};





// Implementation:

class QueryStateBase {virtual void dyncast(){}};

template <> class QueryState<int64_t> : public QueryStateBase {
public:
    int64_t m_state;
    size_t m_match_count;
    size_t m_limit;

    template <Action action> bool uses_val(void)
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
            m_state = (int64_t)akku;
        else
            TIGHTDB_ASSERT(false);
    }

    template <Action action, bool pattern, class Callback>
    inline bool match(size_t index, uint64_t indexpattern, int64_t value, Callback callback)
    {
        if (pattern) {
            if (action == act_Count) {
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
            if(value > m_state)
                m_state = value;
        }
        else if (action == act_Min) {
            if(value < m_state)
                m_state = value;
        }
        else if (action == act_Sum)
            m_state += value;
        else if (action == act_Count) {
            m_state++;
            m_match_count = size_t(m_state);
        }
        else if (action == act_FindAll)
            ((Array*)m_state)->add(index);
        else if (action == act_ReturnFirst) {
            m_state = index;
            return false;
        }
        else
            TIGHTDB_ASSERT(false);
        return true;
    }
};

// Used only for Basic-types: currently float and double
template <class R> class QueryState : public QueryStateBase {
public:
    R m_state;
    size_t m_match_count;
    size_t m_limit;

    template <Action action> bool uses_val()
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

    template <Action action, bool pattern, class Callback, typename resulttype>
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
        return true;
    }
};



inline Array::Array(size_t ref, ArrayParent* parent, size_t pndx, Allocator& alloc) TIGHTDB_NOEXCEPT:
    m_data(NULL), m_len(0), m_capacity(0), m_width(0), m_isNode(false), m_hasRefs(false),
    m_parent(parent), m_parentNdx(pndx), m_alloc(alloc), m_lbound(0), m_ubound(0)
{
    init_from_ref(ref);
}

inline Array::Array(ColumnDef type, ArrayParent* parent, size_t pndx, Allocator& alloc):
    m_data(NULL), m_len(0), m_capacity(0), m_width(0), m_isNode(false), m_hasRefs(false),
    m_parent(parent), m_parentNdx(pndx), m_alloc(alloc), m_lbound(0), m_ubound(0)
{
    const size_t ref = create_empty_array(type, alloc); // Throws
    init_from_ref(ref);
    update_ref_in_parent();
}

// Creates new array (but invalid, call UpdateRef or SetType to init)
inline Array::Array(Allocator& alloc) TIGHTDB_NOEXCEPT:
    m_data(NULL), m_ref(0), m_len(0), m_capacity(0), m_width((size_t)-1), m_isNode(false),
    m_parent(NULL), m_parentNdx(0), m_alloc(alloc) {}

// Copy-constructor
// Note that this array now own the ref. Should only be used when
// the source array goes away right after (like return values from functions)
inline Array::Array(const Array& src) TIGHTDB_NOEXCEPT:
    ArrayParent(), m_parent(src.m_parent), m_parentNdx(src.m_parentNdx), m_alloc(src.m_alloc)
{
    const size_t ref = src.GetRef();
    init_from_ref(ref);
    src.Invalidate();
}

// Fastest way to instantiate an Array. For use with GetDirect() that only fills out m_width, m_data
// and a few other basic things needed for read-only access. Or for use if you just want a way to call
// some methods written in Array.*
inline Array::Array(no_prealloc_tag) TIGHTDB_NOEXCEPT: m_alloc(Allocator::get_default()) {}


inline int64_t Array::back() const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(m_len);
    return Get(m_len-1);
}

inline int64_t Array::Get(std::size_t ndx) const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(ndx < m_len);
    return (this->*m_getter)(ndx);

// Two ideas that are not efficient but may be worth looking into again:
/*
    // Assume correct width is found early in TIGHTDB_TEMPEX, which is the case for B tree offsets that
    // are probably either 2^16 long. Turns out to be 25% faster if found immediately, but 50-300% slower
    // if found later
    TIGHTDB_TEMPEX(return Get, (ndx));
*/
/*
    // Slightly slower in both of the if-cases. Also needs an matchcount m_len check too, to avoid
    // reading beyond array.
    if (m_width >= 8 && m_len > ndx + 7)
        return Get<64>(ndx >> m_shift) & m_widthmask;
    else
        return (this->*m_getter)(ndx);
*/
}

inline std::size_t Array::GetAsRef(std::size_t ndx) const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(ndx < m_len);
    TIGHTDB_ASSERT(m_hasRefs);
    const int64_t v = Get(ndx);
    return to_ref(v);
}

inline std::size_t Array::GetAsSizeT(std::size_t ndx) const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(ndx < m_len);
    const int64_t v = Get(ndx);
    return to_size_t(v);
}


inline Array Array::GetSubArray(std::size_t ndx) const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(ndx < m_len);
    TIGHTDB_ASSERT(m_hasRefs);

    const std::size_t ref = std::size_t(Get(ndx));
    TIGHTDB_ASSERT(ref);

    return Array(ref, const_cast<Array*>(this), ndx, m_alloc); // FIXME: Constness is not propagated to the sub-array. This constitutes a real problem, because modifying the returned array genrally causes the parent to be modified too.
}


inline bool Array::is_index_node(std::size_t ref, const Allocator& alloc)
{
    TIGHTDB_ASSERT(ref);
    return get_indexflag_from_header(static_cast<char*>(alloc.Translate(ref)));
}



//-------------------------------------------------

inline bool Array::get_isleaf_from_header(const char* header) TIGHTDB_NOEXCEPT
{
    const uint8_t* h = reinterpret_cast<const uint8_t*>(header);
    return (h[0] & 0x80) == 0;
}
inline bool Array::get_hasrefs_from_header(const char* header) TIGHTDB_NOEXCEPT
{
    const uint8_t* h = reinterpret_cast<const uint8_t*>(header);
    return (h[0] & 0x40) != 0;
}
inline bool Array::get_indexflag_from_header(const char* header) TIGHTDB_NOEXCEPT
{
    const uint8_t* h = reinterpret_cast<const uint8_t*>(header);
    return (h[0] & 0x20) != 0;
}
inline Array::WidthType Array::get_wtype_from_header(const char* header) TIGHTDB_NOEXCEPT
{
    const uint8_t* h = reinterpret_cast<const uint8_t*>(header);
    return WidthType((h[0] & 0x18) >> 3);
}
inline int Array::get_width_from_header(const char* header) TIGHTDB_NOEXCEPT
{
    const uint8_t* h = reinterpret_cast<const uint8_t*>(header);
    return (1 << (h[0] & 0x07)) >> 1;
}
inline std::size_t Array::get_len_from_header(const char* header) TIGHTDB_NOEXCEPT
{
    const uint8_t* h = reinterpret_cast<const uint8_t*>(header);
    return (std::size_t(h[1]) << 16) + (std::size_t(h[2]) << 8) + h[3];
}
inline std::size_t Array::get_capacity_from_header(const char* header) TIGHTDB_NOEXCEPT
{
    const uint8_t* h = reinterpret_cast<const uint8_t*>(header);
    return (std::size_t(h[4]) << 16) + (std::size_t(h[5]) << 8) + h[6];
}
inline bool Array::get_isnode_from_header(const char* header) TIGHTDB_NOEXCEPT
{
    return !get_isleaf_from_header(header);
}


inline char* Array::get_data_from_header(char* header) TIGHTDB_NOEXCEPT
{
    return header + 8;
}
inline char* Array::get_header_from_data(char* data) TIGHTDB_NOEXCEPT
{
    return data - 8;
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
inline std::size_t Array::get_len_from_header() const TIGHTDB_NOEXCEPT
{
    return get_len_from_header(get_header_from_data(m_data));
}
inline std::size_t Array::get_capacity_from_header() const TIGHTDB_NOEXCEPT
{
    return get_capacity_from_header(get_header_from_data(m_data));
}
inline bool Array::get_isnode_from_header() const TIGHTDB_NOEXCEPT
{
    return get_isnode_from_header(get_header_from_data(m_data));
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
    // 0: bits      (width/8) * length
    // 1: multiply  width * length
    // 2: ignore    1 * length
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

inline void Array::set_header_len(std::size_t value, char* header) TIGHTDB_NOEXCEPT
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

inline void Array::set_header_isnode(bool value, char* header) TIGHTDB_NOEXCEPT
{
    set_header_isleaf(!value, header);
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
inline void Array::set_header_len(std::size_t value) TIGHTDB_NOEXCEPT
{
    set_header_len(value, get_header_from_data(m_data));
}
inline void Array::set_header_capacity(std::size_t value) TIGHTDB_NOEXCEPT
{
    set_header_capacity(value, get_header_from_data(m_data));
}
inline void Array::set_header_isnode(bool value) TIGHTDB_NOEXCEPT
{
    set_header_isnode(value, get_header_from_data(m_data));
}


inline Array::ColumnDef Array::get_coldef_from_header(const char* header) TIGHTDB_NOEXCEPT
{
    if (!get_isleaf_from_header(header)) return coldef_InnerNode;
    if (get_hasrefs_from_header(header)) return coldef_HasRefs;
    return coldef_Normal;
}


inline std::size_t Array::get_alloc_size_from_header(const char* header) TIGHTDB_NOEXCEPT
{
    // Calculate full length of array in bytes, including padding
    // for 64bit alignment (that may be composed of random bits)
    std::size_t size = get_len_from_header(header);

    // Adjust length to number of bytes
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
    return size;
}


inline void Array::init_header(char* header, bool is_node, bool has_refs, WidthType width_type,
                               int width, std::size_t length, std::size_t capacity) TIGHTDB_NOEXCEPT
{
    // Note: Since the header layout contains unallocated bit and/or
    // bytes, it is important that we put the entire 8 byte header
    // into a well defined state initially. Note also: The C++11
    // standard does not guarantee that int64_t is available on all
    // platforms.
    *reinterpret_cast<int64_t*>(header) = 0;
    set_header_isnode(is_node, header);
    set_header_hasrefs(has_refs, header);
    set_header_wtype(width_type, header);
    set_header_width(width, header);
    set_header_len(length, header);
    set_header_capacity(capacity, header);
}


//-------------------------------------------------

template<class S> size_t Array::Write(S& out, bool recurse, bool persist) const
{
    TIGHTDB_ASSERT(IsValid());

    // Ignore un-changed arrays when persisting
    if (persist && m_alloc.IsReadOnly(m_ref)) return m_ref;

    if (recurse && m_hasRefs) {
        // Temp array for updated refs
        Array newRefs(m_isNode ? coldef_InnerNode : coldef_HasRefs);

        // Make sure that all flags are retained
        if (IsIndexNode())
            newRefs.SetIsIndexNode(true);

        // First write out all sub-arrays
        const size_t count = size();
        for (size_t i = 0; i < count; ++i) {
            const size_t ref = GetAsRef(i);
            if (ref == 0 || ref & 0x1) {
                // zero-refs and refs that are not 64-aligned do not point to sub-trees
                newRefs.add(ref);
            }
            else if (persist && m_alloc.IsReadOnly(ref)) {
                // Ignore un-changed arrays when persisting
                newRefs.add(ref);
            }
            else {
                const Array sub(ref, NULL, 0, GetAllocator());
                const size_t sub_pos = sub.Write(out, true, persist);
                TIGHTDB_ASSERT((sub_pos & 0x7) == 0); // 64bit alignment
                newRefs.add(sub_pos);
            }
        }

        // Write out the replacement array
        // (but don't write sub-tree as it has alredy been written)
        const size_t refs_pos = newRefs.Write(out, false, persist);

        // Clean-up
        newRefs.SetType(coldef_Normal); // avoid recursive del
        newRefs.Destroy();

        return refs_pos; // Return position
    }

    // TODO: replace capacity with checksum

    // Write array
    const char* header = get_header_from_data(m_data);
    std::size_t size = get_alloc_size_from_header(header);
    const size_t array_pos = out.write(header, size);
    TIGHTDB_ASSERT((array_pos & 0x7) == 0); /// 64bit alignment

    return array_pos; // Return position of this array
}

template<class S> void Array::WriteAt(std::size_t pos, S& out) const
{
    TIGHTDB_ASSERT(IsValid());

    // TODO: replace capacity with checksum

    // Write array
    const char* header = get_header_from_data(m_data);
    std::size_t size = get_alloc_size_from_header(header);
    out.WriteAt(pos, header, size);
}

inline void Array::move_assign(Array& a)
{
    // FIXME: It will likely be a lot better for the optimizer if we
    // did a member-wise copy, rather than recreating the state from
    // the referenced data. This is important because TableView, for
    // example, relies on long chains of moves to be optimized away
    // completely. This change should be a 'no-brainer'.
    Destroy();
    UpdateRef(a.GetRef());
    a.Invalidate();
}

inline size_t Array::create_empty_array(ColumnDef type, Allocator& alloc)
{
    return create_empty_array(type, wtype_Bits, alloc); // Throws
}

inline void Array::update_ref_in_parent()
{
    if (!m_parent) return;
    m_parent->update_child_ref(m_parentNdx, m_ref);
}


inline void Array::update_child_ref(size_t child_ndx, size_t new_ref)
{
    Set(child_ndx, new_ref);
}

inline size_t Array::get_child_ref(size_t child_ndx) const TIGHTDB_NOEXCEPT
{
    return GetAsRef(child_ndx);
}



//*************************************************************************************
// Finding code                                                                       *
//*************************************************************************************

template <size_t w> int64_t Array::GetUniversal(const char* const data, const size_t ndx) const
{
    if (w == 0) {
        return 0;
    }
    else if (w == 1) {
        const size_t offset = ndx >> 3;
        return (data[offset] >> (ndx & 7)) & 0x01;
    }
    else if (w == 2) {
        const size_t offset = ndx >> 2;
        return (data[offset] >> ((ndx & 3) << 1)) & 0x03;
    }
    else if (w == 4) {
        const size_t offset = ndx >> 1;
        return (data[offset] >> ((ndx & 1) << 2)) & 0x0F;
    }
    else if (w == 8) {
        return *((const signed char*)(data + ndx));
    }
    else if (w == 16) {
        const size_t offset = ndx * 2;
        return *(const int16_t*)(data + offset);
    }
    else if (w == 32) {
        const size_t offset = ndx * 4;
        return *(const int32_t*)(data + offset);
    }
    else if (w == 64) {
        const size_t offset = ndx * 8;
        return *((const int64_t*)(data + offset));
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
template <Action action, class Callback> bool Array::find_action(size_t index, int64_t value, QueryState<int64_t>* state, Callback callback) const
{
    return state->match<action, false, Callback>(index, 0, value, callback);
}
template <Action action, class Callback> bool Array::find_action_pattern(size_t index, uint64_t pattern, QueryState<int64_t>* state, Callback callback) const
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
template <class cond2, Action action, size_t bitwidth, class Callback> void Array::find_optimized(int64_t value, size_t start, size_t end, size_t baseindex, QueryState<int64_t>* state, Callback callback) const
{
    cond2 C;
    TIGHTDB_ASSERT(start <= m_len && (end <= m_len || end == (size_t)-1) && start <= end);

    // Test first few items with no initial time overhead
    if (start > 0) {
        if (m_len > start && C(Get<bitwidth>(start), value) && start < end) { if (!find_action<action, Callback>(start + baseindex, Get<bitwidth>(start), state, callback)) return;} ++start;
        if (m_len > start && C(Get<bitwidth>(start), value) && start < end) { if (!find_action<action, Callback>(start + baseindex, Get<bitwidth>(start), state, callback)) return;} ++start;
        if (m_len > start && C(Get<bitwidth>(start), value) && start < end) { if (!find_action<action, Callback>(start + baseindex, Get<bitwidth>(start), state, callback)) return;} ++start;
        if (m_len > start && C(Get<bitwidth>(start), value) && start < end) { if (!find_action<action, Callback>(start + baseindex, Get<bitwidth>(start), state, callback)) return;} ++start;
    }

    if (!(m_len > start && start < end))
        return;

    if (end == (size_t)-1) end = m_len;

    // Return immediately if no items in array can match (such as if cond2 == Greater and value == 100 and m_ubound == 15).
    if (!C.can_match(value, m_lbound, m_ubound))
        return;

    // call find_action() on all items in array if all items are guaranteed to match (such as cond2 == NotEqual and value == 100 and m_ubound == 15)
    if (C.will_match(value, m_lbound, m_ubound)) {
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
                    return;
        }
        return;
    }

    // finder cannot handle this bitwidth
    TIGHTDB_ASSERT(m_width != 0);

#if defined(TIGHTDB_COMPILER_SSE)
    if ((cpuid_sse<42>() &&                                  (end - start >= sizeof(__m128i) && m_width >= 8))
    ||  (cpuid_sse<30>() && (SameType<cond2, Equal>::value && end - start >= sizeof(__m128i) && m_width >= 8 && m_width < 64))) {

        // FindSSE() must start at 16-byte boundary, so search area before that using CompareEquality()
        __m128i* const a = (__m128i *)round_up(m_data + start * bitwidth / 8, sizeof(__m128i));
        __m128i* const b = (__m128i *)round_down(m_data + end * bitwidth / 8, sizeof(__m128i));

        if (!Compare<cond2, action, bitwidth, Callback>(value, start, (reinterpret_cast<char*>(a) - m_data) * 8 / no0(bitwidth), baseindex, state, callback))
            return;

        // Search aligned area with SSE
        if (b > a) {
            if (cpuid_sse<42>()) {
                if (!FindSSE<cond2, action, bitwidth, Callback>(value, a, b - a, state, baseindex + ((reinterpret_cast<char*>(a) - m_data) * 8 / no0(bitwidth)), callback))
                    return;
                }
                else if (cpuid_sse<30>()) {

                if (!FindSSE<Equal, action, bitwidth, Callback>(value, a, b - a, state, baseindex + ((reinterpret_cast<char*>(a) - m_data) * 8 / no0(bitwidth)), callback))
                    return;
                }
        }

        // Search remainder with CompareEquality()
        if (!Compare<cond2, action, bitwidth, Callback>(value, (reinterpret_cast<char*>(b) - m_data) * 8 / no0(bitwidth), end, baseindex, state, callback))
            return;

        return;
    }
    else {
        Compare<cond2, action, bitwidth, Callback>(value, start, end, baseindex, state, callback);
        return;
    }
#else
Compare<cond2, action, bitwidth, Callback>(value, start, end, baseindex, state, callback);
return;
#endif
}

template <size_t width> inline int64_t Array::LowerBits(void) const
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
template <size_t width> inline bool Array::TestZero(uint64_t value) const
{
    uint64_t hasZeroByte;
    uint64_t lower = LowerBits<width>();
    uint64_t upper = LowerBits<width>() * 1ULL << (width == 0 ? 0 : (width - 1ULL));
    hasZeroByte = (value - lower) & ~value & upper;
    return hasZeroByte != 0;
}

// Finds first zero (if eq == true) or non-zero (if eq == false) element in v and returns its position.
// IMPORTANT: This function assumes that at least 1 item matches (test this with TestZero() or other means first)!
template <bool eq, size_t width>size_t Array::FindZero(uint64_t v) const
{
    size_t start = 0;
    uint64_t hasZeroByte;
    uint64_t mask = (width == 64 ? ~0ULL : ((1ULL << (width == 64 ? 0 : width)) - 1ULL)); // Warning free way of computing (1ULL << width) - 1

    if (eq == (((v >> (width * start)) & mask) == 0)) {
        return 0;
    }

    // Bisection optimization, speeds up small bitwidths with high match frequency. More partions than 2 do NOT pay off because
    // the work done by TestZero() is wasted for the cases where the value exists in first half, but useful if it exists in last
    // half. Sweet spot turns out to be the widths and partitions below.
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
        TIGHTDB_ASSERT(start <= 8 * sizeof(v)); // You must only call FindZero() if you are sure that at least 1 item matches
        start++;
    }

    return start;
}

// Generate a magic constant used for later bithacks
template <bool gt, size_t width>int64_t Array::FindGTLT_Magic(int64_t v) const
{
    uint64_t mask1 = (width == 64 ? ~0ULL : ((1ULL << (width == 64 ? 0 : width)) - 1ULL)); // Warning free way of computing (1ULL << width) - 1
    uint64_t mask2 = mask1 >> 1;
    uint64_t magic = gt ? (~0ULL / no0(mask1) * (mask2 - v)) : (~0ULL / no0(mask1) * v);
    return magic;
}

template <bool gt, Action action, size_t width, class Callback> bool Array::FindGTLT_Fast(uint64_t chunk, uint64_t magic, QueryState<int64_t>* state, size_t baseindex, Callback callback) const
{
    // Tests if a a chunk of values contains values that are greater (if gt == true) or less (if gt == false) than v.
    // Fast, but limited to work when all values in the chunk are positive.

    uint64_t mask1 = (width == 64 ? ~0ULL : ((1ULL << (width == 64 ? 0 : width)) - 1ULL)); // Warning free way of computing (1ULL << width) - 1
    uint64_t mask2 = mask1 >> 1;
    uint64_t m = gt ? (((chunk + magic) | chunk) & ~0ULL / no0(mask1) * (mask2 + 1)) : ((chunk - magic) & ~chunk&~0ULL/no0(mask1)*(mask2+1));
    size_t p = 0;
    while(m) {
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


template <bool gt, Action action, size_t width, class Callback> bool Array::FindGTLT(int64_t v, uint64_t chunk, QueryState<int64_t>* state, size_t baseindex, Callback callback) const
{
    // Fínd items in 'chunk' that are greater (if gt == true) or smaller (if gt == false) than 'v'. Fixme, __forceinline can make it crash in vS2010 - find out why
    if (width == 1) {
        for (size_t t = 0; t < 64; t++) {
            if (gt ? (int64_t)(chunk & 0x1) > v : (int64_t)(chunk & 0x1) < v) {if (!find_action<action, Callback>( t + baseindex, (int64_t)(chunk & 0x1), state, callback)) return false;} chunk >>= 1;
        }
    }
    else if (width == 2) {
        // Alot (50% +) faster than loop/compiler-unrolled loop
        if (gt ? (int64_t)(chunk & 0x3) > v : (int64_t)(chunk & 0x3) < v) {if (!find_action<action, Callback>( 0 + baseindex, (int64_t)(chunk & 0x3), state, callback)) return false;} chunk >>= 2;
        if (gt ? (int64_t)(chunk & 0x3) > v : (int64_t)(chunk & 0x3) < v) {if (!find_action<action, Callback>( 1 + baseindex, (int64_t)(chunk & 0x3), state, callback)) return false;} chunk >>= 2;
        if (gt ? (int64_t)(chunk & 0x3) > v : (int64_t)(chunk & 0x3) < v) {if (!find_action<action, Callback>( 2 + baseindex, (int64_t)(chunk & 0x3), state, callback)) return false;} chunk >>= 2;
        if (gt ? (int64_t)(chunk & 0x3) > v : (int64_t)(chunk & 0x3) < v) {if (!find_action<action, Callback>( 3 + baseindex, (int64_t)(chunk & 0x3), state, callback)) return false;} chunk >>= 2;
        if (gt ? (int64_t)(chunk & 0x3) > v : (int64_t)(chunk & 0x3) < v) {if (!find_action<action, Callback>( 4 + baseindex, (int64_t)(chunk & 0x3), state, callback)) return false;} chunk >>= 2;
        if (gt ? (int64_t)(chunk & 0x3) > v : (int64_t)(chunk & 0x3) < v) {if (!find_action<action, Callback>( 5 + baseindex, (int64_t)(chunk & 0x3), state, callback)) return false;} chunk >>= 2;
        if (gt ? (int64_t)(chunk & 0x3) > v : (int64_t)(chunk & 0x3) < v) {if (!find_action<action, Callback>( 6 + baseindex, (int64_t)(chunk & 0x3), state, callback)) return false;} chunk >>= 2;
        if (gt ? (int64_t)(chunk & 0x3) > v : (int64_t)(chunk & 0x3) < v) {if (!find_action<action, Callback>( 7 + baseindex, (int64_t)(chunk & 0x3), state, callback)) return false;} chunk >>= 2;

        if (gt ? (int64_t)(chunk & 0x3) > v : (int64_t)(chunk & 0x3) < v) {if (!find_action<action, Callback>( 8 + baseindex, (int64_t)(chunk & 0x3), state, callback)) return false;} chunk >>= 2;
        if (gt ? (int64_t)(chunk & 0x3) > v : (int64_t)(chunk & 0x3) < v) {if (!find_action<action, Callback>( 9 + baseindex, (int64_t)(chunk & 0x3), state, callback)) return false;} chunk >>= 2;
        if (gt ? (int64_t)(chunk & 0x3) > v : (int64_t)(chunk & 0x3) < v) {if (!find_action<action, Callback>( 10 + baseindex, (int64_t)(chunk & 0x3), state, callback)) return false;} chunk >>= 2;
        if (gt ? (int64_t)(chunk & 0x3) > v : (int64_t)(chunk & 0x3) < v) {if (!find_action<action, Callback>( 11 + baseindex, (int64_t)(chunk & 0x3), state, callback)) return false;} chunk >>= 2;
        if (gt ? (int64_t)(chunk & 0x3) > v : (int64_t)(chunk & 0x3) < v) {if (!find_action<action, Callback>( 12 + baseindex, (int64_t)(chunk & 0x3), state, callback)) return false;} chunk >>= 2;
        if (gt ? (int64_t)(chunk & 0x3) > v : (int64_t)(chunk & 0x3) < v) {if (!find_action<action, Callback>( 13 + baseindex, (int64_t)(chunk & 0x3), state, callback)) return false;} chunk >>= 2;
        if (gt ? (int64_t)(chunk & 0x3) > v : (int64_t)(chunk & 0x3) < v) {if (!find_action<action, Callback>( 14 + baseindex, (int64_t)(chunk & 0x3), state, callback)) return false;} chunk >>= 2;
        if (gt ? (int64_t)(chunk & 0x3) > v : (int64_t)(chunk & 0x3) < v) {if (!find_action<action, Callback>( 15 + baseindex, (int64_t)(chunk & 0x3), state, callback)) return false;} chunk >>= 2;

        if (gt ? (int64_t)(chunk & 0x3) > v : (int64_t)(chunk & 0x3) < v) {if (!find_action<action, Callback>( 16 + baseindex, (int64_t)(chunk & 0x3), state, callback)) return false;} chunk >>= 2;
        if (gt ? (int64_t)(chunk & 0x3) > v : (int64_t)(chunk & 0x3) < v) {if (!find_action<action, Callback>( 17 + baseindex, (int64_t)(chunk & 0x3), state, callback)) return false;} chunk >>= 2;
        if (gt ? (int64_t)(chunk & 0x3) > v : (int64_t)(chunk & 0x3) < v) {if (!find_action<action, Callback>( 18 + baseindex, (int64_t)(chunk & 0x3), state, callback)) return false;} chunk >>= 2;
        if (gt ? (int64_t)(chunk & 0x3) > v : (int64_t)(chunk & 0x3) < v) {if (!find_action<action, Callback>( 19 + baseindex, (int64_t)(chunk & 0x3), state, callback)) return false;} chunk >>= 2;
        if (gt ? (int64_t)(chunk & 0x3) > v : (int64_t)(chunk & 0x3) < v) {if (!find_action<action, Callback>( 20 + baseindex, (int64_t)(chunk & 0x3), state, callback)) return false;} chunk >>= 2;
        if (gt ? (int64_t)(chunk & 0x3) > v : (int64_t)(chunk & 0x3) < v) {if (!find_action<action, Callback>( 21 + baseindex, (int64_t)(chunk & 0x3), state, callback)) return false;} chunk >>= 2;
        if (gt ? (int64_t)(chunk & 0x3) > v : (int64_t)(chunk & 0x3) < v) {if (!find_action<action, Callback>( 22 + baseindex, (int64_t)(chunk & 0x3), state, callback)) return false;} chunk >>= 2;
        if (gt ? (int64_t)(chunk & 0x3) > v : (int64_t)(chunk & 0x3) < v) {if (!find_action<action, Callback>( 23 + baseindex, (int64_t)(chunk & 0x3), state, callback)) return false;} chunk >>= 2;

        if (gt ? (int64_t)(chunk & 0x3) > v : (int64_t)(chunk & 0x3) < v) {if (!find_action<action, Callback>( 24 + baseindex, (int64_t)(chunk & 0x3), state, callback)) return false;} chunk >>= 2;
        if (gt ? (int64_t)(chunk & 0x3) > v : (int64_t)(chunk & 0x3) < v) {if (!find_action<action, Callback>( 25 + baseindex, (int64_t)(chunk & 0x3), state, callback)) return false;} chunk >>= 2;
        if (gt ? (int64_t)(chunk & 0x3) > v : (int64_t)(chunk & 0x3) < v) {if (!find_action<action, Callback>( 26 + baseindex, (int64_t)(chunk & 0x3), state, callback)) return false;} chunk >>= 2;
        if (gt ? (int64_t)(chunk & 0x3) > v : (int64_t)(chunk & 0x3) < v) {if (!find_action<action, Callback>( 27 + baseindex, (int64_t)(chunk & 0x3), state, callback)) return false;} chunk >>= 2;
        if (gt ? (int64_t)(chunk & 0x3) > v : (int64_t)(chunk & 0x3) < v) {if (!find_action<action, Callback>( 28 + baseindex, (int64_t)(chunk & 0x3), state, callback)) return false;} chunk >>= 2;
        if (gt ? (int64_t)(chunk & 0x3) > v : (int64_t)(chunk & 0x3) < v) {if (!find_action<action, Callback>( 29 + baseindex, (int64_t)(chunk & 0x3), state, callback)) return false;} chunk >>= 2;
        if (gt ? (int64_t)(chunk & 0x3) > v : (int64_t)(chunk & 0x3) < v) {if (!find_action<action, Callback>( 30 + baseindex, (int64_t)(chunk & 0x3), state, callback)) return false;} chunk >>= 2;
        if (gt ? (int64_t)(chunk & 0x3) > v : (int64_t)(chunk & 0x3) < v) {if (!find_action<action, Callback>( 31 + baseindex, (int64_t)(chunk & 0x3), state, callback)) return false;} chunk >>= 2;
    }
    else if (width == 4) {
        // 128 ms:
        if (gt ? (int64_t)(chunk & 0xf) > v : (int64_t)(chunk & 0xf) < v) {if (!find_action<action, Callback>( 0 + baseindex, (int64_t)(chunk & 0xf), state, callback)) return false;} chunk >>= 4;
        if (gt ? (int64_t)(chunk & 0xf) > v : (int64_t)(chunk & 0xf) < v) {if (!find_action<action, Callback>( 1 + baseindex, (int64_t)(chunk & 0xf), state, callback)) return false;} chunk >>= 4;
        if (gt ? (int64_t)(chunk & 0xf) > v : (int64_t)(chunk & 0xf) < v) {if (!find_action<action, Callback>( 2 + baseindex, (int64_t)(chunk & 0xf), state, callback)) return false;} chunk >>= 4;
        if (gt ? (int64_t)(chunk & 0xf) > v : (int64_t)(chunk & 0xf) < v) {if (!find_action<action, Callback>( 3 + baseindex, (int64_t)(chunk & 0xf), state, callback)) return false;} chunk >>= 4;
        if (gt ? (int64_t)(chunk & 0xf) > v : (int64_t)(chunk & 0xf) < v) {if (!find_action<action, Callback>( 4 + baseindex, (int64_t)(chunk & 0xf), state, callback)) return false;} chunk >>= 4;
        if (gt ? (int64_t)(chunk & 0xf) > v : (int64_t)(chunk & 0xf) < v) {if (!find_action<action, Callback>( 5 + baseindex, (int64_t)(chunk & 0xf), state, callback)) return false;} chunk >>= 4;
        if (gt ? (int64_t)(chunk & 0xf) > v : (int64_t)(chunk & 0xf) < v) {if (!find_action<action, Callback>( 6 + baseindex, (int64_t)(chunk & 0xf), state, callback)) return false;} chunk >>= 4;
        if (gt ? (int64_t)(chunk & 0xf) > v : (int64_t)(chunk & 0xf) < v) {if (!find_action<action, Callback>( 7 + baseindex, (int64_t)(chunk & 0xf), state, callback)) return false;} chunk >>= 4;

        if (gt ? (int64_t)(chunk & 0xf) > v : (int64_t)(chunk & 0xf) < v) {if (!find_action<action, Callback>( 8 + baseindex, (int64_t)(chunk & 0xf), state, callback)) return false;} chunk >>= 4;
        if (gt ? (int64_t)(chunk & 0xf) > v : (int64_t)(chunk & 0xf) < v) {if (!find_action<action, Callback>( 9 + baseindex, (int64_t)(chunk & 0xf), state, callback)) return false;} chunk >>= 4;
        if (gt ? (int64_t)(chunk & 0xf) > v : (int64_t)(chunk & 0xf) < v) {if (!find_action<action, Callback>( 10 + baseindex, (int64_t)(chunk & 0xf), state, callback)) return false;} chunk >>= 4;
        if (gt ? (int64_t)(chunk & 0xf) > v : (int64_t)(chunk & 0xf) < v) {if (!find_action<action, Callback>( 11 + baseindex, (int64_t)(chunk & 0xf), state, callback)) return false;} chunk >>= 4;
        if (gt ? (int64_t)(chunk & 0xf) > v : (int64_t)(chunk & 0xf) < v) {if (!find_action<action, Callback>( 12 + baseindex, (int64_t)(chunk & 0xf), state, callback)) return false;} chunk >>= 4;
        if (gt ? (int64_t)(chunk & 0xf) > v : (int64_t)(chunk & 0xf) < v) {if (!find_action<action, Callback>( 13 + baseindex, (int64_t)(chunk & 0xf), state, callback)) return false;} chunk >>= 4;
        if (gt ? (int64_t)(chunk & 0xf) > v : (int64_t)(chunk & 0xf) < v) {if (!find_action<action, Callback>( 14 + baseindex, (int64_t)(chunk & 0xf), state, callback)) return false;} chunk >>= 4;
        if (gt ? (int64_t)(chunk & 0xf) > v : (int64_t)(chunk & 0xf) < v) {if (!find_action<action, Callback>( 15 + baseindex, (int64_t)(chunk & 0xf), state, callback)) return false;} chunk >>= 4;

        // 187 ms:
        // if (gt ? (int64_t)(chunk >> 0*4) & 0xf > v : (int64_t)(chunk >> 0*4) & 0xf < v) return 0;
    }
    else if (width == 8) {
        // 88 ms:
        if (gt ? (char)chunk > v : (char)chunk < v) {if (!find_action<action, Callback>( 0 + baseindex, (char)chunk, state, callback)) return false;} chunk >>= 8;
        if (gt ? (char)chunk > v : (char)chunk < v) {if (!find_action<action, Callback>( 1 + baseindex, (char)chunk, state, callback)) return false;} chunk >>= 8;
        if (gt ? (char)chunk > v : (char)chunk < v) {if (!find_action<action, Callback>( 2 + baseindex, (char)chunk, state, callback)) return false;} chunk >>= 8;
        if (gt ? (char)chunk > v : (char)chunk < v) {if (!find_action<action, Callback>( 3 + baseindex, (char)chunk, state, callback)) return false;} chunk >>= 8;
        if (gt ? (char)chunk > v : (char)chunk < v) {if (!find_action<action, Callback>( 4 + baseindex, (char)chunk, state, callback)) return false;} chunk >>= 8;
        if (gt ? (char)chunk > v : (char)chunk < v) {if (!find_action<action, Callback>( 5 + baseindex, (char)chunk, state, callback)) return false;} chunk >>= 8;
        if (gt ? (char)chunk > v : (char)chunk < v) {if (!find_action<action, Callback>( 6 + baseindex, (char)chunk, state, callback)) return false;} chunk >>= 8;
        if (gt ? (char)chunk > v : (char)chunk < v) {if (!find_action<action, Callback>( 7 + baseindex, (char)chunk, state, callback)) return false;} chunk >>= 8;

        //97 ms ms:
        // if (gt ? (char)(chunk >> 0*8) > v : (char)(chunk >> 0*8) < v) return 0;
    }
    else if (width == 16) {

        if (gt ? (short int)(chunk >> 0*16) > v : (short int)(chunk >> 0*16) < v) {if (!find_action<action, Callback>( 0 + baseindex, (short int)(chunk >> 0*16), state, callback)) return false;};
        if (gt ? (short int)(chunk >> 1*16) > v : (short int)(chunk >> 1*16) < v) {if (!find_action<action, Callback>( 1 + baseindex, (short int)(chunk >> 1*16), state, callback)) return false;};
        if (gt ? (short int)(chunk >> 2*16) > v : (short int)(chunk >> 2*16) < v) {if (!find_action<action, Callback>( 2 + baseindex, (short int)(chunk >> 2*16), state, callback)) return false;};
        if (gt ? (short int)(chunk >> 3*16) > v : (short int)(chunk >> 3*16) < v) {if (!find_action<action, Callback>( 3 + baseindex, (short int)(chunk >> 3*16), state, callback)) return false;};

        /*
        // Faster but disabled due to bug in VC2010 compiler (fixed in 2012 toolchain) where last 'if' is errorneously optimized away
        if (gt ? (short int)chunk > v : (short int)chunk < v) {if (!state->AddPositiveLocal(0 + baseindex); else return 0;} chunk >>= 16;
        if (gt ? (short int)chunk > v : (short int)chunk < v) {if (!state->AddPositiveLocal(1 + baseindex); else return 1;} chunk >>= 16;
        if (gt ? (short int)chunk > v : (short int)chunk < v) {if (!state->AddPositiveLocal(2 + baseindex); else return 2;} chunk >>= 16;
        if (gt ? (short int)chunk > v : (short int)chunk < v) {if (!state->AddPositiveLocal(3 + baseindex); else return 3;} chunk >>= 16;

        // Following illustrates it:
        #include <stdint.h>
        #include <stdio.h>
        #include <stdlib.h>

        size_t bug(int64_t v, uint64_t chunk)
        {
            bool gt = true;

            if (gt ? (short int)chunk > v : (short int)chunk < v) {return 0;} chunk >>= 16;
            if (gt ? (short int)chunk > v : (short int)chunk < v) {return 1;} chunk >>= 16;
            if (gt ? (short int)chunk > v : (short int)chunk < v) {return 2;} chunk >>= 16;
            if (gt ? (short int)chunk > v : (short int)chunk < v) {return 3;} chunk >>= 16;

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
        if (gt ? (int)chunk > v : (int)chunk < v) {if (!find_action<action, Callback>( 0 + baseindex, (int)chunk, state, callback)) return false;} chunk >>= 32;
        if (gt ? (int)chunk > v : (int)chunk < v) {if (!find_action<action, Callback>( 1 + baseindex, (int)chunk, state, callback)) return false;} chunk >>= 32;
    }
    else if (width == 64) {
        if (gt ? (int64_t)v > v : (int64_t)(v) < v) {if (!find_action<action, Callback>( 0 + baseindex, (int64_t)v, state, callback)) return false;};
    }

    return true;
}


template <bool eq, Action action, size_t width, class Callback> inline bool Array::CompareEquality(int64_t value, size_t start, size_t end, size_t baseindex, QueryState<int64_t>* state, Callback callback) const
{
    // Find items in this Array that are equal (eq == true) or different (eq = false) from 'value'

    TIGHTDB_ASSERT(start <= m_len && (end <= m_len || end == (size_t)-1) && start <= end);

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
        const int64_t* p = (const int64_t*)(m_data + (start * width / 8));
        const int64_t* const e = (int64_t*)(m_data + (end * width / 8)) - 1;
        const uint64_t mask = (width == 64 ? ~0ULL : ((1ULL << (width == 64 ? 0 : width)) - 1ULL)); // Warning free way of computing (1ULL << width) - 1
        const uint64_t valuemask = ~0ULL / no0(mask) * (value & mask); // the "== ? :" is to avoid division by 0 compiler error

        while (p < e) {
            uint64_t chunk = *p;
            uint64_t v2 = chunk ^ valuemask;
            start = (p - (int64_t *)m_data) * 8 * 8 / no0(width);
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
        start = (p - (int64_t *)m_data) * 8 * 8 / no0(width);
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

// There exists a couple of find() functions that take more or less template arguments. Always call the one that takes as most as possible to get
// best performance.

template <class cond, Action action, size_t bitwidth> void Array::find(int64_t value, size_t start, size_t end, size_t baseindex, QueryState<int64_t>* state) const
{
    find<cond, action, bitwidth>(value, start, end, baseindex, state, CallbackDummy());
}

template <class cond, Action action, class Callback> void Array::find(int64_t value, size_t start, size_t end, size_t baseindex, QueryState<int64_t>* state, Callback callback) const
{
    TIGHTDB_TEMPEX4(find, cond, action, m_width, Callback, (value, start, end, baseindex, state, callback));
}

template <class cond, Action action, size_t bitwidth, class Callback> void Array::find(int64_t value, size_t start, size_t end, size_t baseindex, QueryState<int64_t>* state, Callback callback) const
{
#ifdef TIGHTDB_DEBUG
    Array r_arr;
    QueryState<int64_t> r_state;
    Array *accu = (Array*)state->m_state;
    r_state.m_state = (int64_t)&r_arr;

    if (action == act_FindAll) {
        for (size_t t = 0; t < accu->size(); t++)
            r_arr.add(accu->Get(t));
    }
    else {
        r_state.m_state = state->m_state;
    }
#endif
    find_optimized<cond, action, bitwidth, Callback>(value, start, end, baseindex, state, callback);

#ifdef TIGHTDB_DEBUG

    if (action == act_Max || action == act_Min || action == act_Sum || action == act_Count || action == act_ReturnFirst || action == act_Count) {
        find_reference<cond, action, bitwidth, Callback>(value, start, end, baseindex, &r_state, callback);
        if (action == act_FindAll)
            TIGHTDB_ASSERT(accu->Compare(r_arr));
        else
            TIGHTDB_ASSERT(state->m_state == r_state.m_state);
    }
    r_arr.Destroy();
#endif

}

template <class cond2, Action action, size_t bitwidth, class Callback> int64_t Array::find_reference(int64_t value, size_t start, size_t end, size_t baseindex, QueryState<int64_t>* state, Callback callback) const
{
    // Reference implementation of find_optimized for bug testing
    (void)callback;

    if (end > size())
        end = size();

    for (size_t t = start; t < end; t++) {
        int64_t v = Get(t);

        if (SameType<cond2, None>::value || (SameType<cond2, Equal>::value && v == value) || (SameType<cond2, NotEqual>::value && v != value) || (SameType<cond2, Greater>::value && v > value) || (SameType<cond2, Less>::value && v < value)) {
            if (action == act_ReturnFirst) {
                state->m_state = t;
                return false;
            }
            else if (action == act_Sum)
                state->m_state += v;
            else if (action == act_Max && v > state->m_state)
                    state->m_state = v;
            else if (action == act_Min && v < state->m_state)
                    state->m_state = v;
            else if (action == act_Count)
                state->m_state++;
            else if (action == act_FindAll)
                ((Array*)state->m_state)->add(t + baseindex);
        }

    }

    if (action == act_ReturnFirst)
        return false;
    else
        return true;
}

#ifdef TIGHTDB_COMPILER_SSE
// 'items' is the number of 16-byte SSE chunks. Returns index of packed element relative to first integer of first chunk
template <class cond2, Action action, size_t width, class Callback> size_t Array::FindSSE(int64_t value, __m128i *data, size_t items, QueryState<int64_t>* state, size_t baseindex, Callback callback) const
{
    cond2 C;
    int cond = C.condition();

    (void) baseindex;
    (void) state;

    int resmask;
    __m128i search = {0};
    __m128i compare = {0};
    size_t i = 0;

    if (width == 8)
        search = _mm_set1_epi8((char)value);
    else if (width == 16)
        search = _mm_set1_epi16((short int)value);
    else if (width == 32)
        search = _mm_set1_epi32((int)value);
    else if (width == 64) {
        search = _mm_set_epi64x(value, value);
    }

    // Search loop. Unrolling it has been tested to NOT increase performance (apparently mem bound)
    for (i = 0; i < items; ++i) {
        // equal / not-equal
        if (cond == cond_Equal || cond == cond_NotEqual) {
            if (width == 8)
                compare = _mm_cmpeq_epi8(data[i], search);
            if (width == 16)
                compare = _mm_cmpeq_epi16(data[i], search);
            if (width == 32)
                compare = _mm_cmpeq_epi32(data[i], search);
            if (width == 64) {
                compare = _mm_cmpeq_epi64(data[i], search); // SSE 4.2 only
            }
        }

        // greater
        else if (cond == cond_Greater) {
            if (width == 8)
                compare = _mm_cmpgt_epi8(data[i], search);
            if (width == 16)
                compare = _mm_cmpgt_epi16(data[i], search);
            if (width == 32)
                compare = _mm_cmpgt_epi32(data[i], search);
            if (width == 64)
                compare = _mm_cmpgt_epi64(data[i], search);
        }
        // less
        else if (cond == cond_Less) {
            if (width == 8)
                compare = _mm_cmplt_epi8(data[i], search);
            if (width == 16)
                compare = _mm_cmplt_epi16(data[i], search);
            if (width == 32)
                compare = _mm_cmplt_epi32(data[i], search);
            if (width == 64){
                // There exists no _mm_cmplt_epi64 instruction, so emulate it. _mm_set1_epi8(0xff) is pre-calculated by compiler.
                compare = _mm_cmpeq_epi64(data[i], search);
                compare = _mm_andnot_si128(compare, _mm_set1_epi32(0xffffffff));
            }
        }

        resmask = _mm_movemask_epi8(compare);

        if (cond == cond_NotEqual)
            resmask = ~resmask & 0x0000ffff;

        size_t s = i * sizeof(__m128i) * 8 / no0(width);

        while (resmask != 0) {

            uint64_t upper = LowerBits<width / 8>() << (no0(width / 8) - 1);
            uint64_t pattern = resmask & upper; // fixme, bits at wrong offsets. Only OK because we only use them in 'count' aggregate
            if (find_action_pattern<action, Callback>(s + baseindex, pattern, state, callback))
                break;

            size_t idx = FirstSetBit(resmask) * 8 / no0(width);
            s += idx;
            if (!find_action<action, Callback>( s + baseindex, GetUniversal<width>((const char *)data, s), state, callback))
                return false;
            resmask >>= (idx + 1) * no0(width) / 8;
            ++s;
        }
    }

    return true;
}
#endif //TIGHTDB_COMPILER_SSE

// If gt = true: Find element(s) which are greater than value
// If gt = false: Find element(s) which are smaller than value
template <class cond2, Action action, size_t bitwidth, class Callback> bool Array::Compare(int64_t value, size_t start, size_t end, size_t baseindex, QueryState<int64_t>* state, Callback callback) const
{
    cond2 C;
    int cond = C.condition();
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

// If gt = true: Find elements that are greater than value
// If gt = false: Find elements that are smaller than value
template <bool gt, Action action, size_t bitwidth, class Callback> bool Array::CompareRelation(int64_t value, size_t start, size_t end, size_t baseindex, QueryState<int64_t>* state, Callback callback) const
{
    TIGHTDB_ASSERT(start <= m_len && (end <= m_len || end == (size_t)-1) && start <= end);
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

    const int64_t* p = (const int64_t*)(m_data + (start * bitwidth / 8));
    const int64_t* const e = (int64_t*)(m_data + (end * bitwidth / 8)) - 1;

    // Matches are rare enough to setup fast linear search for remaining items. We use
    // bit hacks from http://graphics.stanford.edu/~seander/bithacks.html#HasLessInWord

    if (bitwidth == 1 || bitwidth == 2 || bitwidth == 4 || bitwidth == 8 || bitwidth == 16) {
        uint64_t magic = FindGTLT_Magic<gt, bitwidth>(value);

        // Bit hacks only work if searched item <= 127 for 'greater than' and item <= 128 for 'less than'
        if (value != int64_t((magic & mask)) && value >= 0 && bitwidth >= 2 && value <= (int64_t)((mask >> 1) - (gt ? 1 : 0))) {
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
                    idx = FindGTLT_Fast<gt, action, bitwidth, Callback>(v, magic, state, (p - (int64_t *)m_data) * 8 * 8 / no0(bitwidth) + baseindex, callback);
                }
                else
                    idx = FindGTLT<gt, action, bitwidth, Callback>(value, v, state, (p - (int64_t *)m_data) * 8 * 8 / no0(bitwidth) + baseindex, callback);

                if (!idx)
                    return false;
                ++p;
            }
        }
        else {
            // 24 ms
            while (p < e) {
                int64_t v = *p;
                if (!FindGTLT<gt, action, bitwidth, Callback>(value, v, state, (p - (int64_t *)m_data) * 8 * 8 / no0(bitwidth) + baseindex, callback))
                    return false;
                ++p;
            }
        }
        start = (p - (int64_t *)m_data) * 8 * 8 / no0(bitwidth);
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

template <class cond> size_t Array::find_first(int64_t value, size_t start, size_t end) const
{
    cond C;
    TIGHTDB_ASSERT(start <= m_len && (end <= m_len || end == (size_t)-1) && start <= end);
    QueryState<int64_t> state;
    state.m_state = not_found;
    Finder finder = m_finder[C.condition()];
    (this->*finder)(value, start, end, 0, &state);

    return size_t(state.m_state);
}

//*************************************************************************************
// Finding code ends                                                                  *
//*************************************************************************************




} // namespace tightdb

#endif // TIGHTDB_ARRAY_HPP
