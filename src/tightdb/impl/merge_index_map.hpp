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
#ifndef TIGHTDB_IMPL_MERGE_INDEX_MAP_HPP
#define TIGHTDB_IMPL_MERGE_INDEX_MAP_HPP

namespace tightdb {
namespace _impl {



class MergeIndexMap {
public:
    // FIXME: Consider extracting (timestamp, peer_id) into a template parameter that implements
    // operator<, perhaps called "Tiebreaker" or something similar. The contents of the two are
    // never used for anything but comparison.
      struct Entry {
        ssize_t begin;
        ssize_t diff;
        uint64_t timestamp;
        uint64_t peer_id;
    };

    explicit MergeIndexMap(uint64_t self_peer_id) : m_self_peer_id(self_peer_id) {}

    typedef std::vector<Entry>::iterator iterator;
    typedef std::vector<Entry>::const_iterator const_iterator;
    typedef Entry value_type;
    const_iterator begin() const { return m_entries.begin(); }
    const_iterator end()   const { return m_entries.end();   }
    size_t size() const { return m_entries.size(); }
    void reserve(size_t capacity) { m_entries.reserve(capacity); }
    const Entry& back() const { return m_entries.back(); }
    const Entry& front() const { return m_entries.front(); }

    // This is to register insertions that the incoming commit doesn't know about.
    void unknown_insertion_at(size_t ndx, size_t num_rows, uint64_t timestamp, uint64_t peer_id);
    // This is to register insertions that the incoming commit *does* know about, because they
    // originated with the peer.
    void known_insertion_at(size_t ndx, size_t num_rows);

    size_t transform_insert(size_t ndx, size_t num_rows, uint64_t timestamp, uint64_t peer_id);
    size_t transform_set(size_t ndx, uint64_t timestamp, uint64_t peer_id);
    size_t transform_delete(size_t ndx, size_t num_rows, uint64_t timestamp, uint64_t peer_id);

    void clear() { m_entries.clear(); }
    void debug_print() const;
private:
    std::vector<Entry> m_entries;
    uint64_t m_self_peer_id;
    iterator begin() { return m_entries.begin(); }
    iterator end()   { return m_entries.end();   }

    void adjust_diffs_from_by(iterator it, ssize_t by);
    void adjust_begins_from_by(iterator it, ssize_t by);
};


// This returns an iterator pointing to the *last* element of the container for which
// `begin+diff` is less than or equal to `ndx`.
template <class It>
It upper_bound_begin_diff(It i0, It i1, size_t ndx_)
{
    ssize_t ndx = ssize_t(ndx_);
    // FIXME: Use bound functions
    It it = i0;
    while (it != i1) {
        if (ndx < (it->begin + it->diff)) {
            break;
        }
        ++it;
    }
    return it;
}


template <class It>
It upper_bound_begin(It i0, It i1, size_t ndx_)
{
    ssize_t ndx = ssize_t(ndx_);
    It it = i1;
    // FIXME: Use bound functions
    while (it != i0) {
        It tmp = it - 1;
        if (ndx >= tmp->begin) {
            break;
        }
        it = tmp;
    }
    return it;
}


inline void MergeIndexMap::adjust_diffs_from_by(MergeIndexMap::iterator it, ssize_t by)
{
    for (; it != end(); ++it) {
        it->diff += by;
    }
}


inline void MergeIndexMap::adjust_begins_from_by(MergeIndexMap::iterator it, ssize_t by)
{
    for (; it != end(); ++it) {
        it->begin += by;
    }
}


inline void
MergeIndexMap::unknown_insertion_at(size_t ndx_, size_t num_rows, uint64_t timestamp, uint64_t peer_id)
{
    ssize_t ndx = ssize_t(ndx_);
    iterator it = upper_bound_begin_diff(begin(), end(), ndx);
    adjust_diffs_from_by(it, ssize_t(num_rows));
    ssize_t diff = 0;
    if (it != begin()) {
        diff = (it - 1)->diff;
    }
    Entry new_entry;
    new_entry.begin = ndx - diff;
    new_entry.diff = diff + num_rows;
    new_entry.timestamp = timestamp;
    new_entry.peer_id = peer_id;
    m_entries.insert(it, new_entry);
}


inline void
MergeIndexMap::known_insertion_at(size_t ndx, size_t num_rows)
{
    iterator it = upper_bound_begin_diff(begin(), end(), ndx);
    adjust_begins_from_by(it, num_rows);
}


inline size_t
MergeIndexMap::transform_insert(size_t ndx_, size_t num_rows, uint64_t timestamp, uint64_t peer_id)
{
    ssize_t ndx = ssize_t(ndx_);

    iterator it = upper_bound_begin(begin(), end(), ndx);

    while (it != begin()) {
        iterator tmp = it - 1;
        if (ndx > tmp->begin) {
           break;
        }
        if (ndx == tmp->begin) {
            if (timestamp > tmp->timestamp) {
                break;
            }
            if (timestamp == tmp->timestamp) {
                if (peer_id > m_self_peer_id) {
                    break;
                }
                TIGHTDB_ASSERT(peer_id < m_self_peer_id);
            }
        }
        it = tmp;
    }

    adjust_begins_from_by(it, num_rows);

    ssize_t diff = 0;
    if (it != begin()) {
        diff = (it - 1)->diff;
    }
    TIGHTDB_ASSERT(-diff <= ndx);
    return size_t(ndx + diff);
}


inline size_t
MergeIndexMap::transform_set(size_t ndx_, uint64_t timestamp, uint64_t peer_id)
{
    static_cast<void>(timestamp);
    static_cast<void>(peer_id);

    ssize_t ndx = ssize_t(ndx_);

    iterator it = upper_bound_begin(begin(), end(), ndx);
    ssize_t diff = 0;
    if (it != begin()) {
        diff = (it - 1)->diff;
    }
    TIGHTDB_ASSERT(-diff <= ndx);
    return size_t(ndx + diff);
}

inline void
MergeIndexMap::debug_print() const
{
    for (const_iterator it = begin(); it != end(); ++it) {
        size_t i = it - begin();
        std::cout << i << ": (begin: " << it->begin << ", diff: " << it->diff << ", timestamp: " << it->timestamp << ", peer_id: " << it->peer_id << ")\n";
    }
}


}
}

#endif // TIGHTDB_IMPL_MERGE_INDEX_MAP_HPP
