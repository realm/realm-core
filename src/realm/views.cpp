#include <realm/views.hpp>

using namespace realm;

// Re-sort view according to last used criterias
void RowIndexes::sort(Sorter& sorting_predicate)
{
    size_t sz = size();
    if (sz == 0)
        return;

    std::vector<size_t> v;
    v.reserve(sz);
    // always put any detached refs at the end of the sort
    // FIXME: reconsider if this is the right thing to do
    // FIXME: consider specialized implementations in derived classes
    // (handling detached refs is not required in linkviews)
    size_t detached_ref_count = 0;
    for (size_t t = 0; t < sz; t++) {
        size_t ndx = m_row_indexes.get(t);
        if (ndx != detached_ref) {
            v.push_back(ndx);
        }
        else
            ++detached_ref_count;
    }
    sorting_predicate.init(this);
    std::stable_sort(v.begin(), v.end(), sorting_predicate);
    m_row_indexes.clear();
    for (size_t t = 0; t < sz; t++)
        m_row_indexes.add(v[t]);
    for (size_t t = 0; t < detached_ref_count; ++t)
        m_row_indexes.add(-1);
}

// FIXME: this only works (and is only used) for row indexes with memory
// managed by the default allocator, e.q. for TableViews.
RowIndexes::RowIndexes(const RowIndexes& source, ConstSourcePayload mode)
    : m_row_indexes(Column::unattached_root_tag(), Allocator::get_default())
{
#ifdef REALM_COOKIE_CHECK
    cookie = source.cookie;
#endif
    if (mode == ConstSourcePayload::Copy) {
        const Array* root = source.m_row_indexes.get_root_array();
        if (root) {
            // we only clone if there is something to clone:
            //m_row_indexes.destroy();
            // MemRef mem = root->clone_deep(Allocator::get_default());
            MemRef mem = source.m_row_indexes.clone_deep(Allocator::get_default());
            m_row_indexes.init_from_ref(Allocator::get_default(), mem.m_ref);
        }
    }
}

RowIndexes::RowIndexes(RowIndexes& source, MutableSourcePayload)
    : m_row_indexes(Column::unattached_root_tag(), Allocator::get_default())
{
    // move the data payload, but make sure to leave the source array intact or
    // attempts to reuse it for a query rerun will crash (or assert, if lucky)
    Array* src_root = source.m_row_indexes.get_root_array();
    source.m_row_indexes.detach();
    // There really *has* to be a way where we don't need to first create an empty
    // array, and then destroy it
    m_row_indexes.init_from_ref(Allocator::get_default(), src_root->get_mem().m_ref);

#ifdef REALM_COOKIE_CHECK
    cookie = source.cookie;
#endif
}
