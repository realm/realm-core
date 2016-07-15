#include <realm/column_link.hpp>
#include <realm/table.hpp>
#include <realm/impl/sequential_getter.hpp>
#include <realm/views.hpp>

using namespace realm;

LinkChain::LinkChain(size_t single_index)
    : link_translator(nullptr)
{
    column_indices.push_back(single_index);
}

LinkChain::LinkChain(std::vector<size_t> chain)
    : column_indices(chain), link_translator(nullptr)
{
    REALM_ASSERT(column_indices.size() >= 1);
}

LinkChain::~LinkChain()
{
    if (link_translator && link_translator.unique()) {
        link_translator->destroy();
    }
}

const ColumnBase& LinkChain::init(const ColumnBase* cb)
{
    REALM_ASSERT(cb != nullptr);
    typedef _impl::TableFriend tf;
    if (link_translator && link_translator.unique()) {
        link_translator->destroy();
    }

    if (column_indices.size() > 1) {
        size_t num_rows = cb->size();
        ref_type ref = IntNullColumn::create(cb->get_alloc());
        link_translator.reset(new IntNullColumn(cb->get_alloc(), ref));
        link_translator->insert_rows(0, num_rows, 0, true);

        std::vector<const LinkColumn*> link_cols;
        const Table* linked_table = nullptr;
        const ColumnBase* next_col = cb;
        for (size_t link_ndx = 0; link_ndx < column_indices.size() - 1; link_ndx++) {
            const LinkColumn* link_col = dynamic_cast<const LinkColumn*>(next_col);
            // Only last column in link chain is allowed to be non-link
            if (!link_col) {
                throw LogicError(LogicError::type_mismatch);
            }
            link_cols.push_back(link_col);
            linked_table = &link_col->get_target_table();
            next_col = &tf::get_column(*linked_table, column_indices[link_ndx + 1]);
        }

        for (size_t row_ndx = 0; row_ndx < num_rows; row_ndx++) {
            size_t translated_index = row_ndx;
            bool set_null = false;
            for (size_t link_ndx = 0; link_ndx < link_cols.size(); link_ndx++) {
                if (link_cols[link_ndx]->is_null(translated_index)) {
                    set_null = true;
                    break;
                }
                else {
                    translated_index = link_cols[link_ndx]->get_link(translated_index);
                }
            }
            if (set_null) {
                link_translator->set_null(row_ndx);
            }
            else {
                link_translator->set(row_ndx, translated_index);
            }
        }
        REALM_ASSERT(linked_table);
        const ColumnBase& last_col_in_chain = tf::get_column(*linked_table, column_indices[column_indices.size() - 1]);
        return last_col_in_chain;
    }
    return *cb; // no link chain, return original column
}

util::Optional<int64_t> LinkChain::translate(size_t index) const
{
    util::Optional<int64_t> value(index);
    if (link_translator) {
        REALM_ASSERT(index < link_translator->size());
        value = link_translator->get_val(index);
    }
    return value;
}


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
        int64_t ndx = m_row_indexes.get(t);
        if (ndx != detached_ref) {
            v.push_back(ndx);
        }
        else
            ++detached_ref_count;
    }
    sorting_predicate.init(this);
    std::stable_sort(v.begin(), v.end(), sorting_predicate);
    m_row_indexes.clear();
    for (size_t t = 0; t < sz - detached_ref_count; t++)
        m_row_indexes.add(v[t]);
    for (size_t t = 0; t < detached_ref_count; ++t)
        m_row_indexes.add(-1);
}

// FIXME: this only works (and is only used) for row indexes with memory
// managed by the default allocator, e.q. for TableViews.
RowIndexes::RowIndexes(const RowIndexes& source, ConstSourcePayload mode)
    : m_row_indexes()
{
#ifdef REALM_COOKIE_CHECK
    cookie = source.cookie;
#endif
    if (mode == ConstSourcePayload::Copy) {
        if (source.m_row_indexes.is_attached()) {
            // we only clone if there is something to clone:
            //m_row_indexes.destroy();
            // MemRef mem = root->clone_deep(Allocator::get_default());
            MemRef mem = source.m_row_indexes.clone_deep(Allocator::get_default());
            m_row_indexes.destroy();
            m_row_indexes.init_from_mem(Allocator::get_default(), mem);
        }
    }
}

RowIndexes::RowIndexes(RowIndexes& source, MutableSourcePayload)
    : m_row_indexes()
{
    // move the data payload, but make sure to leave the source array intact or
    // attempts to reuse it for a query rerun will crash (or assert, if lucky)
    // There really *has* to be a way where we don't need to first create an empty
    // array, and then destroy it
    if (source.m_row_indexes.is_attached()) {
        m_row_indexes.detach();
        m_row_indexes.init_from_mem(Allocator::get_default(), source.m_row_indexes.get_mem());
        source.m_row_indexes.init_from_ref(Allocator::get_default(), IntegerColumn::create(Allocator::get_default()));
    }

#ifdef REALM_COOKIE_CHECK
    cookie = source.cookie;
#endif
}
