#ifndef REALM_VIEWS_HPP
#define REALM_VIEWS_HPP

#include <realm/column.hpp>
#include <realm/column_string_enum.hpp>
#include <realm/handover_defs.hpp>
#include <realm/index_string.hpp>
#include <realm/impl/destroy_guard.hpp>

namespace realm {

const int64_t detached_ref = -1;

class RowIndexes;

struct IndexPair {
    IndexPair(size_t col_ndx, size_t view_ndx)
        : index_in_column(col_ndx), index_in_view(view_ndx) {}
    size_t index_in_column;
    size_t index_in_view;
};

class SharedArray {
public:
    SharedArray(Allocator& alloc, size_t array_size)
    : m_storage(alloc)
    {
        MemRef mem = ArrayIntNull::create_array(Array::Type::type_Normal, false, array_size, 0, alloc);
        m_storage.init_from_mem(mem);
        m_guard.reset(&m_storage);
    }
    size_t size() { return m_storage.size(); }
    ArrayIntNull::value_type get_val(size_t ndx) { return m_storage.get(ndx); }
    void set(size_t ndx, size_t value) { m_storage.set(ndx, value); }
    void set_null(size_t ndx) { m_storage.set_null(ndx); }
private:
    // The order of the following variables matters for destruction.
    ArrayIntNull m_storage;
    realm::_impl::DestroyGuard<ArrayIntNull> m_guard;
};

class LinkChain
{
public:
    LinkChain(size_t single_index);
    LinkChain(std::vector<size_t> chain);
    size_t operator[](size_t index) const { return m_column_indices[index]; }
    size_t size() const { return m_column_indices.size(); }
    util::Optional<int64_t> translate(size_t index) const;
    const ColumnBase& init(const ColumnBase* cb, IntegerColumn* row_indexes);
    void cleanup() { m_link_translator.reset(); }
private:
    std::vector<size_t> m_column_indices;
    std::shared_ptr<SharedArray> m_link_translator;
};

// This class is for common functionality of ListView and LinkView which inherit from it. Currently it only
// supports sorting.
class RowIndexes
{
public:
    RowIndexes(IntegerColumn::unattached_root_tag urt, realm::Allocator& alloc) :
#ifdef REALM_COOKIE_CHECK
        cookie(cookie_expected),
#endif
        m_row_indexes(urt, alloc)
    {}

    RowIndexes(IntegerColumn&& col) :
#ifdef REALM_COOKIE_CHECK
        cookie(cookie_expected),
#endif
        m_row_indexes(std::move(col))
    {}

    RowIndexes(const RowIndexes& source, ConstSourcePayload mode);
    RowIndexes(RowIndexes& source, MutableSourcePayload mode);

    virtual ~RowIndexes()
    {
#ifdef REALM_COOKIE_CHECK
        cookie = 0x7765697633333333; // 0x77656976 = 'view'; 0x33333333 = '3333' = destructed
#endif
    }

    // Return a column of the table that m_row_indexes are pointing at (which is the target table for LinkList and
    // parent table for TableView)
    virtual const ColumnBase& get_column_base(size_t index) const = 0;

    virtual size_t size() const = 0;

    // These two methods are overridden by TableView and LinkView.
    virtual uint_fast64_t sync_if_needed() const = 0;
    virtual bool is_in_sync() const { return true; }

    void check_cookie() const
    {
#ifdef REALM_COOKIE_CHECK
        REALM_ASSERT_RELEASE(cookie == cookie_expected);
#endif
    }

    // Predicate for std::sort
    struct Sorter
    {
        Sorter() {}

        Sorter(const std::vector<LinkChain>& columns, const std::vector<bool>& ascending)
            : m_link_chains(columns), m_ascending(ascending) {}

        bool operator()(IndexPair i, IndexPair j) const
        {
            for (size_t t = 0; t < m_columns.size(); t++) {

                size_t index_i = i.index_in_column;
                size_t index_j = j.index_in_column;

                if (m_link_chains[t].size() > 1) {
                    util::Optional<int64_t> translated_i = m_link_chains[t].translate(i.index_in_view);
                    util::Optional<int64_t> translated_j = m_link_chains[t].translate(j.index_in_view);
                    bool valid1 = bool(translated_i);
                    bool valid2 = bool(translated_j);

                    if (!valid1 && !valid2) {
                        if (t == m_link_chains.size() - 1) {
                            return false; // Two nulls in last sort column
                        }
                        else {
                            continue;   // Check next column for order
                        }
                    }
                    else if (!valid1 || !valid2) {
                        // Sort null links at the end if m_ascending[t], else at beginning
                        return valid1 ? m_ascending[t] : !m_ascending[t];
                    }

                    // We stored unsigned index values from a size_t type so cast is harmless.
                    index_i = static_cast<size_t>(translated_i.value());
                    index_j = static_cast<size_t>(translated_j.value());
                }

                // todo/fixme, special treatment of StringEnumColumn by calling StringEnumColumn::compare_values()
                // instead of the general ColumnTemplate::compare_values() becuse it cannot overload inherited
                // `int64_t get_val()` of Column. Such column inheritance needs to be cleaned up
                int c;
                if (const StringEnumColumn* cse = m_string_enum_columns[t])
                    c = cse->compare_values(index_i, index_j);
                else
                    c = m_columns[t]->compare_values(index_i, index_j);

                if (c != 0)
                    return m_ascending[t] ? c > 0 : c < 0;
            }
            return false; // row(index1) == row(index2)
        }

        void init(RowIndexes* row_indexes)
        {
            REALM_ASSERT(row_indexes);
            m_columns.clear();
            m_string_enum_columns.clear();
            m_columns.resize(m_link_chains.size(), nullptr);
            m_string_enum_columns.resize(m_link_chains.size(), nullptr);

            for (size_t i = 0; i < m_link_chains.size(); i++) {
                REALM_ASSERT_EX(m_link_chains[i].size() >= 1, m_link_chains[i].size());
                const ColumnBase& cb = row_indexes->get_column_base(m_link_chains[i][0]);
                const ColumnBase& end_of_chain = m_link_chains[i].init(&cb, &(row_indexes->m_row_indexes));
                const ColumnTemplateBase* ctb = dynamic_cast<const ColumnTemplateBase*>(&end_of_chain);
                REALM_ASSERT(ctb);
                if (const StringEnumColumn* cse = dynamic_cast<const StringEnumColumn*>(&end_of_chain))
                    m_string_enum_columns[i] = cse;
                else
                    m_columns[i] = ctb;
            }
        }

        void cleanup()
        {
            for (size_t i = 0; i < m_link_chains.size(); i++) {
                m_link_chains[i].cleanup();
            }
        }

        explicit operator bool() const { return !m_link_chains.empty(); }

        std::vector<LinkChain> m_link_chains;
        std::vector<bool> m_ascending;
        std::vector<const ColumnTemplateBase*> m_columns;
        std::vector<const StringEnumColumn*> m_string_enum_columns;
    };

    void sort(Sorter& sorting_predicate);

#ifdef REALM_COOKIE_CHECK
    static const uint64_t cookie_expected = 0x7765697677777777ull; // 0x77656976 = 'view'; 0x77777777 = '7777' = alive
    uint64_t cookie;
#endif

    IntegerColumn m_row_indexes;
};

} // namespace realm

#endif // REALM_VIEWS_HPP
