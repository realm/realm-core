#ifndef __TDB_COLUMN_TABLE__
#define __TDB_COLUMN_TABLE__

#include <map>
#include "Column.h"

class Table;


/**
 * Base class for any column that can contain subtables.
 */
class ColumnSubtableParent: public Column
{
public:
	// Overriding method in Column.
	virtual void subtable_wrapper_destroyed(size_t subtable_ndx);

protected:
	struct SubtableMap
	{
		SubtableMap(Allocator &alloc): m_indices(alloc, false), m_wrappers(alloc, false) {}

		~SubtableMap()
		{
			if (m_indices.IsValid()) {
				assert(m_indices.IsEmpty());
				m_indices.Destroy();
				m_wrappers.Destroy();
			}
		}

		bool empty() const { return !m_indices.IsValid() || m_indices.IsEmpty(); }

		Table *find(size_t subtable_ndx) const
		{
			if (!m_indices.IsValid()) return 0;
			size_t const pos = m_indices.Find(subtable_ndx);
			return pos != size_t(-1) ? reinterpret_cast<Table *>(m_wrappers.Get(pos)) : 0;
		}

		void insert(size_t subtable_ndx, Table *wrapper)
		{
			if (!m_indices.IsValid()) {
				m_indices.SetType(COLUMN_NORMAL);
				m_wrappers.SetType(COLUMN_NORMAL);
			}
			m_indices.Add(subtable_ndx);
			m_wrappers.Add(reinterpret_cast<unsigned long>(wrapper));
		}

		void remove(size_t subtable_ndx)
		{
			assert(m_indices.IsValid());
			size_t const pos = m_indices.Find(subtable_ndx);
			assert(pos != size_t(-1));
			m_indices.Delete(pos);
			m_wrappers.Delete(pos);
		}

	private:
		Array m_indices;
		Array m_wrappers;
	};

	Table const *const m_table;

	mutable SubtableMap m_subtable_map;

	ColumnSubtableParent(ArrayParent *parent_array, size_t parent_ndx,
						 Allocator &alloc, Table const *tab):
		Column(COLUMN_HASREFS, parent_array, parent_ndx, alloc),
		m_table(tab), m_subtable_map(GetDefaultAllocator()) {}

	ColumnSubtableParent(size_t ref, ArrayParent *parent_array, size_t parent_ndx,
						 Allocator &alloc, Table const *tab):
		Column(ref, parent_array, parent_ndx, alloc),
		m_table(tab), m_subtable_map(GetDefaultAllocator()) {}

	void save_subtable_wrapper(size_t subtable_ndx, Table *subtable) const;
};

class ColumnTable : public ColumnSubtableParent {
public:
	/**
	 * Create a table column and have it instantiate a new array
	 * structure.
	 *
	 * \param tab If this column is used as part of a table you must
	 * pass a pointer to that table. Otherwise you may pass null.
	 */
	ColumnTable(size_t ref_specSet, ArrayParent *parent, size_t pndx,
				Allocator& alloc, Table const *tab);

	/**
	 * Create a table column and attach it to an already existing
	 * array structure.
	 *
	 * \param tab If this column is used as part of a table you must
	 * pass a pointer to that table. Otherwise you may pass null.
	 */
	ColumnTable(size_t ref_column, size_t ref_specSet, ArrayParent *parent, size_t pndx,
				Allocator &alloc, Table const *tab);

	/**
	 * The returned table pointer must always end up being wrapped in
	 * an instance of BasicTableRef.
	 */
	Table *get_subtable_ptr(size_t ndx) const;

	size_t GetTableSize(size_t ndx) const;

	bool Add();
	void Insert(size_t ndx);
	void Delete(size_t ndx);
	void Clear(size_t ndx);

#ifdef _DEBUG
	void Verify() const;
#endif //_DEBUG

protected:

#ifdef _DEBUG
	virtual void LeafToDot(std::ostream& out, const Array& array) const;
#endif //_DEBUG

	size_t m_ref_specSet;
};

#endif //__TDB_COLUMN_TABLE__
