#ifndef __TDB_GROUP__
#define __TDB_GROUP__

#include "Table.h"
#include "AllocSlab.h"

class Group {
public:
	Group();
	Group(const char* filename);
	~Group();

	Table& GetTable(const char* name);
	template<class T> T& GetTable(const char* name);

	// Serialization
	void Write(const char* filepath);
	void Write(std::ostream &out);

private:
	// Member variables
	SlabAlloc m_alloc;
	Array m_top;
	Array m_tables;
	ArrayString m_tableNames;
	Array m_cachedtables;
};

template<class T> T& Group::GetTable(const char* name) {
	const size_t n = m_tableNames.Find(name);
	if (n == -1) {
		// Create new table
		T* const t = new T(m_alloc);
		t->SetParent(&m_tables, m_tables.Size());

		m_tables.Add(t->GetRef());
		m_tableNames.Add(name);
		m_cachedtables.Add((intptr_t)t);

		return *t;
	}
	else {
		// Get table from cache if exists, else create
		T* t = (T*)m_cachedtables.Get(n);
		if (!t) {
			const size_t ref = m_tables.Get(n);
			t = new T(m_alloc, ref, &m_tables, n);
			m_cachedtables.Set(n, (intptr_t)t);
		}
		return *t;
	}
}
#endif //__TDB_GROUP__
