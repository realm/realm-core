#ifndef __TDB_GROUP__
#define __TDB_GROUP__

#include "Table.h"
#include "AllocSlab.h"

class Group {
public:
	Group();
	Group(const char* filename);
	Group(const char* buffer, size_t len);
	~Group();

	size_t GetTableCount() const;
	const char* GetTableName(size_t table_ndx) const;
	bool HasTable(const char* name) const;
	Table& GetTable(const char* name);
	template<class T> T& GetTable(const char* name);

	// Serialization
	void Write(const char* filepath);
	char* WriteToMem(size_t& len);

#ifdef _DEBUG
	void Verify();
	void Print() const;
	MemStats Stats();
	void EnableMemDiagnostics(bool enable=true) {m_alloc.EnableDebug(enable);}
#endif //_DEBUG

private:
	void Create();
	template<class S> size_t Write(S& out);

	// Member variables
	SlabAlloc m_alloc;
	Array m_top;
	Array m_tables;
	ArrayString m_tableNames;
	Array m_cachedtables;
};

// Templates

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

template<class S>
size_t Group::Write(S& out) {
	// Space for ref to top array
    out.write("\0\0\0\0\0\0\0\0", 8);
    size_t pos = 8;

	// Write tables
	Array tables(COLUMN_HASREFS);
	for (size_t i = 0; i < m_tables.Size(); ++i) {
		// Instantiate table if not in cache
		Table* t = (Table*)m_cachedtables.Get(i);
		if (!t) {
			const size_t ref = m_tables.Get(i);
			t = new Table(m_alloc, ref, &m_tables, i);
			m_cachedtables.Set(i, (intptr_t)t);
		}

		// Write the table
		const size_t tablePos = t->Write(out, pos);
		tables.Add(tablePos);
	}

	// Write table names
	const size_t tableNamesPos = pos;
	pos += m_tableNames.Write(out);

	// Write list of tables
	const size_t tablesPos = pos;
	pos += tables.Write(out);

	// Write top
	Array top(COLUMN_HASREFS);
	top.Add(tableNamesPos);
	top.Add(tablesPos);
	const size_t topPos = pos;
	pos += top.Write(out);

	// top ref
	out.seekp(0);
	out.write((const char*)&topPos, 8);

	// Clean-up
	tables.SetType(COLUMN_NORMAL); // avoid recursive del
	top.SetType(COLUMN_NORMAL);
	tables.Destroy();
	top.Destroy();

	// return bytes written
	return pos;
}

#endif //__TDB_GROUP__
