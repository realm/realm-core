#include "Group.h"
#include <assert.h>
#include <iostream>
#include <fstream>

Group::Group() : m_top(COLUMN_HASREFS, NULL, 0, m_alloc), m_tables(COLUMN_HASREFS, NULL, 0, m_alloc), m_tableNames(NULL, 0, m_alloc)
{
	m_top.Add(m_tableNames.GetRef());
	m_top.Add(m_tables.GetRef());

	m_tableNames.SetParent(&m_top, 0);
	m_tables.SetParent(&m_top, 1);
}

Group::Group(const char* filename) : m_top(m_alloc), m_tables(m_alloc), m_tableNames(m_alloc)
{
	// Memory map file
	m_alloc.SetShared(filename);

	// Get ref for table top array
	const size_t top_ref = m_alloc.GetTopRef();

	// Instantiate top arrays
	m_top.UpdateRef(top_ref);
	assert(m_top.Size() == 2);

	m_tableNames.UpdateRef(m_top.Get(0));
	m_tables.UpdateRef(m_top.Get(1));
	m_tableNames.SetParent(&m_top, 0);
	m_tables.SetParent(&m_top, 1);

	// Make room for pointers to cached tables
	for (size_t i = 0; i < m_tables.Size(); ++i) {
		m_cachedtables.Add(0);
	}

#ifdef _DEBUG
	Verify();
#endif //_DEBUG
}

Group::~Group() {
	for (size_t i = 0; i < m_tables.Size(); ++i) {
		Table* const t = (Table*)m_cachedtables.Get(i);
		t->Invalidate(); // don't destroy subtree yet
		delete t;
	}
	m_cachedtables.Destroy();

	// Recursively deletes entire tree
	m_top.Destroy();
}

Table& Group::GetTable(const char* name) {
	const size_t n = m_tableNames.Find(name);
	if (n == (size_t)-1) {
		// Create new table
		Table* const t = new Table(m_alloc);
		t->SetParent(&m_tables, m_tables.Size());

		m_tables.Add(t->GetRef());
		m_tableNames.Add(name);
		m_cachedtables.Add((intptr_t)t);

		return *t;
	}
	else {
		// Get table from cache if exists, else create
		Table* t = (Table*)m_cachedtables.Get(n);
		if (!t) {
			const size_t ref = m_tables.Get(n);
			t = new Table(m_alloc, ref, &m_tables, n);
			m_cachedtables.Set(n, (intptr_t)t);
		}
		return *t;
	}
}

void Group::Write(const char* filepath) {
	std::ofstream out(filepath, std::ios_base::out|std::ios_base::binary);
	Write(out);
    out.close();
}



#ifdef _DEBUG

void Group::Verify() {
	for (size_t i = 0; i < m_tables.Size(); ++i) {
		// Get table from cache if exists, else create
		Table* t = (Table*)m_cachedtables.Get(i);
		if (!t) {
			const size_t ref = m_tables.Get(i);
			t = new Table(m_alloc, ref, &m_tables, i);
			m_cachedtables.Set(i, (intptr_t)t);
		}
		t->Verify();
	}
}

void Group::Print() const {
	m_alloc.Print();
}

#endif //_DEBUG