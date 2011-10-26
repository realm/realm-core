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

	m_tableNames.UpdateRef((size_t)m_top.Get(0));
	m_tables.UpdateRef((size_t)m_top.Get(1));
	m_tableNames.SetParent(&m_top, 0);
	m_tables.SetParent(&m_top, 1);

	// Make room for pointers to cached tables
	for (size_t i = 0; i < m_tables.Size(); ++i) {
		m_cachedtables.Add(0);
	}
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
	if (n == -1) {
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
			const size_t ref = (size_t)m_tables.Get(n);
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

void Group::Write(std::ostream &out) {
	// Space for ref to top array
    out.write("\0\0\0\0\0\0\0\0", 8);
    size_t pos = 8;

	// Write tables
	Array tables(COLUMN_HASREFS);
	for (size_t i = 0; i < m_tables.Size(); ++i) {
		// Instantiate table if not in cache
		Table* t = (Table*)m_cachedtables.Get(i);
		if (!t) {
			const size_t ref = (size_t)m_tables.Get(i);
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
}
