#include "Group.h"
#include <assert.h>
#include <iostream>
#include <fstream>

Group::Group() : m_top(COLUMN_HASREFS, NULL, 0, m_alloc), m_tables(COLUMN_HASREFS, NULL, 0, m_alloc), m_tableNames(NULL, 0, m_alloc), m_isValid(true)
{
	m_top.Add(m_tableNames.GetRef());
	m_top.Add(m_tables.GetRef());

	m_tableNames.SetParent(&m_top, 0);
	m_tables.SetParent(&m_top, 1);
}

Group::Group(const char* filename) : m_top(m_alloc), m_tables(m_alloc), m_tableNames(m_alloc), m_isValid(false) {
	assert(filename);

	// Memory map file
	m_isValid = m_alloc.SetShared(filename);

	if (m_isValid) Create();
}

Group::Group(const char* buffer, size_t len) : m_top(m_alloc), m_tables(m_alloc), m_tableNames(m_alloc), m_isValid(false) {
	assert(buffer);

	// Memory map file
	m_isValid = m_alloc.SetSharedBuffer(buffer, len);

	if (m_isValid) Create();
}

void Group::Create() {
	// Get ref for table top array
	const size_t top_ref = m_alloc.GetTopRef();

	// Instantiate top arrays
	m_top.UpdateRef(top_ref);
	assert(m_top.Size() == 2);

	m_tableNames.UpdateRef(m_top.GetAsRef(0));
	m_tables.UpdateRef(m_top.GetAsRef(1));
	m_tableNames.SetParent(&m_top, 0);
	m_tables.SetParent(&m_top, 1);

	// Make room for pointers to cached tables
	for (size_t i = 0; i < m_tables.Size(); ++i) {
		m_cachedtables.Add(0);
	}

#ifdef _DEBUG
//	Verify();
#endif //_DEBUG
}

Group::~Group() {
	for (size_t i = 0; i < m_tables.Size(); ++i) {
		TopLevelTable* const t = (TopLevelTable*)m_cachedtables.Get(i);
		delete t;
	}
	m_cachedtables.Destroy();

	// Recursively deletes entire tree
	m_top.Destroy();
}

size_t Group::GetTableCount() const {
	return m_tableNames.Size();
}

const char* Group::GetTableName(size_t table_ndx) const {
	assert(table_ndx < m_tableNames.Size());
	return m_tableNames.Get(table_ndx);
}

bool Group::HasTable(const char* name) const {
	const size_t n = m_tableNames.Find(name);
	return (n != (size_t)-1);
}

TopLevelTable& Group::GetTable(const char* name) {
	const size_t n = m_tableNames.Find(name);
	if (n == (size_t)-1) {
		// Create new table
		TopLevelTable* const t = new TopLevelTable(m_alloc);
		t->SetParent(&m_tables, m_tables.Size());

		m_tables.Add(t->GetRef());
		m_tableNames.Add(name);
		m_cachedtables.Add((intptr_t)t);

		return *t;
	}
	else {
		// Get table from cache if exists, else create
		TopLevelTable* t = (TopLevelTable*)m_cachedtables.Get(n);
		if (!t) {
			const size_t ref = m_tables.GetAsRef(n);
			t = new TopLevelTable(m_alloc, ref, &m_tables, n);
			m_cachedtables.Set(n, (intptr_t)t);
		}
		return *t;
	}
}

void Group::Write(const char* filepath) {
	assert(filepath);

	std::ofstream out(filepath, std::ios_base::out|std::ios_base::binary);
	assert(out);

	Write(out);
    out.close();
}


class MemoryOStream {
public:
	MemoryOStream(size_t size) : m_pos(0), m_buffer(NULL) {
		m_buffer = (char*)malloc(size);
	}

	bool IsValid() const {return m_buffer != NULL;}

	void write(const char* p, size_t n) {
		memcpy(m_buffer+m_pos, p, n);
		m_pos += n;
	}
	void seekp(size_t pos) {m_pos = pos;}

	char* ReleaseBuffer() {
		char* tmp = m_buffer;
		m_buffer = NULL; // invalidate
		return tmp;
	}
private:
	size_t m_pos;
	char* m_buffer;
};

char* Group::WriteToMem(size_t& len) {
	// Get max possible size of buffer
	const size_t max_size = m_alloc.GetTotalSize();

	MemoryOStream out(max_size);
	if (!out.IsValid()) return NULL; // alloc failed

	len = Write(out);
	return out.ReleaseBuffer();
}

#ifdef _DEBUG

void Group::Verify() {
	for (size_t i = 0; i < m_tables.Size(); ++i) {
		// Get table from cache if exists, else create
		TopLevelTable* t = (TopLevelTable*)m_cachedtables.Get(i);
		if (!t) {
			const size_t ref = m_tables.GetAsRef(i);
			t = new TopLevelTable(m_alloc, ref, &m_tables, i);
			m_cachedtables.Set(i, (intptr_t)t);
		}
		t->Verify();
	}
}

MemStats Group::Stats() {
	MemStats stats;

	for (size_t i = 0; i < m_tables.Size(); ++i) {
		// Get table from cache if exists, else create
		TopLevelTable* t = (TopLevelTable*)m_cachedtables.Get(i);
		if (!t) {
			const size_t ref = m_tables.GetAsRef(i);
			t = new TopLevelTable(m_alloc, ref, &m_tables, i);
			m_cachedtables.Set(i, (intptr_t)t);
		}
		const MemStats m = t->Stats();
		stats.Add(m);
	}
	return stats;
}


void Group::Print() const {
	m_alloc.Print();
}

#endif //_DEBUG