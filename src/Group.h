#ifndef __TDB_GROUP__
#define __TDB_GROUP__

#include "Table.h"
#include "AllocSlab.h"

class Group {
public:
	Group(const char* filename);

	Table GetTable();
	template<class T> T GetTable();

private:
	SlabAlloc m_alloc;
};

template<class T> T Group::GetTable() {
	// Get ref for table top array
	const size_t ref = m_alloc.GetTopRef();

	return T(m_alloc, ref, "fromGroup");
}

#endif //__TDB_GROUP__
