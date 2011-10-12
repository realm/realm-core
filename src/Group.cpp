#include "Group.h"

Group::Group(const char* filename) {
	m_alloc.SetShared(filename);
}

Table Group::GetTable() {
	// Get ref for table top array
	const size_t ref = m_alloc.GetTopRef();

	return Table(m_alloc, ref, "fromGroup");
}
