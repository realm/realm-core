#ifndef __TDB_GROUP_WRITER__
#define __TDB_GROUP_WRITER__

#ifdef _MSC_VER
#include "win32/stdint.h"
#else
#include <stdint.h> // unint8_t etc
#endif
#include <cstdlib> // size_t

namespace tightdb {

// Pre-declarations
class Group;
class SlabAlloc;

class GroupWriter {
public:
    GroupWriter(Group& group);
    
	bool IsValid() const;
    
	void Commit();
    
    size_t write(const char* p, size_t n);
    void WriteAt(size_t pos, const char* p, size_t n);
    
private:
	void DoCommit(uint64_t topPos);
    
	// Member variables
	Group&     m_group;
	SlabAlloc& m_alloc;
	size_t     m_len;
	int        m_fd;
};
    
} //namespace tightdb

#endif //__TDB_GROUP_WRITER__