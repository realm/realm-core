
#include <vector>
#include <string>
#include <algorithm>
#ifdef _MSC_VER
	#include "win32\stdint.h"
#endif
#include <stdio.h>
#include <Column.h>

using namespace std;

class VerifiedInteger {
    vector<int64_t> v;
	Column u;
public:
	void Add(int64_t value);
	void Insert(size_t ndx, int64_t value);
	void Insert(size_t ndx, const char *value);
	int64_t Get(size_t ndx);
	void Set(size_t ndx, int64_t value);
	void Delete(size_t ndx);
	void Clear();
	size_t Find(int64_t value);
	void FindAll(Column &c, int64_t value, size_t start = 0, size_t end = -1);
	size_t Size(void);
	bool Verify(void);
	bool ConditionalVerify(void);
	void VerifyNeighbours(size_t ndx);
	void Destroy(void);
};

