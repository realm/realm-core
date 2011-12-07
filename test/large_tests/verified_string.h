#ifndef VER_STR_H
#define VER_STR_H

#include <vector>
#include <string>
#include <algorithm>
#ifdef _MSC_VER
	#include "win32\stdint.h"
#endif
#include <stdio.h>
#include "ColumnString.h"

using namespace std;

class VerifiedString {
    vector<string> v;
	AdaptiveStringColumn u;
public:
	void Add(const char *value);
	void Insert(size_t ndx, const char *value);
	const char *Get(size_t ndx);
	void Set(size_t ndx, const char *value);
	void Delete(size_t ndx);
	void Clear();
	size_t Find(const char *value);
	void FindAll(Column &c, const char *value, size_t start = 0, size_t end = -1);
	size_t Size(void);
	bool Verify(void);
	bool ConditionalVerify(void);
	void VerifyNeighbours(size_t ndx);
	void Destroy(void);

};

#endif