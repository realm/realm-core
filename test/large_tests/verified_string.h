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

class VerifiedString {
    std::vector<std::string> v;
	tightdb::AdaptiveStringColumn u;
public:
	void Add(const char *value);
	void Insert(std::size_t ndx, const char *value);
	const char *Get(std::size_t ndx);
	void Set(std::size_t ndx, const char *value);
	void Delete(std::size_t ndx);
	void Clear();
	std::size_t Find(const char *value);
	void FindAll(tightdb::Array &c, const char *value, std::size_t start = 0, std::size_t end = -1);
	std::size_t Size(void);
	bool Verify(void);
	bool ConditionalVerify(void);
	void VerifyNeighbours(std::size_t ndx);
	void Destroy(void);

};

#endif
