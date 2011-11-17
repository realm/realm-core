
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
};

void VerifiedString::VerifyNeighbours(size_t ndx) {
	if(v.size() > ndx)
		assert(v[ndx] == u.Get(ndx));

	if(ndx > 0)
		assert(v[ndx - 1] == u.Get(ndx - 1));

	if(v.size() > ndx + 1)
		assert(v[ndx + 1] == u.Get(ndx + 1));
}

void VerifiedString::Add(const char * value) {
	v.push_back(value);
	u.Add(value);
	assert(v.size() == u.Size());
	VerifyNeighbours(v.size());
	assert(ConditionalVerify());
}


void VerifiedString::Insert(size_t ndx, const char * value) {
	v.insert(v.begin() + ndx, value);
	u.Insert(ndx, value);
	assert(v.size() == u.Size());
	VerifyNeighbours(ndx);
	assert(ConditionalVerify());
}


const char *VerifiedString::Get(size_t ndx) {
	assert(v[ndx] == u.Get(ndx));
	return v[ndx].c_str();
}

void VerifiedString::Set(size_t ndx, const char *value) {
	v[ndx] = value;
	u.Set(ndx, value);
	VerifyNeighbours(ndx);
	assert(ConditionalVerify());
}

void VerifiedString::Delete(size_t ndx) {
	v.erase(v.begin() + ndx);
	u.Delete(ndx);
	assert(v.size() == u.Size());
	VerifyNeighbours(ndx);
	assert(ConditionalVerify());
}

void VerifiedString::Clear() {
	v.clear();
	u.Clear();
	assert(v.size() == u.Size());
	assert(ConditionalVerify());
}

size_t VerifiedString::Find(const char *value) {
	std::vector<string>::iterator it = std::find(v.begin(), v.end(), value);
	size_t ndx = std::distance(v.begin(), it);
	size_t index2 = u.Find(value);
	assert(ndx == index2);
	return ndx;
}

 size_t VerifiedString::Size(void) {
	assert(v.size() == u.Size());
	return v.size();
}

// todo/fixme, end ignored
 void VerifiedString::FindAll(Column &c, const char *value, size_t start, size_t end) {
	std::vector<string>::iterator it = v.begin();
	std::vector<size_t> result;
	while(it != v.end()) {
		it = std::find(it, v.end(), value);
		size_t ndx = std::distance(v.begin(), it);
		if(ndx < v.size()) {
			result.push_back(ndx);
			it++;
		}
	}

	c.Clear();

	u.FindAll(c, value);
	if (c.Size() != result.size())
		assert(false);
	for(size_t t = 0; t < result.size(); ++t) {
		if (result[t] != c.Get(t)) 
			assert(false);
	}

	return;
}

 bool VerifiedString::Verify(void) {
	assert(u.Size() == v.size());
	if (u.Size() != v.size())
		return false;

	for(size_t t = 0; t < v.size(); ++t) {
		assert(v[t] == u.Get(t));
		if (v[t] != u.Get(t)) 
			return false;
	}
	return true;
}

// makes it run amortized the same time complexity as original, even though the row count grows
bool VerifiedString::ConditionalVerify(void)
{
	if(((uint64_t)rand() * (uint64_t)rand())  % (v.size() / 10 + 1) == 0) {
		return Verify();
	}
	else {
		return true;
	}
}
