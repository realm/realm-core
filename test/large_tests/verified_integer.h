
#include <vector>
#include <string>
#include <algorithm>
#ifdef _MSC_VER
	#include "win32\stdint.h"
#endif
#include <stdio.h>

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
};

void VerifiedInteger::VerifyNeighbours(size_t ndx) {
	if(v.size() > ndx)
		assert(v[ndx] == u.Get(ndx));

	if(ndx > 0)
		assert(v[ndx - 1] == u.Get(ndx - 1));

	if(v.size() > ndx + 1)
		assert(v[ndx + 1] == u.Get(ndx + 1));
}

void VerifiedInteger::Add(int64_t value) {
	v.push_back(value);
	u.Add(value);
	assert(v.size() == u.Size());
	VerifyNeighbours(v.size());
	assert(ConditionalVerify());
}

void VerifiedInteger::Insert(size_t ndx, int64_t value) {
	v.insert(v.begin() + ndx, value);
	u.Insert(ndx, value);
	assert(v.size() == u.Size());
	VerifyNeighbours(ndx);
	assert(ConditionalVerify());
}

int64_t VerifiedInteger::Get(size_t ndx) {
	assert(v[ndx] == u.Get(ndx));
	return v[ndx];
}

 void VerifiedInteger::Set(size_t ndx, int64_t value) {
	v[ndx] = value;
	u.Set(ndx, value);
	VerifyNeighbours(ndx);
	assert(ConditionalVerify());
}

 void VerifiedInteger::Delete(size_t ndx) {
	v.erase(v.begin() + ndx);
	u.Delete(ndx);
	assert(v.size() == u.Size());
	VerifyNeighbours(ndx);
	assert(ConditionalVerify());
}

 void VerifiedInteger::Clear() {
	v.clear();
	u.Clear();
	assert(v.size() == u.Size());
	assert(ConditionalVerify());
}

 size_t VerifiedInteger::Find(int64_t value) {
	std::vector<int64_t>::iterator it = std::find(v.begin(), v.end(), value);
	size_t ndx = std::distance(v.begin(), it);
	size_t index2 = u.Find(value);
	assert(ndx == index2);
	return ndx;
}

 size_t VerifiedInteger::Size(void) {
	assert(v.size() == u.Size());
	return v.size();
}

// todo/fixme, end ignored
 void VerifiedInteger::FindAll(Column &c, int64_t value, size_t start, size_t end) {
	std::vector<int64_t>::iterator it = v.begin();
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

bool VerifiedInteger::Verify(void) {
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
bool VerifiedInteger::ConditionalVerify(void)
{
	if(((uint64_t)rand() * (uint64_t)rand())  % (v.size() / 10 + 1) == 0) {
		return Verify();
	}
	else {
		return true;
	}
}


