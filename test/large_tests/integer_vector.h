
#include <vector>
#include <string>
#include <algorithm>
#include "win32\stdint.h"
#include <stdio.h>
#include <conio.h>

using namespace std;

template <class T>
class VerifiedInteger {
    vector<int64_t> v;
	Column u;
public:
	void Add(uint64_t value);
	void Insert(size_t ndx, uint64_t value);
	void Insert(size_t ndx, const char *value);
	int64_t Get(size_t ndx);
	void Set(size_t ndx, int64_t value);
	void Delete(size_t ndx);
	void Clear();
	size_t Find(int64_t value);
	void FindAll(Column &c, int64_t value, size_t start = 0, size_t end = -1);
	size_t Size(void);
	bool Verify(void);
};

template<class T>void VerifiedInteger<T>::Add(uint64_t value) {
	v.push_back(value);
	u.Add(value);
	assert(Verify());
}


template<class T>void VerifiedInteger<T>::Insert(size_t ndx, uint64_t value) {
	v.insert(v.begin() + ndx, value);
	u.Insert(ndx, value);
	assert(Verify());
	
}


template<class T> int64_t VerifiedInteger<T>::Get(size_t ndx) {
	assert(v[ndx] == u.Get(ndx));
	return v[ndx];
}

template<class T> void VerifiedInteger<T>::Set(size_t ndx, int64_t value) {
	v[ndx] = value;
	u.Set(ndx, value);
	assert(Verify());
}

template<class T> void VerifiedInteger<T>::Delete(size_t ndx) {
	v.erase(v.begin() + ndx);
	u.Delete(ndx);
	assert(Verify());
}

template<class T> void VerifiedInteger<T>::Clear() {
	v.clear();
	u.Clear();
	assert(Verify());
}

template<class T> size_t VerifiedInteger<T>::Find(int64_t value) {
	std::vector<V>::iterator it = std::find(v.begin(), v.end(), value);
	size_t ndx = std::distance(v.begin(), it);
	size_t index2 = u.Find(value);
	assert(ndx == index2);
	return ndx;
}

template<class T> size_t VerifiedInteger<T>::Size(void) {
	assert(v.size() == u.Size());
	return v.size();
}

// todo/fixme, end ignored
template<class T> void VerifiedInteger<T>::FindAll(Column &c, int64_t value, size_t start, size_t end) {
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

	c.Clear(); // todo/fixme, this could be a bug in tightdb where it doesn't clear destination first

	u.FindAll(c, value);
	if (c.Size() != result.size())
		assert(false);
	for(size_t t = 0; t < result.size(); ++t) {
		if (result[t] != c.Get(t)) 
			assert(false);
	}

	return;
}

template<class T> bool VerifiedInteger<T>::Verify(void) {
	if (u.Size() != v.size())
		return false;

	for(size_t t = 0; t < v.size(); ++t) {
		if (v[t] != u.Get(t)) 
			return false;
	}
	return true;
}

