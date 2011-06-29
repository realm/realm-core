#include <cstdlib>
#include <cassert>
#include <cstring>
#include <cstdio> // debug

#include "Column.h"

AdaptiveStringColumn::AdaptiveStringColumn() {
}

AdaptiveStringColumn::~AdaptiveStringColumn() {
}

const char* AdaptiveStringColumn::Get(size_t ndx) const {
	return m_array.Get(ndx);
}

bool AdaptiveStringColumn::Set(size_t ndx, const char* value) {
	return Set(ndx, value, strlen(value));
}

bool AdaptiveStringColumn::Set(size_t ndx, const char* value, size_t len) {
	return m_array.Set(ndx, value, len);
}

bool AdaptiveStringColumn::Add() {
	return m_array.Add();
}

bool AdaptiveStringColumn::Add(const char* value) {
	return m_array.Add(value);
}

bool AdaptiveStringColumn::Insert(size_t ndx, const char* value, size_t len) {
	return m_array.Insert(ndx, value, len);
}

void AdaptiveStringColumn::Delete(size_t ndx) {
	m_array.Delete(ndx);
}

size_t AdaptiveStringColumn::Find(const char* value) const {
	assert(value);
	return Find(value, strlen(value));
}

size_t AdaptiveStringColumn::Find(const char* value, size_t len) const {
	assert(value);
	return m_array.Find(value, len);
}

#ifdef _DEBUG

void AdaptiveStringColumn::ToDot(FILE* f, bool) const {
	m_array.ToDot(f);
}

#endif //_DEBUG
