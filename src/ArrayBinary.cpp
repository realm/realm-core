#include "ArrayBinary.h"
#include "ArrayBlob.h"
#include <assert.h>
#include "win32/types.h"

ArrayBinary::ArrayBinary(ArrayParent *parent, size_t pndx, Allocator& alloc) : Array(COLUMN_HASREFS, parent, pndx, alloc), m_offsets(COLUMN_NORMAL, NULL, 0, alloc), m_blob(NULL, 0, alloc) {
	// Add subarrays for long string
	Array::Add(m_offsets.GetRef());
	Array::Add(m_blob.GetRef());
	m_offsets.SetParent(this, 0);
	m_blob.SetParent(this, 1);
}

ArrayBinary::ArrayBinary(size_t ref, const ArrayParent *parent, size_t pndx, Allocator& alloc) : Array(ref, parent, pndx, alloc), m_offsets(Array::GetAsRef(0), static_cast<ArrayParent *>(NULL), 0, alloc), m_blob(Array::GetAsRef(1), static_cast<ArrayParent *>(NULL), 0, alloc) {
	assert(HasRefs() && !IsNode()); // HasRefs indicates that this is a long string
	assert(Array::Size() == 2);
	assert(m_blob.Size() ==(size_t)(m_offsets.IsEmpty() ? 0 : m_offsets.Back()));

	m_offsets.SetParent(this, 0);
	m_blob.SetParent(this, 1);
}

// Creates new array (but invalid, call UpdateRef to init)
//ArrayBinary::ArrayBinary(Allocator& alloc) : Array(alloc) {}

ArrayBinary::~ArrayBinary() {
}

bool ArrayBinary::IsEmpty() const {
	return m_offsets.IsEmpty();
}

size_t ArrayBinary::Size() const {
	return m_offsets.Size();
}

const void* ArrayBinary::Get(size_t ndx) const {
	assert(ndx < m_offsets.Size());

	const size_t offset = ndx ? m_offsets.GetAsRef(ndx-1) : 0;
	return m_blob.Get(offset);
}

size_t ArrayBinary::GetLen(size_t ndx) const {
	assert(ndx < m_offsets.Size());

	const size_t start = ndx ? m_offsets.GetAsRef(ndx-1) : 0;
	const size_t end = m_offsets.GetAsRef(ndx);

	return end - start;
}

void ArrayBinary::Add(const void* value, size_t len) {
	assert(len == 0 || value);

	m_blob.Add((void*)value, len);
	m_offsets.Add(m_offsets.IsEmpty() ? len : m_offsets.Back() + len);
}

void ArrayBinary::Set(size_t ndx, const void* value, size_t len) {
	assert(ndx < m_offsets.Size());
	assert(len == 0 || value);

	const size_t start = ndx ? m_offsets.GetAsRef(ndx-1) : 0;
	const size_t current_end = m_offsets.GetAsRef(ndx);
	const ssize_t diff =  (start + len) - current_end;

	m_blob.Replace(start, current_end, (void*)value, len);
	m_offsets.Adjust(ndx, diff);
}

void ArrayBinary::Insert(size_t ndx, const void* value, size_t len) {
	assert(ndx <= m_offsets.Size());
	assert(len == 0 || value);

	const size_t pos = ndx ? m_offsets.GetAsRef(ndx-1) : 0;

	m_blob.Insert(pos, (void*)value, len);
	m_offsets.Insert(ndx, pos + len);
	m_offsets.Adjust(ndx+1, len);
}

void ArrayBinary::Delete(size_t ndx) {
	assert(ndx < m_offsets.Size());

	const size_t start = ndx ? m_offsets.GetAsRef(ndx-1) : 0;
	const size_t end = m_offsets.GetAsRef(ndx);

	m_blob.Delete(start, end);
	m_offsets.Delete(ndx);
	m_offsets.Adjust(ndx, int64_t(start) - end);
}

void ArrayBinary::Resize(size_t ndx) {
	assert(ndx < m_offsets.Size());
	
	const size_t len = ndx ? (size_t)m_offsets.Get(ndx-1) : 0;
	
	m_offsets.Resize(ndx);
	m_blob.Resize(len);
}

void ArrayBinary::Clear() {
	m_blob.Clear();
	m_offsets.Clear();
}

#ifdef _DEBUG

void ArrayBinary::ToDot(std::ostream& out, const char* title) const {
	const size_t ref = GetRef();
	
	out << "subgraph cluster_binary" << ref << " {" << std::endl;
	out << " label = \"ArrayBinary";
	if (title) out << "\\n'" << title << "'";
	out << "\";" << std::endl;
	
	Array::ToDot(out, "binary_top");
	m_offsets.ToDot(out, "offsets");
	m_blob.ToDot(out, "blob");
	
	out << "}" << std::endl;
}

#endif //_DEBUG
