#ifndef __TIGHTDB_H__
#define __TIGHTDB_H__

#include "Table.h"
#include <vector>

#include "query/QueryInterface.h"

using namespace std;

#define TDB_QUERY(QueryName, TableName) \
class QueryName : public TableName##Query { \
public: \
QueryName()

#define TDB_QUERY_OPT(QueryName, TableName) \
class QueryName : public TableName##Query { \
public: \
QueryName

#define TDB_QUERY_END }; \




#define TDB_TABLE_1(TableName, CType1, CName1) \
class TableName##Query { \
protected: \
	QueryAccessor##CType1 CName1; \
}; \
\
\
class TableName : public TopLevelTable { \
public: \
	TableName(Allocator& alloc=GetDefaultAllocator()) : TopLevelTable(alloc) { \
		RegisterColumn(Accessor##CType1::type, #CName1); \
		\
		CName1.Create(this, 0); \
	}; \
	class Cursor : public CursorBase { \
	public: \
		Cursor(TableName& table, size_t ndx) : CursorBase(table, ndx) { \
			CName1.Create(this, 0); \
		} \
		Cursor(const TableName& table, size_t ndx) : CursorBase(const_cast<TableName&>(table), ndx) { \
			CName1.Create(this, 0); \
		} \
		Cursor(const Cursor& v) : CursorBase(v) { \
			CName1.Create(this, 0); \
		} \
		Accessor##CType1 CName1; \
	}; \
\
	void Add(Type##CType1 CName1) { \
		const size_t ndx = GetSize(); \
		Insert##CType1 (0, ndx, CName1); \
		InsertDone(); \
	} \
\
	Cursor Add() {return Cursor(*this, AddRow());} \
	Cursor Get(size_t ndx) {return Cursor(*this, ndx);} \
	Cursor operator[](size_t ndx) {return Cursor(*this, ndx);} \
	const Cursor operator[](size_t ndx) const {return Cursor(*this, ndx);} \
	Cursor operator[](int ndx) {return Cursor(*this, (ndx < 0) ? GetSize() + ndx : ndx);} \
	Cursor Back() {return Cursor(*this, m_size-1);} \
\
	size_t Find(const TableName##Query&) const {return (size_t)-1;} \
	TableName FindAll(const TableName##Query&) const {return TableName();} \
	TableName Sort() const {return TableName();} \
	TableName Range(int, int) const {return TableName();} \
	TableName Limit(size_t) const {return TableName();} \
\
	ColumnProxy##CType1 CName1; \
protected: \
	friend class Group; \
	TableName(Allocator& alloc, size_t ref, Array* parent, size_t pndx) : TopLevelTable(alloc, ref, parent, pndx) {}; \
private:\
	TableName(const TableName&) {} \
	TableName& operator=(const TableName&) {return *this;} \
};




#define TDB_TABLE_2(TableName, CType1, CName1, CType2, CName2) \
class TableName##Query { \
protected: \
	QueryAccessor##CType1 CName1; \
	QueryAccessor##CType2 CName2; \
}; \
\
class TableName : public TopLevelTable { \
public: \
	TableName(Allocator& alloc=GetDefaultAllocator()) : TopLevelTable(alloc) { \
		RegisterColumn(Accessor##CType1::type, #CName1); \
		RegisterColumn(Accessor##CType2::type, #CName2); \
		\
		CName1.Create(this, 0); \
		CName2.Create(this, 1); \
	}; \
	\
	\
class TestQuery : public Query {\
public:\
	TestQuery() : CName1(0), CName2(1) {\
		CName1.SetQuery(this);\
		CName2.SetQuery(this);\
	}\
	TestQuery(const TestQuery& copy) : Query(copy), CName1(0), CName2(1) { \
		CName1.SetQuery(this);\
		CName2.SetQuery(this);\
	} \
	class TestQueryQueryAccessorInt : private XQueryAccessorInt {\
	public:\
		TestQueryQueryAccessorInt(size_t column_id) : XQueryAccessorInt(column_id) {}\
		void SetQuery(Query* query) {m_query = query;}\
		\
		TestQuery& Equal(int64_t value) {return (TestQuery &)XQueryAccessorInt::Equal(value);}\
		TestQuery& NotEqual(int64_t value) {return (TestQuery &)XQueryAccessorInt::NotEqual(value);}\
		TestQuery& Greater(int64_t value) {return (TestQuery &)XQueryAccessorInt::Greater(value);}\
		TestQuery& Less(int64_t value) {return (TestQuery &)XQueryAccessorInt::Less(value);}\
		TestQuery& Between(int64_t from, int64_t to) {return (TestQuery &)XQueryAccessorInt::Between(from, to);}\
	};\
	\
	template <class T> class TestQueryQueryAccessorEnum : public TestQueryQueryAccessorInt {\
	public:\
		TestQueryQueryAccessorEnum<T>(size_t column_id) : TestQueryQueryAccessorInt(column_id) {}\
	}; \
		\
	class TestQueryQueryAccessorString : private XQueryAccessorString {\
	public:\
		TestQueryQueryAccessorString(size_t column_id) : XQueryAccessorString(column_id) {}\
		void SetQuery(Query* query) {m_query = query;}\
		\
		TestQuery& Equal(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::Equal(value, CaseSensitive);}\
		TestQuery& NotEqual(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::NotEqual(value, CaseSensitive);}\
		TestQuery& BeginsWith(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::BeginsWith(value, CaseSensitive);}\
		TestQuery& Contains(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::Contains(value, CaseSensitive);}\
	};\
	class TestQueryQueryAccessorBool : private XQueryAccessorBool {\
	public:\
		TestQueryQueryAccessorBool(size_t column_id) : XQueryAccessorBool(column_id) {}\
		void SetQuery(Query* query) {m_query = query;}\
		\
		TestQuery& Equal(bool value) {return (TestQuery &)XQueryAccessorBool::Equal(value);}\
	};\
	TestQueryQueryAccessor##CType1 CName1;\
	TestQueryQueryAccessor##CType2 CName2;\
	TestQuery& LeftParan(void) {Query::LeftParan(); return *this;}; \
	TestQuery& Or(void) {Query::Or(); return *this;}; \
	TestQuery& RightParan(void) {Query::RightParan(); return *this;}; \
};\
\
	TestQuery GetQuery() {return TestQuery();} \
	\
	\
	class Cursor : public CursorBase { \
	public: \
		Cursor(TableName& table, size_t ndx) : CursorBase(table, ndx) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
		} \
		Cursor(const TableName& table, size_t ndx) : CursorBase(const_cast<TableName&>(table), ndx) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
		} \
		Cursor(const Cursor& v) : CursorBase(v) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
		} \
		Accessor##CType1 CName1; \
		Accessor##CType2 CName2; \
	}; \
\
	void Add(Type##CType1 CName1, Type##CType2 CName2) { \
		const size_t ndx = GetSize(); \
		Insert##CType1 (0, ndx, CName1); \
		Insert##CType2 (1, ndx, CName2); \
		InsertDone(); \
	} \
\
	Cursor Add() {return Cursor(*this, AddRow());} \
	Cursor Get(size_t ndx) {return Cursor(*this, ndx);} \
	Cursor operator[](size_t ndx) {return Cursor(*this, ndx);} \
	const Cursor operator[](size_t ndx) const {return Cursor(*this, ndx);} \
	Cursor operator[](int ndx) {return Cursor(*this, (ndx < 0) ? GetSize() + ndx : ndx);} \
	Cursor Back() {return Cursor(*this, m_size-1);} \
	const Cursor Back() const {return Cursor(*this, m_size-1);} \
\
	size_t Find(const TableName##Query&) const {return (size_t)-1;} \
	TableName FindAll(const TableName##Query&) const {return TableName();} \
	TableName Sort() const {return TableName();} \
	TableName Range(int, int) const {return TableName();} \
	TableName Limit(size_t) const {return TableName();} \
\
	ColumnProxy##CType1 CName1; \
	ColumnProxy##CType2 CName2; \
protected: \
	friend class Group; \
	TableName(Allocator& alloc, size_t ref, Array* parent, size_t pndx) : TopLevelTable(alloc, ref, parent, pndx) {}; \
private:\
	TableName(const TableName&) {} \
	TableName& operator=(const TableName&) {return *this;} \
};


#define TDB_TABLE_4(TableName, CType1, CName1, CType2, CName2, CType3, CName3, CType4, CName4) \
class TableName##Query { \
protected: \
	QueryAccessor##CType1 CName1; \
	QueryAccessor##CType2 CName2; \
	QueryAccessor##CType3 CName3; \
	QueryAccessor##CType4 CName4; \
}; \
class TableName : public TopLevelTable { \
public: \
	TableName(Allocator& alloc=GetDefaultAllocator()) : TopLevelTable(alloc) { \
		RegisterColumn(Accessor##CType1::type,  #CName1 ); \
		RegisterColumn(Accessor##CType2::type,  #CName2 ); \
		RegisterColumn(Accessor##CType3::type,  #CName3 ); \
		RegisterColumn(Accessor##CType4::type,  #CName4 ); \
		\
		CName1.Create(this, 0); \
		CName2.Create(this, 1); \
		CName3.Create(this, 2); \
		CName4.Create(this, 3); \
	}; \
	class Cursor : public CursorBase { \
	public: \
		Cursor(TableName& table, size_t ndx) : CursorBase(table, ndx) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
		} \
		Cursor(const Cursor& v) : CursorBase(v) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
		} \
		Accessor##CType1 CName1; \
		Accessor##CType2 CName2; \
		Accessor##CType3 CName3; \
		Accessor##CType4 CName4; \
	}; \
\
	void Add(Type##CType1 v1, Type##CType2 v2,Type##CType3 v3, Type##CType4 v4) { \
		const size_t ndx = GetSize(); \
		Insert##CType1 (0, ndx, v1); \
		Insert##CType2 (1, ndx, v2); \
		Insert##CType3 (2, ndx, v3); \
		Insert##CType4 (3, ndx, v4); \
		InsertDone(); \
	} \
	void Insert(size_t ndx, Type##CType1 v1, Type##CType2 v2,Type##CType3 v3, Type##CType4 v4) { \
		Insert##CType1 (0, ndx, v1); \
		Insert##CType2 (1, ndx, v2); \
		Insert##CType3 (2, ndx, v3); \
		Insert##CType4 (3, ndx, v4); \
		InsertDone(); \
	} \
\
	Cursor Add() {return Cursor(*this, AddRow());} \
	Cursor Get(size_t ndx) {return Cursor(*this, ndx);} \
	Cursor operator[](size_t ndx) {return Cursor(*this, ndx);} \
	Cursor operator[](int ndx) {return Cursor(*this, (ndx < 0) ? GetSize() + ndx : ndx);} \
\
	size_t Find(const TableName##Query&) const {return (size_t)-1;} \
	TableName FindAll(const TableName##Query&) const {return TableName();} \
	TableName Sort() const {return TableName();} \
	TableName Range(int, int) const {return TableName();} \
	TableName Limit(size_t) const {return TableName();} \
\
	ColumnProxy##CType1 CName1; \
	ColumnProxy##CType2 CName2; \
	ColumnProxy##CType3 CName3; \
	ColumnProxy##CType4 CName4; \
protected: \
friend class Group; \
	TableName(Allocator& alloc, size_t ref, Array* parent, size_t pndx) : TopLevelTable(alloc, ref, parent, pndx) {}; \
private:\
	TableName(const TableName&) {} \
	TableName& operator=(const TableName&) {return *this;} \
};

#endif //__TIGHTDB_H__