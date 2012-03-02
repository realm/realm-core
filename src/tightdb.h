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
class TableName : public TopLevelTable { \
public: \
	TableName(Allocator& alloc=GetDefaultAllocator()) : TopLevelTable(alloc) { \
		RegisterColumn(Accessor##CType1::type, #CName1); \
\
		CName1.Create(this, 0); \
	}; \
\
	class TestQuery : public Query { \
	public: \
		TestQuery() : CName1(0) { \
			CName1.SetQuery(this); \
		} \
\
		TestQuery(const TestQuery& copy) : Query(copy), CName1(0) { \
			CName1.SetQuery(this); \
		} \
\
		class TestQueryQueryAccessorInt : private XQueryAccessorInt { \
		public: \
			TestQueryQueryAccessorInt(size_t column_id) : XQueryAccessorInt(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(int64_t value) {return (TestQuery &)XQueryAccessorInt::Equal(value);} \
			TestQuery& NotEqual(int64_t value) {return (TestQuery &)XQueryAccessorInt::NotEqual(value);} \
			TestQuery& Greater(int64_t value) {return (TestQuery &)XQueryAccessorInt::Greater(value);} \
			TestQuery& Less(int64_t value) {return (TestQuery &)XQueryAccessorInt::Less(value);} \
			TestQuery& Between(int64_t from, int64_t to) {return (TestQuery &)XQueryAccessorInt::Between(from, to);} \
		}; \
\
		template <class T> class TestQueryQueryAccessorEnum : public TestQueryQueryAccessorInt { \
		public: \
			TestQueryQueryAccessorEnum<T>(size_t column_id) : TestQueryQueryAccessorInt(column_id) {} \
		}; \
\
		class TestQueryQueryAccessorString : private XQueryAccessorString { \
		public: \
			TestQueryQueryAccessorString(size_t column_id) : XQueryAccessorString(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::Equal(value, CaseSensitive);} \
			TestQuery& NotEqual(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::NotEqual(value, CaseSensitive);} \
			TestQuery& BeginsWith(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::BeginsWith(value, CaseSensitive);} \
			TestQuery& EndsWith(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::EndsWith(value, CaseSensitive);} \
			TestQuery& Contains(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::Contains(value, CaseSensitive);} \
		}; \
\
		class TestQueryQueryAccessorBool : private XQueryAccessorBool { \
		public: \
			TestQueryQueryAccessorBool(size_t column_id) : XQueryAccessorBool(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(bool value) {return (TestQuery &)XQueryAccessorBool::Equal(value);} \
		}; \
\
		TestQueryQueryAccessor##CType1 CName1; \
\
		TestQuery& LeftParan(void) {Query::LeftParan(); return *this;}; \
		TestQuery& Or(void) {Query::Or(); return *this;}; \
		TestQuery& RightParan(void) {Query::RightParan(); return *this;}; \
		TestQuery& Subtable(size_t column) {Query::Subtable(column); return *this;}; \
		TestQuery& Parent() {Query::Parent(); return *this;}; \
	}; \
\
	TestQuery GetQuery() {return TestQuery();} \
\
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
	void Add(tdbType##CType1 CName1) { \
		const size_t ndx = GetSize(); \
		Insert##CType1 (0, ndx, CName1); \
		InsertDone(); \
	} \
\
	void Insert(size_t ndx, tdbType##CType1 CName1) { \
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
	const Cursor Back() const {return Cursor(*this, m_size-1);} \
\
	size_t Find(const TableName##Query&) const {return (size_t)-1;} \
	TableName FindAll(const TableName##Query&) const {return TableName();} \
	TableName Sort() const {return TableName();} \
	TableName Range(int, int) const {return TableName();} \
	TableName Limit(size_t) const {return TableName();} \
\
	ColumnProxy##CType1 CName1; \
\
protected: \
	friend class Group; \
	TableName(Allocator& alloc, size_t ref, Array* parent, size_t pndx) : TopLevelTable(alloc, ref, parent, pndx) {}; \
\
private: \
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
	class TestQuery : public Query { \
	public: \
		TestQuery() : CName1(0), CName2(1) { \
			CName1.SetQuery(this); \
			CName2.SetQuery(this); \
		} \
\
		TestQuery(const TestQuery& copy) : Query(copy), CName1(0), CName2(1) { \
			CName1.SetQuery(this); \
			CName2.SetQuery(this); \
		} \
\
		class TestQueryQueryAccessorInt : private XQueryAccessorInt { \
		public: \
			TestQueryQueryAccessorInt(size_t column_id) : XQueryAccessorInt(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(int64_t value) {return (TestQuery &)XQueryAccessorInt::Equal(value);} \
			TestQuery& NotEqual(int64_t value) {return (TestQuery &)XQueryAccessorInt::NotEqual(value);} \
			TestQuery& Greater(int64_t value) {return (TestQuery &)XQueryAccessorInt::Greater(value);} \
			TestQuery& Less(int64_t value) {return (TestQuery &)XQueryAccessorInt::Less(value);} \
			TestQuery& Between(int64_t from, int64_t to) {return (TestQuery &)XQueryAccessorInt::Between(from, to);} \
		}; \
\
		template <class T> class TestQueryQueryAccessorEnum : public TestQueryQueryAccessorInt { \
		public: \
			TestQueryQueryAccessorEnum<T>(size_t column_id) : TestQueryQueryAccessorInt(column_id) {} \
		}; \
\
		class TestQueryQueryAccessorString : private XQueryAccessorString { \
		public: \
			TestQueryQueryAccessorString(size_t column_id) : XQueryAccessorString(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::Equal(value, CaseSensitive);} \
			TestQuery& NotEqual(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::NotEqual(value, CaseSensitive);} \
			TestQuery& BeginsWith(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::BeginsWith(value, CaseSensitive);} \
			TestQuery& EndsWith(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::EndsWith(value, CaseSensitive);} \
			TestQuery& Contains(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::Contains(value, CaseSensitive);} \
		}; \
\
		class TestQueryQueryAccessorBool : private XQueryAccessorBool { \
		public: \
			TestQueryQueryAccessorBool(size_t column_id) : XQueryAccessorBool(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(bool value) {return (TestQuery &)XQueryAccessorBool::Equal(value);} \
		}; \
\
		TestQueryQueryAccessor##CType1 CName1; \
		TestQueryQueryAccessor##CType2 CName2; \
\
		TestQuery& LeftParan(void) {Query::LeftParan(); return *this;}; \
		TestQuery& Or(void) {Query::Or(); return *this;}; \
		TestQuery& RightParan(void) {Query::RightParan(); return *this;}; \
		TestQuery& Subtable(size_t column) {Query::Subtable(column); return *this;}; \
		TestQuery& Parent() {Query::Parent(); return *this;}; \
	}; \
\
	TestQuery GetQuery() {return TestQuery();} \
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
	void Add(tdbType##CType1 CName1, tdbType##CType2 CName2) { \
		const size_t ndx = GetSize(); \
		Insert##CType1 (0, ndx, CName1); \
		Insert##CType2 (1, ndx, CName2); \
		InsertDone(); \
	} \
\
	void Insert(size_t ndx, tdbType##CType1 CName1, tdbType##CType2 CName2) { \
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
\
protected: \
	friend class Group; \
	TableName(Allocator& alloc, size_t ref, Array* parent, size_t pndx) : TopLevelTable(alloc, ref, parent, pndx) {}; \
\
private: \
	TableName(const TableName&) {} \
	TableName& operator=(const TableName&) {return *this;} \
};



#define TDB_TABLE_3(TableName, CType1, CName1, CType2, CName2, CType3, CName3) \
class TableName##Query { \
protected: \
	QueryAccessor##CType1 CName1; \
	QueryAccessor##CType2 CName2; \
	QueryAccessor##CType3 CName3; \
}; \
\
class TableName : public TopLevelTable { \
public: \
	TableName(Allocator& alloc=GetDefaultAllocator()) : TopLevelTable(alloc) { \
		RegisterColumn(Accessor##CType1::type, #CName1); \
		RegisterColumn(Accessor##CType2::type, #CName2); \
		RegisterColumn(Accessor##CType3::type, #CName3); \
\
		CName1.Create(this, 0); \
		CName2.Create(this, 1); \
		CName3.Create(this, 2); \
	}; \
\
	class TestQuery : public Query { \
	public: \
		TestQuery() : CName1(0), CName2(1), CName3(2) { \
			CName1.SetQuery(this); \
			CName2.SetQuery(this); \
			CName3.SetQuery(this); \
		} \
\
		TestQuery(const TestQuery& copy) : Query(copy), CName1(0), CName2(1), CName3(2) { \
			CName1.SetQuery(this); \
			CName2.SetQuery(this); \
			CName3.SetQuery(this); \
		} \
\
		class TestQueryQueryAccessorInt : private XQueryAccessorInt { \
		public: \
			TestQueryQueryAccessorInt(size_t column_id) : XQueryAccessorInt(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(int64_t value) {return (TestQuery &)XQueryAccessorInt::Equal(value);} \
			TestQuery& NotEqual(int64_t value) {return (TestQuery &)XQueryAccessorInt::NotEqual(value);} \
			TestQuery& Greater(int64_t value) {return (TestQuery &)XQueryAccessorInt::Greater(value);} \
			TestQuery& Less(int64_t value) {return (TestQuery &)XQueryAccessorInt::Less(value);} \
			TestQuery& Between(int64_t from, int64_t to) {return (TestQuery &)XQueryAccessorInt::Between(from, to);} \
		}; \
\
		template <class T> class TestQueryQueryAccessorEnum : public TestQueryQueryAccessorInt { \
		public: \
			TestQueryQueryAccessorEnum<T>(size_t column_id) : TestQueryQueryAccessorInt(column_id) {} \
		}; \
\
		class TestQueryQueryAccessorString : private XQueryAccessorString { \
		public: \
			TestQueryQueryAccessorString(size_t column_id) : XQueryAccessorString(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::Equal(value, CaseSensitive);} \
			TestQuery& NotEqual(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::NotEqual(value, CaseSensitive);} \
			TestQuery& BeginsWith(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::BeginsWith(value, CaseSensitive);} \
			TestQuery& EndsWith(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::EndsWith(value, CaseSensitive);} \
			TestQuery& Contains(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::Contains(value, CaseSensitive);} \
		}; \
\
		class TestQueryQueryAccessorBool : private XQueryAccessorBool { \
		public: \
			TestQueryQueryAccessorBool(size_t column_id) : XQueryAccessorBool(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(bool value) {return (TestQuery &)XQueryAccessorBool::Equal(value);} \
		}; \
\
		TestQueryQueryAccessor##CType1 CName1; \
		TestQueryQueryAccessor##CType2 CName2; \
		TestQueryQueryAccessor##CType3 CName3; \
\
		TestQuery& LeftParan(void) {Query::LeftParan(); return *this;}; \
		TestQuery& Or(void) {Query::Or(); return *this;}; \
		TestQuery& RightParan(void) {Query::RightParan(); return *this;}; \
		TestQuery& Subtable(size_t column) {Query::Subtable(column); return *this;}; \
		TestQuery& Parent() {Query::Parent(); return *this;}; \
	}; \
\
	TestQuery GetQuery() {return TestQuery();} \
\
	class Cursor : public CursorBase { \
	public: \
		Cursor(TableName& table, size_t ndx) : CursorBase(table, ndx) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
		} \
		Cursor(const TableName& table, size_t ndx) : CursorBase(const_cast<TableName&>(table), ndx) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
		} \
		Cursor(const Cursor& v) : CursorBase(v) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
		} \
		Accessor##CType1 CName1; \
		Accessor##CType2 CName2; \
		Accessor##CType3 CName3; \
	}; \
\
	void Add(tdbType##CType1 CName1, tdbType##CType2 CName2, tdbType##CType3 CName3) { \
		const size_t ndx = GetSize(); \
		Insert##CType1 (0, ndx, CName1); \
		Insert##CType2 (1, ndx, CName2); \
		Insert##CType3 (2, ndx, CName3); \
		InsertDone(); \
	} \
\
	void Insert(size_t ndx, tdbType##CType1 CName1, tdbType##CType2 CName2, tdbType##CType3 CName3) { \
		Insert##CType1 (0, ndx, CName1); \
		Insert##CType2 (1, ndx, CName2); \
		Insert##CType3 (2, ndx, CName3); \
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
	ColumnProxy##CType3 CName3; \
\
protected: \
	friend class Group; \
	TableName(Allocator& alloc, size_t ref, Array* parent, size_t pndx) : TopLevelTable(alloc, ref, parent, pndx) {}; \
\
private: \
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
\
class TableName : public TopLevelTable { \
public: \
	TableName(Allocator& alloc=GetDefaultAllocator()) : TopLevelTable(alloc) { \
		RegisterColumn(Accessor##CType1::type, #CName1); \
		RegisterColumn(Accessor##CType2::type, #CName2); \
		RegisterColumn(Accessor##CType3::type, #CName3); \
		RegisterColumn(Accessor##CType4::type, #CName4); \
\
		CName1.Create(this, 0); \
		CName2.Create(this, 1); \
		CName3.Create(this, 2); \
		CName4.Create(this, 3); \
	}; \
\
	class TestQuery : public Query { \
	public: \
		TestQuery() : CName1(0), CName2(1), CName3(2), CName4(3) { \
			CName1.SetQuery(this); \
			CName2.SetQuery(this); \
			CName3.SetQuery(this); \
			CName4.SetQuery(this); \
		} \
\
		TestQuery(const TestQuery& copy) : Query(copy), CName1(0), CName2(1), CName3(2), CName4(3) { \
			CName1.SetQuery(this); \
			CName2.SetQuery(this); \
			CName3.SetQuery(this); \
			CName4.SetQuery(this); \
		} \
\
		class TestQueryQueryAccessorInt : private XQueryAccessorInt { \
		public: \
			TestQueryQueryAccessorInt(size_t column_id) : XQueryAccessorInt(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(int64_t value) {return (TestQuery &)XQueryAccessorInt::Equal(value);} \
			TestQuery& NotEqual(int64_t value) {return (TestQuery &)XQueryAccessorInt::NotEqual(value);} \
			TestQuery& Greater(int64_t value) {return (TestQuery &)XQueryAccessorInt::Greater(value);} \
			TestQuery& Less(int64_t value) {return (TestQuery &)XQueryAccessorInt::Less(value);} \
			TestQuery& Between(int64_t from, int64_t to) {return (TestQuery &)XQueryAccessorInt::Between(from, to);} \
		}; \
\
		template <class T> class TestQueryQueryAccessorEnum : public TestQueryQueryAccessorInt { \
		public: \
			TestQueryQueryAccessorEnum<T>(size_t column_id) : TestQueryQueryAccessorInt(column_id) {} \
		}; \
\
		class TestQueryQueryAccessorString : private XQueryAccessorString { \
		public: \
			TestQueryQueryAccessorString(size_t column_id) : XQueryAccessorString(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::Equal(value, CaseSensitive);} \
			TestQuery& NotEqual(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::NotEqual(value, CaseSensitive);} \
			TestQuery& BeginsWith(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::BeginsWith(value, CaseSensitive);} \
			TestQuery& EndsWith(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::EndsWith(value, CaseSensitive);} \
			TestQuery& Contains(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::Contains(value, CaseSensitive);} \
		}; \
\
		class TestQueryQueryAccessorBool : private XQueryAccessorBool { \
		public: \
			TestQueryQueryAccessorBool(size_t column_id) : XQueryAccessorBool(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(bool value) {return (TestQuery &)XQueryAccessorBool::Equal(value);} \
		}; \
\
		TestQueryQueryAccessor##CType1 CName1; \
		TestQueryQueryAccessor##CType2 CName2; \
		TestQueryQueryAccessor##CType3 CName3; \
		TestQueryQueryAccessor##CType4 CName4; \
\
		TestQuery& LeftParan(void) {Query::LeftParan(); return *this;}; \
		TestQuery& Or(void) {Query::Or(); return *this;}; \
		TestQuery& RightParan(void) {Query::RightParan(); return *this;}; \
		TestQuery& Subtable(size_t column) {Query::Subtable(column); return *this;}; \
		TestQuery& Parent() {Query::Parent(); return *this;}; \
	}; \
\
	TestQuery GetQuery() {return TestQuery();} \
\
	class Cursor : public CursorBase { \
	public: \
		Cursor(TableName& table, size_t ndx) : CursorBase(table, ndx) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
		} \
		Cursor(const TableName& table, size_t ndx) : CursorBase(const_cast<TableName&>(table), ndx) { \
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
	void Add(tdbType##CType1 CName1, tdbType##CType2 CName2, tdbType##CType3 CName3, tdbType##CType4 CName4) { \
		const size_t ndx = GetSize(); \
		Insert##CType1 (0, ndx, CName1); \
		Insert##CType2 (1, ndx, CName2); \
		Insert##CType3 (2, ndx, CName3); \
		Insert##CType4 (3, ndx, CName4); \
		InsertDone(); \
	} \
\
	void Insert(size_t ndx, tdbType##CType1 CName1, tdbType##CType2 CName2, tdbType##CType3 CName3, tdbType##CType4 CName4) { \
		Insert##CType1 (0, ndx, CName1); \
		Insert##CType2 (1, ndx, CName2); \
		Insert##CType3 (2, ndx, CName3); \
		Insert##CType4 (3, ndx, CName4); \
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
	ColumnProxy##CType3 CName3; \
	ColumnProxy##CType4 CName4; \
\
protected: \
	friend class Group; \
	TableName(Allocator& alloc, size_t ref, Array* parent, size_t pndx) : TopLevelTable(alloc, ref, parent, pndx) {}; \
\
private: \
	TableName(const TableName&) {} \
	TableName& operator=(const TableName&) {return *this;} \
};



#define TDB_TABLE_5(TableName, CType1, CName1, CType2, CName2, CType3, CName3, CType4, CName4, CType5, CName5) \
class TableName##Query { \
protected: \
	QueryAccessor##CType1 CName1; \
	QueryAccessor##CType2 CName2; \
	QueryAccessor##CType3 CName3; \
	QueryAccessor##CType4 CName4; \
	QueryAccessor##CType5 CName5; \
}; \
\
class TableName : public TopLevelTable { \
public: \
	TableName(Allocator& alloc=GetDefaultAllocator()) : TopLevelTable(alloc) { \
		RegisterColumn(Accessor##CType1::type, #CName1); \
		RegisterColumn(Accessor##CType2::type, #CName2); \
		RegisterColumn(Accessor##CType3::type, #CName3); \
		RegisterColumn(Accessor##CType4::type, #CName4); \
		RegisterColumn(Accessor##CType5::type, #CName5); \
\
		CName1.Create(this, 0); \
		CName2.Create(this, 1); \
		CName3.Create(this, 2); \
		CName4.Create(this, 3); \
		CName5.Create(this, 4); \
	}; \
\
	class TestQuery : public Query { \
	public: \
		TestQuery() : CName1(0), CName2(1), CName3(2), CName4(3), CName5(4) { \
			CName1.SetQuery(this); \
			CName2.SetQuery(this); \
			CName3.SetQuery(this); \
			CName4.SetQuery(this); \
			CName5.SetQuery(this); \
		} \
\
		TestQuery(const TestQuery& copy) : Query(copy), CName1(0), CName2(1), CName3(2), CName4(3), CName5(4) { \
			CName1.SetQuery(this); \
			CName2.SetQuery(this); \
			CName3.SetQuery(this); \
			CName4.SetQuery(this); \
			CName5.SetQuery(this); \
		} \
\
		class TestQueryQueryAccessorInt : private XQueryAccessorInt { \
		public: \
			TestQueryQueryAccessorInt(size_t column_id) : XQueryAccessorInt(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(int64_t value) {return (TestQuery &)XQueryAccessorInt::Equal(value);} \
			TestQuery& NotEqual(int64_t value) {return (TestQuery &)XQueryAccessorInt::NotEqual(value);} \
			TestQuery& Greater(int64_t value) {return (TestQuery &)XQueryAccessorInt::Greater(value);} \
			TestQuery& Less(int64_t value) {return (TestQuery &)XQueryAccessorInt::Less(value);} \
			TestQuery& Between(int64_t from, int64_t to) {return (TestQuery &)XQueryAccessorInt::Between(from, to);} \
		}; \
\
		template <class T> class TestQueryQueryAccessorEnum : public TestQueryQueryAccessorInt { \
		public: \
			TestQueryQueryAccessorEnum<T>(size_t column_id) : TestQueryQueryAccessorInt(column_id) {} \
		}; \
\
		class TestQueryQueryAccessorString : private XQueryAccessorString { \
		public: \
			TestQueryQueryAccessorString(size_t column_id) : XQueryAccessorString(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::Equal(value, CaseSensitive);} \
			TestQuery& NotEqual(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::NotEqual(value, CaseSensitive);} \
			TestQuery& BeginsWith(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::BeginsWith(value, CaseSensitive);} \
			TestQuery& EndsWith(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::EndsWith(value, CaseSensitive);} \
			TestQuery& Contains(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::Contains(value, CaseSensitive);} \
		}; \
\
		class TestQueryQueryAccessorBool : private XQueryAccessorBool { \
		public: \
			TestQueryQueryAccessorBool(size_t column_id) : XQueryAccessorBool(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(bool value) {return (TestQuery &)XQueryAccessorBool::Equal(value);} \
		}; \
\
		TestQueryQueryAccessor##CType1 CName1; \
		TestQueryQueryAccessor##CType2 CName2; \
		TestQueryQueryAccessor##CType3 CName3; \
		TestQueryQueryAccessor##CType4 CName4; \
		TestQueryQueryAccessor##CType5 CName5; \
\
		TestQuery& LeftParan(void) {Query::LeftParan(); return *this;}; \
		TestQuery& Or(void) {Query::Or(); return *this;}; \
		TestQuery& RightParan(void) {Query::RightParan(); return *this;}; \
		TestQuery& Subtable(size_t column) {Query::Subtable(column); return *this;}; \
		TestQuery& Parent() {Query::Parent(); return *this;}; \
	}; \
\
	TestQuery GetQuery() {return TestQuery();} \
\
	class Cursor : public CursorBase { \
	public: \
		Cursor(TableName& table, size_t ndx) : CursorBase(table, ndx) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
		} \
		Cursor(const TableName& table, size_t ndx) : CursorBase(const_cast<TableName&>(table), ndx) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
		} \
		Cursor(const Cursor& v) : CursorBase(v) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
		} \
		Accessor##CType1 CName1; \
		Accessor##CType2 CName2; \
		Accessor##CType3 CName3; \
		Accessor##CType4 CName4; \
		Accessor##CType5 CName5; \
	}; \
\
	void Add(tdbType##CType1 CName1, tdbType##CType2 CName2, tdbType##CType3 CName3, tdbType##CType4 CName4, tdbType##CType5 CName5) { \
		const size_t ndx = GetSize(); \
		Insert##CType1 (0, ndx, CName1); \
		Insert##CType2 (1, ndx, CName2); \
		Insert##CType3 (2, ndx, CName3); \
		Insert##CType4 (3, ndx, CName4); \
		Insert##CType5 (4, ndx, CName5); \
		InsertDone(); \
	} \
\
	void Insert(size_t ndx, tdbType##CType1 CName1, tdbType##CType2 CName2, tdbType##CType3 CName3, tdbType##CType4 CName4, tdbType##CType5 CName5) { \
		Insert##CType1 (0, ndx, CName1); \
		Insert##CType2 (1, ndx, CName2); \
		Insert##CType3 (2, ndx, CName3); \
		Insert##CType4 (3, ndx, CName4); \
		Insert##CType5 (4, ndx, CName5); \
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
	ColumnProxy##CType3 CName3; \
	ColumnProxy##CType4 CName4; \
	ColumnProxy##CType5 CName5; \
\
protected: \
	friend class Group; \
	TableName(Allocator& alloc, size_t ref, Array* parent, size_t pndx) : TopLevelTable(alloc, ref, parent, pndx) {}; \
\
private: \
	TableName(const TableName&) {} \
	TableName& operator=(const TableName&) {return *this;} \
};



#define TDB_TABLE_6(TableName, CType1, CName1, CType2, CName2, CType3, CName3, CType4, CName4, CType5, CName5, CType6, CName6) \
class TableName##Query { \
protected: \
	QueryAccessor##CType1 CName1; \
	QueryAccessor##CType2 CName2; \
	QueryAccessor##CType3 CName3; \
	QueryAccessor##CType4 CName4; \
	QueryAccessor##CType5 CName5; \
	QueryAccessor##CType6 CName6; \
}; \
\
class TableName : public TopLevelTable { \
public: \
	TableName(Allocator& alloc=GetDefaultAllocator()) : TopLevelTable(alloc) { \
		RegisterColumn(Accessor##CType1::type, #CName1); \
		RegisterColumn(Accessor##CType2::type, #CName2); \
		RegisterColumn(Accessor##CType3::type, #CName3); \
		RegisterColumn(Accessor##CType4::type, #CName4); \
		RegisterColumn(Accessor##CType5::type, #CName5); \
		RegisterColumn(Accessor##CType6::type, #CName6); \
\
		CName1.Create(this, 0); \
		CName2.Create(this, 1); \
		CName3.Create(this, 2); \
		CName4.Create(this, 3); \
		CName5.Create(this, 4); \
		CName6.Create(this, 5); \
	}; \
\
	class TestQuery : public Query { \
	public: \
		TestQuery() : CName1(0), CName2(1), CName3(2), CName4(3), CName5(4), CName6(5) { \
			CName1.SetQuery(this); \
			CName2.SetQuery(this); \
			CName3.SetQuery(this); \
			CName4.SetQuery(this); \
			CName5.SetQuery(this); \
			CName6.SetQuery(this); \
		} \
\
		TestQuery(const TestQuery& copy) : Query(copy), CName1(0), CName2(1), CName3(2), CName4(3), CName5(4), CName6(5) { \
			CName1.SetQuery(this); \
			CName2.SetQuery(this); \
			CName3.SetQuery(this); \
			CName4.SetQuery(this); \
			CName5.SetQuery(this); \
			CName6.SetQuery(this); \
		} \
\
		class TestQueryQueryAccessorInt : private XQueryAccessorInt { \
		public: \
			TestQueryQueryAccessorInt(size_t column_id) : XQueryAccessorInt(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(int64_t value) {return (TestQuery &)XQueryAccessorInt::Equal(value);} \
			TestQuery& NotEqual(int64_t value) {return (TestQuery &)XQueryAccessorInt::NotEqual(value);} \
			TestQuery& Greater(int64_t value) {return (TestQuery &)XQueryAccessorInt::Greater(value);} \
			TestQuery& Less(int64_t value) {return (TestQuery &)XQueryAccessorInt::Less(value);} \
			TestQuery& Between(int64_t from, int64_t to) {return (TestQuery &)XQueryAccessorInt::Between(from, to);} \
		}; \
\
		template <class T> class TestQueryQueryAccessorEnum : public TestQueryQueryAccessorInt { \
		public: \
			TestQueryQueryAccessorEnum<T>(size_t column_id) : TestQueryQueryAccessorInt(column_id) {} \
		}; \
\
		class TestQueryQueryAccessorString : private XQueryAccessorString { \
		public: \
			TestQueryQueryAccessorString(size_t column_id) : XQueryAccessorString(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::Equal(value, CaseSensitive);} \
			TestQuery& NotEqual(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::NotEqual(value, CaseSensitive);} \
			TestQuery& BeginsWith(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::BeginsWith(value, CaseSensitive);} \
			TestQuery& EndsWith(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::EndsWith(value, CaseSensitive);} \
			TestQuery& Contains(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::Contains(value, CaseSensitive);} \
		}; \
\
		class TestQueryQueryAccessorBool : private XQueryAccessorBool { \
		public: \
			TestQueryQueryAccessorBool(size_t column_id) : XQueryAccessorBool(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(bool value) {return (TestQuery &)XQueryAccessorBool::Equal(value);} \
		}; \
\
		TestQueryQueryAccessor##CType1 CName1; \
		TestQueryQueryAccessor##CType2 CName2; \
		TestQueryQueryAccessor##CType3 CName3; \
		TestQueryQueryAccessor##CType4 CName4; \
		TestQueryQueryAccessor##CType5 CName5; \
		TestQueryQueryAccessor##CType6 CName6; \
\
		TestQuery& LeftParan(void) {Query::LeftParan(); return *this;}; \
		TestQuery& Or(void) {Query::Or(); return *this;}; \
		TestQuery& RightParan(void) {Query::RightParan(); return *this;}; \
		TestQuery& Subtable(size_t column) {Query::Subtable(column); return *this;}; \
		TestQuery& Parent() {Query::Parent(); return *this;}; \
	}; \
\
	TestQuery GetQuery() {return TestQuery();} \
\
	class Cursor : public CursorBase { \
	public: \
		Cursor(TableName& table, size_t ndx) : CursorBase(table, ndx) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
		} \
		Cursor(const TableName& table, size_t ndx) : CursorBase(const_cast<TableName&>(table), ndx) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
		} \
		Cursor(const Cursor& v) : CursorBase(v) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
		} \
		Accessor##CType1 CName1; \
		Accessor##CType2 CName2; \
		Accessor##CType3 CName3; \
		Accessor##CType4 CName4; \
		Accessor##CType5 CName5; \
		Accessor##CType6 CName6; \
	}; \
\
	void Add(tdbType##CType1 CName1, tdbType##CType2 CName2, tdbType##CType3 CName3, tdbType##CType4 CName4, tdbType##CType5 CName5, tdbType##CType6 CName6) { \
		const size_t ndx = GetSize(); \
		Insert##CType1 (0, ndx, CName1); \
		Insert##CType2 (1, ndx, CName2); \
		Insert##CType3 (2, ndx, CName3); \
		Insert##CType4 (3, ndx, CName4); \
		Insert##CType5 (4, ndx, CName5); \
		Insert##CType6 (5, ndx, CName6); \
		InsertDone(); \
	} \
\
	void Insert(size_t ndx, tdbType##CType1 CName1, tdbType##CType2 CName2, tdbType##CType3 CName3, tdbType##CType4 CName4, tdbType##CType5 CName5, tdbType##CType6 CName6) { \
		Insert##CType1 (0, ndx, CName1); \
		Insert##CType2 (1, ndx, CName2); \
		Insert##CType3 (2, ndx, CName3); \
		Insert##CType4 (3, ndx, CName4); \
		Insert##CType5 (4, ndx, CName5); \
		Insert##CType6 (5, ndx, CName6); \
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
	ColumnProxy##CType3 CName3; \
	ColumnProxy##CType4 CName4; \
	ColumnProxy##CType5 CName5; \
	ColumnProxy##CType6 CName6; \
\
protected: \
	friend class Group; \
	TableName(Allocator& alloc, size_t ref, Array* parent, size_t pndx) : TopLevelTable(alloc, ref, parent, pndx) {}; \
\
private: \
	TableName(const TableName&) {} \
	TableName& operator=(const TableName&) {return *this;} \
};



#define TDB_TABLE_7(TableName, CType1, CName1, CType2, CName2, CType3, CName3, CType4, CName4, CType5, CName5, CType6, CName6, CType7, CName7) \
class TableName##Query { \
protected: \
	QueryAccessor##CType1 CName1; \
	QueryAccessor##CType2 CName2; \
	QueryAccessor##CType3 CName3; \
	QueryAccessor##CType4 CName4; \
	QueryAccessor##CType5 CName5; \
	QueryAccessor##CType6 CName6; \
	QueryAccessor##CType7 CName7; \
}; \
\
class TableName : public TopLevelTable { \
public: \
	TableName(Allocator& alloc=GetDefaultAllocator()) : TopLevelTable(alloc) { \
		RegisterColumn(Accessor##CType1::type, #CName1); \
		RegisterColumn(Accessor##CType2::type, #CName2); \
		RegisterColumn(Accessor##CType3::type, #CName3); \
		RegisterColumn(Accessor##CType4::type, #CName4); \
		RegisterColumn(Accessor##CType5::type, #CName5); \
		RegisterColumn(Accessor##CType6::type, #CName6); \
		RegisterColumn(Accessor##CType7::type, #CName7); \
\
		CName1.Create(this, 0); \
		CName2.Create(this, 1); \
		CName3.Create(this, 2); \
		CName4.Create(this, 3); \
		CName5.Create(this, 4); \
		CName6.Create(this, 5); \
		CName7.Create(this, 6); \
	}; \
\
	class TestQuery : public Query { \
	public: \
		TestQuery() : CName1(0), CName2(1), CName3(2), CName4(3), CName5(4), CName6(5), CName7(6) { \
			CName1.SetQuery(this); \
			CName2.SetQuery(this); \
			CName3.SetQuery(this); \
			CName4.SetQuery(this); \
			CName5.SetQuery(this); \
			CName6.SetQuery(this); \
			CName7.SetQuery(this); \
		} \
\
		TestQuery(const TestQuery& copy) : Query(copy), CName1(0), CName2(1), CName3(2), CName4(3), CName5(4), CName6(5), CName7(6) { \
			CName1.SetQuery(this); \
			CName2.SetQuery(this); \
			CName3.SetQuery(this); \
			CName4.SetQuery(this); \
			CName5.SetQuery(this); \
			CName6.SetQuery(this); \
			CName7.SetQuery(this); \
		} \
\
		class TestQueryQueryAccessorInt : private XQueryAccessorInt { \
		public: \
			TestQueryQueryAccessorInt(size_t column_id) : XQueryAccessorInt(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(int64_t value) {return (TestQuery &)XQueryAccessorInt::Equal(value);} \
			TestQuery& NotEqual(int64_t value) {return (TestQuery &)XQueryAccessorInt::NotEqual(value);} \
			TestQuery& Greater(int64_t value) {return (TestQuery &)XQueryAccessorInt::Greater(value);} \
			TestQuery& Less(int64_t value) {return (TestQuery &)XQueryAccessorInt::Less(value);} \
			TestQuery& Between(int64_t from, int64_t to) {return (TestQuery &)XQueryAccessorInt::Between(from, to);} \
		}; \
\
		template <class T> class TestQueryQueryAccessorEnum : public TestQueryQueryAccessorInt { \
		public: \
			TestQueryQueryAccessorEnum<T>(size_t column_id) : TestQueryQueryAccessorInt(column_id) {} \
		}; \
\
		class TestQueryQueryAccessorString : private XQueryAccessorString { \
		public: \
			TestQueryQueryAccessorString(size_t column_id) : XQueryAccessorString(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::Equal(value, CaseSensitive);} \
			TestQuery& NotEqual(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::NotEqual(value, CaseSensitive);} \
			TestQuery& BeginsWith(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::BeginsWith(value, CaseSensitive);} \
			TestQuery& EndsWith(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::EndsWith(value, CaseSensitive);} \
			TestQuery& Contains(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::Contains(value, CaseSensitive);} \
		}; \
\
		class TestQueryQueryAccessorBool : private XQueryAccessorBool { \
		public: \
			TestQueryQueryAccessorBool(size_t column_id) : XQueryAccessorBool(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(bool value) {return (TestQuery &)XQueryAccessorBool::Equal(value);} \
		}; \
\
		TestQueryQueryAccessor##CType1 CName1; \
		TestQueryQueryAccessor##CType2 CName2; \
		TestQueryQueryAccessor##CType3 CName3; \
		TestQueryQueryAccessor##CType4 CName4; \
		TestQueryQueryAccessor##CType5 CName5; \
		TestQueryQueryAccessor##CType6 CName6; \
		TestQueryQueryAccessor##CType7 CName7; \
\
		TestQuery& LeftParan(void) {Query::LeftParan(); return *this;}; \
		TestQuery& Or(void) {Query::Or(); return *this;}; \
		TestQuery& RightParan(void) {Query::RightParan(); return *this;}; \
		TestQuery& Subtable(size_t column) {Query::Subtable(column); return *this;}; \
		TestQuery& Parent() {Query::Parent(); return *this;}; \
	}; \
\
	TestQuery GetQuery() {return TestQuery();} \
\
	class Cursor : public CursorBase { \
	public: \
		Cursor(TableName& table, size_t ndx) : CursorBase(table, ndx) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
		} \
		Cursor(const TableName& table, size_t ndx) : CursorBase(const_cast<TableName&>(table), ndx) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
		} \
		Cursor(const Cursor& v) : CursorBase(v) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
		} \
		Accessor##CType1 CName1; \
		Accessor##CType2 CName2; \
		Accessor##CType3 CName3; \
		Accessor##CType4 CName4; \
		Accessor##CType5 CName5; \
		Accessor##CType6 CName6; \
		Accessor##CType7 CName7; \
	}; \
\
	void Add(tdbType##CType1 CName1, tdbType##CType2 CName2, tdbType##CType3 CName3, tdbType##CType4 CName4, tdbType##CType5 CName5, tdbType##CType6 CName6, tdbType##CType7 CName7) { \
		const size_t ndx = GetSize(); \
		Insert##CType1 (0, ndx, CName1); \
		Insert##CType2 (1, ndx, CName2); \
		Insert##CType3 (2, ndx, CName3); \
		Insert##CType4 (3, ndx, CName4); \
		Insert##CType5 (4, ndx, CName5); \
		Insert##CType6 (5, ndx, CName6); \
		Insert##CType7 (6, ndx, CName7); \
		InsertDone(); \
	} \
\
	void Insert(size_t ndx, tdbType##CType1 CName1, tdbType##CType2 CName2, tdbType##CType3 CName3, tdbType##CType4 CName4, tdbType##CType5 CName5, tdbType##CType6 CName6, tdbType##CType7 CName7) { \
		Insert##CType1 (0, ndx, CName1); \
		Insert##CType2 (1, ndx, CName2); \
		Insert##CType3 (2, ndx, CName3); \
		Insert##CType4 (3, ndx, CName4); \
		Insert##CType5 (4, ndx, CName5); \
		Insert##CType6 (5, ndx, CName6); \
		Insert##CType7 (6, ndx, CName7); \
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
	ColumnProxy##CType3 CName3; \
	ColumnProxy##CType4 CName4; \
	ColumnProxy##CType5 CName5; \
	ColumnProxy##CType6 CName6; \
	ColumnProxy##CType7 CName7; \
\
protected: \
	friend class Group; \
	TableName(Allocator& alloc, size_t ref, Array* parent, size_t pndx) : TopLevelTable(alloc, ref, parent, pndx) {}; \
\
private: \
	TableName(const TableName&) {} \
	TableName& operator=(const TableName&) {return *this;} \
};



#define TDB_TABLE_8(TableName, CType1, CName1, CType2, CName2, CType3, CName3, CType4, CName4, CType5, CName5, CType6, CName6, CType7, CName7, CType8, CName8) \
class TableName##Query { \
protected: \
	QueryAccessor##CType1 CName1; \
	QueryAccessor##CType2 CName2; \
	QueryAccessor##CType3 CName3; \
	QueryAccessor##CType4 CName4; \
	QueryAccessor##CType5 CName5; \
	QueryAccessor##CType6 CName6; \
	QueryAccessor##CType7 CName7; \
	QueryAccessor##CType8 CName8; \
}; \
\
class TableName : public TopLevelTable { \
public: \
	TableName(Allocator& alloc=GetDefaultAllocator()) : TopLevelTable(alloc) { \
		RegisterColumn(Accessor##CType1::type, #CName1); \
		RegisterColumn(Accessor##CType2::type, #CName2); \
		RegisterColumn(Accessor##CType3::type, #CName3); \
		RegisterColumn(Accessor##CType4::type, #CName4); \
		RegisterColumn(Accessor##CType5::type, #CName5); \
		RegisterColumn(Accessor##CType6::type, #CName6); \
		RegisterColumn(Accessor##CType7::type, #CName7); \
		RegisterColumn(Accessor##CType8::type, #CName8); \
\
		CName1.Create(this, 0); \
		CName2.Create(this, 1); \
		CName3.Create(this, 2); \
		CName4.Create(this, 3); \
		CName5.Create(this, 4); \
		CName6.Create(this, 5); \
		CName7.Create(this, 6); \
		CName8.Create(this, 7); \
	}; \
\
	class TestQuery : public Query { \
	public: \
		TestQuery() : CName1(0), CName2(1), CName3(2), CName4(3), CName5(4), CName6(5), CName7(6), CName8(7) { \
			CName1.SetQuery(this); \
			CName2.SetQuery(this); \
			CName3.SetQuery(this); \
			CName4.SetQuery(this); \
			CName5.SetQuery(this); \
			CName6.SetQuery(this); \
			CName7.SetQuery(this); \
			CName8.SetQuery(this); \
		} \
\
		TestQuery(const TestQuery& copy) : Query(copy), CName1(0), CName2(1), CName3(2), CName4(3), CName5(4), CName6(5), CName7(6), CName8(7) { \
			CName1.SetQuery(this); \
			CName2.SetQuery(this); \
			CName3.SetQuery(this); \
			CName4.SetQuery(this); \
			CName5.SetQuery(this); \
			CName6.SetQuery(this); \
			CName7.SetQuery(this); \
			CName8.SetQuery(this); \
		} \
\
		class TestQueryQueryAccessorInt : private XQueryAccessorInt { \
		public: \
			TestQueryQueryAccessorInt(size_t column_id) : XQueryAccessorInt(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(int64_t value) {return (TestQuery &)XQueryAccessorInt::Equal(value);} \
			TestQuery& NotEqual(int64_t value) {return (TestQuery &)XQueryAccessorInt::NotEqual(value);} \
			TestQuery& Greater(int64_t value) {return (TestQuery &)XQueryAccessorInt::Greater(value);} \
			TestQuery& Less(int64_t value) {return (TestQuery &)XQueryAccessorInt::Less(value);} \
			TestQuery& Between(int64_t from, int64_t to) {return (TestQuery &)XQueryAccessorInt::Between(from, to);} \
		}; \
\
		template <class T> class TestQueryQueryAccessorEnum : public TestQueryQueryAccessorInt { \
		public: \
			TestQueryQueryAccessorEnum<T>(size_t column_id) : TestQueryQueryAccessorInt(column_id) {} \
		}; \
\
		class TestQueryQueryAccessorString : private XQueryAccessorString { \
		public: \
			TestQueryQueryAccessorString(size_t column_id) : XQueryAccessorString(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::Equal(value, CaseSensitive);} \
			TestQuery& NotEqual(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::NotEqual(value, CaseSensitive);} \
			TestQuery& BeginsWith(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::BeginsWith(value, CaseSensitive);} \
			TestQuery& EndsWith(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::EndsWith(value, CaseSensitive);} \
			TestQuery& Contains(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::Contains(value, CaseSensitive);} \
		}; \
\
		class TestQueryQueryAccessorBool : private XQueryAccessorBool { \
		public: \
			TestQueryQueryAccessorBool(size_t column_id) : XQueryAccessorBool(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(bool value) {return (TestQuery &)XQueryAccessorBool::Equal(value);} \
		}; \
\
		TestQueryQueryAccessor##CType1 CName1; \
		TestQueryQueryAccessor##CType2 CName2; \
		TestQueryQueryAccessor##CType3 CName3; \
		TestQueryQueryAccessor##CType4 CName4; \
		TestQueryQueryAccessor##CType5 CName5; \
		TestQueryQueryAccessor##CType6 CName6; \
		TestQueryQueryAccessor##CType7 CName7; \
		TestQueryQueryAccessor##CType8 CName8; \
\
		TestQuery& LeftParan(void) {Query::LeftParan(); return *this;}; \
		TestQuery& Or(void) {Query::Or(); return *this;}; \
		TestQuery& RightParan(void) {Query::RightParan(); return *this;}; \
		TestQuery& Subtable(size_t column) {Query::Subtable(column); return *this;}; \
		TestQuery& Parent() {Query::Parent(); return *this;}; \
	}; \
\
	TestQuery GetQuery() {return TestQuery();} \
\
	class Cursor : public CursorBase { \
	public: \
		Cursor(TableName& table, size_t ndx) : CursorBase(table, ndx) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
		} \
		Cursor(const TableName& table, size_t ndx) : CursorBase(const_cast<TableName&>(table), ndx) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
		} \
		Cursor(const Cursor& v) : CursorBase(v) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
		} \
		Accessor##CType1 CName1; \
		Accessor##CType2 CName2; \
		Accessor##CType3 CName3; \
		Accessor##CType4 CName4; \
		Accessor##CType5 CName5; \
		Accessor##CType6 CName6; \
		Accessor##CType7 CName7; \
		Accessor##CType8 CName8; \
	}; \
\
	void Add(tdbType##CType1 CName1, tdbType##CType2 CName2, tdbType##CType3 CName3, tdbType##CType4 CName4, tdbType##CType5 CName5, tdbType##CType6 CName6, tdbType##CType7 CName7, tdbType##CType8 CName8) { \
		const size_t ndx = GetSize(); \
		Insert##CType1 (0, ndx, CName1); \
		Insert##CType2 (1, ndx, CName2); \
		Insert##CType3 (2, ndx, CName3); \
		Insert##CType4 (3, ndx, CName4); \
		Insert##CType5 (4, ndx, CName5); \
		Insert##CType6 (5, ndx, CName6); \
		Insert##CType7 (6, ndx, CName7); \
		Insert##CType8 (7, ndx, CName8); \
		InsertDone(); \
	} \
\
	void Insert(size_t ndx, tdbType##CType1 CName1, tdbType##CType2 CName2, tdbType##CType3 CName3, tdbType##CType4 CName4, tdbType##CType5 CName5, tdbType##CType6 CName6, tdbType##CType7 CName7, tdbType##CType8 CName8) { \
		Insert##CType1 (0, ndx, CName1); \
		Insert##CType2 (1, ndx, CName2); \
		Insert##CType3 (2, ndx, CName3); \
		Insert##CType4 (3, ndx, CName4); \
		Insert##CType5 (4, ndx, CName5); \
		Insert##CType6 (5, ndx, CName6); \
		Insert##CType7 (6, ndx, CName7); \
		Insert##CType8 (7, ndx, CName8); \
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
	ColumnProxy##CType3 CName3; \
	ColumnProxy##CType4 CName4; \
	ColumnProxy##CType5 CName5; \
	ColumnProxy##CType6 CName6; \
	ColumnProxy##CType7 CName7; \
	ColumnProxy##CType8 CName8; \
\
protected: \
	friend class Group; \
	TableName(Allocator& alloc, size_t ref, Array* parent, size_t pndx) : TopLevelTable(alloc, ref, parent, pndx) {}; \
\
private: \
	TableName(const TableName&) {} \
	TableName& operator=(const TableName&) {return *this;} \
};



#define TDB_TABLE_9(TableName, CType1, CName1, CType2, CName2, CType3, CName3, CType4, CName4, CType5, CName5, CType6, CName6, CType7, CName7, CType8, CName8, CType9, CName9) \
class TableName##Query { \
protected: \
	QueryAccessor##CType1 CName1; \
	QueryAccessor##CType2 CName2; \
	QueryAccessor##CType3 CName3; \
	QueryAccessor##CType4 CName4; \
	QueryAccessor##CType5 CName5; \
	QueryAccessor##CType6 CName6; \
	QueryAccessor##CType7 CName7; \
	QueryAccessor##CType8 CName8; \
	QueryAccessor##CType9 CName9; \
}; \
\
class TableName : public TopLevelTable { \
public: \
	TableName(Allocator& alloc=GetDefaultAllocator()) : TopLevelTable(alloc) { \
		RegisterColumn(Accessor##CType1::type, #CName1); \
		RegisterColumn(Accessor##CType2::type, #CName2); \
		RegisterColumn(Accessor##CType3::type, #CName3); \
		RegisterColumn(Accessor##CType4::type, #CName4); \
		RegisterColumn(Accessor##CType5::type, #CName5); \
		RegisterColumn(Accessor##CType6::type, #CName6); \
		RegisterColumn(Accessor##CType7::type, #CName7); \
		RegisterColumn(Accessor##CType8::type, #CName8); \
		RegisterColumn(Accessor##CType9::type, #CName9); \
\
		CName1.Create(this, 0); \
		CName2.Create(this, 1); \
		CName3.Create(this, 2); \
		CName4.Create(this, 3); \
		CName5.Create(this, 4); \
		CName6.Create(this, 5); \
		CName7.Create(this, 6); \
		CName8.Create(this, 7); \
		CName9.Create(this, 8); \
	}; \
\
	class TestQuery : public Query { \
	public: \
		TestQuery() : CName1(0), CName2(1), CName3(2), CName4(3), CName5(4), CName6(5), CName7(6), CName8(7), CName9(8) { \
			CName1.SetQuery(this); \
			CName2.SetQuery(this); \
			CName3.SetQuery(this); \
			CName4.SetQuery(this); \
			CName5.SetQuery(this); \
			CName6.SetQuery(this); \
			CName7.SetQuery(this); \
			CName8.SetQuery(this); \
			CName9.SetQuery(this); \
		} \
\
		TestQuery(const TestQuery& copy) : Query(copy), CName1(0), CName2(1), CName3(2), CName4(3), CName5(4), CName6(5), CName7(6), CName8(7), CName9(8) { \
			CName1.SetQuery(this); \
			CName2.SetQuery(this); \
			CName3.SetQuery(this); \
			CName4.SetQuery(this); \
			CName5.SetQuery(this); \
			CName6.SetQuery(this); \
			CName7.SetQuery(this); \
			CName8.SetQuery(this); \
			CName9.SetQuery(this); \
		} \
\
		class TestQueryQueryAccessorInt : private XQueryAccessorInt { \
		public: \
			TestQueryQueryAccessorInt(size_t column_id) : XQueryAccessorInt(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(int64_t value) {return (TestQuery &)XQueryAccessorInt::Equal(value);} \
			TestQuery& NotEqual(int64_t value) {return (TestQuery &)XQueryAccessorInt::NotEqual(value);} \
			TestQuery& Greater(int64_t value) {return (TestQuery &)XQueryAccessorInt::Greater(value);} \
			TestQuery& Less(int64_t value) {return (TestQuery &)XQueryAccessorInt::Less(value);} \
			TestQuery& Between(int64_t from, int64_t to) {return (TestQuery &)XQueryAccessorInt::Between(from, to);} \
		}; \
\
		template <class T> class TestQueryQueryAccessorEnum : public TestQueryQueryAccessorInt { \
		public: \
			TestQueryQueryAccessorEnum<T>(size_t column_id) : TestQueryQueryAccessorInt(column_id) {} \
		}; \
\
		class TestQueryQueryAccessorString : private XQueryAccessorString { \
		public: \
			TestQueryQueryAccessorString(size_t column_id) : XQueryAccessorString(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::Equal(value, CaseSensitive);} \
			TestQuery& NotEqual(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::NotEqual(value, CaseSensitive);} \
			TestQuery& BeginsWith(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::BeginsWith(value, CaseSensitive);} \
			TestQuery& EndsWith(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::EndsWith(value, CaseSensitive);} \
			TestQuery& Contains(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::Contains(value, CaseSensitive);} \
		}; \
\
		class TestQueryQueryAccessorBool : private XQueryAccessorBool { \
		public: \
			TestQueryQueryAccessorBool(size_t column_id) : XQueryAccessorBool(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(bool value) {return (TestQuery &)XQueryAccessorBool::Equal(value);} \
		}; \
\
		TestQueryQueryAccessor##CType1 CName1; \
		TestQueryQueryAccessor##CType2 CName2; \
		TestQueryQueryAccessor##CType3 CName3; \
		TestQueryQueryAccessor##CType4 CName4; \
		TestQueryQueryAccessor##CType5 CName5; \
		TestQueryQueryAccessor##CType6 CName6; \
		TestQueryQueryAccessor##CType7 CName7; \
		TestQueryQueryAccessor##CType8 CName8; \
		TestQueryQueryAccessor##CType9 CName9; \
\
		TestQuery& LeftParan(void) {Query::LeftParan(); return *this;}; \
		TestQuery& Or(void) {Query::Or(); return *this;}; \
		TestQuery& RightParan(void) {Query::RightParan(); return *this;}; \
		TestQuery& Subtable(size_t column) {Query::Subtable(column); return *this;}; \
		TestQuery& Parent() {Query::Parent(); return *this;}; \
	}; \
\
	TestQuery GetQuery() {return TestQuery();} \
\
	class Cursor : public CursorBase { \
	public: \
		Cursor(TableName& table, size_t ndx) : CursorBase(table, ndx) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
		} \
		Cursor(const TableName& table, size_t ndx) : CursorBase(const_cast<TableName&>(table), ndx) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
		} \
		Cursor(const Cursor& v) : CursorBase(v) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
		} \
		Accessor##CType1 CName1; \
		Accessor##CType2 CName2; \
		Accessor##CType3 CName3; \
		Accessor##CType4 CName4; \
		Accessor##CType5 CName5; \
		Accessor##CType6 CName6; \
		Accessor##CType7 CName7; \
		Accessor##CType8 CName8; \
		Accessor##CType9 CName9; \
	}; \
\
	void Add(tdbType##CType1 CName1, tdbType##CType2 CName2, tdbType##CType3 CName3, tdbType##CType4 CName4, tdbType##CType5 CName5, tdbType##CType6 CName6, tdbType##CType7 CName7, tdbType##CType8 CName8, tdbType##CType9 CName9) { \
		const size_t ndx = GetSize(); \
		Insert##CType1 (0, ndx, CName1); \
		Insert##CType2 (1, ndx, CName2); \
		Insert##CType3 (2, ndx, CName3); \
		Insert##CType4 (3, ndx, CName4); \
		Insert##CType5 (4, ndx, CName5); \
		Insert##CType6 (5, ndx, CName6); \
		Insert##CType7 (6, ndx, CName7); \
		Insert##CType8 (7, ndx, CName8); \
		Insert##CType9 (8, ndx, CName9); \
		InsertDone(); \
	} \
\
	void Insert(size_t ndx, tdbType##CType1 CName1, tdbType##CType2 CName2, tdbType##CType3 CName3, tdbType##CType4 CName4, tdbType##CType5 CName5, tdbType##CType6 CName6, tdbType##CType7 CName7, tdbType##CType8 CName8, tdbType##CType9 CName9) { \
		Insert##CType1 (0, ndx, CName1); \
		Insert##CType2 (1, ndx, CName2); \
		Insert##CType3 (2, ndx, CName3); \
		Insert##CType4 (3, ndx, CName4); \
		Insert##CType5 (4, ndx, CName5); \
		Insert##CType6 (5, ndx, CName6); \
		Insert##CType7 (6, ndx, CName7); \
		Insert##CType8 (7, ndx, CName8); \
		Insert##CType9 (8, ndx, CName9); \
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
	ColumnProxy##CType3 CName3; \
	ColumnProxy##CType4 CName4; \
	ColumnProxy##CType5 CName5; \
	ColumnProxy##CType6 CName6; \
	ColumnProxy##CType7 CName7; \
	ColumnProxy##CType8 CName8; \
	ColumnProxy##CType9 CName9; \
\
protected: \
	friend class Group; \
	TableName(Allocator& alloc, size_t ref, Array* parent, size_t pndx) : TopLevelTable(alloc, ref, parent, pndx) {}; \
\
private: \
	TableName(const TableName&) {} \
	TableName& operator=(const TableName&) {return *this;} \
};



#define TDB_TABLE_10(TableName, CType1, CName1, CType2, CName2, CType3, CName3, CType4, CName4, CType5, CName5, CType6, CName6, CType7, CName7, CType8, CName8, CType9, CName9, CType10, CName10) \
class TableName##Query { \
protected: \
	QueryAccessor##CType1 CName1; \
	QueryAccessor##CType2 CName2; \
	QueryAccessor##CType3 CName3; \
	QueryAccessor##CType4 CName4; \
	QueryAccessor##CType5 CName5; \
	QueryAccessor##CType6 CName6; \
	QueryAccessor##CType7 CName7; \
	QueryAccessor##CType8 CName8; \
	QueryAccessor##CType9 CName9; \
	QueryAccessor##CType10 CName10; \
}; \
\
class TableName : public TopLevelTable { \
public: \
	TableName(Allocator& alloc=GetDefaultAllocator()) : TopLevelTable(alloc) { \
		RegisterColumn(Accessor##CType1::type, #CName1); \
		RegisterColumn(Accessor##CType2::type, #CName2); \
		RegisterColumn(Accessor##CType3::type, #CName3); \
		RegisterColumn(Accessor##CType4::type, #CName4); \
		RegisterColumn(Accessor##CType5::type, #CName5); \
		RegisterColumn(Accessor##CType6::type, #CName6); \
		RegisterColumn(Accessor##CType7::type, #CName7); \
		RegisterColumn(Accessor##CType8::type, #CName8); \
		RegisterColumn(Accessor##CType9::type, #CName9); \
		RegisterColumn(Accessor##CType10::type, #CName10); \
\
		CName1.Create(this, 0); \
		CName2.Create(this, 1); \
		CName3.Create(this, 2); \
		CName4.Create(this, 3); \
		CName5.Create(this, 4); \
		CName6.Create(this, 5); \
		CName7.Create(this, 6); \
		CName8.Create(this, 7); \
		CName9.Create(this, 8); \
		CName10.Create(this, 9); \
	}; \
\
	class TestQuery : public Query { \
	public: \
		TestQuery() : CName1(0), CName2(1), CName3(2), CName4(3), CName5(4), CName6(5), CName7(6), CName8(7), CName9(8), CName10(9) { \
			CName1.SetQuery(this); \
			CName2.SetQuery(this); \
			CName3.SetQuery(this); \
			CName4.SetQuery(this); \
			CName5.SetQuery(this); \
			CName6.SetQuery(this); \
			CName7.SetQuery(this); \
			CName8.SetQuery(this); \
			CName9.SetQuery(this); \
			CName10.SetQuery(this); \
		} \
\
		TestQuery(const TestQuery& copy) : Query(copy), CName1(0), CName2(1), CName3(2), CName4(3), CName5(4), CName6(5), CName7(6), CName8(7), CName9(8), CName10(9) { \
			CName1.SetQuery(this); \
			CName2.SetQuery(this); \
			CName3.SetQuery(this); \
			CName4.SetQuery(this); \
			CName5.SetQuery(this); \
			CName6.SetQuery(this); \
			CName7.SetQuery(this); \
			CName8.SetQuery(this); \
			CName9.SetQuery(this); \
			CName10.SetQuery(this); \
		} \
\
		class TestQueryQueryAccessorInt : private XQueryAccessorInt { \
		public: \
			TestQueryQueryAccessorInt(size_t column_id) : XQueryAccessorInt(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(int64_t value) {return (TestQuery &)XQueryAccessorInt::Equal(value);} \
			TestQuery& NotEqual(int64_t value) {return (TestQuery &)XQueryAccessorInt::NotEqual(value);} \
			TestQuery& Greater(int64_t value) {return (TestQuery &)XQueryAccessorInt::Greater(value);} \
			TestQuery& Less(int64_t value) {return (TestQuery &)XQueryAccessorInt::Less(value);} \
			TestQuery& Between(int64_t from, int64_t to) {return (TestQuery &)XQueryAccessorInt::Between(from, to);} \
		}; \
\
		template <class T> class TestQueryQueryAccessorEnum : public TestQueryQueryAccessorInt { \
		public: \
			TestQueryQueryAccessorEnum<T>(size_t column_id) : TestQueryQueryAccessorInt(column_id) {} \
		}; \
\
		class TestQueryQueryAccessorString : private XQueryAccessorString { \
		public: \
			TestQueryQueryAccessorString(size_t column_id) : XQueryAccessorString(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::Equal(value, CaseSensitive);} \
			TestQuery& NotEqual(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::NotEqual(value, CaseSensitive);} \
			TestQuery& BeginsWith(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::BeginsWith(value, CaseSensitive);} \
			TestQuery& EndsWith(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::EndsWith(value, CaseSensitive);} \
			TestQuery& Contains(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::Contains(value, CaseSensitive);} \
		}; \
\
		class TestQueryQueryAccessorBool : private XQueryAccessorBool { \
		public: \
			TestQueryQueryAccessorBool(size_t column_id) : XQueryAccessorBool(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(bool value) {return (TestQuery &)XQueryAccessorBool::Equal(value);} \
		}; \
\
		TestQueryQueryAccessor##CType1 CName1; \
		TestQueryQueryAccessor##CType2 CName2; \
		TestQueryQueryAccessor##CType3 CName3; \
		TestQueryQueryAccessor##CType4 CName4; \
		TestQueryQueryAccessor##CType5 CName5; \
		TestQueryQueryAccessor##CType6 CName6; \
		TestQueryQueryAccessor##CType7 CName7; \
		TestQueryQueryAccessor##CType8 CName8; \
		TestQueryQueryAccessor##CType9 CName9; \
		TestQueryQueryAccessor##CType10 CName10; \
\
		TestQuery& LeftParan(void) {Query::LeftParan(); return *this;}; \
		TestQuery& Or(void) {Query::Or(); return *this;}; \
		TestQuery& RightParan(void) {Query::RightParan(); return *this;}; \
		TestQuery& Subtable(size_t column) {Query::Subtable(column); return *this;}; \
		TestQuery& Parent() {Query::Parent(); return *this;}; \
	}; \
\
	TestQuery GetQuery() {return TestQuery();} \
\
	class Cursor : public CursorBase { \
	public: \
		Cursor(TableName& table, size_t ndx) : CursorBase(table, ndx) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
		} \
		Cursor(const TableName& table, size_t ndx) : CursorBase(const_cast<TableName&>(table), ndx) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
		} \
		Cursor(const Cursor& v) : CursorBase(v) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
		} \
		Accessor##CType1 CName1; \
		Accessor##CType2 CName2; \
		Accessor##CType3 CName3; \
		Accessor##CType4 CName4; \
		Accessor##CType5 CName5; \
		Accessor##CType6 CName6; \
		Accessor##CType7 CName7; \
		Accessor##CType8 CName8; \
		Accessor##CType9 CName9; \
		Accessor##CType10 CName10; \
	}; \
\
	void Add(tdbType##CType1 CName1, tdbType##CType2 CName2, tdbType##CType3 CName3, tdbType##CType4 CName4, tdbType##CType5 CName5, tdbType##CType6 CName6, tdbType##CType7 CName7, tdbType##CType8 CName8, tdbType##CType9 CName9, tdbType##CType10 CName10) { \
		const size_t ndx = GetSize(); \
		Insert##CType1 (0, ndx, CName1); \
		Insert##CType2 (1, ndx, CName2); \
		Insert##CType3 (2, ndx, CName3); \
		Insert##CType4 (3, ndx, CName4); \
		Insert##CType5 (4, ndx, CName5); \
		Insert##CType6 (5, ndx, CName6); \
		Insert##CType7 (6, ndx, CName7); \
		Insert##CType8 (7, ndx, CName8); \
		Insert##CType9 (8, ndx, CName9); \
		Insert##CType10 (9, ndx, CName10); \
		InsertDone(); \
	} \
\
	void Insert(size_t ndx, tdbType##CType1 CName1, tdbType##CType2 CName2, tdbType##CType3 CName3, tdbType##CType4 CName4, tdbType##CType5 CName5, tdbType##CType6 CName6, tdbType##CType7 CName7, tdbType##CType8 CName8, tdbType##CType9 CName9, tdbType##CType10 CName10) { \
		Insert##CType1 (0, ndx, CName1); \
		Insert##CType2 (1, ndx, CName2); \
		Insert##CType3 (2, ndx, CName3); \
		Insert##CType4 (3, ndx, CName4); \
		Insert##CType5 (4, ndx, CName5); \
		Insert##CType6 (5, ndx, CName6); \
		Insert##CType7 (6, ndx, CName7); \
		Insert##CType8 (7, ndx, CName8); \
		Insert##CType9 (8, ndx, CName9); \
		Insert##CType10 (9, ndx, CName10); \
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
	ColumnProxy##CType3 CName3; \
	ColumnProxy##CType4 CName4; \
	ColumnProxy##CType5 CName5; \
	ColumnProxy##CType6 CName6; \
	ColumnProxy##CType7 CName7; \
	ColumnProxy##CType8 CName8; \
	ColumnProxy##CType9 CName9; \
	ColumnProxy##CType10 CName10; \
\
protected: \
	friend class Group; \
	TableName(Allocator& alloc, size_t ref, Array* parent, size_t pndx) : TopLevelTable(alloc, ref, parent, pndx) {}; \
\
private: \
	TableName(const TableName&) {} \
	TableName& operator=(const TableName&) {return *this;} \
};



#define TDB_TABLE_11(TableName, CType1, CName1, CType2, CName2, CType3, CName3, CType4, CName4, CType5, CName5, CType6, CName6, CType7, CName7, CType8, CName8, CType9, CName9, CType10, CName10, CType11, CName11) \
class TableName##Query { \
protected: \
	QueryAccessor##CType1 CName1; \
	QueryAccessor##CType2 CName2; \
	QueryAccessor##CType3 CName3; \
	QueryAccessor##CType4 CName4; \
	QueryAccessor##CType5 CName5; \
	QueryAccessor##CType6 CName6; \
	QueryAccessor##CType7 CName7; \
	QueryAccessor##CType8 CName8; \
	QueryAccessor##CType9 CName9; \
	QueryAccessor##CType10 CName10; \
	QueryAccessor##CType11 CName11; \
}; \
\
class TableName : public TopLevelTable { \
public: \
	TableName(Allocator& alloc=GetDefaultAllocator()) : TopLevelTable(alloc) { \
		RegisterColumn(Accessor##CType1::type, #CName1); \
		RegisterColumn(Accessor##CType2::type, #CName2); \
		RegisterColumn(Accessor##CType3::type, #CName3); \
		RegisterColumn(Accessor##CType4::type, #CName4); \
		RegisterColumn(Accessor##CType5::type, #CName5); \
		RegisterColumn(Accessor##CType6::type, #CName6); \
		RegisterColumn(Accessor##CType7::type, #CName7); \
		RegisterColumn(Accessor##CType8::type, #CName8); \
		RegisterColumn(Accessor##CType9::type, #CName9); \
		RegisterColumn(Accessor##CType10::type, #CName10); \
		RegisterColumn(Accessor##CType11::type, #CName11); \
\
		CName1.Create(this, 0); \
		CName2.Create(this, 1); \
		CName3.Create(this, 2); \
		CName4.Create(this, 3); \
		CName5.Create(this, 4); \
		CName6.Create(this, 5); \
		CName7.Create(this, 6); \
		CName8.Create(this, 7); \
		CName9.Create(this, 8); \
		CName10.Create(this, 9); \
		CName11.Create(this, 10); \
	}; \
\
	class TestQuery : public Query { \
	public: \
		TestQuery() : CName1(0), CName2(1), CName3(2), CName4(3), CName5(4), CName6(5), CName7(6), CName8(7), CName9(8), CName10(9), CName11(10) { \
			CName1.SetQuery(this); \
			CName2.SetQuery(this); \
			CName3.SetQuery(this); \
			CName4.SetQuery(this); \
			CName5.SetQuery(this); \
			CName6.SetQuery(this); \
			CName7.SetQuery(this); \
			CName8.SetQuery(this); \
			CName9.SetQuery(this); \
			CName10.SetQuery(this); \
			CName11.SetQuery(this); \
		} \
\
		TestQuery(const TestQuery& copy) : Query(copy), CName1(0), CName2(1), CName3(2), CName4(3), CName5(4), CName6(5), CName7(6), CName8(7), CName9(8), CName10(9), CName11(10) { \
			CName1.SetQuery(this); \
			CName2.SetQuery(this); \
			CName3.SetQuery(this); \
			CName4.SetQuery(this); \
			CName5.SetQuery(this); \
			CName6.SetQuery(this); \
			CName7.SetQuery(this); \
			CName8.SetQuery(this); \
			CName9.SetQuery(this); \
			CName10.SetQuery(this); \
			CName11.SetQuery(this); \
		} \
\
		class TestQueryQueryAccessorInt : private XQueryAccessorInt { \
		public: \
			TestQueryQueryAccessorInt(size_t column_id) : XQueryAccessorInt(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(int64_t value) {return (TestQuery &)XQueryAccessorInt::Equal(value);} \
			TestQuery& NotEqual(int64_t value) {return (TestQuery &)XQueryAccessorInt::NotEqual(value);} \
			TestQuery& Greater(int64_t value) {return (TestQuery &)XQueryAccessorInt::Greater(value);} \
			TestQuery& Less(int64_t value) {return (TestQuery &)XQueryAccessorInt::Less(value);} \
			TestQuery& Between(int64_t from, int64_t to) {return (TestQuery &)XQueryAccessorInt::Between(from, to);} \
		}; \
\
		template <class T> class TestQueryQueryAccessorEnum : public TestQueryQueryAccessorInt { \
		public: \
			TestQueryQueryAccessorEnum<T>(size_t column_id) : TestQueryQueryAccessorInt(column_id) {} \
		}; \
\
		class TestQueryQueryAccessorString : private XQueryAccessorString { \
		public: \
			TestQueryQueryAccessorString(size_t column_id) : XQueryAccessorString(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::Equal(value, CaseSensitive);} \
			TestQuery& NotEqual(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::NotEqual(value, CaseSensitive);} \
			TestQuery& BeginsWith(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::BeginsWith(value, CaseSensitive);} \
			TestQuery& EndsWith(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::EndsWith(value, CaseSensitive);} \
			TestQuery& Contains(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::Contains(value, CaseSensitive);} \
		}; \
\
		class TestQueryQueryAccessorBool : private XQueryAccessorBool { \
		public: \
			TestQueryQueryAccessorBool(size_t column_id) : XQueryAccessorBool(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(bool value) {return (TestQuery &)XQueryAccessorBool::Equal(value);} \
		}; \
\
		TestQueryQueryAccessor##CType1 CName1; \
		TestQueryQueryAccessor##CType2 CName2; \
		TestQueryQueryAccessor##CType3 CName3; \
		TestQueryQueryAccessor##CType4 CName4; \
		TestQueryQueryAccessor##CType5 CName5; \
		TestQueryQueryAccessor##CType6 CName6; \
		TestQueryQueryAccessor##CType7 CName7; \
		TestQueryQueryAccessor##CType8 CName8; \
		TestQueryQueryAccessor##CType9 CName9; \
		TestQueryQueryAccessor##CType10 CName10; \
		TestQueryQueryAccessor##CType11 CName11; \
\
		TestQuery& LeftParan(void) {Query::LeftParan(); return *this;}; \
		TestQuery& Or(void) {Query::Or(); return *this;}; \
		TestQuery& RightParan(void) {Query::RightParan(); return *this;}; \
		TestQuery& Subtable(size_t column) {Query::Subtable(column); return *this;}; \
		TestQuery& Parent() {Query::Parent(); return *this;}; \
	}; \
\
	TestQuery GetQuery() {return TestQuery();} \
\
	class Cursor : public CursorBase { \
	public: \
		Cursor(TableName& table, size_t ndx) : CursorBase(table, ndx) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
		} \
		Cursor(const TableName& table, size_t ndx) : CursorBase(const_cast<TableName&>(table), ndx) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
		} \
		Cursor(const Cursor& v) : CursorBase(v) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
		} \
		Accessor##CType1 CName1; \
		Accessor##CType2 CName2; \
		Accessor##CType3 CName3; \
		Accessor##CType4 CName4; \
		Accessor##CType5 CName5; \
		Accessor##CType6 CName6; \
		Accessor##CType7 CName7; \
		Accessor##CType8 CName8; \
		Accessor##CType9 CName9; \
		Accessor##CType10 CName10; \
		Accessor##CType11 CName11; \
	}; \
\
	void Add(tdbType##CType1 CName1, tdbType##CType2 CName2, tdbType##CType3 CName3, tdbType##CType4 CName4, tdbType##CType5 CName5, tdbType##CType6 CName6, tdbType##CType7 CName7, tdbType##CType8 CName8, tdbType##CType9 CName9, tdbType##CType10 CName10, tdbType##CType11 CName11) { \
		const size_t ndx = GetSize(); \
		Insert##CType1 (0, ndx, CName1); \
		Insert##CType2 (1, ndx, CName2); \
		Insert##CType3 (2, ndx, CName3); \
		Insert##CType4 (3, ndx, CName4); \
		Insert##CType5 (4, ndx, CName5); \
		Insert##CType6 (5, ndx, CName6); \
		Insert##CType7 (6, ndx, CName7); \
		Insert##CType8 (7, ndx, CName8); \
		Insert##CType9 (8, ndx, CName9); \
		Insert##CType10 (9, ndx, CName10); \
		Insert##CType11 (10, ndx, CName11); \
		InsertDone(); \
	} \
\
	void Insert(size_t ndx, tdbType##CType1 CName1, tdbType##CType2 CName2, tdbType##CType3 CName3, tdbType##CType4 CName4, tdbType##CType5 CName5, tdbType##CType6 CName6, tdbType##CType7 CName7, tdbType##CType8 CName8, tdbType##CType9 CName9, tdbType##CType10 CName10, tdbType##CType11 CName11) { \
		Insert##CType1 (0, ndx, CName1); \
		Insert##CType2 (1, ndx, CName2); \
		Insert##CType3 (2, ndx, CName3); \
		Insert##CType4 (3, ndx, CName4); \
		Insert##CType5 (4, ndx, CName5); \
		Insert##CType6 (5, ndx, CName6); \
		Insert##CType7 (6, ndx, CName7); \
		Insert##CType8 (7, ndx, CName8); \
		Insert##CType9 (8, ndx, CName9); \
		Insert##CType10 (9, ndx, CName10); \
		Insert##CType11 (10, ndx, CName11); \
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
	ColumnProxy##CType3 CName3; \
	ColumnProxy##CType4 CName4; \
	ColumnProxy##CType5 CName5; \
	ColumnProxy##CType6 CName6; \
	ColumnProxy##CType7 CName7; \
	ColumnProxy##CType8 CName8; \
	ColumnProxy##CType9 CName9; \
	ColumnProxy##CType10 CName10; \
	ColumnProxy##CType11 CName11; \
\
protected: \
	friend class Group; \
	TableName(Allocator& alloc, size_t ref, Array* parent, size_t pndx) : TopLevelTable(alloc, ref, parent, pndx) {}; \
\
private: \
	TableName(const TableName&) {} \
	TableName& operator=(const TableName&) {return *this;} \
};



#define TDB_TABLE_12(TableName, CType1, CName1, CType2, CName2, CType3, CName3, CType4, CName4, CType5, CName5, CType6, CName6, CType7, CName7, CType8, CName8, CType9, CName9, CType10, CName10, CType11, CName11, CType12, CName12) \
class TableName##Query { \
protected: \
	QueryAccessor##CType1 CName1; \
	QueryAccessor##CType2 CName2; \
	QueryAccessor##CType3 CName3; \
	QueryAccessor##CType4 CName4; \
	QueryAccessor##CType5 CName5; \
	QueryAccessor##CType6 CName6; \
	QueryAccessor##CType7 CName7; \
	QueryAccessor##CType8 CName8; \
	QueryAccessor##CType9 CName9; \
	QueryAccessor##CType10 CName10; \
	QueryAccessor##CType11 CName11; \
	QueryAccessor##CType12 CName12; \
}; \
\
class TableName : public TopLevelTable { \
public: \
	TableName(Allocator& alloc=GetDefaultAllocator()) : TopLevelTable(alloc) { \
		RegisterColumn(Accessor##CType1::type, #CName1); \
		RegisterColumn(Accessor##CType2::type, #CName2); \
		RegisterColumn(Accessor##CType3::type, #CName3); \
		RegisterColumn(Accessor##CType4::type, #CName4); \
		RegisterColumn(Accessor##CType5::type, #CName5); \
		RegisterColumn(Accessor##CType6::type, #CName6); \
		RegisterColumn(Accessor##CType7::type, #CName7); \
		RegisterColumn(Accessor##CType8::type, #CName8); \
		RegisterColumn(Accessor##CType9::type, #CName9); \
		RegisterColumn(Accessor##CType10::type, #CName10); \
		RegisterColumn(Accessor##CType11::type, #CName11); \
		RegisterColumn(Accessor##CType12::type, #CName12); \
\
		CName1.Create(this, 0); \
		CName2.Create(this, 1); \
		CName3.Create(this, 2); \
		CName4.Create(this, 3); \
		CName5.Create(this, 4); \
		CName6.Create(this, 5); \
		CName7.Create(this, 6); \
		CName8.Create(this, 7); \
		CName9.Create(this, 8); \
		CName10.Create(this, 9); \
		CName11.Create(this, 10); \
		CName12.Create(this, 11); \
	}; \
\
	class TestQuery : public Query { \
	public: \
		TestQuery() : CName1(0), CName2(1), CName3(2), CName4(3), CName5(4), CName6(5), CName7(6), CName8(7), CName9(8), CName10(9), CName11(10), CName12(11) { \
			CName1.SetQuery(this); \
			CName2.SetQuery(this); \
			CName3.SetQuery(this); \
			CName4.SetQuery(this); \
			CName5.SetQuery(this); \
			CName6.SetQuery(this); \
			CName7.SetQuery(this); \
			CName8.SetQuery(this); \
			CName9.SetQuery(this); \
			CName10.SetQuery(this); \
			CName11.SetQuery(this); \
			CName12.SetQuery(this); \
		} \
\
		TestQuery(const TestQuery& copy) : Query(copy), CName1(0), CName2(1), CName3(2), CName4(3), CName5(4), CName6(5), CName7(6), CName8(7), CName9(8), CName10(9), CName11(10), CName12(11) { \
			CName1.SetQuery(this); \
			CName2.SetQuery(this); \
			CName3.SetQuery(this); \
			CName4.SetQuery(this); \
			CName5.SetQuery(this); \
			CName6.SetQuery(this); \
			CName7.SetQuery(this); \
			CName8.SetQuery(this); \
			CName9.SetQuery(this); \
			CName10.SetQuery(this); \
			CName11.SetQuery(this); \
			CName12.SetQuery(this); \
		} \
\
		class TestQueryQueryAccessorInt : private XQueryAccessorInt { \
		public: \
			TestQueryQueryAccessorInt(size_t column_id) : XQueryAccessorInt(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(int64_t value) {return (TestQuery &)XQueryAccessorInt::Equal(value);} \
			TestQuery& NotEqual(int64_t value) {return (TestQuery &)XQueryAccessorInt::NotEqual(value);} \
			TestQuery& Greater(int64_t value) {return (TestQuery &)XQueryAccessorInt::Greater(value);} \
			TestQuery& Less(int64_t value) {return (TestQuery &)XQueryAccessorInt::Less(value);} \
			TestQuery& Between(int64_t from, int64_t to) {return (TestQuery &)XQueryAccessorInt::Between(from, to);} \
		}; \
\
		template <class T> class TestQueryQueryAccessorEnum : public TestQueryQueryAccessorInt { \
		public: \
			TestQueryQueryAccessorEnum<T>(size_t column_id) : TestQueryQueryAccessorInt(column_id) {} \
		}; \
\
		class TestQueryQueryAccessorString : private XQueryAccessorString { \
		public: \
			TestQueryQueryAccessorString(size_t column_id) : XQueryAccessorString(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::Equal(value, CaseSensitive);} \
			TestQuery& NotEqual(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::NotEqual(value, CaseSensitive);} \
			TestQuery& BeginsWith(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::BeginsWith(value, CaseSensitive);} \
			TestQuery& EndsWith(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::EndsWith(value, CaseSensitive);} \
			TestQuery& Contains(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::Contains(value, CaseSensitive);} \
		}; \
\
		class TestQueryQueryAccessorBool : private XQueryAccessorBool { \
		public: \
			TestQueryQueryAccessorBool(size_t column_id) : XQueryAccessorBool(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(bool value) {return (TestQuery &)XQueryAccessorBool::Equal(value);} \
		}; \
\
		TestQueryQueryAccessor##CType1 CName1; \
		TestQueryQueryAccessor##CType2 CName2; \
		TestQueryQueryAccessor##CType3 CName3; \
		TestQueryQueryAccessor##CType4 CName4; \
		TestQueryQueryAccessor##CType5 CName5; \
		TestQueryQueryAccessor##CType6 CName6; \
		TestQueryQueryAccessor##CType7 CName7; \
		TestQueryQueryAccessor##CType8 CName8; \
		TestQueryQueryAccessor##CType9 CName9; \
		TestQueryQueryAccessor##CType10 CName10; \
		TestQueryQueryAccessor##CType11 CName11; \
		TestQueryQueryAccessor##CType12 CName12; \
\
		TestQuery& LeftParan(void) {Query::LeftParan(); return *this;}; \
		TestQuery& Or(void) {Query::Or(); return *this;}; \
		TestQuery& RightParan(void) {Query::RightParan(); return *this;}; \
		TestQuery& Subtable(size_t column) {Query::Subtable(column); return *this;}; \
		TestQuery& Parent() {Query::Parent(); return *this;}; \
	}; \
\
	TestQuery GetQuery() {return TestQuery();} \
\
	class Cursor : public CursorBase { \
	public: \
		Cursor(TableName& table, size_t ndx) : CursorBase(table, ndx) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
		} \
		Cursor(const TableName& table, size_t ndx) : CursorBase(const_cast<TableName&>(table), ndx) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
		} \
		Cursor(const Cursor& v) : CursorBase(v) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
		} \
		Accessor##CType1 CName1; \
		Accessor##CType2 CName2; \
		Accessor##CType3 CName3; \
		Accessor##CType4 CName4; \
		Accessor##CType5 CName5; \
		Accessor##CType6 CName6; \
		Accessor##CType7 CName7; \
		Accessor##CType8 CName8; \
		Accessor##CType9 CName9; \
		Accessor##CType10 CName10; \
		Accessor##CType11 CName11; \
		Accessor##CType12 CName12; \
	}; \
\
	void Add(tdbType##CType1 CName1, tdbType##CType2 CName2, tdbType##CType3 CName3, tdbType##CType4 CName4, tdbType##CType5 CName5, tdbType##CType6 CName6, tdbType##CType7 CName7, tdbType##CType8 CName8, tdbType##CType9 CName9, tdbType##CType10 CName10, tdbType##CType11 CName11, tdbType##CType12 CName12) { \
		const size_t ndx = GetSize(); \
		Insert##CType1 (0, ndx, CName1); \
		Insert##CType2 (1, ndx, CName2); \
		Insert##CType3 (2, ndx, CName3); \
		Insert##CType4 (3, ndx, CName4); \
		Insert##CType5 (4, ndx, CName5); \
		Insert##CType6 (5, ndx, CName6); \
		Insert##CType7 (6, ndx, CName7); \
		Insert##CType8 (7, ndx, CName8); \
		Insert##CType9 (8, ndx, CName9); \
		Insert##CType10 (9, ndx, CName10); \
		Insert##CType11 (10, ndx, CName11); \
		Insert##CType12 (11, ndx, CName12); \
		InsertDone(); \
	} \
\
	void Insert(size_t ndx, tdbType##CType1 CName1, tdbType##CType2 CName2, tdbType##CType3 CName3, tdbType##CType4 CName4, tdbType##CType5 CName5, tdbType##CType6 CName6, tdbType##CType7 CName7, tdbType##CType8 CName8, tdbType##CType9 CName9, tdbType##CType10 CName10, tdbType##CType11 CName11, tdbType##CType12 CName12) { \
		Insert##CType1 (0, ndx, CName1); \
		Insert##CType2 (1, ndx, CName2); \
		Insert##CType3 (2, ndx, CName3); \
		Insert##CType4 (3, ndx, CName4); \
		Insert##CType5 (4, ndx, CName5); \
		Insert##CType6 (5, ndx, CName6); \
		Insert##CType7 (6, ndx, CName7); \
		Insert##CType8 (7, ndx, CName8); \
		Insert##CType9 (8, ndx, CName9); \
		Insert##CType10 (9, ndx, CName10); \
		Insert##CType11 (10, ndx, CName11); \
		Insert##CType12 (11, ndx, CName12); \
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
	ColumnProxy##CType3 CName3; \
	ColumnProxy##CType4 CName4; \
	ColumnProxy##CType5 CName5; \
	ColumnProxy##CType6 CName6; \
	ColumnProxy##CType7 CName7; \
	ColumnProxy##CType8 CName8; \
	ColumnProxy##CType9 CName9; \
	ColumnProxy##CType10 CName10; \
	ColumnProxy##CType11 CName11; \
	ColumnProxy##CType12 CName12; \
\
protected: \
	friend class Group; \
	TableName(Allocator& alloc, size_t ref, Array* parent, size_t pndx) : TopLevelTable(alloc, ref, parent, pndx) {}; \
\
private: \
	TableName(const TableName&) {} \
	TableName& operator=(const TableName&) {return *this;} \
};



#define TDB_TABLE_13(TableName, CType1, CName1, CType2, CName2, CType3, CName3, CType4, CName4, CType5, CName5, CType6, CName6, CType7, CName7, CType8, CName8, CType9, CName9, CType10, CName10, CType11, CName11, CType12, CName12, CType13, CName13) \
class TableName##Query { \
protected: \
	QueryAccessor##CType1 CName1; \
	QueryAccessor##CType2 CName2; \
	QueryAccessor##CType3 CName3; \
	QueryAccessor##CType4 CName4; \
	QueryAccessor##CType5 CName5; \
	QueryAccessor##CType6 CName6; \
	QueryAccessor##CType7 CName7; \
	QueryAccessor##CType8 CName8; \
	QueryAccessor##CType9 CName9; \
	QueryAccessor##CType10 CName10; \
	QueryAccessor##CType11 CName11; \
	QueryAccessor##CType12 CName12; \
	QueryAccessor##CType13 CName13; \
}; \
\
class TableName : public TopLevelTable { \
public: \
	TableName(Allocator& alloc=GetDefaultAllocator()) : TopLevelTable(alloc) { \
		RegisterColumn(Accessor##CType1::type, #CName1); \
		RegisterColumn(Accessor##CType2::type, #CName2); \
		RegisterColumn(Accessor##CType3::type, #CName3); \
		RegisterColumn(Accessor##CType4::type, #CName4); \
		RegisterColumn(Accessor##CType5::type, #CName5); \
		RegisterColumn(Accessor##CType6::type, #CName6); \
		RegisterColumn(Accessor##CType7::type, #CName7); \
		RegisterColumn(Accessor##CType8::type, #CName8); \
		RegisterColumn(Accessor##CType9::type, #CName9); \
		RegisterColumn(Accessor##CType10::type, #CName10); \
		RegisterColumn(Accessor##CType11::type, #CName11); \
		RegisterColumn(Accessor##CType12::type, #CName12); \
		RegisterColumn(Accessor##CType13::type, #CName13); \
\
		CName1.Create(this, 0); \
		CName2.Create(this, 1); \
		CName3.Create(this, 2); \
		CName4.Create(this, 3); \
		CName5.Create(this, 4); \
		CName6.Create(this, 5); \
		CName7.Create(this, 6); \
		CName8.Create(this, 7); \
		CName9.Create(this, 8); \
		CName10.Create(this, 9); \
		CName11.Create(this, 10); \
		CName12.Create(this, 11); \
		CName13.Create(this, 12); \
	}; \
\
	class TestQuery : public Query { \
	public: \
		TestQuery() : CName1(0), CName2(1), CName3(2), CName4(3), CName5(4), CName6(5), CName7(6), CName8(7), CName9(8), CName10(9), CName11(10), CName12(11), CName13(12) { \
			CName1.SetQuery(this); \
			CName2.SetQuery(this); \
			CName3.SetQuery(this); \
			CName4.SetQuery(this); \
			CName5.SetQuery(this); \
			CName6.SetQuery(this); \
			CName7.SetQuery(this); \
			CName8.SetQuery(this); \
			CName9.SetQuery(this); \
			CName10.SetQuery(this); \
			CName11.SetQuery(this); \
			CName12.SetQuery(this); \
			CName13.SetQuery(this); \
		} \
\
		TestQuery(const TestQuery& copy) : Query(copy), CName1(0), CName2(1), CName3(2), CName4(3), CName5(4), CName6(5), CName7(6), CName8(7), CName9(8), CName10(9), CName11(10), CName12(11), CName13(12) { \
			CName1.SetQuery(this); \
			CName2.SetQuery(this); \
			CName3.SetQuery(this); \
			CName4.SetQuery(this); \
			CName5.SetQuery(this); \
			CName6.SetQuery(this); \
			CName7.SetQuery(this); \
			CName8.SetQuery(this); \
			CName9.SetQuery(this); \
			CName10.SetQuery(this); \
			CName11.SetQuery(this); \
			CName12.SetQuery(this); \
			CName13.SetQuery(this); \
		} \
\
		class TestQueryQueryAccessorInt : private XQueryAccessorInt { \
		public: \
			TestQueryQueryAccessorInt(size_t column_id) : XQueryAccessorInt(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(int64_t value) {return (TestQuery &)XQueryAccessorInt::Equal(value);} \
			TestQuery& NotEqual(int64_t value) {return (TestQuery &)XQueryAccessorInt::NotEqual(value);} \
			TestQuery& Greater(int64_t value) {return (TestQuery &)XQueryAccessorInt::Greater(value);} \
			TestQuery& Less(int64_t value) {return (TestQuery &)XQueryAccessorInt::Less(value);} \
			TestQuery& Between(int64_t from, int64_t to) {return (TestQuery &)XQueryAccessorInt::Between(from, to);} \
		}; \
\
		template <class T> class TestQueryQueryAccessorEnum : public TestQueryQueryAccessorInt { \
		public: \
			TestQueryQueryAccessorEnum<T>(size_t column_id) : TestQueryQueryAccessorInt(column_id) {} \
		}; \
\
		class TestQueryQueryAccessorString : private XQueryAccessorString { \
		public: \
			TestQueryQueryAccessorString(size_t column_id) : XQueryAccessorString(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::Equal(value, CaseSensitive);} \
			TestQuery& NotEqual(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::NotEqual(value, CaseSensitive);} \
			TestQuery& BeginsWith(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::BeginsWith(value, CaseSensitive);} \
			TestQuery& EndsWith(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::EndsWith(value, CaseSensitive);} \
			TestQuery& Contains(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::Contains(value, CaseSensitive);} \
		}; \
\
		class TestQueryQueryAccessorBool : private XQueryAccessorBool { \
		public: \
			TestQueryQueryAccessorBool(size_t column_id) : XQueryAccessorBool(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(bool value) {return (TestQuery &)XQueryAccessorBool::Equal(value);} \
		}; \
\
		TestQueryQueryAccessor##CType1 CName1; \
		TestQueryQueryAccessor##CType2 CName2; \
		TestQueryQueryAccessor##CType3 CName3; \
		TestQueryQueryAccessor##CType4 CName4; \
		TestQueryQueryAccessor##CType5 CName5; \
		TestQueryQueryAccessor##CType6 CName6; \
		TestQueryQueryAccessor##CType7 CName7; \
		TestQueryQueryAccessor##CType8 CName8; \
		TestQueryQueryAccessor##CType9 CName9; \
		TestQueryQueryAccessor##CType10 CName10; \
		TestQueryQueryAccessor##CType11 CName11; \
		TestQueryQueryAccessor##CType12 CName12; \
		TestQueryQueryAccessor##CType13 CName13; \
\
		TestQuery& LeftParan(void) {Query::LeftParan(); return *this;}; \
		TestQuery& Or(void) {Query::Or(); return *this;}; \
		TestQuery& RightParan(void) {Query::RightParan(); return *this;}; \
		TestQuery& Subtable(size_t column) {Query::Subtable(column); return *this;}; \
		TestQuery& Parent() {Query::Parent(); return *this;}; \
	}; \
\
	TestQuery GetQuery() {return TestQuery();} \
\
	class Cursor : public CursorBase { \
	public: \
		Cursor(TableName& table, size_t ndx) : CursorBase(table, ndx) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
		} \
		Cursor(const TableName& table, size_t ndx) : CursorBase(const_cast<TableName&>(table), ndx) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
		} \
		Cursor(const Cursor& v) : CursorBase(v) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
		} \
		Accessor##CType1 CName1; \
		Accessor##CType2 CName2; \
		Accessor##CType3 CName3; \
		Accessor##CType4 CName4; \
		Accessor##CType5 CName5; \
		Accessor##CType6 CName6; \
		Accessor##CType7 CName7; \
		Accessor##CType8 CName8; \
		Accessor##CType9 CName9; \
		Accessor##CType10 CName10; \
		Accessor##CType11 CName11; \
		Accessor##CType12 CName12; \
		Accessor##CType13 CName13; \
	}; \
\
	void Add(tdbType##CType1 CName1, tdbType##CType2 CName2, tdbType##CType3 CName3, tdbType##CType4 CName4, tdbType##CType5 CName5, tdbType##CType6 CName6, tdbType##CType7 CName7, tdbType##CType8 CName8, tdbType##CType9 CName9, tdbType##CType10 CName10, tdbType##CType11 CName11, tdbType##CType12 CName12, tdbType##CType13 CName13) { \
		const size_t ndx = GetSize(); \
		Insert##CType1 (0, ndx, CName1); \
		Insert##CType2 (1, ndx, CName2); \
		Insert##CType3 (2, ndx, CName3); \
		Insert##CType4 (3, ndx, CName4); \
		Insert##CType5 (4, ndx, CName5); \
		Insert##CType6 (5, ndx, CName6); \
		Insert##CType7 (6, ndx, CName7); \
		Insert##CType8 (7, ndx, CName8); \
		Insert##CType9 (8, ndx, CName9); \
		Insert##CType10 (9, ndx, CName10); \
		Insert##CType11 (10, ndx, CName11); \
		Insert##CType12 (11, ndx, CName12); \
		Insert##CType13 (12, ndx, CName13); \
		InsertDone(); \
	} \
\
	void Insert(size_t ndx, tdbType##CType1 CName1, tdbType##CType2 CName2, tdbType##CType3 CName3, tdbType##CType4 CName4, tdbType##CType5 CName5, tdbType##CType6 CName6, tdbType##CType7 CName7, tdbType##CType8 CName8, tdbType##CType9 CName9, tdbType##CType10 CName10, tdbType##CType11 CName11, tdbType##CType12 CName12, tdbType##CType13 CName13) { \
		Insert##CType1 (0, ndx, CName1); \
		Insert##CType2 (1, ndx, CName2); \
		Insert##CType3 (2, ndx, CName3); \
		Insert##CType4 (3, ndx, CName4); \
		Insert##CType5 (4, ndx, CName5); \
		Insert##CType6 (5, ndx, CName6); \
		Insert##CType7 (6, ndx, CName7); \
		Insert##CType8 (7, ndx, CName8); \
		Insert##CType9 (8, ndx, CName9); \
		Insert##CType10 (9, ndx, CName10); \
		Insert##CType11 (10, ndx, CName11); \
		Insert##CType12 (11, ndx, CName12); \
		Insert##CType13 (12, ndx, CName13); \
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
	ColumnProxy##CType3 CName3; \
	ColumnProxy##CType4 CName4; \
	ColumnProxy##CType5 CName5; \
	ColumnProxy##CType6 CName6; \
	ColumnProxy##CType7 CName7; \
	ColumnProxy##CType8 CName8; \
	ColumnProxy##CType9 CName9; \
	ColumnProxy##CType10 CName10; \
	ColumnProxy##CType11 CName11; \
	ColumnProxy##CType12 CName12; \
	ColumnProxy##CType13 CName13; \
\
protected: \
	friend class Group; \
	TableName(Allocator& alloc, size_t ref, Array* parent, size_t pndx) : TopLevelTable(alloc, ref, parent, pndx) {}; \
\
private: \
	TableName(const TableName&) {} \
	TableName& operator=(const TableName&) {return *this;} \
};



#define TDB_TABLE_14(TableName, CType1, CName1, CType2, CName2, CType3, CName3, CType4, CName4, CType5, CName5, CType6, CName6, CType7, CName7, CType8, CName8, CType9, CName9, CType10, CName10, CType11, CName11, CType12, CName12, CType13, CName13, CType14, CName14) \
class TableName##Query { \
protected: \
	QueryAccessor##CType1 CName1; \
	QueryAccessor##CType2 CName2; \
	QueryAccessor##CType3 CName3; \
	QueryAccessor##CType4 CName4; \
	QueryAccessor##CType5 CName5; \
	QueryAccessor##CType6 CName6; \
	QueryAccessor##CType7 CName7; \
	QueryAccessor##CType8 CName8; \
	QueryAccessor##CType9 CName9; \
	QueryAccessor##CType10 CName10; \
	QueryAccessor##CType11 CName11; \
	QueryAccessor##CType12 CName12; \
	QueryAccessor##CType13 CName13; \
	QueryAccessor##CType14 CName14; \
}; \
\
class TableName : public TopLevelTable { \
public: \
	TableName(Allocator& alloc=GetDefaultAllocator()) : TopLevelTable(alloc) { \
		RegisterColumn(Accessor##CType1::type, #CName1); \
		RegisterColumn(Accessor##CType2::type, #CName2); \
		RegisterColumn(Accessor##CType3::type, #CName3); \
		RegisterColumn(Accessor##CType4::type, #CName4); \
		RegisterColumn(Accessor##CType5::type, #CName5); \
		RegisterColumn(Accessor##CType6::type, #CName6); \
		RegisterColumn(Accessor##CType7::type, #CName7); \
		RegisterColumn(Accessor##CType8::type, #CName8); \
		RegisterColumn(Accessor##CType9::type, #CName9); \
		RegisterColumn(Accessor##CType10::type, #CName10); \
		RegisterColumn(Accessor##CType11::type, #CName11); \
		RegisterColumn(Accessor##CType12::type, #CName12); \
		RegisterColumn(Accessor##CType13::type, #CName13); \
		RegisterColumn(Accessor##CType14::type, #CName14); \
\
		CName1.Create(this, 0); \
		CName2.Create(this, 1); \
		CName3.Create(this, 2); \
		CName4.Create(this, 3); \
		CName5.Create(this, 4); \
		CName6.Create(this, 5); \
		CName7.Create(this, 6); \
		CName8.Create(this, 7); \
		CName9.Create(this, 8); \
		CName10.Create(this, 9); \
		CName11.Create(this, 10); \
		CName12.Create(this, 11); \
		CName13.Create(this, 12); \
		CName14.Create(this, 13); \
	}; \
\
	class TestQuery : public Query { \
	public: \
		TestQuery() : CName1(0), CName2(1), CName3(2), CName4(3), CName5(4), CName6(5), CName7(6), CName8(7), CName9(8), CName10(9), CName11(10), CName12(11), CName13(12), CName14(13) { \
			CName1.SetQuery(this); \
			CName2.SetQuery(this); \
			CName3.SetQuery(this); \
			CName4.SetQuery(this); \
			CName5.SetQuery(this); \
			CName6.SetQuery(this); \
			CName7.SetQuery(this); \
			CName8.SetQuery(this); \
			CName9.SetQuery(this); \
			CName10.SetQuery(this); \
			CName11.SetQuery(this); \
			CName12.SetQuery(this); \
			CName13.SetQuery(this); \
			CName14.SetQuery(this); \
		} \
\
		TestQuery(const TestQuery& copy) : Query(copy), CName1(0), CName2(1), CName3(2), CName4(3), CName5(4), CName6(5), CName7(6), CName8(7), CName9(8), CName10(9), CName11(10), CName12(11), CName13(12), CName14(13) { \
			CName1.SetQuery(this); \
			CName2.SetQuery(this); \
			CName3.SetQuery(this); \
			CName4.SetQuery(this); \
			CName5.SetQuery(this); \
			CName6.SetQuery(this); \
			CName7.SetQuery(this); \
			CName8.SetQuery(this); \
			CName9.SetQuery(this); \
			CName10.SetQuery(this); \
			CName11.SetQuery(this); \
			CName12.SetQuery(this); \
			CName13.SetQuery(this); \
			CName14.SetQuery(this); \
		} \
\
		class TestQueryQueryAccessorInt : private XQueryAccessorInt { \
		public: \
			TestQueryQueryAccessorInt(size_t column_id) : XQueryAccessorInt(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(int64_t value) {return (TestQuery &)XQueryAccessorInt::Equal(value);} \
			TestQuery& NotEqual(int64_t value) {return (TestQuery &)XQueryAccessorInt::NotEqual(value);} \
			TestQuery& Greater(int64_t value) {return (TestQuery &)XQueryAccessorInt::Greater(value);} \
			TestQuery& Less(int64_t value) {return (TestQuery &)XQueryAccessorInt::Less(value);} \
			TestQuery& Between(int64_t from, int64_t to) {return (TestQuery &)XQueryAccessorInt::Between(from, to);} \
		}; \
\
		template <class T> class TestQueryQueryAccessorEnum : public TestQueryQueryAccessorInt { \
		public: \
			TestQueryQueryAccessorEnum<T>(size_t column_id) : TestQueryQueryAccessorInt(column_id) {} \
		}; \
\
		class TestQueryQueryAccessorString : private XQueryAccessorString { \
		public: \
			TestQueryQueryAccessorString(size_t column_id) : XQueryAccessorString(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::Equal(value, CaseSensitive);} \
			TestQuery& NotEqual(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::NotEqual(value, CaseSensitive);} \
			TestQuery& BeginsWith(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::BeginsWith(value, CaseSensitive);} \
			TestQuery& EndsWith(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::EndsWith(value, CaseSensitive);} \
			TestQuery& Contains(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::Contains(value, CaseSensitive);} \
		}; \
\
		class TestQueryQueryAccessorBool : private XQueryAccessorBool { \
		public: \
			TestQueryQueryAccessorBool(size_t column_id) : XQueryAccessorBool(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(bool value) {return (TestQuery &)XQueryAccessorBool::Equal(value);} \
		}; \
\
		TestQueryQueryAccessor##CType1 CName1; \
		TestQueryQueryAccessor##CType2 CName2; \
		TestQueryQueryAccessor##CType3 CName3; \
		TestQueryQueryAccessor##CType4 CName4; \
		TestQueryQueryAccessor##CType5 CName5; \
		TestQueryQueryAccessor##CType6 CName6; \
		TestQueryQueryAccessor##CType7 CName7; \
		TestQueryQueryAccessor##CType8 CName8; \
		TestQueryQueryAccessor##CType9 CName9; \
		TestQueryQueryAccessor##CType10 CName10; \
		TestQueryQueryAccessor##CType11 CName11; \
		TestQueryQueryAccessor##CType12 CName12; \
		TestQueryQueryAccessor##CType13 CName13; \
		TestQueryQueryAccessor##CType14 CName14; \
\
		TestQuery& LeftParan(void) {Query::LeftParan(); return *this;}; \
		TestQuery& Or(void) {Query::Or(); return *this;}; \
		TestQuery& RightParan(void) {Query::RightParan(); return *this;}; \
		TestQuery& Subtable(size_t column) {Query::Subtable(column); return *this;}; \
		TestQuery& Parent() {Query::Parent(); return *this;}; \
	}; \
\
	TestQuery GetQuery() {return TestQuery();} \
\
	class Cursor : public CursorBase { \
	public: \
		Cursor(TableName& table, size_t ndx) : CursorBase(table, ndx) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
		} \
		Cursor(const TableName& table, size_t ndx) : CursorBase(const_cast<TableName&>(table), ndx) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
		} \
		Cursor(const Cursor& v) : CursorBase(v) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
		} \
		Accessor##CType1 CName1; \
		Accessor##CType2 CName2; \
		Accessor##CType3 CName3; \
		Accessor##CType4 CName4; \
		Accessor##CType5 CName5; \
		Accessor##CType6 CName6; \
		Accessor##CType7 CName7; \
		Accessor##CType8 CName8; \
		Accessor##CType9 CName9; \
		Accessor##CType10 CName10; \
		Accessor##CType11 CName11; \
		Accessor##CType12 CName12; \
		Accessor##CType13 CName13; \
		Accessor##CType14 CName14; \
	}; \
\
	void Add(tdbType##CType1 CName1, tdbType##CType2 CName2, tdbType##CType3 CName3, tdbType##CType4 CName4, tdbType##CType5 CName5, tdbType##CType6 CName6, tdbType##CType7 CName7, tdbType##CType8 CName8, tdbType##CType9 CName9, tdbType##CType10 CName10, tdbType##CType11 CName11, tdbType##CType12 CName12, tdbType##CType13 CName13, tdbType##CType14 CName14) { \
		const size_t ndx = GetSize(); \
		Insert##CType1 (0, ndx, CName1); \
		Insert##CType2 (1, ndx, CName2); \
		Insert##CType3 (2, ndx, CName3); \
		Insert##CType4 (3, ndx, CName4); \
		Insert##CType5 (4, ndx, CName5); \
		Insert##CType6 (5, ndx, CName6); \
		Insert##CType7 (6, ndx, CName7); \
		Insert##CType8 (7, ndx, CName8); \
		Insert##CType9 (8, ndx, CName9); \
		Insert##CType10 (9, ndx, CName10); \
		Insert##CType11 (10, ndx, CName11); \
		Insert##CType12 (11, ndx, CName12); \
		Insert##CType13 (12, ndx, CName13); \
		Insert##CType14 (13, ndx, CName14); \
		InsertDone(); \
	} \
\
	void Insert(size_t ndx, tdbType##CType1 CName1, tdbType##CType2 CName2, tdbType##CType3 CName3, tdbType##CType4 CName4, tdbType##CType5 CName5, tdbType##CType6 CName6, tdbType##CType7 CName7, tdbType##CType8 CName8, tdbType##CType9 CName9, tdbType##CType10 CName10, tdbType##CType11 CName11, tdbType##CType12 CName12, tdbType##CType13 CName13, tdbType##CType14 CName14) { \
		Insert##CType1 (0, ndx, CName1); \
		Insert##CType2 (1, ndx, CName2); \
		Insert##CType3 (2, ndx, CName3); \
		Insert##CType4 (3, ndx, CName4); \
		Insert##CType5 (4, ndx, CName5); \
		Insert##CType6 (5, ndx, CName6); \
		Insert##CType7 (6, ndx, CName7); \
		Insert##CType8 (7, ndx, CName8); \
		Insert##CType9 (8, ndx, CName9); \
		Insert##CType10 (9, ndx, CName10); \
		Insert##CType11 (10, ndx, CName11); \
		Insert##CType12 (11, ndx, CName12); \
		Insert##CType13 (12, ndx, CName13); \
		Insert##CType14 (13, ndx, CName14); \
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
	ColumnProxy##CType3 CName3; \
	ColumnProxy##CType4 CName4; \
	ColumnProxy##CType5 CName5; \
	ColumnProxy##CType6 CName6; \
	ColumnProxy##CType7 CName7; \
	ColumnProxy##CType8 CName8; \
	ColumnProxy##CType9 CName9; \
	ColumnProxy##CType10 CName10; \
	ColumnProxy##CType11 CName11; \
	ColumnProxy##CType12 CName12; \
	ColumnProxy##CType13 CName13; \
	ColumnProxy##CType14 CName14; \
\
protected: \
	friend class Group; \
	TableName(Allocator& alloc, size_t ref, Array* parent, size_t pndx) : TopLevelTable(alloc, ref, parent, pndx) {}; \
\
private: \
	TableName(const TableName&) {} \
	TableName& operator=(const TableName&) {return *this;} \
};



#define TDB_TABLE_15(TableName, CType1, CName1, CType2, CName2, CType3, CName3, CType4, CName4, CType5, CName5, CType6, CName6, CType7, CName7, CType8, CName8, CType9, CName9, CType10, CName10, CType11, CName11, CType12, CName12, CType13, CName13, CType14, CName14, CType15, CName15) \
class TableName##Query { \
protected: \
	QueryAccessor##CType1 CName1; \
	QueryAccessor##CType2 CName2; \
	QueryAccessor##CType3 CName3; \
	QueryAccessor##CType4 CName4; \
	QueryAccessor##CType5 CName5; \
	QueryAccessor##CType6 CName6; \
	QueryAccessor##CType7 CName7; \
	QueryAccessor##CType8 CName8; \
	QueryAccessor##CType9 CName9; \
	QueryAccessor##CType10 CName10; \
	QueryAccessor##CType11 CName11; \
	QueryAccessor##CType12 CName12; \
	QueryAccessor##CType13 CName13; \
	QueryAccessor##CType14 CName14; \
	QueryAccessor##CType15 CName15; \
}; \
\
class TableName : public TopLevelTable { \
public: \
	TableName(Allocator& alloc=GetDefaultAllocator()) : TopLevelTable(alloc) { \
		RegisterColumn(Accessor##CType1::type, #CName1); \
		RegisterColumn(Accessor##CType2::type, #CName2); \
		RegisterColumn(Accessor##CType3::type, #CName3); \
		RegisterColumn(Accessor##CType4::type, #CName4); \
		RegisterColumn(Accessor##CType5::type, #CName5); \
		RegisterColumn(Accessor##CType6::type, #CName6); \
		RegisterColumn(Accessor##CType7::type, #CName7); \
		RegisterColumn(Accessor##CType8::type, #CName8); \
		RegisterColumn(Accessor##CType9::type, #CName9); \
		RegisterColumn(Accessor##CType10::type, #CName10); \
		RegisterColumn(Accessor##CType11::type, #CName11); \
		RegisterColumn(Accessor##CType12::type, #CName12); \
		RegisterColumn(Accessor##CType13::type, #CName13); \
		RegisterColumn(Accessor##CType14::type, #CName14); \
		RegisterColumn(Accessor##CType15::type, #CName15); \
\
		CName1.Create(this, 0); \
		CName2.Create(this, 1); \
		CName3.Create(this, 2); \
		CName4.Create(this, 3); \
		CName5.Create(this, 4); \
		CName6.Create(this, 5); \
		CName7.Create(this, 6); \
		CName8.Create(this, 7); \
		CName9.Create(this, 8); \
		CName10.Create(this, 9); \
		CName11.Create(this, 10); \
		CName12.Create(this, 11); \
		CName13.Create(this, 12); \
		CName14.Create(this, 13); \
		CName15.Create(this, 14); \
	}; \
\
	class TestQuery : public Query { \
	public: \
		TestQuery() : CName1(0), CName2(1), CName3(2), CName4(3), CName5(4), CName6(5), CName7(6), CName8(7), CName9(8), CName10(9), CName11(10), CName12(11), CName13(12), CName14(13), CName15(14) { \
			CName1.SetQuery(this); \
			CName2.SetQuery(this); \
			CName3.SetQuery(this); \
			CName4.SetQuery(this); \
			CName5.SetQuery(this); \
			CName6.SetQuery(this); \
			CName7.SetQuery(this); \
			CName8.SetQuery(this); \
			CName9.SetQuery(this); \
			CName10.SetQuery(this); \
			CName11.SetQuery(this); \
			CName12.SetQuery(this); \
			CName13.SetQuery(this); \
			CName14.SetQuery(this); \
			CName15.SetQuery(this); \
		} \
\
		TestQuery(const TestQuery& copy) : Query(copy), CName1(0), CName2(1), CName3(2), CName4(3), CName5(4), CName6(5), CName7(6), CName8(7), CName9(8), CName10(9), CName11(10), CName12(11), CName13(12), CName14(13), CName15(14) { \
			CName1.SetQuery(this); \
			CName2.SetQuery(this); \
			CName3.SetQuery(this); \
			CName4.SetQuery(this); \
			CName5.SetQuery(this); \
			CName6.SetQuery(this); \
			CName7.SetQuery(this); \
			CName8.SetQuery(this); \
			CName9.SetQuery(this); \
			CName10.SetQuery(this); \
			CName11.SetQuery(this); \
			CName12.SetQuery(this); \
			CName13.SetQuery(this); \
			CName14.SetQuery(this); \
			CName15.SetQuery(this); \
		} \
\
		class TestQueryQueryAccessorInt : private XQueryAccessorInt { \
		public: \
			TestQueryQueryAccessorInt(size_t column_id) : XQueryAccessorInt(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(int64_t value) {return (TestQuery &)XQueryAccessorInt::Equal(value);} \
			TestQuery& NotEqual(int64_t value) {return (TestQuery &)XQueryAccessorInt::NotEqual(value);} \
			TestQuery& Greater(int64_t value) {return (TestQuery &)XQueryAccessorInt::Greater(value);} \
			TestQuery& Less(int64_t value) {return (TestQuery &)XQueryAccessorInt::Less(value);} \
			TestQuery& Between(int64_t from, int64_t to) {return (TestQuery &)XQueryAccessorInt::Between(from, to);} \
		}; \
\
		template <class T> class TestQueryQueryAccessorEnum : public TestQueryQueryAccessorInt { \
		public: \
			TestQueryQueryAccessorEnum<T>(size_t column_id) : TestQueryQueryAccessorInt(column_id) {} \
		}; \
\
		class TestQueryQueryAccessorString : private XQueryAccessorString { \
		public: \
			TestQueryQueryAccessorString(size_t column_id) : XQueryAccessorString(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::Equal(value, CaseSensitive);} \
			TestQuery& NotEqual(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::NotEqual(value, CaseSensitive);} \
			TestQuery& BeginsWith(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::BeginsWith(value, CaseSensitive);} \
			TestQuery& EndsWith(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::EndsWith(value, CaseSensitive);} \
			TestQuery& Contains(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::Contains(value, CaseSensitive);} \
		}; \
\
		class TestQueryQueryAccessorBool : private XQueryAccessorBool { \
		public: \
			TestQueryQueryAccessorBool(size_t column_id) : XQueryAccessorBool(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(bool value) {return (TestQuery &)XQueryAccessorBool::Equal(value);} \
		}; \
\
		TestQueryQueryAccessor##CType1 CName1; \
		TestQueryQueryAccessor##CType2 CName2; \
		TestQueryQueryAccessor##CType3 CName3; \
		TestQueryQueryAccessor##CType4 CName4; \
		TestQueryQueryAccessor##CType5 CName5; \
		TestQueryQueryAccessor##CType6 CName6; \
		TestQueryQueryAccessor##CType7 CName7; \
		TestQueryQueryAccessor##CType8 CName8; \
		TestQueryQueryAccessor##CType9 CName9; \
		TestQueryQueryAccessor##CType10 CName10; \
		TestQueryQueryAccessor##CType11 CName11; \
		TestQueryQueryAccessor##CType12 CName12; \
		TestQueryQueryAccessor##CType13 CName13; \
		TestQueryQueryAccessor##CType14 CName14; \
		TestQueryQueryAccessor##CType15 CName15; \
\
		TestQuery& LeftParan(void) {Query::LeftParan(); return *this;}; \
		TestQuery& Or(void) {Query::Or(); return *this;}; \
		TestQuery& RightParan(void) {Query::RightParan(); return *this;}; \
		TestQuery& Subtable(size_t column) {Query::Subtable(column); return *this;}; \
		TestQuery& Parent() {Query::Parent(); return *this;}; \
	}; \
\
	TestQuery GetQuery() {return TestQuery();} \
\
	class Cursor : public CursorBase { \
	public: \
		Cursor(TableName& table, size_t ndx) : CursorBase(table, ndx) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
		} \
		Cursor(const TableName& table, size_t ndx) : CursorBase(const_cast<TableName&>(table), ndx) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
		} \
		Cursor(const Cursor& v) : CursorBase(v) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
		} \
		Accessor##CType1 CName1; \
		Accessor##CType2 CName2; \
		Accessor##CType3 CName3; \
		Accessor##CType4 CName4; \
		Accessor##CType5 CName5; \
		Accessor##CType6 CName6; \
		Accessor##CType7 CName7; \
		Accessor##CType8 CName8; \
		Accessor##CType9 CName9; \
		Accessor##CType10 CName10; \
		Accessor##CType11 CName11; \
		Accessor##CType12 CName12; \
		Accessor##CType13 CName13; \
		Accessor##CType14 CName14; \
		Accessor##CType15 CName15; \
	}; \
\
	void Add(tdbType##CType1 CName1, tdbType##CType2 CName2, tdbType##CType3 CName3, tdbType##CType4 CName4, tdbType##CType5 CName5, tdbType##CType6 CName6, tdbType##CType7 CName7, tdbType##CType8 CName8, tdbType##CType9 CName9, tdbType##CType10 CName10, tdbType##CType11 CName11, tdbType##CType12 CName12, tdbType##CType13 CName13, tdbType##CType14 CName14, tdbType##CType15 CName15) { \
		const size_t ndx = GetSize(); \
		Insert##CType1 (0, ndx, CName1); \
		Insert##CType2 (1, ndx, CName2); \
		Insert##CType3 (2, ndx, CName3); \
		Insert##CType4 (3, ndx, CName4); \
		Insert##CType5 (4, ndx, CName5); \
		Insert##CType6 (5, ndx, CName6); \
		Insert##CType7 (6, ndx, CName7); \
		Insert##CType8 (7, ndx, CName8); \
		Insert##CType9 (8, ndx, CName9); \
		Insert##CType10 (9, ndx, CName10); \
		Insert##CType11 (10, ndx, CName11); \
		Insert##CType12 (11, ndx, CName12); \
		Insert##CType13 (12, ndx, CName13); \
		Insert##CType14 (13, ndx, CName14); \
		Insert##CType15 (14, ndx, CName15); \
		InsertDone(); \
	} \
\
	void Insert(size_t ndx, tdbType##CType1 CName1, tdbType##CType2 CName2, tdbType##CType3 CName3, tdbType##CType4 CName4, tdbType##CType5 CName5, tdbType##CType6 CName6, tdbType##CType7 CName7, tdbType##CType8 CName8, tdbType##CType9 CName9, tdbType##CType10 CName10, tdbType##CType11 CName11, tdbType##CType12 CName12, tdbType##CType13 CName13, tdbType##CType14 CName14, tdbType##CType15 CName15) { \
		Insert##CType1 (0, ndx, CName1); \
		Insert##CType2 (1, ndx, CName2); \
		Insert##CType3 (2, ndx, CName3); \
		Insert##CType4 (3, ndx, CName4); \
		Insert##CType5 (4, ndx, CName5); \
		Insert##CType6 (5, ndx, CName6); \
		Insert##CType7 (6, ndx, CName7); \
		Insert##CType8 (7, ndx, CName8); \
		Insert##CType9 (8, ndx, CName9); \
		Insert##CType10 (9, ndx, CName10); \
		Insert##CType11 (10, ndx, CName11); \
		Insert##CType12 (11, ndx, CName12); \
		Insert##CType13 (12, ndx, CName13); \
		Insert##CType14 (13, ndx, CName14); \
		Insert##CType15 (14, ndx, CName15); \
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
	ColumnProxy##CType3 CName3; \
	ColumnProxy##CType4 CName4; \
	ColumnProxy##CType5 CName5; \
	ColumnProxy##CType6 CName6; \
	ColumnProxy##CType7 CName7; \
	ColumnProxy##CType8 CName8; \
	ColumnProxy##CType9 CName9; \
	ColumnProxy##CType10 CName10; \
	ColumnProxy##CType11 CName11; \
	ColumnProxy##CType12 CName12; \
	ColumnProxy##CType13 CName13; \
	ColumnProxy##CType14 CName14; \
	ColumnProxy##CType15 CName15; \
\
protected: \
	friend class Group; \
	TableName(Allocator& alloc, size_t ref, Array* parent, size_t pndx) : TopLevelTable(alloc, ref, parent, pndx) {}; \
\
private: \
	TableName(const TableName&) {} \
	TableName& operator=(const TableName&) {return *this;} \
};



#define TDB_TABLE_16(TableName, CType1, CName1, CType2, CName2, CType3, CName3, CType4, CName4, CType5, CName5, CType6, CName6, CType7, CName7, CType8, CName8, CType9, CName9, CType10, CName10, CType11, CName11, CType12, CName12, CType13, CName13, CType14, CName14, CType15, CName15, CType16, CName16) \
class TableName##Query { \
protected: \
	QueryAccessor##CType1 CName1; \
	QueryAccessor##CType2 CName2; \
	QueryAccessor##CType3 CName3; \
	QueryAccessor##CType4 CName4; \
	QueryAccessor##CType5 CName5; \
	QueryAccessor##CType6 CName6; \
	QueryAccessor##CType7 CName7; \
	QueryAccessor##CType8 CName8; \
	QueryAccessor##CType9 CName9; \
	QueryAccessor##CType10 CName10; \
	QueryAccessor##CType11 CName11; \
	QueryAccessor##CType12 CName12; \
	QueryAccessor##CType13 CName13; \
	QueryAccessor##CType14 CName14; \
	QueryAccessor##CType15 CName15; \
	QueryAccessor##CType16 CName16; \
}; \
\
class TableName : public TopLevelTable { \
public: \
	TableName(Allocator& alloc=GetDefaultAllocator()) : TopLevelTable(alloc) { \
		RegisterColumn(Accessor##CType1::type, #CName1); \
		RegisterColumn(Accessor##CType2::type, #CName2); \
		RegisterColumn(Accessor##CType3::type, #CName3); \
		RegisterColumn(Accessor##CType4::type, #CName4); \
		RegisterColumn(Accessor##CType5::type, #CName5); \
		RegisterColumn(Accessor##CType6::type, #CName6); \
		RegisterColumn(Accessor##CType7::type, #CName7); \
		RegisterColumn(Accessor##CType8::type, #CName8); \
		RegisterColumn(Accessor##CType9::type, #CName9); \
		RegisterColumn(Accessor##CType10::type, #CName10); \
		RegisterColumn(Accessor##CType11::type, #CName11); \
		RegisterColumn(Accessor##CType12::type, #CName12); \
		RegisterColumn(Accessor##CType13::type, #CName13); \
		RegisterColumn(Accessor##CType14::type, #CName14); \
		RegisterColumn(Accessor##CType15::type, #CName15); \
		RegisterColumn(Accessor##CType16::type, #CName16); \
\
		CName1.Create(this, 0); \
		CName2.Create(this, 1); \
		CName3.Create(this, 2); \
		CName4.Create(this, 3); \
		CName5.Create(this, 4); \
		CName6.Create(this, 5); \
		CName7.Create(this, 6); \
		CName8.Create(this, 7); \
		CName9.Create(this, 8); \
		CName10.Create(this, 9); \
		CName11.Create(this, 10); \
		CName12.Create(this, 11); \
		CName13.Create(this, 12); \
		CName14.Create(this, 13); \
		CName15.Create(this, 14); \
		CName16.Create(this, 15); \
	}; \
\
	class TestQuery : public Query { \
	public: \
		TestQuery() : CName1(0), CName2(1), CName3(2), CName4(3), CName5(4), CName6(5), CName7(6), CName8(7), CName9(8), CName10(9), CName11(10), CName12(11), CName13(12), CName14(13), CName15(14), CName16(15) { \
			CName1.SetQuery(this); \
			CName2.SetQuery(this); \
			CName3.SetQuery(this); \
			CName4.SetQuery(this); \
			CName5.SetQuery(this); \
			CName6.SetQuery(this); \
			CName7.SetQuery(this); \
			CName8.SetQuery(this); \
			CName9.SetQuery(this); \
			CName10.SetQuery(this); \
			CName11.SetQuery(this); \
			CName12.SetQuery(this); \
			CName13.SetQuery(this); \
			CName14.SetQuery(this); \
			CName15.SetQuery(this); \
			CName16.SetQuery(this); \
		} \
\
		TestQuery(const TestQuery& copy) : Query(copy), CName1(0), CName2(1), CName3(2), CName4(3), CName5(4), CName6(5), CName7(6), CName8(7), CName9(8), CName10(9), CName11(10), CName12(11), CName13(12), CName14(13), CName15(14), CName16(15) { \
			CName1.SetQuery(this); \
			CName2.SetQuery(this); \
			CName3.SetQuery(this); \
			CName4.SetQuery(this); \
			CName5.SetQuery(this); \
			CName6.SetQuery(this); \
			CName7.SetQuery(this); \
			CName8.SetQuery(this); \
			CName9.SetQuery(this); \
			CName10.SetQuery(this); \
			CName11.SetQuery(this); \
			CName12.SetQuery(this); \
			CName13.SetQuery(this); \
			CName14.SetQuery(this); \
			CName15.SetQuery(this); \
			CName16.SetQuery(this); \
		} \
\
		class TestQueryQueryAccessorInt : private XQueryAccessorInt { \
		public: \
			TestQueryQueryAccessorInt(size_t column_id) : XQueryAccessorInt(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(int64_t value) {return (TestQuery &)XQueryAccessorInt::Equal(value);} \
			TestQuery& NotEqual(int64_t value) {return (TestQuery &)XQueryAccessorInt::NotEqual(value);} \
			TestQuery& Greater(int64_t value) {return (TestQuery &)XQueryAccessorInt::Greater(value);} \
			TestQuery& Less(int64_t value) {return (TestQuery &)XQueryAccessorInt::Less(value);} \
			TestQuery& Between(int64_t from, int64_t to) {return (TestQuery &)XQueryAccessorInt::Between(from, to);} \
		}; \
\
		template <class T> class TestQueryQueryAccessorEnum : public TestQueryQueryAccessorInt { \
		public: \
			TestQueryQueryAccessorEnum<T>(size_t column_id) : TestQueryQueryAccessorInt(column_id) {} \
		}; \
\
		class TestQueryQueryAccessorString : private XQueryAccessorString { \
		public: \
			TestQueryQueryAccessorString(size_t column_id) : XQueryAccessorString(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::Equal(value, CaseSensitive);} \
			TestQuery& NotEqual(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::NotEqual(value, CaseSensitive);} \
			TestQuery& BeginsWith(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::BeginsWith(value, CaseSensitive);} \
			TestQuery& EndsWith(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::EndsWith(value, CaseSensitive);} \
			TestQuery& Contains(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::Contains(value, CaseSensitive);} \
		}; \
\
		class TestQueryQueryAccessorBool : private XQueryAccessorBool { \
		public: \
			TestQueryQueryAccessorBool(size_t column_id) : XQueryAccessorBool(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(bool value) {return (TestQuery &)XQueryAccessorBool::Equal(value);} \
		}; \
\
		TestQueryQueryAccessor##CType1 CName1; \
		TestQueryQueryAccessor##CType2 CName2; \
		TestQueryQueryAccessor##CType3 CName3; \
		TestQueryQueryAccessor##CType4 CName4; \
		TestQueryQueryAccessor##CType5 CName5; \
		TestQueryQueryAccessor##CType6 CName6; \
		TestQueryQueryAccessor##CType7 CName7; \
		TestQueryQueryAccessor##CType8 CName8; \
		TestQueryQueryAccessor##CType9 CName9; \
		TestQueryQueryAccessor##CType10 CName10; \
		TestQueryQueryAccessor##CType11 CName11; \
		TestQueryQueryAccessor##CType12 CName12; \
		TestQueryQueryAccessor##CType13 CName13; \
		TestQueryQueryAccessor##CType14 CName14; \
		TestQueryQueryAccessor##CType15 CName15; \
		TestQueryQueryAccessor##CType16 CName16; \
\
		TestQuery& LeftParan(void) {Query::LeftParan(); return *this;}; \
		TestQuery& Or(void) {Query::Or(); return *this;}; \
		TestQuery& RightParan(void) {Query::RightParan(); return *this;}; \
		TestQuery& Subtable(size_t column) {Query::Subtable(column); return *this;}; \
		TestQuery& Parent() {Query::Parent(); return *this;}; \
	}; \
\
	TestQuery GetQuery() {return TestQuery();} \
\
	class Cursor : public CursorBase { \
	public: \
		Cursor(TableName& table, size_t ndx) : CursorBase(table, ndx) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
		} \
		Cursor(const TableName& table, size_t ndx) : CursorBase(const_cast<TableName&>(table), ndx) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
		} \
		Cursor(const Cursor& v) : CursorBase(v) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
		} \
		Accessor##CType1 CName1; \
		Accessor##CType2 CName2; \
		Accessor##CType3 CName3; \
		Accessor##CType4 CName4; \
		Accessor##CType5 CName5; \
		Accessor##CType6 CName6; \
		Accessor##CType7 CName7; \
		Accessor##CType8 CName8; \
		Accessor##CType9 CName9; \
		Accessor##CType10 CName10; \
		Accessor##CType11 CName11; \
		Accessor##CType12 CName12; \
		Accessor##CType13 CName13; \
		Accessor##CType14 CName14; \
		Accessor##CType15 CName15; \
		Accessor##CType16 CName16; \
	}; \
\
	void Add(tdbType##CType1 CName1, tdbType##CType2 CName2, tdbType##CType3 CName3, tdbType##CType4 CName4, tdbType##CType5 CName5, tdbType##CType6 CName6, tdbType##CType7 CName7, tdbType##CType8 CName8, tdbType##CType9 CName9, tdbType##CType10 CName10, tdbType##CType11 CName11, tdbType##CType12 CName12, tdbType##CType13 CName13, tdbType##CType14 CName14, tdbType##CType15 CName15, tdbType##CType16 CName16) { \
		const size_t ndx = GetSize(); \
		Insert##CType1 (0, ndx, CName1); \
		Insert##CType2 (1, ndx, CName2); \
		Insert##CType3 (2, ndx, CName3); \
		Insert##CType4 (3, ndx, CName4); \
		Insert##CType5 (4, ndx, CName5); \
		Insert##CType6 (5, ndx, CName6); \
		Insert##CType7 (6, ndx, CName7); \
		Insert##CType8 (7, ndx, CName8); \
		Insert##CType9 (8, ndx, CName9); \
		Insert##CType10 (9, ndx, CName10); \
		Insert##CType11 (10, ndx, CName11); \
		Insert##CType12 (11, ndx, CName12); \
		Insert##CType13 (12, ndx, CName13); \
		Insert##CType14 (13, ndx, CName14); \
		Insert##CType15 (14, ndx, CName15); \
		Insert##CType16 (15, ndx, CName16); \
		InsertDone(); \
	} \
\
	void Insert(size_t ndx, tdbType##CType1 CName1, tdbType##CType2 CName2, tdbType##CType3 CName3, tdbType##CType4 CName4, tdbType##CType5 CName5, tdbType##CType6 CName6, tdbType##CType7 CName7, tdbType##CType8 CName8, tdbType##CType9 CName9, tdbType##CType10 CName10, tdbType##CType11 CName11, tdbType##CType12 CName12, tdbType##CType13 CName13, tdbType##CType14 CName14, tdbType##CType15 CName15, tdbType##CType16 CName16) { \
		Insert##CType1 (0, ndx, CName1); \
		Insert##CType2 (1, ndx, CName2); \
		Insert##CType3 (2, ndx, CName3); \
		Insert##CType4 (3, ndx, CName4); \
		Insert##CType5 (4, ndx, CName5); \
		Insert##CType6 (5, ndx, CName6); \
		Insert##CType7 (6, ndx, CName7); \
		Insert##CType8 (7, ndx, CName8); \
		Insert##CType9 (8, ndx, CName9); \
		Insert##CType10 (9, ndx, CName10); \
		Insert##CType11 (10, ndx, CName11); \
		Insert##CType12 (11, ndx, CName12); \
		Insert##CType13 (12, ndx, CName13); \
		Insert##CType14 (13, ndx, CName14); \
		Insert##CType15 (14, ndx, CName15); \
		Insert##CType16 (15, ndx, CName16); \
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
	ColumnProxy##CType3 CName3; \
	ColumnProxy##CType4 CName4; \
	ColumnProxy##CType5 CName5; \
	ColumnProxy##CType6 CName6; \
	ColumnProxy##CType7 CName7; \
	ColumnProxy##CType8 CName8; \
	ColumnProxy##CType9 CName9; \
	ColumnProxy##CType10 CName10; \
	ColumnProxy##CType11 CName11; \
	ColumnProxy##CType12 CName12; \
	ColumnProxy##CType13 CName13; \
	ColumnProxy##CType14 CName14; \
	ColumnProxy##CType15 CName15; \
	ColumnProxy##CType16 CName16; \
\
protected: \
	friend class Group; \
	TableName(Allocator& alloc, size_t ref, Array* parent, size_t pndx) : TopLevelTable(alloc, ref, parent, pndx) {}; \
\
private: \
	TableName(const TableName&) {} \
	TableName& operator=(const TableName&) {return *this;} \
};



#define TDB_TABLE_17(TableName, CType1, CName1, CType2, CName2, CType3, CName3, CType4, CName4, CType5, CName5, CType6, CName6, CType7, CName7, CType8, CName8, CType9, CName9, CType10, CName10, CType11, CName11, CType12, CName12, CType13, CName13, CType14, CName14, CType15, CName15, CType16, CName16, CType17, CName17) \
class TableName##Query { \
protected: \
	QueryAccessor##CType1 CName1; \
	QueryAccessor##CType2 CName2; \
	QueryAccessor##CType3 CName3; \
	QueryAccessor##CType4 CName4; \
	QueryAccessor##CType5 CName5; \
	QueryAccessor##CType6 CName6; \
	QueryAccessor##CType7 CName7; \
	QueryAccessor##CType8 CName8; \
	QueryAccessor##CType9 CName9; \
	QueryAccessor##CType10 CName10; \
	QueryAccessor##CType11 CName11; \
	QueryAccessor##CType12 CName12; \
	QueryAccessor##CType13 CName13; \
	QueryAccessor##CType14 CName14; \
	QueryAccessor##CType15 CName15; \
	QueryAccessor##CType16 CName16; \
	QueryAccessor##CType17 CName17; \
}; \
\
class TableName : public TopLevelTable { \
public: \
	TableName(Allocator& alloc=GetDefaultAllocator()) : TopLevelTable(alloc) { \
		RegisterColumn(Accessor##CType1::type, #CName1); \
		RegisterColumn(Accessor##CType2::type, #CName2); \
		RegisterColumn(Accessor##CType3::type, #CName3); \
		RegisterColumn(Accessor##CType4::type, #CName4); \
		RegisterColumn(Accessor##CType5::type, #CName5); \
		RegisterColumn(Accessor##CType6::type, #CName6); \
		RegisterColumn(Accessor##CType7::type, #CName7); \
		RegisterColumn(Accessor##CType8::type, #CName8); \
		RegisterColumn(Accessor##CType9::type, #CName9); \
		RegisterColumn(Accessor##CType10::type, #CName10); \
		RegisterColumn(Accessor##CType11::type, #CName11); \
		RegisterColumn(Accessor##CType12::type, #CName12); \
		RegisterColumn(Accessor##CType13::type, #CName13); \
		RegisterColumn(Accessor##CType14::type, #CName14); \
		RegisterColumn(Accessor##CType15::type, #CName15); \
		RegisterColumn(Accessor##CType16::type, #CName16); \
		RegisterColumn(Accessor##CType17::type, #CName17); \
\
		CName1.Create(this, 0); \
		CName2.Create(this, 1); \
		CName3.Create(this, 2); \
		CName4.Create(this, 3); \
		CName5.Create(this, 4); \
		CName6.Create(this, 5); \
		CName7.Create(this, 6); \
		CName8.Create(this, 7); \
		CName9.Create(this, 8); \
		CName10.Create(this, 9); \
		CName11.Create(this, 10); \
		CName12.Create(this, 11); \
		CName13.Create(this, 12); \
		CName14.Create(this, 13); \
		CName15.Create(this, 14); \
		CName16.Create(this, 15); \
		CName17.Create(this, 16); \
	}; \
\
	class TestQuery : public Query { \
	public: \
		TestQuery() : CName1(0), CName2(1), CName3(2), CName4(3), CName5(4), CName6(5), CName7(6), CName8(7), CName9(8), CName10(9), CName11(10), CName12(11), CName13(12), CName14(13), CName15(14), CName16(15), CName17(16) { \
			CName1.SetQuery(this); \
			CName2.SetQuery(this); \
			CName3.SetQuery(this); \
			CName4.SetQuery(this); \
			CName5.SetQuery(this); \
			CName6.SetQuery(this); \
			CName7.SetQuery(this); \
			CName8.SetQuery(this); \
			CName9.SetQuery(this); \
			CName10.SetQuery(this); \
			CName11.SetQuery(this); \
			CName12.SetQuery(this); \
			CName13.SetQuery(this); \
			CName14.SetQuery(this); \
			CName15.SetQuery(this); \
			CName16.SetQuery(this); \
			CName17.SetQuery(this); \
		} \
\
		TestQuery(const TestQuery& copy) : Query(copy), CName1(0), CName2(1), CName3(2), CName4(3), CName5(4), CName6(5), CName7(6), CName8(7), CName9(8), CName10(9), CName11(10), CName12(11), CName13(12), CName14(13), CName15(14), CName16(15), CName17(16) { \
			CName1.SetQuery(this); \
			CName2.SetQuery(this); \
			CName3.SetQuery(this); \
			CName4.SetQuery(this); \
			CName5.SetQuery(this); \
			CName6.SetQuery(this); \
			CName7.SetQuery(this); \
			CName8.SetQuery(this); \
			CName9.SetQuery(this); \
			CName10.SetQuery(this); \
			CName11.SetQuery(this); \
			CName12.SetQuery(this); \
			CName13.SetQuery(this); \
			CName14.SetQuery(this); \
			CName15.SetQuery(this); \
			CName16.SetQuery(this); \
			CName17.SetQuery(this); \
		} \
\
		class TestQueryQueryAccessorInt : private XQueryAccessorInt { \
		public: \
			TestQueryQueryAccessorInt(size_t column_id) : XQueryAccessorInt(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(int64_t value) {return (TestQuery &)XQueryAccessorInt::Equal(value);} \
			TestQuery& NotEqual(int64_t value) {return (TestQuery &)XQueryAccessorInt::NotEqual(value);} \
			TestQuery& Greater(int64_t value) {return (TestQuery &)XQueryAccessorInt::Greater(value);} \
			TestQuery& Less(int64_t value) {return (TestQuery &)XQueryAccessorInt::Less(value);} \
			TestQuery& Between(int64_t from, int64_t to) {return (TestQuery &)XQueryAccessorInt::Between(from, to);} \
		}; \
\
		template <class T> class TestQueryQueryAccessorEnum : public TestQueryQueryAccessorInt { \
		public: \
			TestQueryQueryAccessorEnum<T>(size_t column_id) : TestQueryQueryAccessorInt(column_id) {} \
		}; \
\
		class TestQueryQueryAccessorString : private XQueryAccessorString { \
		public: \
			TestQueryQueryAccessorString(size_t column_id) : XQueryAccessorString(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::Equal(value, CaseSensitive);} \
			TestQuery& NotEqual(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::NotEqual(value, CaseSensitive);} \
			TestQuery& BeginsWith(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::BeginsWith(value, CaseSensitive);} \
			TestQuery& EndsWith(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::EndsWith(value, CaseSensitive);} \
			TestQuery& Contains(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::Contains(value, CaseSensitive);} \
		}; \
\
		class TestQueryQueryAccessorBool : private XQueryAccessorBool { \
		public: \
			TestQueryQueryAccessorBool(size_t column_id) : XQueryAccessorBool(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(bool value) {return (TestQuery &)XQueryAccessorBool::Equal(value);} \
		}; \
\
		TestQueryQueryAccessor##CType1 CName1; \
		TestQueryQueryAccessor##CType2 CName2; \
		TestQueryQueryAccessor##CType3 CName3; \
		TestQueryQueryAccessor##CType4 CName4; \
		TestQueryQueryAccessor##CType5 CName5; \
		TestQueryQueryAccessor##CType6 CName6; \
		TestQueryQueryAccessor##CType7 CName7; \
		TestQueryQueryAccessor##CType8 CName8; \
		TestQueryQueryAccessor##CType9 CName9; \
		TestQueryQueryAccessor##CType10 CName10; \
		TestQueryQueryAccessor##CType11 CName11; \
		TestQueryQueryAccessor##CType12 CName12; \
		TestQueryQueryAccessor##CType13 CName13; \
		TestQueryQueryAccessor##CType14 CName14; \
		TestQueryQueryAccessor##CType15 CName15; \
		TestQueryQueryAccessor##CType16 CName16; \
		TestQueryQueryAccessor##CType17 CName17; \
\
		TestQuery& LeftParan(void) {Query::LeftParan(); return *this;}; \
		TestQuery& Or(void) {Query::Or(); return *this;}; \
		TestQuery& RightParan(void) {Query::RightParan(); return *this;}; \
		TestQuery& Subtable(size_t column) {Query::Subtable(column); return *this;}; \
		TestQuery& Parent() {Query::Parent(); return *this;}; \
	}; \
\
	TestQuery GetQuery() {return TestQuery();} \
\
	class Cursor : public CursorBase { \
	public: \
		Cursor(TableName& table, size_t ndx) : CursorBase(table, ndx) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
			CName17.Create(this, 16); \
		} \
		Cursor(const TableName& table, size_t ndx) : CursorBase(const_cast<TableName&>(table), ndx) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
			CName17.Create(this, 16); \
		} \
		Cursor(const Cursor& v) : CursorBase(v) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
			CName17.Create(this, 16); \
		} \
		Accessor##CType1 CName1; \
		Accessor##CType2 CName2; \
		Accessor##CType3 CName3; \
		Accessor##CType4 CName4; \
		Accessor##CType5 CName5; \
		Accessor##CType6 CName6; \
		Accessor##CType7 CName7; \
		Accessor##CType8 CName8; \
		Accessor##CType9 CName9; \
		Accessor##CType10 CName10; \
		Accessor##CType11 CName11; \
		Accessor##CType12 CName12; \
		Accessor##CType13 CName13; \
		Accessor##CType14 CName14; \
		Accessor##CType15 CName15; \
		Accessor##CType16 CName16; \
		Accessor##CType17 CName17; \
	}; \
\
	void Add(tdbType##CType1 CName1, tdbType##CType2 CName2, tdbType##CType3 CName3, tdbType##CType4 CName4, tdbType##CType5 CName5, tdbType##CType6 CName6, tdbType##CType7 CName7, tdbType##CType8 CName8, tdbType##CType9 CName9, tdbType##CType10 CName10, tdbType##CType11 CName11, tdbType##CType12 CName12, tdbType##CType13 CName13, tdbType##CType14 CName14, tdbType##CType15 CName15, tdbType##CType16 CName16, tdbType##CType17 CName17) { \
		const size_t ndx = GetSize(); \
		Insert##CType1 (0, ndx, CName1); \
		Insert##CType2 (1, ndx, CName2); \
		Insert##CType3 (2, ndx, CName3); \
		Insert##CType4 (3, ndx, CName4); \
		Insert##CType5 (4, ndx, CName5); \
		Insert##CType6 (5, ndx, CName6); \
		Insert##CType7 (6, ndx, CName7); \
		Insert##CType8 (7, ndx, CName8); \
		Insert##CType9 (8, ndx, CName9); \
		Insert##CType10 (9, ndx, CName10); \
		Insert##CType11 (10, ndx, CName11); \
		Insert##CType12 (11, ndx, CName12); \
		Insert##CType13 (12, ndx, CName13); \
		Insert##CType14 (13, ndx, CName14); \
		Insert##CType15 (14, ndx, CName15); \
		Insert##CType16 (15, ndx, CName16); \
		Insert##CType17 (16, ndx, CName17); \
		InsertDone(); \
	} \
\
	void Insert(size_t ndx, tdbType##CType1 CName1, tdbType##CType2 CName2, tdbType##CType3 CName3, tdbType##CType4 CName4, tdbType##CType5 CName5, tdbType##CType6 CName6, tdbType##CType7 CName7, tdbType##CType8 CName8, tdbType##CType9 CName9, tdbType##CType10 CName10, tdbType##CType11 CName11, tdbType##CType12 CName12, tdbType##CType13 CName13, tdbType##CType14 CName14, tdbType##CType15 CName15, tdbType##CType16 CName16, tdbType##CType17 CName17) { \
		Insert##CType1 (0, ndx, CName1); \
		Insert##CType2 (1, ndx, CName2); \
		Insert##CType3 (2, ndx, CName3); \
		Insert##CType4 (3, ndx, CName4); \
		Insert##CType5 (4, ndx, CName5); \
		Insert##CType6 (5, ndx, CName6); \
		Insert##CType7 (6, ndx, CName7); \
		Insert##CType8 (7, ndx, CName8); \
		Insert##CType9 (8, ndx, CName9); \
		Insert##CType10 (9, ndx, CName10); \
		Insert##CType11 (10, ndx, CName11); \
		Insert##CType12 (11, ndx, CName12); \
		Insert##CType13 (12, ndx, CName13); \
		Insert##CType14 (13, ndx, CName14); \
		Insert##CType15 (14, ndx, CName15); \
		Insert##CType16 (15, ndx, CName16); \
		Insert##CType17 (16, ndx, CName17); \
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
	ColumnProxy##CType3 CName3; \
	ColumnProxy##CType4 CName4; \
	ColumnProxy##CType5 CName5; \
	ColumnProxy##CType6 CName6; \
	ColumnProxy##CType7 CName7; \
	ColumnProxy##CType8 CName8; \
	ColumnProxy##CType9 CName9; \
	ColumnProxy##CType10 CName10; \
	ColumnProxy##CType11 CName11; \
	ColumnProxy##CType12 CName12; \
	ColumnProxy##CType13 CName13; \
	ColumnProxy##CType14 CName14; \
	ColumnProxy##CType15 CName15; \
	ColumnProxy##CType16 CName16; \
	ColumnProxy##CType17 CName17; \
\
protected: \
	friend class Group; \
	TableName(Allocator& alloc, size_t ref, Array* parent, size_t pndx) : TopLevelTable(alloc, ref, parent, pndx) {}; \
\
private: \
	TableName(const TableName&) {} \
	TableName& operator=(const TableName&) {return *this;} \
};



#define TDB_TABLE_18(TableName, CType1, CName1, CType2, CName2, CType3, CName3, CType4, CName4, CType5, CName5, CType6, CName6, CType7, CName7, CType8, CName8, CType9, CName9, CType10, CName10, CType11, CName11, CType12, CName12, CType13, CName13, CType14, CName14, CType15, CName15, CType16, CName16, CType17, CName17, CType18, CName18) \
class TableName##Query { \
protected: \
	QueryAccessor##CType1 CName1; \
	QueryAccessor##CType2 CName2; \
	QueryAccessor##CType3 CName3; \
	QueryAccessor##CType4 CName4; \
	QueryAccessor##CType5 CName5; \
	QueryAccessor##CType6 CName6; \
	QueryAccessor##CType7 CName7; \
	QueryAccessor##CType8 CName8; \
	QueryAccessor##CType9 CName9; \
	QueryAccessor##CType10 CName10; \
	QueryAccessor##CType11 CName11; \
	QueryAccessor##CType12 CName12; \
	QueryAccessor##CType13 CName13; \
	QueryAccessor##CType14 CName14; \
	QueryAccessor##CType15 CName15; \
	QueryAccessor##CType16 CName16; \
	QueryAccessor##CType17 CName17; \
	QueryAccessor##CType18 CName18; \
}; \
\
class TableName : public TopLevelTable { \
public: \
	TableName(Allocator& alloc=GetDefaultAllocator()) : TopLevelTable(alloc) { \
		RegisterColumn(Accessor##CType1::type, #CName1); \
		RegisterColumn(Accessor##CType2::type, #CName2); \
		RegisterColumn(Accessor##CType3::type, #CName3); \
		RegisterColumn(Accessor##CType4::type, #CName4); \
		RegisterColumn(Accessor##CType5::type, #CName5); \
		RegisterColumn(Accessor##CType6::type, #CName6); \
		RegisterColumn(Accessor##CType7::type, #CName7); \
		RegisterColumn(Accessor##CType8::type, #CName8); \
		RegisterColumn(Accessor##CType9::type, #CName9); \
		RegisterColumn(Accessor##CType10::type, #CName10); \
		RegisterColumn(Accessor##CType11::type, #CName11); \
		RegisterColumn(Accessor##CType12::type, #CName12); \
		RegisterColumn(Accessor##CType13::type, #CName13); \
		RegisterColumn(Accessor##CType14::type, #CName14); \
		RegisterColumn(Accessor##CType15::type, #CName15); \
		RegisterColumn(Accessor##CType16::type, #CName16); \
		RegisterColumn(Accessor##CType17::type, #CName17); \
		RegisterColumn(Accessor##CType18::type, #CName18); \
\
		CName1.Create(this, 0); \
		CName2.Create(this, 1); \
		CName3.Create(this, 2); \
		CName4.Create(this, 3); \
		CName5.Create(this, 4); \
		CName6.Create(this, 5); \
		CName7.Create(this, 6); \
		CName8.Create(this, 7); \
		CName9.Create(this, 8); \
		CName10.Create(this, 9); \
		CName11.Create(this, 10); \
		CName12.Create(this, 11); \
		CName13.Create(this, 12); \
		CName14.Create(this, 13); \
		CName15.Create(this, 14); \
		CName16.Create(this, 15); \
		CName17.Create(this, 16); \
		CName18.Create(this, 17); \
	}; \
\
	class TestQuery : public Query { \
	public: \
		TestQuery() : CName1(0), CName2(1), CName3(2), CName4(3), CName5(4), CName6(5), CName7(6), CName8(7), CName9(8), CName10(9), CName11(10), CName12(11), CName13(12), CName14(13), CName15(14), CName16(15), CName17(16), CName18(17) { \
			CName1.SetQuery(this); \
			CName2.SetQuery(this); \
			CName3.SetQuery(this); \
			CName4.SetQuery(this); \
			CName5.SetQuery(this); \
			CName6.SetQuery(this); \
			CName7.SetQuery(this); \
			CName8.SetQuery(this); \
			CName9.SetQuery(this); \
			CName10.SetQuery(this); \
			CName11.SetQuery(this); \
			CName12.SetQuery(this); \
			CName13.SetQuery(this); \
			CName14.SetQuery(this); \
			CName15.SetQuery(this); \
			CName16.SetQuery(this); \
			CName17.SetQuery(this); \
			CName18.SetQuery(this); \
		} \
\
		TestQuery(const TestQuery& copy) : Query(copy), CName1(0), CName2(1), CName3(2), CName4(3), CName5(4), CName6(5), CName7(6), CName8(7), CName9(8), CName10(9), CName11(10), CName12(11), CName13(12), CName14(13), CName15(14), CName16(15), CName17(16), CName18(17) { \
			CName1.SetQuery(this); \
			CName2.SetQuery(this); \
			CName3.SetQuery(this); \
			CName4.SetQuery(this); \
			CName5.SetQuery(this); \
			CName6.SetQuery(this); \
			CName7.SetQuery(this); \
			CName8.SetQuery(this); \
			CName9.SetQuery(this); \
			CName10.SetQuery(this); \
			CName11.SetQuery(this); \
			CName12.SetQuery(this); \
			CName13.SetQuery(this); \
			CName14.SetQuery(this); \
			CName15.SetQuery(this); \
			CName16.SetQuery(this); \
			CName17.SetQuery(this); \
			CName18.SetQuery(this); \
		} \
\
		class TestQueryQueryAccessorInt : private XQueryAccessorInt { \
		public: \
			TestQueryQueryAccessorInt(size_t column_id) : XQueryAccessorInt(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(int64_t value) {return (TestQuery &)XQueryAccessorInt::Equal(value);} \
			TestQuery& NotEqual(int64_t value) {return (TestQuery &)XQueryAccessorInt::NotEqual(value);} \
			TestQuery& Greater(int64_t value) {return (TestQuery &)XQueryAccessorInt::Greater(value);} \
			TestQuery& Less(int64_t value) {return (TestQuery &)XQueryAccessorInt::Less(value);} \
			TestQuery& Between(int64_t from, int64_t to) {return (TestQuery &)XQueryAccessorInt::Between(from, to);} \
		}; \
\
		template <class T> class TestQueryQueryAccessorEnum : public TestQueryQueryAccessorInt { \
		public: \
			TestQueryQueryAccessorEnum<T>(size_t column_id) : TestQueryQueryAccessorInt(column_id) {} \
		}; \
\
		class TestQueryQueryAccessorString : private XQueryAccessorString { \
		public: \
			TestQueryQueryAccessorString(size_t column_id) : XQueryAccessorString(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::Equal(value, CaseSensitive);} \
			TestQuery& NotEqual(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::NotEqual(value, CaseSensitive);} \
			TestQuery& BeginsWith(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::BeginsWith(value, CaseSensitive);} \
			TestQuery& EndsWith(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::EndsWith(value, CaseSensitive);} \
			TestQuery& Contains(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::Contains(value, CaseSensitive);} \
		}; \
\
		class TestQueryQueryAccessorBool : private XQueryAccessorBool { \
		public: \
			TestQueryQueryAccessorBool(size_t column_id) : XQueryAccessorBool(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(bool value) {return (TestQuery &)XQueryAccessorBool::Equal(value);} \
		}; \
\
		TestQueryQueryAccessor##CType1 CName1; \
		TestQueryQueryAccessor##CType2 CName2; \
		TestQueryQueryAccessor##CType3 CName3; \
		TestQueryQueryAccessor##CType4 CName4; \
		TestQueryQueryAccessor##CType5 CName5; \
		TestQueryQueryAccessor##CType6 CName6; \
		TestQueryQueryAccessor##CType7 CName7; \
		TestQueryQueryAccessor##CType8 CName8; \
		TestQueryQueryAccessor##CType9 CName9; \
		TestQueryQueryAccessor##CType10 CName10; \
		TestQueryQueryAccessor##CType11 CName11; \
		TestQueryQueryAccessor##CType12 CName12; \
		TestQueryQueryAccessor##CType13 CName13; \
		TestQueryQueryAccessor##CType14 CName14; \
		TestQueryQueryAccessor##CType15 CName15; \
		TestQueryQueryAccessor##CType16 CName16; \
		TestQueryQueryAccessor##CType17 CName17; \
		TestQueryQueryAccessor##CType18 CName18; \
\
		TestQuery& LeftParan(void) {Query::LeftParan(); return *this;}; \
		TestQuery& Or(void) {Query::Or(); return *this;}; \
		TestQuery& RightParan(void) {Query::RightParan(); return *this;}; \
		TestQuery& Subtable(size_t column) {Query::Subtable(column); return *this;}; \
		TestQuery& Parent() {Query::Parent(); return *this;}; \
	}; \
\
	TestQuery GetQuery() {return TestQuery();} \
\
	class Cursor : public CursorBase { \
	public: \
		Cursor(TableName& table, size_t ndx) : CursorBase(table, ndx) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
			CName17.Create(this, 16); \
			CName18.Create(this, 17); \
		} \
		Cursor(const TableName& table, size_t ndx) : CursorBase(const_cast<TableName&>(table), ndx) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
			CName17.Create(this, 16); \
			CName18.Create(this, 17); \
		} \
		Cursor(const Cursor& v) : CursorBase(v) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
			CName17.Create(this, 16); \
			CName18.Create(this, 17); \
		} \
		Accessor##CType1 CName1; \
		Accessor##CType2 CName2; \
		Accessor##CType3 CName3; \
		Accessor##CType4 CName4; \
		Accessor##CType5 CName5; \
		Accessor##CType6 CName6; \
		Accessor##CType7 CName7; \
		Accessor##CType8 CName8; \
		Accessor##CType9 CName9; \
		Accessor##CType10 CName10; \
		Accessor##CType11 CName11; \
		Accessor##CType12 CName12; \
		Accessor##CType13 CName13; \
		Accessor##CType14 CName14; \
		Accessor##CType15 CName15; \
		Accessor##CType16 CName16; \
		Accessor##CType17 CName17; \
		Accessor##CType18 CName18; \
	}; \
\
	void Add(tdbType##CType1 CName1, tdbType##CType2 CName2, tdbType##CType3 CName3, tdbType##CType4 CName4, tdbType##CType5 CName5, tdbType##CType6 CName6, tdbType##CType7 CName7, tdbType##CType8 CName8, tdbType##CType9 CName9, tdbType##CType10 CName10, tdbType##CType11 CName11, tdbType##CType12 CName12, tdbType##CType13 CName13, tdbType##CType14 CName14, tdbType##CType15 CName15, tdbType##CType16 CName16, tdbType##CType17 CName17, tdbType##CType18 CName18) { \
		const size_t ndx = GetSize(); \
		Insert##CType1 (0, ndx, CName1); \
		Insert##CType2 (1, ndx, CName2); \
		Insert##CType3 (2, ndx, CName3); \
		Insert##CType4 (3, ndx, CName4); \
		Insert##CType5 (4, ndx, CName5); \
		Insert##CType6 (5, ndx, CName6); \
		Insert##CType7 (6, ndx, CName7); \
		Insert##CType8 (7, ndx, CName8); \
		Insert##CType9 (8, ndx, CName9); \
		Insert##CType10 (9, ndx, CName10); \
		Insert##CType11 (10, ndx, CName11); \
		Insert##CType12 (11, ndx, CName12); \
		Insert##CType13 (12, ndx, CName13); \
		Insert##CType14 (13, ndx, CName14); \
		Insert##CType15 (14, ndx, CName15); \
		Insert##CType16 (15, ndx, CName16); \
		Insert##CType17 (16, ndx, CName17); \
		Insert##CType18 (17, ndx, CName18); \
		InsertDone(); \
	} \
\
	void Insert(size_t ndx, tdbType##CType1 CName1, tdbType##CType2 CName2, tdbType##CType3 CName3, tdbType##CType4 CName4, tdbType##CType5 CName5, tdbType##CType6 CName6, tdbType##CType7 CName7, tdbType##CType8 CName8, tdbType##CType9 CName9, tdbType##CType10 CName10, tdbType##CType11 CName11, tdbType##CType12 CName12, tdbType##CType13 CName13, tdbType##CType14 CName14, tdbType##CType15 CName15, tdbType##CType16 CName16, tdbType##CType17 CName17, tdbType##CType18 CName18) { \
		Insert##CType1 (0, ndx, CName1); \
		Insert##CType2 (1, ndx, CName2); \
		Insert##CType3 (2, ndx, CName3); \
		Insert##CType4 (3, ndx, CName4); \
		Insert##CType5 (4, ndx, CName5); \
		Insert##CType6 (5, ndx, CName6); \
		Insert##CType7 (6, ndx, CName7); \
		Insert##CType8 (7, ndx, CName8); \
		Insert##CType9 (8, ndx, CName9); \
		Insert##CType10 (9, ndx, CName10); \
		Insert##CType11 (10, ndx, CName11); \
		Insert##CType12 (11, ndx, CName12); \
		Insert##CType13 (12, ndx, CName13); \
		Insert##CType14 (13, ndx, CName14); \
		Insert##CType15 (14, ndx, CName15); \
		Insert##CType16 (15, ndx, CName16); \
		Insert##CType17 (16, ndx, CName17); \
		Insert##CType18 (17, ndx, CName18); \
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
	ColumnProxy##CType3 CName3; \
	ColumnProxy##CType4 CName4; \
	ColumnProxy##CType5 CName5; \
	ColumnProxy##CType6 CName6; \
	ColumnProxy##CType7 CName7; \
	ColumnProxy##CType8 CName8; \
	ColumnProxy##CType9 CName9; \
	ColumnProxy##CType10 CName10; \
	ColumnProxy##CType11 CName11; \
	ColumnProxy##CType12 CName12; \
	ColumnProxy##CType13 CName13; \
	ColumnProxy##CType14 CName14; \
	ColumnProxy##CType15 CName15; \
	ColumnProxy##CType16 CName16; \
	ColumnProxy##CType17 CName17; \
	ColumnProxy##CType18 CName18; \
\
protected: \
	friend class Group; \
	TableName(Allocator& alloc, size_t ref, Array* parent, size_t pndx) : TopLevelTable(alloc, ref, parent, pndx) {}; \
\
private: \
	TableName(const TableName&) {} \
	TableName& operator=(const TableName&) {return *this;} \
};



#define TDB_TABLE_19(TableName, CType1, CName1, CType2, CName2, CType3, CName3, CType4, CName4, CType5, CName5, CType6, CName6, CType7, CName7, CType8, CName8, CType9, CName9, CType10, CName10, CType11, CName11, CType12, CName12, CType13, CName13, CType14, CName14, CType15, CName15, CType16, CName16, CType17, CName17, CType18, CName18, CType19, CName19) \
class TableName##Query { \
protected: \
	QueryAccessor##CType1 CName1; \
	QueryAccessor##CType2 CName2; \
	QueryAccessor##CType3 CName3; \
	QueryAccessor##CType4 CName4; \
	QueryAccessor##CType5 CName5; \
	QueryAccessor##CType6 CName6; \
	QueryAccessor##CType7 CName7; \
	QueryAccessor##CType8 CName8; \
	QueryAccessor##CType9 CName9; \
	QueryAccessor##CType10 CName10; \
	QueryAccessor##CType11 CName11; \
	QueryAccessor##CType12 CName12; \
	QueryAccessor##CType13 CName13; \
	QueryAccessor##CType14 CName14; \
	QueryAccessor##CType15 CName15; \
	QueryAccessor##CType16 CName16; \
	QueryAccessor##CType17 CName17; \
	QueryAccessor##CType18 CName18; \
	QueryAccessor##CType19 CName19; \
}; \
\
class TableName : public TopLevelTable { \
public: \
	TableName(Allocator& alloc=GetDefaultAllocator()) : TopLevelTable(alloc) { \
		RegisterColumn(Accessor##CType1::type, #CName1); \
		RegisterColumn(Accessor##CType2::type, #CName2); \
		RegisterColumn(Accessor##CType3::type, #CName3); \
		RegisterColumn(Accessor##CType4::type, #CName4); \
		RegisterColumn(Accessor##CType5::type, #CName5); \
		RegisterColumn(Accessor##CType6::type, #CName6); \
		RegisterColumn(Accessor##CType7::type, #CName7); \
		RegisterColumn(Accessor##CType8::type, #CName8); \
		RegisterColumn(Accessor##CType9::type, #CName9); \
		RegisterColumn(Accessor##CType10::type, #CName10); \
		RegisterColumn(Accessor##CType11::type, #CName11); \
		RegisterColumn(Accessor##CType12::type, #CName12); \
		RegisterColumn(Accessor##CType13::type, #CName13); \
		RegisterColumn(Accessor##CType14::type, #CName14); \
		RegisterColumn(Accessor##CType15::type, #CName15); \
		RegisterColumn(Accessor##CType16::type, #CName16); \
		RegisterColumn(Accessor##CType17::type, #CName17); \
		RegisterColumn(Accessor##CType18::type, #CName18); \
		RegisterColumn(Accessor##CType19::type, #CName19); \
\
		CName1.Create(this, 0); \
		CName2.Create(this, 1); \
		CName3.Create(this, 2); \
		CName4.Create(this, 3); \
		CName5.Create(this, 4); \
		CName6.Create(this, 5); \
		CName7.Create(this, 6); \
		CName8.Create(this, 7); \
		CName9.Create(this, 8); \
		CName10.Create(this, 9); \
		CName11.Create(this, 10); \
		CName12.Create(this, 11); \
		CName13.Create(this, 12); \
		CName14.Create(this, 13); \
		CName15.Create(this, 14); \
		CName16.Create(this, 15); \
		CName17.Create(this, 16); \
		CName18.Create(this, 17); \
		CName19.Create(this, 18); \
	}; \
\
	class TestQuery : public Query { \
	public: \
		TestQuery() : CName1(0), CName2(1), CName3(2), CName4(3), CName5(4), CName6(5), CName7(6), CName8(7), CName9(8), CName10(9), CName11(10), CName12(11), CName13(12), CName14(13), CName15(14), CName16(15), CName17(16), CName18(17), CName19(18) { \
			CName1.SetQuery(this); \
			CName2.SetQuery(this); \
			CName3.SetQuery(this); \
			CName4.SetQuery(this); \
			CName5.SetQuery(this); \
			CName6.SetQuery(this); \
			CName7.SetQuery(this); \
			CName8.SetQuery(this); \
			CName9.SetQuery(this); \
			CName10.SetQuery(this); \
			CName11.SetQuery(this); \
			CName12.SetQuery(this); \
			CName13.SetQuery(this); \
			CName14.SetQuery(this); \
			CName15.SetQuery(this); \
			CName16.SetQuery(this); \
			CName17.SetQuery(this); \
			CName18.SetQuery(this); \
			CName19.SetQuery(this); \
		} \
\
		TestQuery(const TestQuery& copy) : Query(copy), CName1(0), CName2(1), CName3(2), CName4(3), CName5(4), CName6(5), CName7(6), CName8(7), CName9(8), CName10(9), CName11(10), CName12(11), CName13(12), CName14(13), CName15(14), CName16(15), CName17(16), CName18(17), CName19(18) { \
			CName1.SetQuery(this); \
			CName2.SetQuery(this); \
			CName3.SetQuery(this); \
			CName4.SetQuery(this); \
			CName5.SetQuery(this); \
			CName6.SetQuery(this); \
			CName7.SetQuery(this); \
			CName8.SetQuery(this); \
			CName9.SetQuery(this); \
			CName10.SetQuery(this); \
			CName11.SetQuery(this); \
			CName12.SetQuery(this); \
			CName13.SetQuery(this); \
			CName14.SetQuery(this); \
			CName15.SetQuery(this); \
			CName16.SetQuery(this); \
			CName17.SetQuery(this); \
			CName18.SetQuery(this); \
			CName19.SetQuery(this); \
		} \
\
		class TestQueryQueryAccessorInt : private XQueryAccessorInt { \
		public: \
			TestQueryQueryAccessorInt(size_t column_id) : XQueryAccessorInt(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(int64_t value) {return (TestQuery &)XQueryAccessorInt::Equal(value);} \
			TestQuery& NotEqual(int64_t value) {return (TestQuery &)XQueryAccessorInt::NotEqual(value);} \
			TestQuery& Greater(int64_t value) {return (TestQuery &)XQueryAccessorInt::Greater(value);} \
			TestQuery& Less(int64_t value) {return (TestQuery &)XQueryAccessorInt::Less(value);} \
			TestQuery& Between(int64_t from, int64_t to) {return (TestQuery &)XQueryAccessorInt::Between(from, to);} \
		}; \
\
		template <class T> class TestQueryQueryAccessorEnum : public TestQueryQueryAccessorInt { \
		public: \
			TestQueryQueryAccessorEnum<T>(size_t column_id) : TestQueryQueryAccessorInt(column_id) {} \
		}; \
\
		class TestQueryQueryAccessorString : private XQueryAccessorString { \
		public: \
			TestQueryQueryAccessorString(size_t column_id) : XQueryAccessorString(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::Equal(value, CaseSensitive);} \
			TestQuery& NotEqual(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::NotEqual(value, CaseSensitive);} \
			TestQuery& BeginsWith(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::BeginsWith(value, CaseSensitive);} \
			TestQuery& EndsWith(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::EndsWith(value, CaseSensitive);} \
			TestQuery& Contains(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::Contains(value, CaseSensitive);} \
		}; \
\
		class TestQueryQueryAccessorBool : private XQueryAccessorBool { \
		public: \
			TestQueryQueryAccessorBool(size_t column_id) : XQueryAccessorBool(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(bool value) {return (TestQuery &)XQueryAccessorBool::Equal(value);} \
		}; \
\
		TestQueryQueryAccessor##CType1 CName1; \
		TestQueryQueryAccessor##CType2 CName2; \
		TestQueryQueryAccessor##CType3 CName3; \
		TestQueryQueryAccessor##CType4 CName4; \
		TestQueryQueryAccessor##CType5 CName5; \
		TestQueryQueryAccessor##CType6 CName6; \
		TestQueryQueryAccessor##CType7 CName7; \
		TestQueryQueryAccessor##CType8 CName8; \
		TestQueryQueryAccessor##CType9 CName9; \
		TestQueryQueryAccessor##CType10 CName10; \
		TestQueryQueryAccessor##CType11 CName11; \
		TestQueryQueryAccessor##CType12 CName12; \
		TestQueryQueryAccessor##CType13 CName13; \
		TestQueryQueryAccessor##CType14 CName14; \
		TestQueryQueryAccessor##CType15 CName15; \
		TestQueryQueryAccessor##CType16 CName16; \
		TestQueryQueryAccessor##CType17 CName17; \
		TestQueryQueryAccessor##CType18 CName18; \
		TestQueryQueryAccessor##CType19 CName19; \
\
		TestQuery& LeftParan(void) {Query::LeftParan(); return *this;}; \
		TestQuery& Or(void) {Query::Or(); return *this;}; \
		TestQuery& RightParan(void) {Query::RightParan(); return *this;}; \
		TestQuery& Subtable(size_t column) {Query::Subtable(column); return *this;}; \
		TestQuery& Parent() {Query::Parent(); return *this;}; \
	}; \
\
	TestQuery GetQuery() {return TestQuery();} \
\
	class Cursor : public CursorBase { \
	public: \
		Cursor(TableName& table, size_t ndx) : CursorBase(table, ndx) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
			CName17.Create(this, 16); \
			CName18.Create(this, 17); \
			CName19.Create(this, 18); \
		} \
		Cursor(const TableName& table, size_t ndx) : CursorBase(const_cast<TableName&>(table), ndx) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
			CName17.Create(this, 16); \
			CName18.Create(this, 17); \
			CName19.Create(this, 18); \
		} \
		Cursor(const Cursor& v) : CursorBase(v) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
			CName17.Create(this, 16); \
			CName18.Create(this, 17); \
			CName19.Create(this, 18); \
		} \
		Accessor##CType1 CName1; \
		Accessor##CType2 CName2; \
		Accessor##CType3 CName3; \
		Accessor##CType4 CName4; \
		Accessor##CType5 CName5; \
		Accessor##CType6 CName6; \
		Accessor##CType7 CName7; \
		Accessor##CType8 CName8; \
		Accessor##CType9 CName9; \
		Accessor##CType10 CName10; \
		Accessor##CType11 CName11; \
		Accessor##CType12 CName12; \
		Accessor##CType13 CName13; \
		Accessor##CType14 CName14; \
		Accessor##CType15 CName15; \
		Accessor##CType16 CName16; \
		Accessor##CType17 CName17; \
		Accessor##CType18 CName18; \
		Accessor##CType19 CName19; \
	}; \
\
	void Add(tdbType##CType1 CName1, tdbType##CType2 CName2, tdbType##CType3 CName3, tdbType##CType4 CName4, tdbType##CType5 CName5, tdbType##CType6 CName6, tdbType##CType7 CName7, tdbType##CType8 CName8, tdbType##CType9 CName9, tdbType##CType10 CName10, tdbType##CType11 CName11, tdbType##CType12 CName12, tdbType##CType13 CName13, tdbType##CType14 CName14, tdbType##CType15 CName15, tdbType##CType16 CName16, tdbType##CType17 CName17, tdbType##CType18 CName18, tdbType##CType19 CName19) { \
		const size_t ndx = GetSize(); \
		Insert##CType1 (0, ndx, CName1); \
		Insert##CType2 (1, ndx, CName2); \
		Insert##CType3 (2, ndx, CName3); \
		Insert##CType4 (3, ndx, CName4); \
		Insert##CType5 (4, ndx, CName5); \
		Insert##CType6 (5, ndx, CName6); \
		Insert##CType7 (6, ndx, CName7); \
		Insert##CType8 (7, ndx, CName8); \
		Insert##CType9 (8, ndx, CName9); \
		Insert##CType10 (9, ndx, CName10); \
		Insert##CType11 (10, ndx, CName11); \
		Insert##CType12 (11, ndx, CName12); \
		Insert##CType13 (12, ndx, CName13); \
		Insert##CType14 (13, ndx, CName14); \
		Insert##CType15 (14, ndx, CName15); \
		Insert##CType16 (15, ndx, CName16); \
		Insert##CType17 (16, ndx, CName17); \
		Insert##CType18 (17, ndx, CName18); \
		Insert##CType19 (18, ndx, CName19); \
		InsertDone(); \
	} \
\
	void Insert(size_t ndx, tdbType##CType1 CName1, tdbType##CType2 CName2, tdbType##CType3 CName3, tdbType##CType4 CName4, tdbType##CType5 CName5, tdbType##CType6 CName6, tdbType##CType7 CName7, tdbType##CType8 CName8, tdbType##CType9 CName9, tdbType##CType10 CName10, tdbType##CType11 CName11, tdbType##CType12 CName12, tdbType##CType13 CName13, tdbType##CType14 CName14, tdbType##CType15 CName15, tdbType##CType16 CName16, tdbType##CType17 CName17, tdbType##CType18 CName18, tdbType##CType19 CName19) { \
		Insert##CType1 (0, ndx, CName1); \
		Insert##CType2 (1, ndx, CName2); \
		Insert##CType3 (2, ndx, CName3); \
		Insert##CType4 (3, ndx, CName4); \
		Insert##CType5 (4, ndx, CName5); \
		Insert##CType6 (5, ndx, CName6); \
		Insert##CType7 (6, ndx, CName7); \
		Insert##CType8 (7, ndx, CName8); \
		Insert##CType9 (8, ndx, CName9); \
		Insert##CType10 (9, ndx, CName10); \
		Insert##CType11 (10, ndx, CName11); \
		Insert##CType12 (11, ndx, CName12); \
		Insert##CType13 (12, ndx, CName13); \
		Insert##CType14 (13, ndx, CName14); \
		Insert##CType15 (14, ndx, CName15); \
		Insert##CType16 (15, ndx, CName16); \
		Insert##CType17 (16, ndx, CName17); \
		Insert##CType18 (17, ndx, CName18); \
		Insert##CType19 (18, ndx, CName19); \
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
	ColumnProxy##CType3 CName3; \
	ColumnProxy##CType4 CName4; \
	ColumnProxy##CType5 CName5; \
	ColumnProxy##CType6 CName6; \
	ColumnProxy##CType7 CName7; \
	ColumnProxy##CType8 CName8; \
	ColumnProxy##CType9 CName9; \
	ColumnProxy##CType10 CName10; \
	ColumnProxy##CType11 CName11; \
	ColumnProxy##CType12 CName12; \
	ColumnProxy##CType13 CName13; \
	ColumnProxy##CType14 CName14; \
	ColumnProxy##CType15 CName15; \
	ColumnProxy##CType16 CName16; \
	ColumnProxy##CType17 CName17; \
	ColumnProxy##CType18 CName18; \
	ColumnProxy##CType19 CName19; \
\
protected: \
	friend class Group; \
	TableName(Allocator& alloc, size_t ref, Array* parent, size_t pndx) : TopLevelTable(alloc, ref, parent, pndx) {}; \
\
private: \
	TableName(const TableName&) {} \
	TableName& operator=(const TableName&) {return *this;} \
};



#define TDB_TABLE_20(TableName, CType1, CName1, CType2, CName2, CType3, CName3, CType4, CName4, CType5, CName5, CType6, CName6, CType7, CName7, CType8, CName8, CType9, CName9, CType10, CName10, CType11, CName11, CType12, CName12, CType13, CName13, CType14, CName14, CType15, CName15, CType16, CName16, CType17, CName17, CType18, CName18, CType19, CName19, CType20, CName20) \
class TableName##Query { \
protected: \
	QueryAccessor##CType1 CName1; \
	QueryAccessor##CType2 CName2; \
	QueryAccessor##CType3 CName3; \
	QueryAccessor##CType4 CName4; \
	QueryAccessor##CType5 CName5; \
	QueryAccessor##CType6 CName6; \
	QueryAccessor##CType7 CName7; \
	QueryAccessor##CType8 CName8; \
	QueryAccessor##CType9 CName9; \
	QueryAccessor##CType10 CName10; \
	QueryAccessor##CType11 CName11; \
	QueryAccessor##CType12 CName12; \
	QueryAccessor##CType13 CName13; \
	QueryAccessor##CType14 CName14; \
	QueryAccessor##CType15 CName15; \
	QueryAccessor##CType16 CName16; \
	QueryAccessor##CType17 CName17; \
	QueryAccessor##CType18 CName18; \
	QueryAccessor##CType19 CName19; \
	QueryAccessor##CType20 CName20; \
}; \
\
class TableName : public TopLevelTable { \
public: \
	TableName(Allocator& alloc=GetDefaultAllocator()) : TopLevelTable(alloc) { \
		RegisterColumn(Accessor##CType1::type, #CName1); \
		RegisterColumn(Accessor##CType2::type, #CName2); \
		RegisterColumn(Accessor##CType3::type, #CName3); \
		RegisterColumn(Accessor##CType4::type, #CName4); \
		RegisterColumn(Accessor##CType5::type, #CName5); \
		RegisterColumn(Accessor##CType6::type, #CName6); \
		RegisterColumn(Accessor##CType7::type, #CName7); \
		RegisterColumn(Accessor##CType8::type, #CName8); \
		RegisterColumn(Accessor##CType9::type, #CName9); \
		RegisterColumn(Accessor##CType10::type, #CName10); \
		RegisterColumn(Accessor##CType11::type, #CName11); \
		RegisterColumn(Accessor##CType12::type, #CName12); \
		RegisterColumn(Accessor##CType13::type, #CName13); \
		RegisterColumn(Accessor##CType14::type, #CName14); \
		RegisterColumn(Accessor##CType15::type, #CName15); \
		RegisterColumn(Accessor##CType16::type, #CName16); \
		RegisterColumn(Accessor##CType17::type, #CName17); \
		RegisterColumn(Accessor##CType18::type, #CName18); \
		RegisterColumn(Accessor##CType19::type, #CName19); \
		RegisterColumn(Accessor##CType20::type, #CName20); \
\
		CName1.Create(this, 0); \
		CName2.Create(this, 1); \
		CName3.Create(this, 2); \
		CName4.Create(this, 3); \
		CName5.Create(this, 4); \
		CName6.Create(this, 5); \
		CName7.Create(this, 6); \
		CName8.Create(this, 7); \
		CName9.Create(this, 8); \
		CName10.Create(this, 9); \
		CName11.Create(this, 10); \
		CName12.Create(this, 11); \
		CName13.Create(this, 12); \
		CName14.Create(this, 13); \
		CName15.Create(this, 14); \
		CName16.Create(this, 15); \
		CName17.Create(this, 16); \
		CName18.Create(this, 17); \
		CName19.Create(this, 18); \
		CName20.Create(this, 19); \
	}; \
\
	class TestQuery : public Query { \
	public: \
		TestQuery() : CName1(0), CName2(1), CName3(2), CName4(3), CName5(4), CName6(5), CName7(6), CName8(7), CName9(8), CName10(9), CName11(10), CName12(11), CName13(12), CName14(13), CName15(14), CName16(15), CName17(16), CName18(17), CName19(18), CName20(19) { \
			CName1.SetQuery(this); \
			CName2.SetQuery(this); \
			CName3.SetQuery(this); \
			CName4.SetQuery(this); \
			CName5.SetQuery(this); \
			CName6.SetQuery(this); \
			CName7.SetQuery(this); \
			CName8.SetQuery(this); \
			CName9.SetQuery(this); \
			CName10.SetQuery(this); \
			CName11.SetQuery(this); \
			CName12.SetQuery(this); \
			CName13.SetQuery(this); \
			CName14.SetQuery(this); \
			CName15.SetQuery(this); \
			CName16.SetQuery(this); \
			CName17.SetQuery(this); \
			CName18.SetQuery(this); \
			CName19.SetQuery(this); \
			CName20.SetQuery(this); \
		} \
\
		TestQuery(const TestQuery& copy) : Query(copy), CName1(0), CName2(1), CName3(2), CName4(3), CName5(4), CName6(5), CName7(6), CName8(7), CName9(8), CName10(9), CName11(10), CName12(11), CName13(12), CName14(13), CName15(14), CName16(15), CName17(16), CName18(17), CName19(18), CName20(19) { \
			CName1.SetQuery(this); \
			CName2.SetQuery(this); \
			CName3.SetQuery(this); \
			CName4.SetQuery(this); \
			CName5.SetQuery(this); \
			CName6.SetQuery(this); \
			CName7.SetQuery(this); \
			CName8.SetQuery(this); \
			CName9.SetQuery(this); \
			CName10.SetQuery(this); \
			CName11.SetQuery(this); \
			CName12.SetQuery(this); \
			CName13.SetQuery(this); \
			CName14.SetQuery(this); \
			CName15.SetQuery(this); \
			CName16.SetQuery(this); \
			CName17.SetQuery(this); \
			CName18.SetQuery(this); \
			CName19.SetQuery(this); \
			CName20.SetQuery(this); \
		} \
\
		class TestQueryQueryAccessorInt : private XQueryAccessorInt { \
		public: \
			TestQueryQueryAccessorInt(size_t column_id) : XQueryAccessorInt(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(int64_t value) {return (TestQuery &)XQueryAccessorInt::Equal(value);} \
			TestQuery& NotEqual(int64_t value) {return (TestQuery &)XQueryAccessorInt::NotEqual(value);} \
			TestQuery& Greater(int64_t value) {return (TestQuery &)XQueryAccessorInt::Greater(value);} \
			TestQuery& Less(int64_t value) {return (TestQuery &)XQueryAccessorInt::Less(value);} \
			TestQuery& Between(int64_t from, int64_t to) {return (TestQuery &)XQueryAccessorInt::Between(from, to);} \
		}; \
\
		template <class T> class TestQueryQueryAccessorEnum : public TestQueryQueryAccessorInt { \
		public: \
			TestQueryQueryAccessorEnum<T>(size_t column_id) : TestQueryQueryAccessorInt(column_id) {} \
		}; \
\
		class TestQueryQueryAccessorString : private XQueryAccessorString { \
		public: \
			TestQueryQueryAccessorString(size_t column_id) : XQueryAccessorString(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::Equal(value, CaseSensitive);} \
			TestQuery& NotEqual(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::NotEqual(value, CaseSensitive);} \
			TestQuery& BeginsWith(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::BeginsWith(value, CaseSensitive);} \
			TestQuery& EndsWith(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::EndsWith(value, CaseSensitive);} \
			TestQuery& Contains(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::Contains(value, CaseSensitive);} \
		}; \
\
		class TestQueryQueryAccessorBool : private XQueryAccessorBool { \
		public: \
			TestQueryQueryAccessorBool(size_t column_id) : XQueryAccessorBool(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(bool value) {return (TestQuery &)XQueryAccessorBool::Equal(value);} \
		}; \
\
		TestQueryQueryAccessor##CType1 CName1; \
		TestQueryQueryAccessor##CType2 CName2; \
		TestQueryQueryAccessor##CType3 CName3; \
		TestQueryQueryAccessor##CType4 CName4; \
		TestQueryQueryAccessor##CType5 CName5; \
		TestQueryQueryAccessor##CType6 CName6; \
		TestQueryQueryAccessor##CType7 CName7; \
		TestQueryQueryAccessor##CType8 CName8; \
		TestQueryQueryAccessor##CType9 CName9; \
		TestQueryQueryAccessor##CType10 CName10; \
		TestQueryQueryAccessor##CType11 CName11; \
		TestQueryQueryAccessor##CType12 CName12; \
		TestQueryQueryAccessor##CType13 CName13; \
		TestQueryQueryAccessor##CType14 CName14; \
		TestQueryQueryAccessor##CType15 CName15; \
		TestQueryQueryAccessor##CType16 CName16; \
		TestQueryQueryAccessor##CType17 CName17; \
		TestQueryQueryAccessor##CType18 CName18; \
		TestQueryQueryAccessor##CType19 CName19; \
		TestQueryQueryAccessor##CType20 CName20; \
\
		TestQuery& LeftParan(void) {Query::LeftParan(); return *this;}; \
		TestQuery& Or(void) {Query::Or(); return *this;}; \
		TestQuery& RightParan(void) {Query::RightParan(); return *this;}; \
		TestQuery& Subtable(size_t column) {Query::Subtable(column); return *this;}; \
		TestQuery& Parent() {Query::Parent(); return *this;}; \
	}; \
\
	TestQuery GetQuery() {return TestQuery();} \
\
	class Cursor : public CursorBase { \
	public: \
		Cursor(TableName& table, size_t ndx) : CursorBase(table, ndx) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
			CName17.Create(this, 16); \
			CName18.Create(this, 17); \
			CName19.Create(this, 18); \
			CName20.Create(this, 19); \
		} \
		Cursor(const TableName& table, size_t ndx) : CursorBase(const_cast<TableName&>(table), ndx) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
			CName17.Create(this, 16); \
			CName18.Create(this, 17); \
			CName19.Create(this, 18); \
			CName20.Create(this, 19); \
		} \
		Cursor(const Cursor& v) : CursorBase(v) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
			CName17.Create(this, 16); \
			CName18.Create(this, 17); \
			CName19.Create(this, 18); \
			CName20.Create(this, 19); \
		} \
		Accessor##CType1 CName1; \
		Accessor##CType2 CName2; \
		Accessor##CType3 CName3; \
		Accessor##CType4 CName4; \
		Accessor##CType5 CName5; \
		Accessor##CType6 CName6; \
		Accessor##CType7 CName7; \
		Accessor##CType8 CName8; \
		Accessor##CType9 CName9; \
		Accessor##CType10 CName10; \
		Accessor##CType11 CName11; \
		Accessor##CType12 CName12; \
		Accessor##CType13 CName13; \
		Accessor##CType14 CName14; \
		Accessor##CType15 CName15; \
		Accessor##CType16 CName16; \
		Accessor##CType17 CName17; \
		Accessor##CType18 CName18; \
		Accessor##CType19 CName19; \
		Accessor##CType20 CName20; \
	}; \
\
	void Add(tdbType##CType1 CName1, tdbType##CType2 CName2, tdbType##CType3 CName3, tdbType##CType4 CName4, tdbType##CType5 CName5, tdbType##CType6 CName6, tdbType##CType7 CName7, tdbType##CType8 CName8, tdbType##CType9 CName9, tdbType##CType10 CName10, tdbType##CType11 CName11, tdbType##CType12 CName12, tdbType##CType13 CName13, tdbType##CType14 CName14, tdbType##CType15 CName15, tdbType##CType16 CName16, tdbType##CType17 CName17, tdbType##CType18 CName18, tdbType##CType19 CName19, tdbType##CType20 CName20) { \
		const size_t ndx = GetSize(); \
		Insert##CType1 (0, ndx, CName1); \
		Insert##CType2 (1, ndx, CName2); \
		Insert##CType3 (2, ndx, CName3); \
		Insert##CType4 (3, ndx, CName4); \
		Insert##CType5 (4, ndx, CName5); \
		Insert##CType6 (5, ndx, CName6); \
		Insert##CType7 (6, ndx, CName7); \
		Insert##CType8 (7, ndx, CName8); \
		Insert##CType9 (8, ndx, CName9); \
		Insert##CType10 (9, ndx, CName10); \
		Insert##CType11 (10, ndx, CName11); \
		Insert##CType12 (11, ndx, CName12); \
		Insert##CType13 (12, ndx, CName13); \
		Insert##CType14 (13, ndx, CName14); \
		Insert##CType15 (14, ndx, CName15); \
		Insert##CType16 (15, ndx, CName16); \
		Insert##CType17 (16, ndx, CName17); \
		Insert##CType18 (17, ndx, CName18); \
		Insert##CType19 (18, ndx, CName19); \
		Insert##CType20 (19, ndx, CName20); \
		InsertDone(); \
	} \
\
	void Insert(size_t ndx, tdbType##CType1 CName1, tdbType##CType2 CName2, tdbType##CType3 CName3, tdbType##CType4 CName4, tdbType##CType5 CName5, tdbType##CType6 CName6, tdbType##CType7 CName7, tdbType##CType8 CName8, tdbType##CType9 CName9, tdbType##CType10 CName10, tdbType##CType11 CName11, tdbType##CType12 CName12, tdbType##CType13 CName13, tdbType##CType14 CName14, tdbType##CType15 CName15, tdbType##CType16 CName16, tdbType##CType17 CName17, tdbType##CType18 CName18, tdbType##CType19 CName19, tdbType##CType20 CName20) { \
		Insert##CType1 (0, ndx, CName1); \
		Insert##CType2 (1, ndx, CName2); \
		Insert##CType3 (2, ndx, CName3); \
		Insert##CType4 (3, ndx, CName4); \
		Insert##CType5 (4, ndx, CName5); \
		Insert##CType6 (5, ndx, CName6); \
		Insert##CType7 (6, ndx, CName7); \
		Insert##CType8 (7, ndx, CName8); \
		Insert##CType9 (8, ndx, CName9); \
		Insert##CType10 (9, ndx, CName10); \
		Insert##CType11 (10, ndx, CName11); \
		Insert##CType12 (11, ndx, CName12); \
		Insert##CType13 (12, ndx, CName13); \
		Insert##CType14 (13, ndx, CName14); \
		Insert##CType15 (14, ndx, CName15); \
		Insert##CType16 (15, ndx, CName16); \
		Insert##CType17 (16, ndx, CName17); \
		Insert##CType18 (17, ndx, CName18); \
		Insert##CType19 (18, ndx, CName19); \
		Insert##CType20 (19, ndx, CName20); \
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
	ColumnProxy##CType3 CName3; \
	ColumnProxy##CType4 CName4; \
	ColumnProxy##CType5 CName5; \
	ColumnProxy##CType6 CName6; \
	ColumnProxy##CType7 CName7; \
	ColumnProxy##CType8 CName8; \
	ColumnProxy##CType9 CName9; \
	ColumnProxy##CType10 CName10; \
	ColumnProxy##CType11 CName11; \
	ColumnProxy##CType12 CName12; \
	ColumnProxy##CType13 CName13; \
	ColumnProxy##CType14 CName14; \
	ColumnProxy##CType15 CName15; \
	ColumnProxy##CType16 CName16; \
	ColumnProxy##CType17 CName17; \
	ColumnProxy##CType18 CName18; \
	ColumnProxy##CType19 CName19; \
	ColumnProxy##CType20 CName20; \
\
protected: \
	friend class Group; \
	TableName(Allocator& alloc, size_t ref, Array* parent, size_t pndx) : TopLevelTable(alloc, ref, parent, pndx) {}; \
\
private: \
	TableName(const TableName&) {} \
	TableName& operator=(const TableName&) {return *this;} \
};



#define TDB_TABLE_21(TableName, CType1, CName1, CType2, CName2, CType3, CName3, CType4, CName4, CType5, CName5, CType6, CName6, CType7, CName7, CType8, CName8, CType9, CName9, CType10, CName10, CType11, CName11, CType12, CName12, CType13, CName13, CType14, CName14, CType15, CName15, CType16, CName16, CType17, CName17, CType18, CName18, CType19, CName19, CType20, CName20, CType21, CName21) \
class TableName##Query { \
protected: \
	QueryAccessor##CType1 CName1; \
	QueryAccessor##CType2 CName2; \
	QueryAccessor##CType3 CName3; \
	QueryAccessor##CType4 CName4; \
	QueryAccessor##CType5 CName5; \
	QueryAccessor##CType6 CName6; \
	QueryAccessor##CType7 CName7; \
	QueryAccessor##CType8 CName8; \
	QueryAccessor##CType9 CName9; \
	QueryAccessor##CType10 CName10; \
	QueryAccessor##CType11 CName11; \
	QueryAccessor##CType12 CName12; \
	QueryAccessor##CType13 CName13; \
	QueryAccessor##CType14 CName14; \
	QueryAccessor##CType15 CName15; \
	QueryAccessor##CType16 CName16; \
	QueryAccessor##CType17 CName17; \
	QueryAccessor##CType18 CName18; \
	QueryAccessor##CType19 CName19; \
	QueryAccessor##CType20 CName20; \
	QueryAccessor##CType21 CName21; \
}; \
\
class TableName : public TopLevelTable { \
public: \
	TableName(Allocator& alloc=GetDefaultAllocator()) : TopLevelTable(alloc) { \
		RegisterColumn(Accessor##CType1::type, #CName1); \
		RegisterColumn(Accessor##CType2::type, #CName2); \
		RegisterColumn(Accessor##CType3::type, #CName3); \
		RegisterColumn(Accessor##CType4::type, #CName4); \
		RegisterColumn(Accessor##CType5::type, #CName5); \
		RegisterColumn(Accessor##CType6::type, #CName6); \
		RegisterColumn(Accessor##CType7::type, #CName7); \
		RegisterColumn(Accessor##CType8::type, #CName8); \
		RegisterColumn(Accessor##CType9::type, #CName9); \
		RegisterColumn(Accessor##CType10::type, #CName10); \
		RegisterColumn(Accessor##CType11::type, #CName11); \
		RegisterColumn(Accessor##CType12::type, #CName12); \
		RegisterColumn(Accessor##CType13::type, #CName13); \
		RegisterColumn(Accessor##CType14::type, #CName14); \
		RegisterColumn(Accessor##CType15::type, #CName15); \
		RegisterColumn(Accessor##CType16::type, #CName16); \
		RegisterColumn(Accessor##CType17::type, #CName17); \
		RegisterColumn(Accessor##CType18::type, #CName18); \
		RegisterColumn(Accessor##CType19::type, #CName19); \
		RegisterColumn(Accessor##CType20::type, #CName20); \
		RegisterColumn(Accessor##CType21::type, #CName21); \
\
		CName1.Create(this, 0); \
		CName2.Create(this, 1); \
		CName3.Create(this, 2); \
		CName4.Create(this, 3); \
		CName5.Create(this, 4); \
		CName6.Create(this, 5); \
		CName7.Create(this, 6); \
		CName8.Create(this, 7); \
		CName9.Create(this, 8); \
		CName10.Create(this, 9); \
		CName11.Create(this, 10); \
		CName12.Create(this, 11); \
		CName13.Create(this, 12); \
		CName14.Create(this, 13); \
		CName15.Create(this, 14); \
		CName16.Create(this, 15); \
		CName17.Create(this, 16); \
		CName18.Create(this, 17); \
		CName19.Create(this, 18); \
		CName20.Create(this, 19); \
		CName21.Create(this, 20); \
	}; \
\
	class TestQuery : public Query { \
	public: \
		TestQuery() : CName1(0), CName2(1), CName3(2), CName4(3), CName5(4), CName6(5), CName7(6), CName8(7), CName9(8), CName10(9), CName11(10), CName12(11), CName13(12), CName14(13), CName15(14), CName16(15), CName17(16), CName18(17), CName19(18), CName20(19), CName21(20) { \
			CName1.SetQuery(this); \
			CName2.SetQuery(this); \
			CName3.SetQuery(this); \
			CName4.SetQuery(this); \
			CName5.SetQuery(this); \
			CName6.SetQuery(this); \
			CName7.SetQuery(this); \
			CName8.SetQuery(this); \
			CName9.SetQuery(this); \
			CName10.SetQuery(this); \
			CName11.SetQuery(this); \
			CName12.SetQuery(this); \
			CName13.SetQuery(this); \
			CName14.SetQuery(this); \
			CName15.SetQuery(this); \
			CName16.SetQuery(this); \
			CName17.SetQuery(this); \
			CName18.SetQuery(this); \
			CName19.SetQuery(this); \
			CName20.SetQuery(this); \
			CName21.SetQuery(this); \
		} \
\
		TestQuery(const TestQuery& copy) : Query(copy), CName1(0), CName2(1), CName3(2), CName4(3), CName5(4), CName6(5), CName7(6), CName8(7), CName9(8), CName10(9), CName11(10), CName12(11), CName13(12), CName14(13), CName15(14), CName16(15), CName17(16), CName18(17), CName19(18), CName20(19), CName21(20) { \
			CName1.SetQuery(this); \
			CName2.SetQuery(this); \
			CName3.SetQuery(this); \
			CName4.SetQuery(this); \
			CName5.SetQuery(this); \
			CName6.SetQuery(this); \
			CName7.SetQuery(this); \
			CName8.SetQuery(this); \
			CName9.SetQuery(this); \
			CName10.SetQuery(this); \
			CName11.SetQuery(this); \
			CName12.SetQuery(this); \
			CName13.SetQuery(this); \
			CName14.SetQuery(this); \
			CName15.SetQuery(this); \
			CName16.SetQuery(this); \
			CName17.SetQuery(this); \
			CName18.SetQuery(this); \
			CName19.SetQuery(this); \
			CName20.SetQuery(this); \
			CName21.SetQuery(this); \
		} \
\
		class TestQueryQueryAccessorInt : private XQueryAccessorInt { \
		public: \
			TestQueryQueryAccessorInt(size_t column_id) : XQueryAccessorInt(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(int64_t value) {return (TestQuery &)XQueryAccessorInt::Equal(value);} \
			TestQuery& NotEqual(int64_t value) {return (TestQuery &)XQueryAccessorInt::NotEqual(value);} \
			TestQuery& Greater(int64_t value) {return (TestQuery &)XQueryAccessorInt::Greater(value);} \
			TestQuery& Less(int64_t value) {return (TestQuery &)XQueryAccessorInt::Less(value);} \
			TestQuery& Between(int64_t from, int64_t to) {return (TestQuery &)XQueryAccessorInt::Between(from, to);} \
		}; \
\
		template <class T> class TestQueryQueryAccessorEnum : public TestQueryQueryAccessorInt { \
		public: \
			TestQueryQueryAccessorEnum<T>(size_t column_id) : TestQueryQueryAccessorInt(column_id) {} \
		}; \
\
		class TestQueryQueryAccessorString : private XQueryAccessorString { \
		public: \
			TestQueryQueryAccessorString(size_t column_id) : XQueryAccessorString(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::Equal(value, CaseSensitive);} \
			TestQuery& NotEqual(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::NotEqual(value, CaseSensitive);} \
			TestQuery& BeginsWith(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::BeginsWith(value, CaseSensitive);} \
			TestQuery& EndsWith(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::EndsWith(value, CaseSensitive);} \
			TestQuery& Contains(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::Contains(value, CaseSensitive);} \
		}; \
\
		class TestQueryQueryAccessorBool : private XQueryAccessorBool { \
		public: \
			TestQueryQueryAccessorBool(size_t column_id) : XQueryAccessorBool(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(bool value) {return (TestQuery &)XQueryAccessorBool::Equal(value);} \
		}; \
\
		TestQueryQueryAccessor##CType1 CName1; \
		TestQueryQueryAccessor##CType2 CName2; \
		TestQueryQueryAccessor##CType3 CName3; \
		TestQueryQueryAccessor##CType4 CName4; \
		TestQueryQueryAccessor##CType5 CName5; \
		TestQueryQueryAccessor##CType6 CName6; \
		TestQueryQueryAccessor##CType7 CName7; \
		TestQueryQueryAccessor##CType8 CName8; \
		TestQueryQueryAccessor##CType9 CName9; \
		TestQueryQueryAccessor##CType10 CName10; \
		TestQueryQueryAccessor##CType11 CName11; \
		TestQueryQueryAccessor##CType12 CName12; \
		TestQueryQueryAccessor##CType13 CName13; \
		TestQueryQueryAccessor##CType14 CName14; \
		TestQueryQueryAccessor##CType15 CName15; \
		TestQueryQueryAccessor##CType16 CName16; \
		TestQueryQueryAccessor##CType17 CName17; \
		TestQueryQueryAccessor##CType18 CName18; \
		TestQueryQueryAccessor##CType19 CName19; \
		TestQueryQueryAccessor##CType20 CName20; \
		TestQueryQueryAccessor##CType21 CName21; \
\
		TestQuery& LeftParan(void) {Query::LeftParan(); return *this;}; \
		TestQuery& Or(void) {Query::Or(); return *this;}; \
		TestQuery& RightParan(void) {Query::RightParan(); return *this;}; \
		TestQuery& Subtable(size_t column) {Query::Subtable(column); return *this;}; \
		TestQuery& Parent() {Query::Parent(); return *this;}; \
	}; \
\
	TestQuery GetQuery() {return TestQuery();} \
\
	class Cursor : public CursorBase { \
	public: \
		Cursor(TableName& table, size_t ndx) : CursorBase(table, ndx) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
			CName17.Create(this, 16); \
			CName18.Create(this, 17); \
			CName19.Create(this, 18); \
			CName20.Create(this, 19); \
			CName21.Create(this, 20); \
		} \
		Cursor(const TableName& table, size_t ndx) : CursorBase(const_cast<TableName&>(table), ndx) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
			CName17.Create(this, 16); \
			CName18.Create(this, 17); \
			CName19.Create(this, 18); \
			CName20.Create(this, 19); \
			CName21.Create(this, 20); \
		} \
		Cursor(const Cursor& v) : CursorBase(v) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
			CName17.Create(this, 16); \
			CName18.Create(this, 17); \
			CName19.Create(this, 18); \
			CName20.Create(this, 19); \
			CName21.Create(this, 20); \
		} \
		Accessor##CType1 CName1; \
		Accessor##CType2 CName2; \
		Accessor##CType3 CName3; \
		Accessor##CType4 CName4; \
		Accessor##CType5 CName5; \
		Accessor##CType6 CName6; \
		Accessor##CType7 CName7; \
		Accessor##CType8 CName8; \
		Accessor##CType9 CName9; \
		Accessor##CType10 CName10; \
		Accessor##CType11 CName11; \
		Accessor##CType12 CName12; \
		Accessor##CType13 CName13; \
		Accessor##CType14 CName14; \
		Accessor##CType15 CName15; \
		Accessor##CType16 CName16; \
		Accessor##CType17 CName17; \
		Accessor##CType18 CName18; \
		Accessor##CType19 CName19; \
		Accessor##CType20 CName20; \
		Accessor##CType21 CName21; \
	}; \
\
	void Add(tdbType##CType1 CName1, tdbType##CType2 CName2, tdbType##CType3 CName3, tdbType##CType4 CName4, tdbType##CType5 CName5, tdbType##CType6 CName6, tdbType##CType7 CName7, tdbType##CType8 CName8, tdbType##CType9 CName9, tdbType##CType10 CName10, tdbType##CType11 CName11, tdbType##CType12 CName12, tdbType##CType13 CName13, tdbType##CType14 CName14, tdbType##CType15 CName15, tdbType##CType16 CName16, tdbType##CType17 CName17, tdbType##CType18 CName18, tdbType##CType19 CName19, tdbType##CType20 CName20, tdbType##CType21 CName21) { \
		const size_t ndx = GetSize(); \
		Insert##CType1 (0, ndx, CName1); \
		Insert##CType2 (1, ndx, CName2); \
		Insert##CType3 (2, ndx, CName3); \
		Insert##CType4 (3, ndx, CName4); \
		Insert##CType5 (4, ndx, CName5); \
		Insert##CType6 (5, ndx, CName6); \
		Insert##CType7 (6, ndx, CName7); \
		Insert##CType8 (7, ndx, CName8); \
		Insert##CType9 (8, ndx, CName9); \
		Insert##CType10 (9, ndx, CName10); \
		Insert##CType11 (10, ndx, CName11); \
		Insert##CType12 (11, ndx, CName12); \
		Insert##CType13 (12, ndx, CName13); \
		Insert##CType14 (13, ndx, CName14); \
		Insert##CType15 (14, ndx, CName15); \
		Insert##CType16 (15, ndx, CName16); \
		Insert##CType17 (16, ndx, CName17); \
		Insert##CType18 (17, ndx, CName18); \
		Insert##CType19 (18, ndx, CName19); \
		Insert##CType20 (19, ndx, CName20); \
		Insert##CType21 (20, ndx, CName21); \
		InsertDone(); \
	} \
\
	void Insert(size_t ndx, tdbType##CType1 CName1, tdbType##CType2 CName2, tdbType##CType3 CName3, tdbType##CType4 CName4, tdbType##CType5 CName5, tdbType##CType6 CName6, tdbType##CType7 CName7, tdbType##CType8 CName8, tdbType##CType9 CName9, tdbType##CType10 CName10, tdbType##CType11 CName11, tdbType##CType12 CName12, tdbType##CType13 CName13, tdbType##CType14 CName14, tdbType##CType15 CName15, tdbType##CType16 CName16, tdbType##CType17 CName17, tdbType##CType18 CName18, tdbType##CType19 CName19, tdbType##CType20 CName20, tdbType##CType21 CName21) { \
		Insert##CType1 (0, ndx, CName1); \
		Insert##CType2 (1, ndx, CName2); \
		Insert##CType3 (2, ndx, CName3); \
		Insert##CType4 (3, ndx, CName4); \
		Insert##CType5 (4, ndx, CName5); \
		Insert##CType6 (5, ndx, CName6); \
		Insert##CType7 (6, ndx, CName7); \
		Insert##CType8 (7, ndx, CName8); \
		Insert##CType9 (8, ndx, CName9); \
		Insert##CType10 (9, ndx, CName10); \
		Insert##CType11 (10, ndx, CName11); \
		Insert##CType12 (11, ndx, CName12); \
		Insert##CType13 (12, ndx, CName13); \
		Insert##CType14 (13, ndx, CName14); \
		Insert##CType15 (14, ndx, CName15); \
		Insert##CType16 (15, ndx, CName16); \
		Insert##CType17 (16, ndx, CName17); \
		Insert##CType18 (17, ndx, CName18); \
		Insert##CType19 (18, ndx, CName19); \
		Insert##CType20 (19, ndx, CName20); \
		Insert##CType21 (20, ndx, CName21); \
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
	ColumnProxy##CType3 CName3; \
	ColumnProxy##CType4 CName4; \
	ColumnProxy##CType5 CName5; \
	ColumnProxy##CType6 CName6; \
	ColumnProxy##CType7 CName7; \
	ColumnProxy##CType8 CName8; \
	ColumnProxy##CType9 CName9; \
	ColumnProxy##CType10 CName10; \
	ColumnProxy##CType11 CName11; \
	ColumnProxy##CType12 CName12; \
	ColumnProxy##CType13 CName13; \
	ColumnProxy##CType14 CName14; \
	ColumnProxy##CType15 CName15; \
	ColumnProxy##CType16 CName16; \
	ColumnProxy##CType17 CName17; \
	ColumnProxy##CType18 CName18; \
	ColumnProxy##CType19 CName19; \
	ColumnProxy##CType20 CName20; \
	ColumnProxy##CType21 CName21; \
\
protected: \
	friend class Group; \
	TableName(Allocator& alloc, size_t ref, Array* parent, size_t pndx) : TopLevelTable(alloc, ref, parent, pndx) {}; \
\
private: \
	TableName(const TableName&) {} \
	TableName& operator=(const TableName&) {return *this;} \
};



#define TDB_TABLE_22(TableName, CType1, CName1, CType2, CName2, CType3, CName3, CType4, CName4, CType5, CName5, CType6, CName6, CType7, CName7, CType8, CName8, CType9, CName9, CType10, CName10, CType11, CName11, CType12, CName12, CType13, CName13, CType14, CName14, CType15, CName15, CType16, CName16, CType17, CName17, CType18, CName18, CType19, CName19, CType20, CName20, CType21, CName21, CType22, CName22) \
class TableName##Query { \
protected: \
	QueryAccessor##CType1 CName1; \
	QueryAccessor##CType2 CName2; \
	QueryAccessor##CType3 CName3; \
	QueryAccessor##CType4 CName4; \
	QueryAccessor##CType5 CName5; \
	QueryAccessor##CType6 CName6; \
	QueryAccessor##CType7 CName7; \
	QueryAccessor##CType8 CName8; \
	QueryAccessor##CType9 CName9; \
	QueryAccessor##CType10 CName10; \
	QueryAccessor##CType11 CName11; \
	QueryAccessor##CType12 CName12; \
	QueryAccessor##CType13 CName13; \
	QueryAccessor##CType14 CName14; \
	QueryAccessor##CType15 CName15; \
	QueryAccessor##CType16 CName16; \
	QueryAccessor##CType17 CName17; \
	QueryAccessor##CType18 CName18; \
	QueryAccessor##CType19 CName19; \
	QueryAccessor##CType20 CName20; \
	QueryAccessor##CType21 CName21; \
	QueryAccessor##CType22 CName22; \
}; \
\
class TableName : public TopLevelTable { \
public: \
	TableName(Allocator& alloc=GetDefaultAllocator()) : TopLevelTable(alloc) { \
		RegisterColumn(Accessor##CType1::type, #CName1); \
		RegisterColumn(Accessor##CType2::type, #CName2); \
		RegisterColumn(Accessor##CType3::type, #CName3); \
		RegisterColumn(Accessor##CType4::type, #CName4); \
		RegisterColumn(Accessor##CType5::type, #CName5); \
		RegisterColumn(Accessor##CType6::type, #CName6); \
		RegisterColumn(Accessor##CType7::type, #CName7); \
		RegisterColumn(Accessor##CType8::type, #CName8); \
		RegisterColumn(Accessor##CType9::type, #CName9); \
		RegisterColumn(Accessor##CType10::type, #CName10); \
		RegisterColumn(Accessor##CType11::type, #CName11); \
		RegisterColumn(Accessor##CType12::type, #CName12); \
		RegisterColumn(Accessor##CType13::type, #CName13); \
		RegisterColumn(Accessor##CType14::type, #CName14); \
		RegisterColumn(Accessor##CType15::type, #CName15); \
		RegisterColumn(Accessor##CType16::type, #CName16); \
		RegisterColumn(Accessor##CType17::type, #CName17); \
		RegisterColumn(Accessor##CType18::type, #CName18); \
		RegisterColumn(Accessor##CType19::type, #CName19); \
		RegisterColumn(Accessor##CType20::type, #CName20); \
		RegisterColumn(Accessor##CType21::type, #CName21); \
		RegisterColumn(Accessor##CType22::type, #CName22); \
\
		CName1.Create(this, 0); \
		CName2.Create(this, 1); \
		CName3.Create(this, 2); \
		CName4.Create(this, 3); \
		CName5.Create(this, 4); \
		CName6.Create(this, 5); \
		CName7.Create(this, 6); \
		CName8.Create(this, 7); \
		CName9.Create(this, 8); \
		CName10.Create(this, 9); \
		CName11.Create(this, 10); \
		CName12.Create(this, 11); \
		CName13.Create(this, 12); \
		CName14.Create(this, 13); \
		CName15.Create(this, 14); \
		CName16.Create(this, 15); \
		CName17.Create(this, 16); \
		CName18.Create(this, 17); \
		CName19.Create(this, 18); \
		CName20.Create(this, 19); \
		CName21.Create(this, 20); \
		CName22.Create(this, 21); \
	}; \
\
	class TestQuery : public Query { \
	public: \
		TestQuery() : CName1(0), CName2(1), CName3(2), CName4(3), CName5(4), CName6(5), CName7(6), CName8(7), CName9(8), CName10(9), CName11(10), CName12(11), CName13(12), CName14(13), CName15(14), CName16(15), CName17(16), CName18(17), CName19(18), CName20(19), CName21(20), CName22(21) { \
			CName1.SetQuery(this); \
			CName2.SetQuery(this); \
			CName3.SetQuery(this); \
			CName4.SetQuery(this); \
			CName5.SetQuery(this); \
			CName6.SetQuery(this); \
			CName7.SetQuery(this); \
			CName8.SetQuery(this); \
			CName9.SetQuery(this); \
			CName10.SetQuery(this); \
			CName11.SetQuery(this); \
			CName12.SetQuery(this); \
			CName13.SetQuery(this); \
			CName14.SetQuery(this); \
			CName15.SetQuery(this); \
			CName16.SetQuery(this); \
			CName17.SetQuery(this); \
			CName18.SetQuery(this); \
			CName19.SetQuery(this); \
			CName20.SetQuery(this); \
			CName21.SetQuery(this); \
			CName22.SetQuery(this); \
		} \
\
		TestQuery(const TestQuery& copy) : Query(copy), CName1(0), CName2(1), CName3(2), CName4(3), CName5(4), CName6(5), CName7(6), CName8(7), CName9(8), CName10(9), CName11(10), CName12(11), CName13(12), CName14(13), CName15(14), CName16(15), CName17(16), CName18(17), CName19(18), CName20(19), CName21(20), CName22(21) { \
			CName1.SetQuery(this); \
			CName2.SetQuery(this); \
			CName3.SetQuery(this); \
			CName4.SetQuery(this); \
			CName5.SetQuery(this); \
			CName6.SetQuery(this); \
			CName7.SetQuery(this); \
			CName8.SetQuery(this); \
			CName9.SetQuery(this); \
			CName10.SetQuery(this); \
			CName11.SetQuery(this); \
			CName12.SetQuery(this); \
			CName13.SetQuery(this); \
			CName14.SetQuery(this); \
			CName15.SetQuery(this); \
			CName16.SetQuery(this); \
			CName17.SetQuery(this); \
			CName18.SetQuery(this); \
			CName19.SetQuery(this); \
			CName20.SetQuery(this); \
			CName21.SetQuery(this); \
			CName22.SetQuery(this); \
		} \
\
		class TestQueryQueryAccessorInt : private XQueryAccessorInt { \
		public: \
			TestQueryQueryAccessorInt(size_t column_id) : XQueryAccessorInt(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(int64_t value) {return (TestQuery &)XQueryAccessorInt::Equal(value);} \
			TestQuery& NotEqual(int64_t value) {return (TestQuery &)XQueryAccessorInt::NotEqual(value);} \
			TestQuery& Greater(int64_t value) {return (TestQuery &)XQueryAccessorInt::Greater(value);} \
			TestQuery& Less(int64_t value) {return (TestQuery &)XQueryAccessorInt::Less(value);} \
			TestQuery& Between(int64_t from, int64_t to) {return (TestQuery &)XQueryAccessorInt::Between(from, to);} \
		}; \
\
		template <class T> class TestQueryQueryAccessorEnum : public TestQueryQueryAccessorInt { \
		public: \
			TestQueryQueryAccessorEnum<T>(size_t column_id) : TestQueryQueryAccessorInt(column_id) {} \
		}; \
\
		class TestQueryQueryAccessorString : private XQueryAccessorString { \
		public: \
			TestQueryQueryAccessorString(size_t column_id) : XQueryAccessorString(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::Equal(value, CaseSensitive);} \
			TestQuery& NotEqual(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::NotEqual(value, CaseSensitive);} \
			TestQuery& BeginsWith(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::BeginsWith(value, CaseSensitive);} \
			TestQuery& EndsWith(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::EndsWith(value, CaseSensitive);} \
			TestQuery& Contains(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::Contains(value, CaseSensitive);} \
		}; \
\
		class TestQueryQueryAccessorBool : private XQueryAccessorBool { \
		public: \
			TestQueryQueryAccessorBool(size_t column_id) : XQueryAccessorBool(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(bool value) {return (TestQuery &)XQueryAccessorBool::Equal(value);} \
		}; \
\
		TestQueryQueryAccessor##CType1 CName1; \
		TestQueryQueryAccessor##CType2 CName2; \
		TestQueryQueryAccessor##CType3 CName3; \
		TestQueryQueryAccessor##CType4 CName4; \
		TestQueryQueryAccessor##CType5 CName5; \
		TestQueryQueryAccessor##CType6 CName6; \
		TestQueryQueryAccessor##CType7 CName7; \
		TestQueryQueryAccessor##CType8 CName8; \
		TestQueryQueryAccessor##CType9 CName9; \
		TestQueryQueryAccessor##CType10 CName10; \
		TestQueryQueryAccessor##CType11 CName11; \
		TestQueryQueryAccessor##CType12 CName12; \
		TestQueryQueryAccessor##CType13 CName13; \
		TestQueryQueryAccessor##CType14 CName14; \
		TestQueryQueryAccessor##CType15 CName15; \
		TestQueryQueryAccessor##CType16 CName16; \
		TestQueryQueryAccessor##CType17 CName17; \
		TestQueryQueryAccessor##CType18 CName18; \
		TestQueryQueryAccessor##CType19 CName19; \
		TestQueryQueryAccessor##CType20 CName20; \
		TestQueryQueryAccessor##CType21 CName21; \
		TestQueryQueryAccessor##CType22 CName22; \
\
		TestQuery& LeftParan(void) {Query::LeftParan(); return *this;}; \
		TestQuery& Or(void) {Query::Or(); return *this;}; \
		TestQuery& RightParan(void) {Query::RightParan(); return *this;}; \
		TestQuery& Subtable(size_t column) {Query::Subtable(column); return *this;}; \
		TestQuery& Parent() {Query::Parent(); return *this;}; \
	}; \
\
	TestQuery GetQuery() {return TestQuery();} \
\
	class Cursor : public CursorBase { \
	public: \
		Cursor(TableName& table, size_t ndx) : CursorBase(table, ndx) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
			CName17.Create(this, 16); \
			CName18.Create(this, 17); \
			CName19.Create(this, 18); \
			CName20.Create(this, 19); \
			CName21.Create(this, 20); \
			CName22.Create(this, 21); \
		} \
		Cursor(const TableName& table, size_t ndx) : CursorBase(const_cast<TableName&>(table), ndx) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
			CName17.Create(this, 16); \
			CName18.Create(this, 17); \
			CName19.Create(this, 18); \
			CName20.Create(this, 19); \
			CName21.Create(this, 20); \
			CName22.Create(this, 21); \
		} \
		Cursor(const Cursor& v) : CursorBase(v) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
			CName17.Create(this, 16); \
			CName18.Create(this, 17); \
			CName19.Create(this, 18); \
			CName20.Create(this, 19); \
			CName21.Create(this, 20); \
			CName22.Create(this, 21); \
		} \
		Accessor##CType1 CName1; \
		Accessor##CType2 CName2; \
		Accessor##CType3 CName3; \
		Accessor##CType4 CName4; \
		Accessor##CType5 CName5; \
		Accessor##CType6 CName6; \
		Accessor##CType7 CName7; \
		Accessor##CType8 CName8; \
		Accessor##CType9 CName9; \
		Accessor##CType10 CName10; \
		Accessor##CType11 CName11; \
		Accessor##CType12 CName12; \
		Accessor##CType13 CName13; \
		Accessor##CType14 CName14; \
		Accessor##CType15 CName15; \
		Accessor##CType16 CName16; \
		Accessor##CType17 CName17; \
		Accessor##CType18 CName18; \
		Accessor##CType19 CName19; \
		Accessor##CType20 CName20; \
		Accessor##CType21 CName21; \
		Accessor##CType22 CName22; \
	}; \
\
	void Add(tdbType##CType1 CName1, tdbType##CType2 CName2, tdbType##CType3 CName3, tdbType##CType4 CName4, tdbType##CType5 CName5, tdbType##CType6 CName6, tdbType##CType7 CName7, tdbType##CType8 CName8, tdbType##CType9 CName9, tdbType##CType10 CName10, tdbType##CType11 CName11, tdbType##CType12 CName12, tdbType##CType13 CName13, tdbType##CType14 CName14, tdbType##CType15 CName15, tdbType##CType16 CName16, tdbType##CType17 CName17, tdbType##CType18 CName18, tdbType##CType19 CName19, tdbType##CType20 CName20, tdbType##CType21 CName21, tdbType##CType22 CName22) { \
		const size_t ndx = GetSize(); \
		Insert##CType1 (0, ndx, CName1); \
		Insert##CType2 (1, ndx, CName2); \
		Insert##CType3 (2, ndx, CName3); \
		Insert##CType4 (3, ndx, CName4); \
		Insert##CType5 (4, ndx, CName5); \
		Insert##CType6 (5, ndx, CName6); \
		Insert##CType7 (6, ndx, CName7); \
		Insert##CType8 (7, ndx, CName8); \
		Insert##CType9 (8, ndx, CName9); \
		Insert##CType10 (9, ndx, CName10); \
		Insert##CType11 (10, ndx, CName11); \
		Insert##CType12 (11, ndx, CName12); \
		Insert##CType13 (12, ndx, CName13); \
		Insert##CType14 (13, ndx, CName14); \
		Insert##CType15 (14, ndx, CName15); \
		Insert##CType16 (15, ndx, CName16); \
		Insert##CType17 (16, ndx, CName17); \
		Insert##CType18 (17, ndx, CName18); \
		Insert##CType19 (18, ndx, CName19); \
		Insert##CType20 (19, ndx, CName20); \
		Insert##CType21 (20, ndx, CName21); \
		Insert##CType22 (21, ndx, CName22); \
		InsertDone(); \
	} \
\
	void Insert(size_t ndx, tdbType##CType1 CName1, tdbType##CType2 CName2, tdbType##CType3 CName3, tdbType##CType4 CName4, tdbType##CType5 CName5, tdbType##CType6 CName6, tdbType##CType7 CName7, tdbType##CType8 CName8, tdbType##CType9 CName9, tdbType##CType10 CName10, tdbType##CType11 CName11, tdbType##CType12 CName12, tdbType##CType13 CName13, tdbType##CType14 CName14, tdbType##CType15 CName15, tdbType##CType16 CName16, tdbType##CType17 CName17, tdbType##CType18 CName18, tdbType##CType19 CName19, tdbType##CType20 CName20, tdbType##CType21 CName21, tdbType##CType22 CName22) { \
		Insert##CType1 (0, ndx, CName1); \
		Insert##CType2 (1, ndx, CName2); \
		Insert##CType3 (2, ndx, CName3); \
		Insert##CType4 (3, ndx, CName4); \
		Insert##CType5 (4, ndx, CName5); \
		Insert##CType6 (5, ndx, CName6); \
		Insert##CType7 (6, ndx, CName7); \
		Insert##CType8 (7, ndx, CName8); \
		Insert##CType9 (8, ndx, CName9); \
		Insert##CType10 (9, ndx, CName10); \
		Insert##CType11 (10, ndx, CName11); \
		Insert##CType12 (11, ndx, CName12); \
		Insert##CType13 (12, ndx, CName13); \
		Insert##CType14 (13, ndx, CName14); \
		Insert##CType15 (14, ndx, CName15); \
		Insert##CType16 (15, ndx, CName16); \
		Insert##CType17 (16, ndx, CName17); \
		Insert##CType18 (17, ndx, CName18); \
		Insert##CType19 (18, ndx, CName19); \
		Insert##CType20 (19, ndx, CName20); \
		Insert##CType21 (20, ndx, CName21); \
		Insert##CType22 (21, ndx, CName22); \
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
	ColumnProxy##CType3 CName3; \
	ColumnProxy##CType4 CName4; \
	ColumnProxy##CType5 CName5; \
	ColumnProxy##CType6 CName6; \
	ColumnProxy##CType7 CName7; \
	ColumnProxy##CType8 CName8; \
	ColumnProxy##CType9 CName9; \
	ColumnProxy##CType10 CName10; \
	ColumnProxy##CType11 CName11; \
	ColumnProxy##CType12 CName12; \
	ColumnProxy##CType13 CName13; \
	ColumnProxy##CType14 CName14; \
	ColumnProxy##CType15 CName15; \
	ColumnProxy##CType16 CName16; \
	ColumnProxy##CType17 CName17; \
	ColumnProxy##CType18 CName18; \
	ColumnProxy##CType19 CName19; \
	ColumnProxy##CType20 CName20; \
	ColumnProxy##CType21 CName21; \
	ColumnProxy##CType22 CName22; \
\
protected: \
	friend class Group; \
	TableName(Allocator& alloc, size_t ref, Array* parent, size_t pndx) : TopLevelTable(alloc, ref, parent, pndx) {}; \
\
private: \
	TableName(const TableName&) {} \
	TableName& operator=(const TableName&) {return *this;} \
};



#define TDB_TABLE_23(TableName, CType1, CName1, CType2, CName2, CType3, CName3, CType4, CName4, CType5, CName5, CType6, CName6, CType7, CName7, CType8, CName8, CType9, CName9, CType10, CName10, CType11, CName11, CType12, CName12, CType13, CName13, CType14, CName14, CType15, CName15, CType16, CName16, CType17, CName17, CType18, CName18, CType19, CName19, CType20, CName20, CType21, CName21, CType22, CName22, CType23, CName23) \
class TableName##Query { \
protected: \
	QueryAccessor##CType1 CName1; \
	QueryAccessor##CType2 CName2; \
	QueryAccessor##CType3 CName3; \
	QueryAccessor##CType4 CName4; \
	QueryAccessor##CType5 CName5; \
	QueryAccessor##CType6 CName6; \
	QueryAccessor##CType7 CName7; \
	QueryAccessor##CType8 CName8; \
	QueryAccessor##CType9 CName9; \
	QueryAccessor##CType10 CName10; \
	QueryAccessor##CType11 CName11; \
	QueryAccessor##CType12 CName12; \
	QueryAccessor##CType13 CName13; \
	QueryAccessor##CType14 CName14; \
	QueryAccessor##CType15 CName15; \
	QueryAccessor##CType16 CName16; \
	QueryAccessor##CType17 CName17; \
	QueryAccessor##CType18 CName18; \
	QueryAccessor##CType19 CName19; \
	QueryAccessor##CType20 CName20; \
	QueryAccessor##CType21 CName21; \
	QueryAccessor##CType22 CName22; \
	QueryAccessor##CType23 CName23; \
}; \
\
class TableName : public TopLevelTable { \
public: \
	TableName(Allocator& alloc=GetDefaultAllocator()) : TopLevelTable(alloc) { \
		RegisterColumn(Accessor##CType1::type, #CName1); \
		RegisterColumn(Accessor##CType2::type, #CName2); \
		RegisterColumn(Accessor##CType3::type, #CName3); \
		RegisterColumn(Accessor##CType4::type, #CName4); \
		RegisterColumn(Accessor##CType5::type, #CName5); \
		RegisterColumn(Accessor##CType6::type, #CName6); \
		RegisterColumn(Accessor##CType7::type, #CName7); \
		RegisterColumn(Accessor##CType8::type, #CName8); \
		RegisterColumn(Accessor##CType9::type, #CName9); \
		RegisterColumn(Accessor##CType10::type, #CName10); \
		RegisterColumn(Accessor##CType11::type, #CName11); \
		RegisterColumn(Accessor##CType12::type, #CName12); \
		RegisterColumn(Accessor##CType13::type, #CName13); \
		RegisterColumn(Accessor##CType14::type, #CName14); \
		RegisterColumn(Accessor##CType15::type, #CName15); \
		RegisterColumn(Accessor##CType16::type, #CName16); \
		RegisterColumn(Accessor##CType17::type, #CName17); \
		RegisterColumn(Accessor##CType18::type, #CName18); \
		RegisterColumn(Accessor##CType19::type, #CName19); \
		RegisterColumn(Accessor##CType20::type, #CName20); \
		RegisterColumn(Accessor##CType21::type, #CName21); \
		RegisterColumn(Accessor##CType22::type, #CName22); \
		RegisterColumn(Accessor##CType23::type, #CName23); \
\
		CName1.Create(this, 0); \
		CName2.Create(this, 1); \
		CName3.Create(this, 2); \
		CName4.Create(this, 3); \
		CName5.Create(this, 4); \
		CName6.Create(this, 5); \
		CName7.Create(this, 6); \
		CName8.Create(this, 7); \
		CName9.Create(this, 8); \
		CName10.Create(this, 9); \
		CName11.Create(this, 10); \
		CName12.Create(this, 11); \
		CName13.Create(this, 12); \
		CName14.Create(this, 13); \
		CName15.Create(this, 14); \
		CName16.Create(this, 15); \
		CName17.Create(this, 16); \
		CName18.Create(this, 17); \
		CName19.Create(this, 18); \
		CName20.Create(this, 19); \
		CName21.Create(this, 20); \
		CName22.Create(this, 21); \
		CName23.Create(this, 22); \
	}; \
\
	class TestQuery : public Query { \
	public: \
		TestQuery() : CName1(0), CName2(1), CName3(2), CName4(3), CName5(4), CName6(5), CName7(6), CName8(7), CName9(8), CName10(9), CName11(10), CName12(11), CName13(12), CName14(13), CName15(14), CName16(15), CName17(16), CName18(17), CName19(18), CName20(19), CName21(20), CName22(21), CName23(22) { \
			CName1.SetQuery(this); \
			CName2.SetQuery(this); \
			CName3.SetQuery(this); \
			CName4.SetQuery(this); \
			CName5.SetQuery(this); \
			CName6.SetQuery(this); \
			CName7.SetQuery(this); \
			CName8.SetQuery(this); \
			CName9.SetQuery(this); \
			CName10.SetQuery(this); \
			CName11.SetQuery(this); \
			CName12.SetQuery(this); \
			CName13.SetQuery(this); \
			CName14.SetQuery(this); \
			CName15.SetQuery(this); \
			CName16.SetQuery(this); \
			CName17.SetQuery(this); \
			CName18.SetQuery(this); \
			CName19.SetQuery(this); \
			CName20.SetQuery(this); \
			CName21.SetQuery(this); \
			CName22.SetQuery(this); \
			CName23.SetQuery(this); \
		} \
\
		TestQuery(const TestQuery& copy) : Query(copy), CName1(0), CName2(1), CName3(2), CName4(3), CName5(4), CName6(5), CName7(6), CName8(7), CName9(8), CName10(9), CName11(10), CName12(11), CName13(12), CName14(13), CName15(14), CName16(15), CName17(16), CName18(17), CName19(18), CName20(19), CName21(20), CName22(21), CName23(22) { \
			CName1.SetQuery(this); \
			CName2.SetQuery(this); \
			CName3.SetQuery(this); \
			CName4.SetQuery(this); \
			CName5.SetQuery(this); \
			CName6.SetQuery(this); \
			CName7.SetQuery(this); \
			CName8.SetQuery(this); \
			CName9.SetQuery(this); \
			CName10.SetQuery(this); \
			CName11.SetQuery(this); \
			CName12.SetQuery(this); \
			CName13.SetQuery(this); \
			CName14.SetQuery(this); \
			CName15.SetQuery(this); \
			CName16.SetQuery(this); \
			CName17.SetQuery(this); \
			CName18.SetQuery(this); \
			CName19.SetQuery(this); \
			CName20.SetQuery(this); \
			CName21.SetQuery(this); \
			CName22.SetQuery(this); \
			CName23.SetQuery(this); \
		} \
\
		class TestQueryQueryAccessorInt : private XQueryAccessorInt { \
		public: \
			TestQueryQueryAccessorInt(size_t column_id) : XQueryAccessorInt(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(int64_t value) {return (TestQuery &)XQueryAccessorInt::Equal(value);} \
			TestQuery& NotEqual(int64_t value) {return (TestQuery &)XQueryAccessorInt::NotEqual(value);} \
			TestQuery& Greater(int64_t value) {return (TestQuery &)XQueryAccessorInt::Greater(value);} \
			TestQuery& Less(int64_t value) {return (TestQuery &)XQueryAccessorInt::Less(value);} \
			TestQuery& Between(int64_t from, int64_t to) {return (TestQuery &)XQueryAccessorInt::Between(from, to);} \
		}; \
\
		template <class T> class TestQueryQueryAccessorEnum : public TestQueryQueryAccessorInt { \
		public: \
			TestQueryQueryAccessorEnum<T>(size_t column_id) : TestQueryQueryAccessorInt(column_id) {} \
		}; \
\
		class TestQueryQueryAccessorString : private XQueryAccessorString { \
		public: \
			TestQueryQueryAccessorString(size_t column_id) : XQueryAccessorString(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::Equal(value, CaseSensitive);} \
			TestQuery& NotEqual(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::NotEqual(value, CaseSensitive);} \
			TestQuery& BeginsWith(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::BeginsWith(value, CaseSensitive);} \
			TestQuery& EndsWith(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::EndsWith(value, CaseSensitive);} \
			TestQuery& Contains(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::Contains(value, CaseSensitive);} \
		}; \
\
		class TestQueryQueryAccessorBool : private XQueryAccessorBool { \
		public: \
			TestQueryQueryAccessorBool(size_t column_id) : XQueryAccessorBool(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(bool value) {return (TestQuery &)XQueryAccessorBool::Equal(value);} \
		}; \
\
		TestQueryQueryAccessor##CType1 CName1; \
		TestQueryQueryAccessor##CType2 CName2; \
		TestQueryQueryAccessor##CType3 CName3; \
		TestQueryQueryAccessor##CType4 CName4; \
		TestQueryQueryAccessor##CType5 CName5; \
		TestQueryQueryAccessor##CType6 CName6; \
		TestQueryQueryAccessor##CType7 CName7; \
		TestQueryQueryAccessor##CType8 CName8; \
		TestQueryQueryAccessor##CType9 CName9; \
		TestQueryQueryAccessor##CType10 CName10; \
		TestQueryQueryAccessor##CType11 CName11; \
		TestQueryQueryAccessor##CType12 CName12; \
		TestQueryQueryAccessor##CType13 CName13; \
		TestQueryQueryAccessor##CType14 CName14; \
		TestQueryQueryAccessor##CType15 CName15; \
		TestQueryQueryAccessor##CType16 CName16; \
		TestQueryQueryAccessor##CType17 CName17; \
		TestQueryQueryAccessor##CType18 CName18; \
		TestQueryQueryAccessor##CType19 CName19; \
		TestQueryQueryAccessor##CType20 CName20; \
		TestQueryQueryAccessor##CType21 CName21; \
		TestQueryQueryAccessor##CType22 CName22; \
		TestQueryQueryAccessor##CType23 CName23; \
\
		TestQuery& LeftParan(void) {Query::LeftParan(); return *this;}; \
		TestQuery& Or(void) {Query::Or(); return *this;}; \
		TestQuery& RightParan(void) {Query::RightParan(); return *this;}; \
		TestQuery& Subtable(size_t column) {Query::Subtable(column); return *this;}; \
		TestQuery& Parent() {Query::Parent(); return *this;}; \
	}; \
\
	TestQuery GetQuery() {return TestQuery();} \
\
	class Cursor : public CursorBase { \
	public: \
		Cursor(TableName& table, size_t ndx) : CursorBase(table, ndx) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
			CName17.Create(this, 16); \
			CName18.Create(this, 17); \
			CName19.Create(this, 18); \
			CName20.Create(this, 19); \
			CName21.Create(this, 20); \
			CName22.Create(this, 21); \
			CName23.Create(this, 22); \
		} \
		Cursor(const TableName& table, size_t ndx) : CursorBase(const_cast<TableName&>(table), ndx) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
			CName17.Create(this, 16); \
			CName18.Create(this, 17); \
			CName19.Create(this, 18); \
			CName20.Create(this, 19); \
			CName21.Create(this, 20); \
			CName22.Create(this, 21); \
			CName23.Create(this, 22); \
		} \
		Cursor(const Cursor& v) : CursorBase(v) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
			CName17.Create(this, 16); \
			CName18.Create(this, 17); \
			CName19.Create(this, 18); \
			CName20.Create(this, 19); \
			CName21.Create(this, 20); \
			CName22.Create(this, 21); \
			CName23.Create(this, 22); \
		} \
		Accessor##CType1 CName1; \
		Accessor##CType2 CName2; \
		Accessor##CType3 CName3; \
		Accessor##CType4 CName4; \
		Accessor##CType5 CName5; \
		Accessor##CType6 CName6; \
		Accessor##CType7 CName7; \
		Accessor##CType8 CName8; \
		Accessor##CType9 CName9; \
		Accessor##CType10 CName10; \
		Accessor##CType11 CName11; \
		Accessor##CType12 CName12; \
		Accessor##CType13 CName13; \
		Accessor##CType14 CName14; \
		Accessor##CType15 CName15; \
		Accessor##CType16 CName16; \
		Accessor##CType17 CName17; \
		Accessor##CType18 CName18; \
		Accessor##CType19 CName19; \
		Accessor##CType20 CName20; \
		Accessor##CType21 CName21; \
		Accessor##CType22 CName22; \
		Accessor##CType23 CName23; \
	}; \
\
	void Add(tdbType##CType1 CName1, tdbType##CType2 CName2, tdbType##CType3 CName3, tdbType##CType4 CName4, tdbType##CType5 CName5, tdbType##CType6 CName6, tdbType##CType7 CName7, tdbType##CType8 CName8, tdbType##CType9 CName9, tdbType##CType10 CName10, tdbType##CType11 CName11, tdbType##CType12 CName12, tdbType##CType13 CName13, tdbType##CType14 CName14, tdbType##CType15 CName15, tdbType##CType16 CName16, tdbType##CType17 CName17, tdbType##CType18 CName18, tdbType##CType19 CName19, tdbType##CType20 CName20, tdbType##CType21 CName21, tdbType##CType22 CName22, tdbType##CType23 CName23) { \
		const size_t ndx = GetSize(); \
		Insert##CType1 (0, ndx, CName1); \
		Insert##CType2 (1, ndx, CName2); \
		Insert##CType3 (2, ndx, CName3); \
		Insert##CType4 (3, ndx, CName4); \
		Insert##CType5 (4, ndx, CName5); \
		Insert##CType6 (5, ndx, CName6); \
		Insert##CType7 (6, ndx, CName7); \
		Insert##CType8 (7, ndx, CName8); \
		Insert##CType9 (8, ndx, CName9); \
		Insert##CType10 (9, ndx, CName10); \
		Insert##CType11 (10, ndx, CName11); \
		Insert##CType12 (11, ndx, CName12); \
		Insert##CType13 (12, ndx, CName13); \
		Insert##CType14 (13, ndx, CName14); \
		Insert##CType15 (14, ndx, CName15); \
		Insert##CType16 (15, ndx, CName16); \
		Insert##CType17 (16, ndx, CName17); \
		Insert##CType18 (17, ndx, CName18); \
		Insert##CType19 (18, ndx, CName19); \
		Insert##CType20 (19, ndx, CName20); \
		Insert##CType21 (20, ndx, CName21); \
		Insert##CType22 (21, ndx, CName22); \
		Insert##CType23 (22, ndx, CName23); \
		InsertDone(); \
	} \
\
	void Insert(size_t ndx, tdbType##CType1 CName1, tdbType##CType2 CName2, tdbType##CType3 CName3, tdbType##CType4 CName4, tdbType##CType5 CName5, tdbType##CType6 CName6, tdbType##CType7 CName7, tdbType##CType8 CName8, tdbType##CType9 CName9, tdbType##CType10 CName10, tdbType##CType11 CName11, tdbType##CType12 CName12, tdbType##CType13 CName13, tdbType##CType14 CName14, tdbType##CType15 CName15, tdbType##CType16 CName16, tdbType##CType17 CName17, tdbType##CType18 CName18, tdbType##CType19 CName19, tdbType##CType20 CName20, tdbType##CType21 CName21, tdbType##CType22 CName22, tdbType##CType23 CName23) { \
		Insert##CType1 (0, ndx, CName1); \
		Insert##CType2 (1, ndx, CName2); \
		Insert##CType3 (2, ndx, CName3); \
		Insert##CType4 (3, ndx, CName4); \
		Insert##CType5 (4, ndx, CName5); \
		Insert##CType6 (5, ndx, CName6); \
		Insert##CType7 (6, ndx, CName7); \
		Insert##CType8 (7, ndx, CName8); \
		Insert##CType9 (8, ndx, CName9); \
		Insert##CType10 (9, ndx, CName10); \
		Insert##CType11 (10, ndx, CName11); \
		Insert##CType12 (11, ndx, CName12); \
		Insert##CType13 (12, ndx, CName13); \
		Insert##CType14 (13, ndx, CName14); \
		Insert##CType15 (14, ndx, CName15); \
		Insert##CType16 (15, ndx, CName16); \
		Insert##CType17 (16, ndx, CName17); \
		Insert##CType18 (17, ndx, CName18); \
		Insert##CType19 (18, ndx, CName19); \
		Insert##CType20 (19, ndx, CName20); \
		Insert##CType21 (20, ndx, CName21); \
		Insert##CType22 (21, ndx, CName22); \
		Insert##CType23 (22, ndx, CName23); \
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
	ColumnProxy##CType3 CName3; \
	ColumnProxy##CType4 CName4; \
	ColumnProxy##CType5 CName5; \
	ColumnProxy##CType6 CName6; \
	ColumnProxy##CType7 CName7; \
	ColumnProxy##CType8 CName8; \
	ColumnProxy##CType9 CName9; \
	ColumnProxy##CType10 CName10; \
	ColumnProxy##CType11 CName11; \
	ColumnProxy##CType12 CName12; \
	ColumnProxy##CType13 CName13; \
	ColumnProxy##CType14 CName14; \
	ColumnProxy##CType15 CName15; \
	ColumnProxy##CType16 CName16; \
	ColumnProxy##CType17 CName17; \
	ColumnProxy##CType18 CName18; \
	ColumnProxy##CType19 CName19; \
	ColumnProxy##CType20 CName20; \
	ColumnProxy##CType21 CName21; \
	ColumnProxy##CType22 CName22; \
	ColumnProxy##CType23 CName23; \
\
protected: \
	friend class Group; \
	TableName(Allocator& alloc, size_t ref, Array* parent, size_t pndx) : TopLevelTable(alloc, ref, parent, pndx) {}; \
\
private: \
	TableName(const TableName&) {} \
	TableName& operator=(const TableName&) {return *this;} \
};



#define TDB_TABLE_24(TableName, CType1, CName1, CType2, CName2, CType3, CName3, CType4, CName4, CType5, CName5, CType6, CName6, CType7, CName7, CType8, CName8, CType9, CName9, CType10, CName10, CType11, CName11, CType12, CName12, CType13, CName13, CType14, CName14, CType15, CName15, CType16, CName16, CType17, CName17, CType18, CName18, CType19, CName19, CType20, CName20, CType21, CName21, CType22, CName22, CType23, CName23, CType24, CName24) \
class TableName##Query { \
protected: \
	QueryAccessor##CType1 CName1; \
	QueryAccessor##CType2 CName2; \
	QueryAccessor##CType3 CName3; \
	QueryAccessor##CType4 CName4; \
	QueryAccessor##CType5 CName5; \
	QueryAccessor##CType6 CName6; \
	QueryAccessor##CType7 CName7; \
	QueryAccessor##CType8 CName8; \
	QueryAccessor##CType9 CName9; \
	QueryAccessor##CType10 CName10; \
	QueryAccessor##CType11 CName11; \
	QueryAccessor##CType12 CName12; \
	QueryAccessor##CType13 CName13; \
	QueryAccessor##CType14 CName14; \
	QueryAccessor##CType15 CName15; \
	QueryAccessor##CType16 CName16; \
	QueryAccessor##CType17 CName17; \
	QueryAccessor##CType18 CName18; \
	QueryAccessor##CType19 CName19; \
	QueryAccessor##CType20 CName20; \
	QueryAccessor##CType21 CName21; \
	QueryAccessor##CType22 CName22; \
	QueryAccessor##CType23 CName23; \
	QueryAccessor##CType24 CName24; \
}; \
\
class TableName : public TopLevelTable { \
public: \
	TableName(Allocator& alloc=GetDefaultAllocator()) : TopLevelTable(alloc) { \
		RegisterColumn(Accessor##CType1::type, #CName1); \
		RegisterColumn(Accessor##CType2::type, #CName2); \
		RegisterColumn(Accessor##CType3::type, #CName3); \
		RegisterColumn(Accessor##CType4::type, #CName4); \
		RegisterColumn(Accessor##CType5::type, #CName5); \
		RegisterColumn(Accessor##CType6::type, #CName6); \
		RegisterColumn(Accessor##CType7::type, #CName7); \
		RegisterColumn(Accessor##CType8::type, #CName8); \
		RegisterColumn(Accessor##CType9::type, #CName9); \
		RegisterColumn(Accessor##CType10::type, #CName10); \
		RegisterColumn(Accessor##CType11::type, #CName11); \
		RegisterColumn(Accessor##CType12::type, #CName12); \
		RegisterColumn(Accessor##CType13::type, #CName13); \
		RegisterColumn(Accessor##CType14::type, #CName14); \
		RegisterColumn(Accessor##CType15::type, #CName15); \
		RegisterColumn(Accessor##CType16::type, #CName16); \
		RegisterColumn(Accessor##CType17::type, #CName17); \
		RegisterColumn(Accessor##CType18::type, #CName18); \
		RegisterColumn(Accessor##CType19::type, #CName19); \
		RegisterColumn(Accessor##CType20::type, #CName20); \
		RegisterColumn(Accessor##CType21::type, #CName21); \
		RegisterColumn(Accessor##CType22::type, #CName22); \
		RegisterColumn(Accessor##CType23::type, #CName23); \
		RegisterColumn(Accessor##CType24::type, #CName24); \
\
		CName1.Create(this, 0); \
		CName2.Create(this, 1); \
		CName3.Create(this, 2); \
		CName4.Create(this, 3); \
		CName5.Create(this, 4); \
		CName6.Create(this, 5); \
		CName7.Create(this, 6); \
		CName8.Create(this, 7); \
		CName9.Create(this, 8); \
		CName10.Create(this, 9); \
		CName11.Create(this, 10); \
		CName12.Create(this, 11); \
		CName13.Create(this, 12); \
		CName14.Create(this, 13); \
		CName15.Create(this, 14); \
		CName16.Create(this, 15); \
		CName17.Create(this, 16); \
		CName18.Create(this, 17); \
		CName19.Create(this, 18); \
		CName20.Create(this, 19); \
		CName21.Create(this, 20); \
		CName22.Create(this, 21); \
		CName23.Create(this, 22); \
		CName24.Create(this, 23); \
	}; \
\
	class TestQuery : public Query { \
	public: \
		TestQuery() : CName1(0), CName2(1), CName3(2), CName4(3), CName5(4), CName6(5), CName7(6), CName8(7), CName9(8), CName10(9), CName11(10), CName12(11), CName13(12), CName14(13), CName15(14), CName16(15), CName17(16), CName18(17), CName19(18), CName20(19), CName21(20), CName22(21), CName23(22), CName24(23) { \
			CName1.SetQuery(this); \
			CName2.SetQuery(this); \
			CName3.SetQuery(this); \
			CName4.SetQuery(this); \
			CName5.SetQuery(this); \
			CName6.SetQuery(this); \
			CName7.SetQuery(this); \
			CName8.SetQuery(this); \
			CName9.SetQuery(this); \
			CName10.SetQuery(this); \
			CName11.SetQuery(this); \
			CName12.SetQuery(this); \
			CName13.SetQuery(this); \
			CName14.SetQuery(this); \
			CName15.SetQuery(this); \
			CName16.SetQuery(this); \
			CName17.SetQuery(this); \
			CName18.SetQuery(this); \
			CName19.SetQuery(this); \
			CName20.SetQuery(this); \
			CName21.SetQuery(this); \
			CName22.SetQuery(this); \
			CName23.SetQuery(this); \
			CName24.SetQuery(this); \
		} \
\
		TestQuery(const TestQuery& copy) : Query(copy), CName1(0), CName2(1), CName3(2), CName4(3), CName5(4), CName6(5), CName7(6), CName8(7), CName9(8), CName10(9), CName11(10), CName12(11), CName13(12), CName14(13), CName15(14), CName16(15), CName17(16), CName18(17), CName19(18), CName20(19), CName21(20), CName22(21), CName23(22), CName24(23) { \
			CName1.SetQuery(this); \
			CName2.SetQuery(this); \
			CName3.SetQuery(this); \
			CName4.SetQuery(this); \
			CName5.SetQuery(this); \
			CName6.SetQuery(this); \
			CName7.SetQuery(this); \
			CName8.SetQuery(this); \
			CName9.SetQuery(this); \
			CName10.SetQuery(this); \
			CName11.SetQuery(this); \
			CName12.SetQuery(this); \
			CName13.SetQuery(this); \
			CName14.SetQuery(this); \
			CName15.SetQuery(this); \
			CName16.SetQuery(this); \
			CName17.SetQuery(this); \
			CName18.SetQuery(this); \
			CName19.SetQuery(this); \
			CName20.SetQuery(this); \
			CName21.SetQuery(this); \
			CName22.SetQuery(this); \
			CName23.SetQuery(this); \
			CName24.SetQuery(this); \
		} \
\
		class TestQueryQueryAccessorInt : private XQueryAccessorInt { \
		public: \
			TestQueryQueryAccessorInt(size_t column_id) : XQueryAccessorInt(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(int64_t value) {return (TestQuery &)XQueryAccessorInt::Equal(value);} \
			TestQuery& NotEqual(int64_t value) {return (TestQuery &)XQueryAccessorInt::NotEqual(value);} \
			TestQuery& Greater(int64_t value) {return (TestQuery &)XQueryAccessorInt::Greater(value);} \
			TestQuery& Less(int64_t value) {return (TestQuery &)XQueryAccessorInt::Less(value);} \
			TestQuery& Between(int64_t from, int64_t to) {return (TestQuery &)XQueryAccessorInt::Between(from, to);} \
		}; \
\
		template <class T> class TestQueryQueryAccessorEnum : public TestQueryQueryAccessorInt { \
		public: \
			TestQueryQueryAccessorEnum<T>(size_t column_id) : TestQueryQueryAccessorInt(column_id) {} \
		}; \
\
		class TestQueryQueryAccessorString : private XQueryAccessorString { \
		public: \
			TestQueryQueryAccessorString(size_t column_id) : XQueryAccessorString(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::Equal(value, CaseSensitive);} \
			TestQuery& NotEqual(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::NotEqual(value, CaseSensitive);} \
			TestQuery& BeginsWith(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::BeginsWith(value, CaseSensitive);} \
			TestQuery& EndsWith(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::EndsWith(value, CaseSensitive);} \
			TestQuery& Contains(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::Contains(value, CaseSensitive);} \
		}; \
\
		class TestQueryQueryAccessorBool : private XQueryAccessorBool { \
		public: \
			TestQueryQueryAccessorBool(size_t column_id) : XQueryAccessorBool(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(bool value) {return (TestQuery &)XQueryAccessorBool::Equal(value);} \
		}; \
\
		TestQueryQueryAccessor##CType1 CName1; \
		TestQueryQueryAccessor##CType2 CName2; \
		TestQueryQueryAccessor##CType3 CName3; \
		TestQueryQueryAccessor##CType4 CName4; \
		TestQueryQueryAccessor##CType5 CName5; \
		TestQueryQueryAccessor##CType6 CName6; \
		TestQueryQueryAccessor##CType7 CName7; \
		TestQueryQueryAccessor##CType8 CName8; \
		TestQueryQueryAccessor##CType9 CName9; \
		TestQueryQueryAccessor##CType10 CName10; \
		TestQueryQueryAccessor##CType11 CName11; \
		TestQueryQueryAccessor##CType12 CName12; \
		TestQueryQueryAccessor##CType13 CName13; \
		TestQueryQueryAccessor##CType14 CName14; \
		TestQueryQueryAccessor##CType15 CName15; \
		TestQueryQueryAccessor##CType16 CName16; \
		TestQueryQueryAccessor##CType17 CName17; \
		TestQueryQueryAccessor##CType18 CName18; \
		TestQueryQueryAccessor##CType19 CName19; \
		TestQueryQueryAccessor##CType20 CName20; \
		TestQueryQueryAccessor##CType21 CName21; \
		TestQueryQueryAccessor##CType22 CName22; \
		TestQueryQueryAccessor##CType23 CName23; \
		TestQueryQueryAccessor##CType24 CName24; \
\
		TestQuery& LeftParan(void) {Query::LeftParan(); return *this;}; \
		TestQuery& Or(void) {Query::Or(); return *this;}; \
		TestQuery& RightParan(void) {Query::RightParan(); return *this;}; \
		TestQuery& Subtable(size_t column) {Query::Subtable(column); return *this;}; \
		TestQuery& Parent() {Query::Parent(); return *this;}; \
	}; \
\
	TestQuery GetQuery() {return TestQuery();} \
\
	class Cursor : public CursorBase { \
	public: \
		Cursor(TableName& table, size_t ndx) : CursorBase(table, ndx) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
			CName17.Create(this, 16); \
			CName18.Create(this, 17); \
			CName19.Create(this, 18); \
			CName20.Create(this, 19); \
			CName21.Create(this, 20); \
			CName22.Create(this, 21); \
			CName23.Create(this, 22); \
			CName24.Create(this, 23); \
		} \
		Cursor(const TableName& table, size_t ndx) : CursorBase(const_cast<TableName&>(table), ndx) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
			CName17.Create(this, 16); \
			CName18.Create(this, 17); \
			CName19.Create(this, 18); \
			CName20.Create(this, 19); \
			CName21.Create(this, 20); \
			CName22.Create(this, 21); \
			CName23.Create(this, 22); \
			CName24.Create(this, 23); \
		} \
		Cursor(const Cursor& v) : CursorBase(v) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
			CName17.Create(this, 16); \
			CName18.Create(this, 17); \
			CName19.Create(this, 18); \
			CName20.Create(this, 19); \
			CName21.Create(this, 20); \
			CName22.Create(this, 21); \
			CName23.Create(this, 22); \
			CName24.Create(this, 23); \
		} \
		Accessor##CType1 CName1; \
		Accessor##CType2 CName2; \
		Accessor##CType3 CName3; \
		Accessor##CType4 CName4; \
		Accessor##CType5 CName5; \
		Accessor##CType6 CName6; \
		Accessor##CType7 CName7; \
		Accessor##CType8 CName8; \
		Accessor##CType9 CName9; \
		Accessor##CType10 CName10; \
		Accessor##CType11 CName11; \
		Accessor##CType12 CName12; \
		Accessor##CType13 CName13; \
		Accessor##CType14 CName14; \
		Accessor##CType15 CName15; \
		Accessor##CType16 CName16; \
		Accessor##CType17 CName17; \
		Accessor##CType18 CName18; \
		Accessor##CType19 CName19; \
		Accessor##CType20 CName20; \
		Accessor##CType21 CName21; \
		Accessor##CType22 CName22; \
		Accessor##CType23 CName23; \
		Accessor##CType24 CName24; \
	}; \
\
	void Add(tdbType##CType1 CName1, tdbType##CType2 CName2, tdbType##CType3 CName3, tdbType##CType4 CName4, tdbType##CType5 CName5, tdbType##CType6 CName6, tdbType##CType7 CName7, tdbType##CType8 CName8, tdbType##CType9 CName9, tdbType##CType10 CName10, tdbType##CType11 CName11, tdbType##CType12 CName12, tdbType##CType13 CName13, tdbType##CType14 CName14, tdbType##CType15 CName15, tdbType##CType16 CName16, tdbType##CType17 CName17, tdbType##CType18 CName18, tdbType##CType19 CName19, tdbType##CType20 CName20, tdbType##CType21 CName21, tdbType##CType22 CName22, tdbType##CType23 CName23, tdbType##CType24 CName24) { \
		const size_t ndx = GetSize(); \
		Insert##CType1 (0, ndx, CName1); \
		Insert##CType2 (1, ndx, CName2); \
		Insert##CType3 (2, ndx, CName3); \
		Insert##CType4 (3, ndx, CName4); \
		Insert##CType5 (4, ndx, CName5); \
		Insert##CType6 (5, ndx, CName6); \
		Insert##CType7 (6, ndx, CName7); \
		Insert##CType8 (7, ndx, CName8); \
		Insert##CType9 (8, ndx, CName9); \
		Insert##CType10 (9, ndx, CName10); \
		Insert##CType11 (10, ndx, CName11); \
		Insert##CType12 (11, ndx, CName12); \
		Insert##CType13 (12, ndx, CName13); \
		Insert##CType14 (13, ndx, CName14); \
		Insert##CType15 (14, ndx, CName15); \
		Insert##CType16 (15, ndx, CName16); \
		Insert##CType17 (16, ndx, CName17); \
		Insert##CType18 (17, ndx, CName18); \
		Insert##CType19 (18, ndx, CName19); \
		Insert##CType20 (19, ndx, CName20); \
		Insert##CType21 (20, ndx, CName21); \
		Insert##CType22 (21, ndx, CName22); \
		Insert##CType23 (22, ndx, CName23); \
		Insert##CType24 (23, ndx, CName24); \
		InsertDone(); \
	} \
\
	void Insert(size_t ndx, tdbType##CType1 CName1, tdbType##CType2 CName2, tdbType##CType3 CName3, tdbType##CType4 CName4, tdbType##CType5 CName5, tdbType##CType6 CName6, tdbType##CType7 CName7, tdbType##CType8 CName8, tdbType##CType9 CName9, tdbType##CType10 CName10, tdbType##CType11 CName11, tdbType##CType12 CName12, tdbType##CType13 CName13, tdbType##CType14 CName14, tdbType##CType15 CName15, tdbType##CType16 CName16, tdbType##CType17 CName17, tdbType##CType18 CName18, tdbType##CType19 CName19, tdbType##CType20 CName20, tdbType##CType21 CName21, tdbType##CType22 CName22, tdbType##CType23 CName23, tdbType##CType24 CName24) { \
		Insert##CType1 (0, ndx, CName1); \
		Insert##CType2 (1, ndx, CName2); \
		Insert##CType3 (2, ndx, CName3); \
		Insert##CType4 (3, ndx, CName4); \
		Insert##CType5 (4, ndx, CName5); \
		Insert##CType6 (5, ndx, CName6); \
		Insert##CType7 (6, ndx, CName7); \
		Insert##CType8 (7, ndx, CName8); \
		Insert##CType9 (8, ndx, CName9); \
		Insert##CType10 (9, ndx, CName10); \
		Insert##CType11 (10, ndx, CName11); \
		Insert##CType12 (11, ndx, CName12); \
		Insert##CType13 (12, ndx, CName13); \
		Insert##CType14 (13, ndx, CName14); \
		Insert##CType15 (14, ndx, CName15); \
		Insert##CType16 (15, ndx, CName16); \
		Insert##CType17 (16, ndx, CName17); \
		Insert##CType18 (17, ndx, CName18); \
		Insert##CType19 (18, ndx, CName19); \
		Insert##CType20 (19, ndx, CName20); \
		Insert##CType21 (20, ndx, CName21); \
		Insert##CType22 (21, ndx, CName22); \
		Insert##CType23 (22, ndx, CName23); \
		Insert##CType24 (23, ndx, CName24); \
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
	ColumnProxy##CType3 CName3; \
	ColumnProxy##CType4 CName4; \
	ColumnProxy##CType5 CName5; \
	ColumnProxy##CType6 CName6; \
	ColumnProxy##CType7 CName7; \
	ColumnProxy##CType8 CName8; \
	ColumnProxy##CType9 CName9; \
	ColumnProxy##CType10 CName10; \
	ColumnProxy##CType11 CName11; \
	ColumnProxy##CType12 CName12; \
	ColumnProxy##CType13 CName13; \
	ColumnProxy##CType14 CName14; \
	ColumnProxy##CType15 CName15; \
	ColumnProxy##CType16 CName16; \
	ColumnProxy##CType17 CName17; \
	ColumnProxy##CType18 CName18; \
	ColumnProxy##CType19 CName19; \
	ColumnProxy##CType20 CName20; \
	ColumnProxy##CType21 CName21; \
	ColumnProxy##CType22 CName22; \
	ColumnProxy##CType23 CName23; \
	ColumnProxy##CType24 CName24; \
\
protected: \
	friend class Group; \
	TableName(Allocator& alloc, size_t ref, Array* parent, size_t pndx) : TopLevelTable(alloc, ref, parent, pndx) {}; \
\
private: \
	TableName(const TableName&) {} \
	TableName& operator=(const TableName&) {return *this;} \
};



#define TDB_TABLE_25(TableName, CType1, CName1, CType2, CName2, CType3, CName3, CType4, CName4, CType5, CName5, CType6, CName6, CType7, CName7, CType8, CName8, CType9, CName9, CType10, CName10, CType11, CName11, CType12, CName12, CType13, CName13, CType14, CName14, CType15, CName15, CType16, CName16, CType17, CName17, CType18, CName18, CType19, CName19, CType20, CName20, CType21, CName21, CType22, CName22, CType23, CName23, CType24, CName24, CType25, CName25) \
class TableName##Query { \
protected: \
	QueryAccessor##CType1 CName1; \
	QueryAccessor##CType2 CName2; \
	QueryAccessor##CType3 CName3; \
	QueryAccessor##CType4 CName4; \
	QueryAccessor##CType5 CName5; \
	QueryAccessor##CType6 CName6; \
	QueryAccessor##CType7 CName7; \
	QueryAccessor##CType8 CName8; \
	QueryAccessor##CType9 CName9; \
	QueryAccessor##CType10 CName10; \
	QueryAccessor##CType11 CName11; \
	QueryAccessor##CType12 CName12; \
	QueryAccessor##CType13 CName13; \
	QueryAccessor##CType14 CName14; \
	QueryAccessor##CType15 CName15; \
	QueryAccessor##CType16 CName16; \
	QueryAccessor##CType17 CName17; \
	QueryAccessor##CType18 CName18; \
	QueryAccessor##CType19 CName19; \
	QueryAccessor##CType20 CName20; \
	QueryAccessor##CType21 CName21; \
	QueryAccessor##CType22 CName22; \
	QueryAccessor##CType23 CName23; \
	QueryAccessor##CType24 CName24; \
	QueryAccessor##CType25 CName25; \
}; \
\
class TableName : public TopLevelTable { \
public: \
	TableName(Allocator& alloc=GetDefaultAllocator()) : TopLevelTable(alloc) { \
		RegisterColumn(Accessor##CType1::type, #CName1); \
		RegisterColumn(Accessor##CType2::type, #CName2); \
		RegisterColumn(Accessor##CType3::type, #CName3); \
		RegisterColumn(Accessor##CType4::type, #CName4); \
		RegisterColumn(Accessor##CType5::type, #CName5); \
		RegisterColumn(Accessor##CType6::type, #CName6); \
		RegisterColumn(Accessor##CType7::type, #CName7); \
		RegisterColumn(Accessor##CType8::type, #CName8); \
		RegisterColumn(Accessor##CType9::type, #CName9); \
		RegisterColumn(Accessor##CType10::type, #CName10); \
		RegisterColumn(Accessor##CType11::type, #CName11); \
		RegisterColumn(Accessor##CType12::type, #CName12); \
		RegisterColumn(Accessor##CType13::type, #CName13); \
		RegisterColumn(Accessor##CType14::type, #CName14); \
		RegisterColumn(Accessor##CType15::type, #CName15); \
		RegisterColumn(Accessor##CType16::type, #CName16); \
		RegisterColumn(Accessor##CType17::type, #CName17); \
		RegisterColumn(Accessor##CType18::type, #CName18); \
		RegisterColumn(Accessor##CType19::type, #CName19); \
		RegisterColumn(Accessor##CType20::type, #CName20); \
		RegisterColumn(Accessor##CType21::type, #CName21); \
		RegisterColumn(Accessor##CType22::type, #CName22); \
		RegisterColumn(Accessor##CType23::type, #CName23); \
		RegisterColumn(Accessor##CType24::type, #CName24); \
		RegisterColumn(Accessor##CType25::type, #CName25); \
\
		CName1.Create(this, 0); \
		CName2.Create(this, 1); \
		CName3.Create(this, 2); \
		CName4.Create(this, 3); \
		CName5.Create(this, 4); \
		CName6.Create(this, 5); \
		CName7.Create(this, 6); \
		CName8.Create(this, 7); \
		CName9.Create(this, 8); \
		CName10.Create(this, 9); \
		CName11.Create(this, 10); \
		CName12.Create(this, 11); \
		CName13.Create(this, 12); \
		CName14.Create(this, 13); \
		CName15.Create(this, 14); \
		CName16.Create(this, 15); \
		CName17.Create(this, 16); \
		CName18.Create(this, 17); \
		CName19.Create(this, 18); \
		CName20.Create(this, 19); \
		CName21.Create(this, 20); \
		CName22.Create(this, 21); \
		CName23.Create(this, 22); \
		CName24.Create(this, 23); \
		CName25.Create(this, 24); \
	}; \
\
	class TestQuery : public Query { \
	public: \
		TestQuery() : CName1(0), CName2(1), CName3(2), CName4(3), CName5(4), CName6(5), CName7(6), CName8(7), CName9(8), CName10(9), CName11(10), CName12(11), CName13(12), CName14(13), CName15(14), CName16(15), CName17(16), CName18(17), CName19(18), CName20(19), CName21(20), CName22(21), CName23(22), CName24(23), CName25(24) { \
			CName1.SetQuery(this); \
			CName2.SetQuery(this); \
			CName3.SetQuery(this); \
			CName4.SetQuery(this); \
			CName5.SetQuery(this); \
			CName6.SetQuery(this); \
			CName7.SetQuery(this); \
			CName8.SetQuery(this); \
			CName9.SetQuery(this); \
			CName10.SetQuery(this); \
			CName11.SetQuery(this); \
			CName12.SetQuery(this); \
			CName13.SetQuery(this); \
			CName14.SetQuery(this); \
			CName15.SetQuery(this); \
			CName16.SetQuery(this); \
			CName17.SetQuery(this); \
			CName18.SetQuery(this); \
			CName19.SetQuery(this); \
			CName20.SetQuery(this); \
			CName21.SetQuery(this); \
			CName22.SetQuery(this); \
			CName23.SetQuery(this); \
			CName24.SetQuery(this); \
			CName25.SetQuery(this); \
		} \
\
		TestQuery(const TestQuery& copy) : Query(copy), CName1(0), CName2(1), CName3(2), CName4(3), CName5(4), CName6(5), CName7(6), CName8(7), CName9(8), CName10(9), CName11(10), CName12(11), CName13(12), CName14(13), CName15(14), CName16(15), CName17(16), CName18(17), CName19(18), CName20(19), CName21(20), CName22(21), CName23(22), CName24(23), CName25(24) { \
			CName1.SetQuery(this); \
			CName2.SetQuery(this); \
			CName3.SetQuery(this); \
			CName4.SetQuery(this); \
			CName5.SetQuery(this); \
			CName6.SetQuery(this); \
			CName7.SetQuery(this); \
			CName8.SetQuery(this); \
			CName9.SetQuery(this); \
			CName10.SetQuery(this); \
			CName11.SetQuery(this); \
			CName12.SetQuery(this); \
			CName13.SetQuery(this); \
			CName14.SetQuery(this); \
			CName15.SetQuery(this); \
			CName16.SetQuery(this); \
			CName17.SetQuery(this); \
			CName18.SetQuery(this); \
			CName19.SetQuery(this); \
			CName20.SetQuery(this); \
			CName21.SetQuery(this); \
			CName22.SetQuery(this); \
			CName23.SetQuery(this); \
			CName24.SetQuery(this); \
			CName25.SetQuery(this); \
		} \
\
		class TestQueryQueryAccessorInt : private XQueryAccessorInt { \
		public: \
			TestQueryQueryAccessorInt(size_t column_id) : XQueryAccessorInt(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(int64_t value) {return (TestQuery &)XQueryAccessorInt::Equal(value);} \
			TestQuery& NotEqual(int64_t value) {return (TestQuery &)XQueryAccessorInt::NotEqual(value);} \
			TestQuery& Greater(int64_t value) {return (TestQuery &)XQueryAccessorInt::Greater(value);} \
			TestQuery& Less(int64_t value) {return (TestQuery &)XQueryAccessorInt::Less(value);} \
			TestQuery& Between(int64_t from, int64_t to) {return (TestQuery &)XQueryAccessorInt::Between(from, to);} \
		}; \
\
		template <class T> class TestQueryQueryAccessorEnum : public TestQueryQueryAccessorInt { \
		public: \
			TestQueryQueryAccessorEnum<T>(size_t column_id) : TestQueryQueryAccessorInt(column_id) {} \
		}; \
\
		class TestQueryQueryAccessorString : private XQueryAccessorString { \
		public: \
			TestQueryQueryAccessorString(size_t column_id) : XQueryAccessorString(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::Equal(value, CaseSensitive);} \
			TestQuery& NotEqual(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::NotEqual(value, CaseSensitive);} \
			TestQuery& BeginsWith(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::BeginsWith(value, CaseSensitive);} \
			TestQuery& EndsWith(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::EndsWith(value, CaseSensitive);} \
			TestQuery& Contains(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::Contains(value, CaseSensitive);} \
		}; \
\
		class TestQueryQueryAccessorBool : private XQueryAccessorBool { \
		public: \
			TestQueryQueryAccessorBool(size_t column_id) : XQueryAccessorBool(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(bool value) {return (TestQuery &)XQueryAccessorBool::Equal(value);} \
		}; \
\
		TestQueryQueryAccessor##CType1 CName1; \
		TestQueryQueryAccessor##CType2 CName2; \
		TestQueryQueryAccessor##CType3 CName3; \
		TestQueryQueryAccessor##CType4 CName4; \
		TestQueryQueryAccessor##CType5 CName5; \
		TestQueryQueryAccessor##CType6 CName6; \
		TestQueryQueryAccessor##CType7 CName7; \
		TestQueryQueryAccessor##CType8 CName8; \
		TestQueryQueryAccessor##CType9 CName9; \
		TestQueryQueryAccessor##CType10 CName10; \
		TestQueryQueryAccessor##CType11 CName11; \
		TestQueryQueryAccessor##CType12 CName12; \
		TestQueryQueryAccessor##CType13 CName13; \
		TestQueryQueryAccessor##CType14 CName14; \
		TestQueryQueryAccessor##CType15 CName15; \
		TestQueryQueryAccessor##CType16 CName16; \
		TestQueryQueryAccessor##CType17 CName17; \
		TestQueryQueryAccessor##CType18 CName18; \
		TestQueryQueryAccessor##CType19 CName19; \
		TestQueryQueryAccessor##CType20 CName20; \
		TestQueryQueryAccessor##CType21 CName21; \
		TestQueryQueryAccessor##CType22 CName22; \
		TestQueryQueryAccessor##CType23 CName23; \
		TestQueryQueryAccessor##CType24 CName24; \
		TestQueryQueryAccessor##CType25 CName25; \
\
		TestQuery& LeftParan(void) {Query::LeftParan(); return *this;}; \
		TestQuery& Or(void) {Query::Or(); return *this;}; \
		TestQuery& RightParan(void) {Query::RightParan(); return *this;}; \
		TestQuery& Subtable(size_t column) {Query::Subtable(column); return *this;}; \
		TestQuery& Parent() {Query::Parent(); return *this;}; \
	}; \
\
	TestQuery GetQuery() {return TestQuery();} \
\
	class Cursor : public CursorBase { \
	public: \
		Cursor(TableName& table, size_t ndx) : CursorBase(table, ndx) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
			CName17.Create(this, 16); \
			CName18.Create(this, 17); \
			CName19.Create(this, 18); \
			CName20.Create(this, 19); \
			CName21.Create(this, 20); \
			CName22.Create(this, 21); \
			CName23.Create(this, 22); \
			CName24.Create(this, 23); \
			CName25.Create(this, 24); \
		} \
		Cursor(const TableName& table, size_t ndx) : CursorBase(const_cast<TableName&>(table), ndx) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
			CName17.Create(this, 16); \
			CName18.Create(this, 17); \
			CName19.Create(this, 18); \
			CName20.Create(this, 19); \
			CName21.Create(this, 20); \
			CName22.Create(this, 21); \
			CName23.Create(this, 22); \
			CName24.Create(this, 23); \
			CName25.Create(this, 24); \
		} \
		Cursor(const Cursor& v) : CursorBase(v) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
			CName17.Create(this, 16); \
			CName18.Create(this, 17); \
			CName19.Create(this, 18); \
			CName20.Create(this, 19); \
			CName21.Create(this, 20); \
			CName22.Create(this, 21); \
			CName23.Create(this, 22); \
			CName24.Create(this, 23); \
			CName25.Create(this, 24); \
		} \
		Accessor##CType1 CName1; \
		Accessor##CType2 CName2; \
		Accessor##CType3 CName3; \
		Accessor##CType4 CName4; \
		Accessor##CType5 CName5; \
		Accessor##CType6 CName6; \
		Accessor##CType7 CName7; \
		Accessor##CType8 CName8; \
		Accessor##CType9 CName9; \
		Accessor##CType10 CName10; \
		Accessor##CType11 CName11; \
		Accessor##CType12 CName12; \
		Accessor##CType13 CName13; \
		Accessor##CType14 CName14; \
		Accessor##CType15 CName15; \
		Accessor##CType16 CName16; \
		Accessor##CType17 CName17; \
		Accessor##CType18 CName18; \
		Accessor##CType19 CName19; \
		Accessor##CType20 CName20; \
		Accessor##CType21 CName21; \
		Accessor##CType22 CName22; \
		Accessor##CType23 CName23; \
		Accessor##CType24 CName24; \
		Accessor##CType25 CName25; \
	}; \
\
	void Add(tdbType##CType1 CName1, tdbType##CType2 CName2, tdbType##CType3 CName3, tdbType##CType4 CName4, tdbType##CType5 CName5, tdbType##CType6 CName6, tdbType##CType7 CName7, tdbType##CType8 CName8, tdbType##CType9 CName9, tdbType##CType10 CName10, tdbType##CType11 CName11, tdbType##CType12 CName12, tdbType##CType13 CName13, tdbType##CType14 CName14, tdbType##CType15 CName15, tdbType##CType16 CName16, tdbType##CType17 CName17, tdbType##CType18 CName18, tdbType##CType19 CName19, tdbType##CType20 CName20, tdbType##CType21 CName21, tdbType##CType22 CName22, tdbType##CType23 CName23, tdbType##CType24 CName24, tdbType##CType25 CName25) { \
		const size_t ndx = GetSize(); \
		Insert##CType1 (0, ndx, CName1); \
		Insert##CType2 (1, ndx, CName2); \
		Insert##CType3 (2, ndx, CName3); \
		Insert##CType4 (3, ndx, CName4); \
		Insert##CType5 (4, ndx, CName5); \
		Insert##CType6 (5, ndx, CName6); \
		Insert##CType7 (6, ndx, CName7); \
		Insert##CType8 (7, ndx, CName8); \
		Insert##CType9 (8, ndx, CName9); \
		Insert##CType10 (9, ndx, CName10); \
		Insert##CType11 (10, ndx, CName11); \
		Insert##CType12 (11, ndx, CName12); \
		Insert##CType13 (12, ndx, CName13); \
		Insert##CType14 (13, ndx, CName14); \
		Insert##CType15 (14, ndx, CName15); \
		Insert##CType16 (15, ndx, CName16); \
		Insert##CType17 (16, ndx, CName17); \
		Insert##CType18 (17, ndx, CName18); \
		Insert##CType19 (18, ndx, CName19); \
		Insert##CType20 (19, ndx, CName20); \
		Insert##CType21 (20, ndx, CName21); \
		Insert##CType22 (21, ndx, CName22); \
		Insert##CType23 (22, ndx, CName23); \
		Insert##CType24 (23, ndx, CName24); \
		Insert##CType25 (24, ndx, CName25); \
		InsertDone(); \
	} \
\
	void Insert(size_t ndx, tdbType##CType1 CName1, tdbType##CType2 CName2, tdbType##CType3 CName3, tdbType##CType4 CName4, tdbType##CType5 CName5, tdbType##CType6 CName6, tdbType##CType7 CName7, tdbType##CType8 CName8, tdbType##CType9 CName9, tdbType##CType10 CName10, tdbType##CType11 CName11, tdbType##CType12 CName12, tdbType##CType13 CName13, tdbType##CType14 CName14, tdbType##CType15 CName15, tdbType##CType16 CName16, tdbType##CType17 CName17, tdbType##CType18 CName18, tdbType##CType19 CName19, tdbType##CType20 CName20, tdbType##CType21 CName21, tdbType##CType22 CName22, tdbType##CType23 CName23, tdbType##CType24 CName24, tdbType##CType25 CName25) { \
		Insert##CType1 (0, ndx, CName1); \
		Insert##CType2 (1, ndx, CName2); \
		Insert##CType3 (2, ndx, CName3); \
		Insert##CType4 (3, ndx, CName4); \
		Insert##CType5 (4, ndx, CName5); \
		Insert##CType6 (5, ndx, CName6); \
		Insert##CType7 (6, ndx, CName7); \
		Insert##CType8 (7, ndx, CName8); \
		Insert##CType9 (8, ndx, CName9); \
		Insert##CType10 (9, ndx, CName10); \
		Insert##CType11 (10, ndx, CName11); \
		Insert##CType12 (11, ndx, CName12); \
		Insert##CType13 (12, ndx, CName13); \
		Insert##CType14 (13, ndx, CName14); \
		Insert##CType15 (14, ndx, CName15); \
		Insert##CType16 (15, ndx, CName16); \
		Insert##CType17 (16, ndx, CName17); \
		Insert##CType18 (17, ndx, CName18); \
		Insert##CType19 (18, ndx, CName19); \
		Insert##CType20 (19, ndx, CName20); \
		Insert##CType21 (20, ndx, CName21); \
		Insert##CType22 (21, ndx, CName22); \
		Insert##CType23 (22, ndx, CName23); \
		Insert##CType24 (23, ndx, CName24); \
		Insert##CType25 (24, ndx, CName25); \
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
	ColumnProxy##CType3 CName3; \
	ColumnProxy##CType4 CName4; \
	ColumnProxy##CType5 CName5; \
	ColumnProxy##CType6 CName6; \
	ColumnProxy##CType7 CName7; \
	ColumnProxy##CType8 CName8; \
	ColumnProxy##CType9 CName9; \
	ColumnProxy##CType10 CName10; \
	ColumnProxy##CType11 CName11; \
	ColumnProxy##CType12 CName12; \
	ColumnProxy##CType13 CName13; \
	ColumnProxy##CType14 CName14; \
	ColumnProxy##CType15 CName15; \
	ColumnProxy##CType16 CName16; \
	ColumnProxy##CType17 CName17; \
	ColumnProxy##CType18 CName18; \
	ColumnProxy##CType19 CName19; \
	ColumnProxy##CType20 CName20; \
	ColumnProxy##CType21 CName21; \
	ColumnProxy##CType22 CName22; \
	ColumnProxy##CType23 CName23; \
	ColumnProxy##CType24 CName24; \
	ColumnProxy##CType25 CName25; \
\
protected: \
	friend class Group; \
	TableName(Allocator& alloc, size_t ref, Array* parent, size_t pndx) : TopLevelTable(alloc, ref, parent, pndx) {}; \
\
private: \
	TableName(const TableName&) {} \
	TableName& operator=(const TableName&) {return *this;} \
};



#define TDB_TABLE_26(TableName, CType1, CName1, CType2, CName2, CType3, CName3, CType4, CName4, CType5, CName5, CType6, CName6, CType7, CName7, CType8, CName8, CType9, CName9, CType10, CName10, CType11, CName11, CType12, CName12, CType13, CName13, CType14, CName14, CType15, CName15, CType16, CName16, CType17, CName17, CType18, CName18, CType19, CName19, CType20, CName20, CType21, CName21, CType22, CName22, CType23, CName23, CType24, CName24, CType25, CName25, CType26, CName26) \
class TableName##Query { \
protected: \
	QueryAccessor##CType1 CName1; \
	QueryAccessor##CType2 CName2; \
	QueryAccessor##CType3 CName3; \
	QueryAccessor##CType4 CName4; \
	QueryAccessor##CType5 CName5; \
	QueryAccessor##CType6 CName6; \
	QueryAccessor##CType7 CName7; \
	QueryAccessor##CType8 CName8; \
	QueryAccessor##CType9 CName9; \
	QueryAccessor##CType10 CName10; \
	QueryAccessor##CType11 CName11; \
	QueryAccessor##CType12 CName12; \
	QueryAccessor##CType13 CName13; \
	QueryAccessor##CType14 CName14; \
	QueryAccessor##CType15 CName15; \
	QueryAccessor##CType16 CName16; \
	QueryAccessor##CType17 CName17; \
	QueryAccessor##CType18 CName18; \
	QueryAccessor##CType19 CName19; \
	QueryAccessor##CType20 CName20; \
	QueryAccessor##CType21 CName21; \
	QueryAccessor##CType22 CName22; \
	QueryAccessor##CType23 CName23; \
	QueryAccessor##CType24 CName24; \
	QueryAccessor##CType25 CName25; \
	QueryAccessor##CType26 CName26; \
}; \
\
class TableName : public TopLevelTable { \
public: \
	TableName(Allocator& alloc=GetDefaultAllocator()) : TopLevelTable(alloc) { \
		RegisterColumn(Accessor##CType1::type, #CName1); \
		RegisterColumn(Accessor##CType2::type, #CName2); \
		RegisterColumn(Accessor##CType3::type, #CName3); \
		RegisterColumn(Accessor##CType4::type, #CName4); \
		RegisterColumn(Accessor##CType5::type, #CName5); \
		RegisterColumn(Accessor##CType6::type, #CName6); \
		RegisterColumn(Accessor##CType7::type, #CName7); \
		RegisterColumn(Accessor##CType8::type, #CName8); \
		RegisterColumn(Accessor##CType9::type, #CName9); \
		RegisterColumn(Accessor##CType10::type, #CName10); \
		RegisterColumn(Accessor##CType11::type, #CName11); \
		RegisterColumn(Accessor##CType12::type, #CName12); \
		RegisterColumn(Accessor##CType13::type, #CName13); \
		RegisterColumn(Accessor##CType14::type, #CName14); \
		RegisterColumn(Accessor##CType15::type, #CName15); \
		RegisterColumn(Accessor##CType16::type, #CName16); \
		RegisterColumn(Accessor##CType17::type, #CName17); \
		RegisterColumn(Accessor##CType18::type, #CName18); \
		RegisterColumn(Accessor##CType19::type, #CName19); \
		RegisterColumn(Accessor##CType20::type, #CName20); \
		RegisterColumn(Accessor##CType21::type, #CName21); \
		RegisterColumn(Accessor##CType22::type, #CName22); \
		RegisterColumn(Accessor##CType23::type, #CName23); \
		RegisterColumn(Accessor##CType24::type, #CName24); \
		RegisterColumn(Accessor##CType25::type, #CName25); \
		RegisterColumn(Accessor##CType26::type, #CName26); \
\
		CName1.Create(this, 0); \
		CName2.Create(this, 1); \
		CName3.Create(this, 2); \
		CName4.Create(this, 3); \
		CName5.Create(this, 4); \
		CName6.Create(this, 5); \
		CName7.Create(this, 6); \
		CName8.Create(this, 7); \
		CName9.Create(this, 8); \
		CName10.Create(this, 9); \
		CName11.Create(this, 10); \
		CName12.Create(this, 11); \
		CName13.Create(this, 12); \
		CName14.Create(this, 13); \
		CName15.Create(this, 14); \
		CName16.Create(this, 15); \
		CName17.Create(this, 16); \
		CName18.Create(this, 17); \
		CName19.Create(this, 18); \
		CName20.Create(this, 19); \
		CName21.Create(this, 20); \
		CName22.Create(this, 21); \
		CName23.Create(this, 22); \
		CName24.Create(this, 23); \
		CName25.Create(this, 24); \
		CName26.Create(this, 25); \
	}; \
\
	class TestQuery : public Query { \
	public: \
		TestQuery() : CName1(0), CName2(1), CName3(2), CName4(3), CName5(4), CName6(5), CName7(6), CName8(7), CName9(8), CName10(9), CName11(10), CName12(11), CName13(12), CName14(13), CName15(14), CName16(15), CName17(16), CName18(17), CName19(18), CName20(19), CName21(20), CName22(21), CName23(22), CName24(23), CName25(24), CName26(25) { \
			CName1.SetQuery(this); \
			CName2.SetQuery(this); \
			CName3.SetQuery(this); \
			CName4.SetQuery(this); \
			CName5.SetQuery(this); \
			CName6.SetQuery(this); \
			CName7.SetQuery(this); \
			CName8.SetQuery(this); \
			CName9.SetQuery(this); \
			CName10.SetQuery(this); \
			CName11.SetQuery(this); \
			CName12.SetQuery(this); \
			CName13.SetQuery(this); \
			CName14.SetQuery(this); \
			CName15.SetQuery(this); \
			CName16.SetQuery(this); \
			CName17.SetQuery(this); \
			CName18.SetQuery(this); \
			CName19.SetQuery(this); \
			CName20.SetQuery(this); \
			CName21.SetQuery(this); \
			CName22.SetQuery(this); \
			CName23.SetQuery(this); \
			CName24.SetQuery(this); \
			CName25.SetQuery(this); \
			CName26.SetQuery(this); \
		} \
\
		TestQuery(const TestQuery& copy) : Query(copy), CName1(0), CName2(1), CName3(2), CName4(3), CName5(4), CName6(5), CName7(6), CName8(7), CName9(8), CName10(9), CName11(10), CName12(11), CName13(12), CName14(13), CName15(14), CName16(15), CName17(16), CName18(17), CName19(18), CName20(19), CName21(20), CName22(21), CName23(22), CName24(23), CName25(24), CName26(25) { \
			CName1.SetQuery(this); \
			CName2.SetQuery(this); \
			CName3.SetQuery(this); \
			CName4.SetQuery(this); \
			CName5.SetQuery(this); \
			CName6.SetQuery(this); \
			CName7.SetQuery(this); \
			CName8.SetQuery(this); \
			CName9.SetQuery(this); \
			CName10.SetQuery(this); \
			CName11.SetQuery(this); \
			CName12.SetQuery(this); \
			CName13.SetQuery(this); \
			CName14.SetQuery(this); \
			CName15.SetQuery(this); \
			CName16.SetQuery(this); \
			CName17.SetQuery(this); \
			CName18.SetQuery(this); \
			CName19.SetQuery(this); \
			CName20.SetQuery(this); \
			CName21.SetQuery(this); \
			CName22.SetQuery(this); \
			CName23.SetQuery(this); \
			CName24.SetQuery(this); \
			CName25.SetQuery(this); \
			CName26.SetQuery(this); \
		} \
\
		class TestQueryQueryAccessorInt : private XQueryAccessorInt { \
		public: \
			TestQueryQueryAccessorInt(size_t column_id) : XQueryAccessorInt(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(int64_t value) {return (TestQuery &)XQueryAccessorInt::Equal(value);} \
			TestQuery& NotEqual(int64_t value) {return (TestQuery &)XQueryAccessorInt::NotEqual(value);} \
			TestQuery& Greater(int64_t value) {return (TestQuery &)XQueryAccessorInt::Greater(value);} \
			TestQuery& Less(int64_t value) {return (TestQuery &)XQueryAccessorInt::Less(value);} \
			TestQuery& Between(int64_t from, int64_t to) {return (TestQuery &)XQueryAccessorInt::Between(from, to);} \
		}; \
\
		template <class T> class TestQueryQueryAccessorEnum : public TestQueryQueryAccessorInt { \
		public: \
			TestQueryQueryAccessorEnum<T>(size_t column_id) : TestQueryQueryAccessorInt(column_id) {} \
		}; \
\
		class TestQueryQueryAccessorString : private XQueryAccessorString { \
		public: \
			TestQueryQueryAccessorString(size_t column_id) : XQueryAccessorString(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::Equal(value, CaseSensitive);} \
			TestQuery& NotEqual(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::NotEqual(value, CaseSensitive);} \
			TestQuery& BeginsWith(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::BeginsWith(value, CaseSensitive);} \
			TestQuery& EndsWith(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::EndsWith(value, CaseSensitive);} \
			TestQuery& Contains(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::Contains(value, CaseSensitive);} \
		}; \
\
		class TestQueryQueryAccessorBool : private XQueryAccessorBool { \
		public: \
			TestQueryQueryAccessorBool(size_t column_id) : XQueryAccessorBool(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(bool value) {return (TestQuery &)XQueryAccessorBool::Equal(value);} \
		}; \
\
		TestQueryQueryAccessor##CType1 CName1; \
		TestQueryQueryAccessor##CType2 CName2; \
		TestQueryQueryAccessor##CType3 CName3; \
		TestQueryQueryAccessor##CType4 CName4; \
		TestQueryQueryAccessor##CType5 CName5; \
		TestQueryQueryAccessor##CType6 CName6; \
		TestQueryQueryAccessor##CType7 CName7; \
		TestQueryQueryAccessor##CType8 CName8; \
		TestQueryQueryAccessor##CType9 CName9; \
		TestQueryQueryAccessor##CType10 CName10; \
		TestQueryQueryAccessor##CType11 CName11; \
		TestQueryQueryAccessor##CType12 CName12; \
		TestQueryQueryAccessor##CType13 CName13; \
		TestQueryQueryAccessor##CType14 CName14; \
		TestQueryQueryAccessor##CType15 CName15; \
		TestQueryQueryAccessor##CType16 CName16; \
		TestQueryQueryAccessor##CType17 CName17; \
		TestQueryQueryAccessor##CType18 CName18; \
		TestQueryQueryAccessor##CType19 CName19; \
		TestQueryQueryAccessor##CType20 CName20; \
		TestQueryQueryAccessor##CType21 CName21; \
		TestQueryQueryAccessor##CType22 CName22; \
		TestQueryQueryAccessor##CType23 CName23; \
		TestQueryQueryAccessor##CType24 CName24; \
		TestQueryQueryAccessor##CType25 CName25; \
		TestQueryQueryAccessor##CType26 CName26; \
\
		TestQuery& LeftParan(void) {Query::LeftParan(); return *this;}; \
		TestQuery& Or(void) {Query::Or(); return *this;}; \
		TestQuery& RightParan(void) {Query::RightParan(); return *this;}; \
		TestQuery& Subtable(size_t column) {Query::Subtable(column); return *this;}; \
		TestQuery& Parent() {Query::Parent(); return *this;}; \
	}; \
\
	TestQuery GetQuery() {return TestQuery();} \
\
	class Cursor : public CursorBase { \
	public: \
		Cursor(TableName& table, size_t ndx) : CursorBase(table, ndx) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
			CName17.Create(this, 16); \
			CName18.Create(this, 17); \
			CName19.Create(this, 18); \
			CName20.Create(this, 19); \
			CName21.Create(this, 20); \
			CName22.Create(this, 21); \
			CName23.Create(this, 22); \
			CName24.Create(this, 23); \
			CName25.Create(this, 24); \
			CName26.Create(this, 25); \
		} \
		Cursor(const TableName& table, size_t ndx) : CursorBase(const_cast<TableName&>(table), ndx) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
			CName17.Create(this, 16); \
			CName18.Create(this, 17); \
			CName19.Create(this, 18); \
			CName20.Create(this, 19); \
			CName21.Create(this, 20); \
			CName22.Create(this, 21); \
			CName23.Create(this, 22); \
			CName24.Create(this, 23); \
			CName25.Create(this, 24); \
			CName26.Create(this, 25); \
		} \
		Cursor(const Cursor& v) : CursorBase(v) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
			CName17.Create(this, 16); \
			CName18.Create(this, 17); \
			CName19.Create(this, 18); \
			CName20.Create(this, 19); \
			CName21.Create(this, 20); \
			CName22.Create(this, 21); \
			CName23.Create(this, 22); \
			CName24.Create(this, 23); \
			CName25.Create(this, 24); \
			CName26.Create(this, 25); \
		} \
		Accessor##CType1 CName1; \
		Accessor##CType2 CName2; \
		Accessor##CType3 CName3; \
		Accessor##CType4 CName4; \
		Accessor##CType5 CName5; \
		Accessor##CType6 CName6; \
		Accessor##CType7 CName7; \
		Accessor##CType8 CName8; \
		Accessor##CType9 CName9; \
		Accessor##CType10 CName10; \
		Accessor##CType11 CName11; \
		Accessor##CType12 CName12; \
		Accessor##CType13 CName13; \
		Accessor##CType14 CName14; \
		Accessor##CType15 CName15; \
		Accessor##CType16 CName16; \
		Accessor##CType17 CName17; \
		Accessor##CType18 CName18; \
		Accessor##CType19 CName19; \
		Accessor##CType20 CName20; \
		Accessor##CType21 CName21; \
		Accessor##CType22 CName22; \
		Accessor##CType23 CName23; \
		Accessor##CType24 CName24; \
		Accessor##CType25 CName25; \
		Accessor##CType26 CName26; \
	}; \
\
	void Add(tdbType##CType1 CName1, tdbType##CType2 CName2, tdbType##CType3 CName3, tdbType##CType4 CName4, tdbType##CType5 CName5, tdbType##CType6 CName6, tdbType##CType7 CName7, tdbType##CType8 CName8, tdbType##CType9 CName9, tdbType##CType10 CName10, tdbType##CType11 CName11, tdbType##CType12 CName12, tdbType##CType13 CName13, tdbType##CType14 CName14, tdbType##CType15 CName15, tdbType##CType16 CName16, tdbType##CType17 CName17, tdbType##CType18 CName18, tdbType##CType19 CName19, tdbType##CType20 CName20, tdbType##CType21 CName21, tdbType##CType22 CName22, tdbType##CType23 CName23, tdbType##CType24 CName24, tdbType##CType25 CName25, tdbType##CType26 CName26) { \
		const size_t ndx = GetSize(); \
		Insert##CType1 (0, ndx, CName1); \
		Insert##CType2 (1, ndx, CName2); \
		Insert##CType3 (2, ndx, CName3); \
		Insert##CType4 (3, ndx, CName4); \
		Insert##CType5 (4, ndx, CName5); \
		Insert##CType6 (5, ndx, CName6); \
		Insert##CType7 (6, ndx, CName7); \
		Insert##CType8 (7, ndx, CName8); \
		Insert##CType9 (8, ndx, CName9); \
		Insert##CType10 (9, ndx, CName10); \
		Insert##CType11 (10, ndx, CName11); \
		Insert##CType12 (11, ndx, CName12); \
		Insert##CType13 (12, ndx, CName13); \
		Insert##CType14 (13, ndx, CName14); \
		Insert##CType15 (14, ndx, CName15); \
		Insert##CType16 (15, ndx, CName16); \
		Insert##CType17 (16, ndx, CName17); \
		Insert##CType18 (17, ndx, CName18); \
		Insert##CType19 (18, ndx, CName19); \
		Insert##CType20 (19, ndx, CName20); \
		Insert##CType21 (20, ndx, CName21); \
		Insert##CType22 (21, ndx, CName22); \
		Insert##CType23 (22, ndx, CName23); \
		Insert##CType24 (23, ndx, CName24); \
		Insert##CType25 (24, ndx, CName25); \
		Insert##CType26 (25, ndx, CName26); \
		InsertDone(); \
	} \
\
	void Insert(size_t ndx, tdbType##CType1 CName1, tdbType##CType2 CName2, tdbType##CType3 CName3, tdbType##CType4 CName4, tdbType##CType5 CName5, tdbType##CType6 CName6, tdbType##CType7 CName7, tdbType##CType8 CName8, tdbType##CType9 CName9, tdbType##CType10 CName10, tdbType##CType11 CName11, tdbType##CType12 CName12, tdbType##CType13 CName13, tdbType##CType14 CName14, tdbType##CType15 CName15, tdbType##CType16 CName16, tdbType##CType17 CName17, tdbType##CType18 CName18, tdbType##CType19 CName19, tdbType##CType20 CName20, tdbType##CType21 CName21, tdbType##CType22 CName22, tdbType##CType23 CName23, tdbType##CType24 CName24, tdbType##CType25 CName25, tdbType##CType26 CName26) { \
		Insert##CType1 (0, ndx, CName1); \
		Insert##CType2 (1, ndx, CName2); \
		Insert##CType3 (2, ndx, CName3); \
		Insert##CType4 (3, ndx, CName4); \
		Insert##CType5 (4, ndx, CName5); \
		Insert##CType6 (5, ndx, CName6); \
		Insert##CType7 (6, ndx, CName7); \
		Insert##CType8 (7, ndx, CName8); \
		Insert##CType9 (8, ndx, CName9); \
		Insert##CType10 (9, ndx, CName10); \
		Insert##CType11 (10, ndx, CName11); \
		Insert##CType12 (11, ndx, CName12); \
		Insert##CType13 (12, ndx, CName13); \
		Insert##CType14 (13, ndx, CName14); \
		Insert##CType15 (14, ndx, CName15); \
		Insert##CType16 (15, ndx, CName16); \
		Insert##CType17 (16, ndx, CName17); \
		Insert##CType18 (17, ndx, CName18); \
		Insert##CType19 (18, ndx, CName19); \
		Insert##CType20 (19, ndx, CName20); \
		Insert##CType21 (20, ndx, CName21); \
		Insert##CType22 (21, ndx, CName22); \
		Insert##CType23 (22, ndx, CName23); \
		Insert##CType24 (23, ndx, CName24); \
		Insert##CType25 (24, ndx, CName25); \
		Insert##CType26 (25, ndx, CName26); \
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
	ColumnProxy##CType3 CName3; \
	ColumnProxy##CType4 CName4; \
	ColumnProxy##CType5 CName5; \
	ColumnProxy##CType6 CName6; \
	ColumnProxy##CType7 CName7; \
	ColumnProxy##CType8 CName8; \
	ColumnProxy##CType9 CName9; \
	ColumnProxy##CType10 CName10; \
	ColumnProxy##CType11 CName11; \
	ColumnProxy##CType12 CName12; \
	ColumnProxy##CType13 CName13; \
	ColumnProxy##CType14 CName14; \
	ColumnProxy##CType15 CName15; \
	ColumnProxy##CType16 CName16; \
	ColumnProxy##CType17 CName17; \
	ColumnProxy##CType18 CName18; \
	ColumnProxy##CType19 CName19; \
	ColumnProxy##CType20 CName20; \
	ColumnProxy##CType21 CName21; \
	ColumnProxy##CType22 CName22; \
	ColumnProxy##CType23 CName23; \
	ColumnProxy##CType24 CName24; \
	ColumnProxy##CType25 CName25; \
	ColumnProxy##CType26 CName26; \
\
protected: \
	friend class Group; \
	TableName(Allocator& alloc, size_t ref, Array* parent, size_t pndx) : TopLevelTable(alloc, ref, parent, pndx) {}; \
\
private: \
	TableName(const TableName&) {} \
	TableName& operator=(const TableName&) {return *this;} \
};



#define TDB_TABLE_27(TableName, CType1, CName1, CType2, CName2, CType3, CName3, CType4, CName4, CType5, CName5, CType6, CName6, CType7, CName7, CType8, CName8, CType9, CName9, CType10, CName10, CType11, CName11, CType12, CName12, CType13, CName13, CType14, CName14, CType15, CName15, CType16, CName16, CType17, CName17, CType18, CName18, CType19, CName19, CType20, CName20, CType21, CName21, CType22, CName22, CType23, CName23, CType24, CName24, CType25, CName25, CType26, CName26, CType27, CName27) \
class TableName##Query { \
protected: \
	QueryAccessor##CType1 CName1; \
	QueryAccessor##CType2 CName2; \
	QueryAccessor##CType3 CName3; \
	QueryAccessor##CType4 CName4; \
	QueryAccessor##CType5 CName5; \
	QueryAccessor##CType6 CName6; \
	QueryAccessor##CType7 CName7; \
	QueryAccessor##CType8 CName8; \
	QueryAccessor##CType9 CName9; \
	QueryAccessor##CType10 CName10; \
	QueryAccessor##CType11 CName11; \
	QueryAccessor##CType12 CName12; \
	QueryAccessor##CType13 CName13; \
	QueryAccessor##CType14 CName14; \
	QueryAccessor##CType15 CName15; \
	QueryAccessor##CType16 CName16; \
	QueryAccessor##CType17 CName17; \
	QueryAccessor##CType18 CName18; \
	QueryAccessor##CType19 CName19; \
	QueryAccessor##CType20 CName20; \
	QueryAccessor##CType21 CName21; \
	QueryAccessor##CType22 CName22; \
	QueryAccessor##CType23 CName23; \
	QueryAccessor##CType24 CName24; \
	QueryAccessor##CType25 CName25; \
	QueryAccessor##CType26 CName26; \
	QueryAccessor##CType27 CName27; \
}; \
\
class TableName : public TopLevelTable { \
public: \
	TableName(Allocator& alloc=GetDefaultAllocator()) : TopLevelTable(alloc) { \
		RegisterColumn(Accessor##CType1::type, #CName1); \
		RegisterColumn(Accessor##CType2::type, #CName2); \
		RegisterColumn(Accessor##CType3::type, #CName3); \
		RegisterColumn(Accessor##CType4::type, #CName4); \
		RegisterColumn(Accessor##CType5::type, #CName5); \
		RegisterColumn(Accessor##CType6::type, #CName6); \
		RegisterColumn(Accessor##CType7::type, #CName7); \
		RegisterColumn(Accessor##CType8::type, #CName8); \
		RegisterColumn(Accessor##CType9::type, #CName9); \
		RegisterColumn(Accessor##CType10::type, #CName10); \
		RegisterColumn(Accessor##CType11::type, #CName11); \
		RegisterColumn(Accessor##CType12::type, #CName12); \
		RegisterColumn(Accessor##CType13::type, #CName13); \
		RegisterColumn(Accessor##CType14::type, #CName14); \
		RegisterColumn(Accessor##CType15::type, #CName15); \
		RegisterColumn(Accessor##CType16::type, #CName16); \
		RegisterColumn(Accessor##CType17::type, #CName17); \
		RegisterColumn(Accessor##CType18::type, #CName18); \
		RegisterColumn(Accessor##CType19::type, #CName19); \
		RegisterColumn(Accessor##CType20::type, #CName20); \
		RegisterColumn(Accessor##CType21::type, #CName21); \
		RegisterColumn(Accessor##CType22::type, #CName22); \
		RegisterColumn(Accessor##CType23::type, #CName23); \
		RegisterColumn(Accessor##CType24::type, #CName24); \
		RegisterColumn(Accessor##CType25::type, #CName25); \
		RegisterColumn(Accessor##CType26::type, #CName26); \
		RegisterColumn(Accessor##CType27::type, #CName27); \
\
		CName1.Create(this, 0); \
		CName2.Create(this, 1); \
		CName3.Create(this, 2); \
		CName4.Create(this, 3); \
		CName5.Create(this, 4); \
		CName6.Create(this, 5); \
		CName7.Create(this, 6); \
		CName8.Create(this, 7); \
		CName9.Create(this, 8); \
		CName10.Create(this, 9); \
		CName11.Create(this, 10); \
		CName12.Create(this, 11); \
		CName13.Create(this, 12); \
		CName14.Create(this, 13); \
		CName15.Create(this, 14); \
		CName16.Create(this, 15); \
		CName17.Create(this, 16); \
		CName18.Create(this, 17); \
		CName19.Create(this, 18); \
		CName20.Create(this, 19); \
		CName21.Create(this, 20); \
		CName22.Create(this, 21); \
		CName23.Create(this, 22); \
		CName24.Create(this, 23); \
		CName25.Create(this, 24); \
		CName26.Create(this, 25); \
		CName27.Create(this, 26); \
	}; \
\
	class TestQuery : public Query { \
	public: \
		TestQuery() : CName1(0), CName2(1), CName3(2), CName4(3), CName5(4), CName6(5), CName7(6), CName8(7), CName9(8), CName10(9), CName11(10), CName12(11), CName13(12), CName14(13), CName15(14), CName16(15), CName17(16), CName18(17), CName19(18), CName20(19), CName21(20), CName22(21), CName23(22), CName24(23), CName25(24), CName26(25), CName27(26) { \
			CName1.SetQuery(this); \
			CName2.SetQuery(this); \
			CName3.SetQuery(this); \
			CName4.SetQuery(this); \
			CName5.SetQuery(this); \
			CName6.SetQuery(this); \
			CName7.SetQuery(this); \
			CName8.SetQuery(this); \
			CName9.SetQuery(this); \
			CName10.SetQuery(this); \
			CName11.SetQuery(this); \
			CName12.SetQuery(this); \
			CName13.SetQuery(this); \
			CName14.SetQuery(this); \
			CName15.SetQuery(this); \
			CName16.SetQuery(this); \
			CName17.SetQuery(this); \
			CName18.SetQuery(this); \
			CName19.SetQuery(this); \
			CName20.SetQuery(this); \
			CName21.SetQuery(this); \
			CName22.SetQuery(this); \
			CName23.SetQuery(this); \
			CName24.SetQuery(this); \
			CName25.SetQuery(this); \
			CName26.SetQuery(this); \
			CName27.SetQuery(this); \
		} \
\
		TestQuery(const TestQuery& copy) : Query(copy), CName1(0), CName2(1), CName3(2), CName4(3), CName5(4), CName6(5), CName7(6), CName8(7), CName9(8), CName10(9), CName11(10), CName12(11), CName13(12), CName14(13), CName15(14), CName16(15), CName17(16), CName18(17), CName19(18), CName20(19), CName21(20), CName22(21), CName23(22), CName24(23), CName25(24), CName26(25), CName27(26) { \
			CName1.SetQuery(this); \
			CName2.SetQuery(this); \
			CName3.SetQuery(this); \
			CName4.SetQuery(this); \
			CName5.SetQuery(this); \
			CName6.SetQuery(this); \
			CName7.SetQuery(this); \
			CName8.SetQuery(this); \
			CName9.SetQuery(this); \
			CName10.SetQuery(this); \
			CName11.SetQuery(this); \
			CName12.SetQuery(this); \
			CName13.SetQuery(this); \
			CName14.SetQuery(this); \
			CName15.SetQuery(this); \
			CName16.SetQuery(this); \
			CName17.SetQuery(this); \
			CName18.SetQuery(this); \
			CName19.SetQuery(this); \
			CName20.SetQuery(this); \
			CName21.SetQuery(this); \
			CName22.SetQuery(this); \
			CName23.SetQuery(this); \
			CName24.SetQuery(this); \
			CName25.SetQuery(this); \
			CName26.SetQuery(this); \
			CName27.SetQuery(this); \
		} \
\
		class TestQueryQueryAccessorInt : private XQueryAccessorInt { \
		public: \
			TestQueryQueryAccessorInt(size_t column_id) : XQueryAccessorInt(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(int64_t value) {return (TestQuery &)XQueryAccessorInt::Equal(value);} \
			TestQuery& NotEqual(int64_t value) {return (TestQuery &)XQueryAccessorInt::NotEqual(value);} \
			TestQuery& Greater(int64_t value) {return (TestQuery &)XQueryAccessorInt::Greater(value);} \
			TestQuery& Less(int64_t value) {return (TestQuery &)XQueryAccessorInt::Less(value);} \
			TestQuery& Between(int64_t from, int64_t to) {return (TestQuery &)XQueryAccessorInt::Between(from, to);} \
		}; \
\
		template <class T> class TestQueryQueryAccessorEnum : public TestQueryQueryAccessorInt { \
		public: \
			TestQueryQueryAccessorEnum<T>(size_t column_id) : TestQueryQueryAccessorInt(column_id) {} \
		}; \
\
		class TestQueryQueryAccessorString : private XQueryAccessorString { \
		public: \
			TestQueryQueryAccessorString(size_t column_id) : XQueryAccessorString(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::Equal(value, CaseSensitive);} \
			TestQuery& NotEqual(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::NotEqual(value, CaseSensitive);} \
			TestQuery& BeginsWith(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::BeginsWith(value, CaseSensitive);} \
			TestQuery& EndsWith(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::EndsWith(value, CaseSensitive);} \
			TestQuery& Contains(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::Contains(value, CaseSensitive);} \
		}; \
\
		class TestQueryQueryAccessorBool : private XQueryAccessorBool { \
		public: \
			TestQueryQueryAccessorBool(size_t column_id) : XQueryAccessorBool(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(bool value) {return (TestQuery &)XQueryAccessorBool::Equal(value);} \
		}; \
\
		TestQueryQueryAccessor##CType1 CName1; \
		TestQueryQueryAccessor##CType2 CName2; \
		TestQueryQueryAccessor##CType3 CName3; \
		TestQueryQueryAccessor##CType4 CName4; \
		TestQueryQueryAccessor##CType5 CName5; \
		TestQueryQueryAccessor##CType6 CName6; \
		TestQueryQueryAccessor##CType7 CName7; \
		TestQueryQueryAccessor##CType8 CName8; \
		TestQueryQueryAccessor##CType9 CName9; \
		TestQueryQueryAccessor##CType10 CName10; \
		TestQueryQueryAccessor##CType11 CName11; \
		TestQueryQueryAccessor##CType12 CName12; \
		TestQueryQueryAccessor##CType13 CName13; \
		TestQueryQueryAccessor##CType14 CName14; \
		TestQueryQueryAccessor##CType15 CName15; \
		TestQueryQueryAccessor##CType16 CName16; \
		TestQueryQueryAccessor##CType17 CName17; \
		TestQueryQueryAccessor##CType18 CName18; \
		TestQueryQueryAccessor##CType19 CName19; \
		TestQueryQueryAccessor##CType20 CName20; \
		TestQueryQueryAccessor##CType21 CName21; \
		TestQueryQueryAccessor##CType22 CName22; \
		TestQueryQueryAccessor##CType23 CName23; \
		TestQueryQueryAccessor##CType24 CName24; \
		TestQueryQueryAccessor##CType25 CName25; \
		TestQueryQueryAccessor##CType26 CName26; \
		TestQueryQueryAccessor##CType27 CName27; \
\
		TestQuery& LeftParan(void) {Query::LeftParan(); return *this;}; \
		TestQuery& Or(void) {Query::Or(); return *this;}; \
		TestQuery& RightParan(void) {Query::RightParan(); return *this;}; \
		TestQuery& Subtable(size_t column) {Query::Subtable(column); return *this;}; \
		TestQuery& Parent() {Query::Parent(); return *this;}; \
	}; \
\
	TestQuery GetQuery() {return TestQuery();} \
\
	class Cursor : public CursorBase { \
	public: \
		Cursor(TableName& table, size_t ndx) : CursorBase(table, ndx) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
			CName17.Create(this, 16); \
			CName18.Create(this, 17); \
			CName19.Create(this, 18); \
			CName20.Create(this, 19); \
			CName21.Create(this, 20); \
			CName22.Create(this, 21); \
			CName23.Create(this, 22); \
			CName24.Create(this, 23); \
			CName25.Create(this, 24); \
			CName26.Create(this, 25); \
			CName27.Create(this, 26); \
		} \
		Cursor(const TableName& table, size_t ndx) : CursorBase(const_cast<TableName&>(table), ndx) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
			CName17.Create(this, 16); \
			CName18.Create(this, 17); \
			CName19.Create(this, 18); \
			CName20.Create(this, 19); \
			CName21.Create(this, 20); \
			CName22.Create(this, 21); \
			CName23.Create(this, 22); \
			CName24.Create(this, 23); \
			CName25.Create(this, 24); \
			CName26.Create(this, 25); \
			CName27.Create(this, 26); \
		} \
		Cursor(const Cursor& v) : CursorBase(v) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
			CName17.Create(this, 16); \
			CName18.Create(this, 17); \
			CName19.Create(this, 18); \
			CName20.Create(this, 19); \
			CName21.Create(this, 20); \
			CName22.Create(this, 21); \
			CName23.Create(this, 22); \
			CName24.Create(this, 23); \
			CName25.Create(this, 24); \
			CName26.Create(this, 25); \
			CName27.Create(this, 26); \
		} \
		Accessor##CType1 CName1; \
		Accessor##CType2 CName2; \
		Accessor##CType3 CName3; \
		Accessor##CType4 CName4; \
		Accessor##CType5 CName5; \
		Accessor##CType6 CName6; \
		Accessor##CType7 CName7; \
		Accessor##CType8 CName8; \
		Accessor##CType9 CName9; \
		Accessor##CType10 CName10; \
		Accessor##CType11 CName11; \
		Accessor##CType12 CName12; \
		Accessor##CType13 CName13; \
		Accessor##CType14 CName14; \
		Accessor##CType15 CName15; \
		Accessor##CType16 CName16; \
		Accessor##CType17 CName17; \
		Accessor##CType18 CName18; \
		Accessor##CType19 CName19; \
		Accessor##CType20 CName20; \
		Accessor##CType21 CName21; \
		Accessor##CType22 CName22; \
		Accessor##CType23 CName23; \
		Accessor##CType24 CName24; \
		Accessor##CType25 CName25; \
		Accessor##CType26 CName26; \
		Accessor##CType27 CName27; \
	}; \
\
	void Add(tdbType##CType1 CName1, tdbType##CType2 CName2, tdbType##CType3 CName3, tdbType##CType4 CName4, tdbType##CType5 CName5, tdbType##CType6 CName6, tdbType##CType7 CName7, tdbType##CType8 CName8, tdbType##CType9 CName9, tdbType##CType10 CName10, tdbType##CType11 CName11, tdbType##CType12 CName12, tdbType##CType13 CName13, tdbType##CType14 CName14, tdbType##CType15 CName15, tdbType##CType16 CName16, tdbType##CType17 CName17, tdbType##CType18 CName18, tdbType##CType19 CName19, tdbType##CType20 CName20, tdbType##CType21 CName21, tdbType##CType22 CName22, tdbType##CType23 CName23, tdbType##CType24 CName24, tdbType##CType25 CName25, tdbType##CType26 CName26, tdbType##CType27 CName27) { \
		const size_t ndx = GetSize(); \
		Insert##CType1 (0, ndx, CName1); \
		Insert##CType2 (1, ndx, CName2); \
		Insert##CType3 (2, ndx, CName3); \
		Insert##CType4 (3, ndx, CName4); \
		Insert##CType5 (4, ndx, CName5); \
		Insert##CType6 (5, ndx, CName6); \
		Insert##CType7 (6, ndx, CName7); \
		Insert##CType8 (7, ndx, CName8); \
		Insert##CType9 (8, ndx, CName9); \
		Insert##CType10 (9, ndx, CName10); \
		Insert##CType11 (10, ndx, CName11); \
		Insert##CType12 (11, ndx, CName12); \
		Insert##CType13 (12, ndx, CName13); \
		Insert##CType14 (13, ndx, CName14); \
		Insert##CType15 (14, ndx, CName15); \
		Insert##CType16 (15, ndx, CName16); \
		Insert##CType17 (16, ndx, CName17); \
		Insert##CType18 (17, ndx, CName18); \
		Insert##CType19 (18, ndx, CName19); \
		Insert##CType20 (19, ndx, CName20); \
		Insert##CType21 (20, ndx, CName21); \
		Insert##CType22 (21, ndx, CName22); \
		Insert##CType23 (22, ndx, CName23); \
		Insert##CType24 (23, ndx, CName24); \
		Insert##CType25 (24, ndx, CName25); \
		Insert##CType26 (25, ndx, CName26); \
		Insert##CType27 (26, ndx, CName27); \
		InsertDone(); \
	} \
\
	void Insert(size_t ndx, tdbType##CType1 CName1, tdbType##CType2 CName2, tdbType##CType3 CName3, tdbType##CType4 CName4, tdbType##CType5 CName5, tdbType##CType6 CName6, tdbType##CType7 CName7, tdbType##CType8 CName8, tdbType##CType9 CName9, tdbType##CType10 CName10, tdbType##CType11 CName11, tdbType##CType12 CName12, tdbType##CType13 CName13, tdbType##CType14 CName14, tdbType##CType15 CName15, tdbType##CType16 CName16, tdbType##CType17 CName17, tdbType##CType18 CName18, tdbType##CType19 CName19, tdbType##CType20 CName20, tdbType##CType21 CName21, tdbType##CType22 CName22, tdbType##CType23 CName23, tdbType##CType24 CName24, tdbType##CType25 CName25, tdbType##CType26 CName26, tdbType##CType27 CName27) { \
		Insert##CType1 (0, ndx, CName1); \
		Insert##CType2 (1, ndx, CName2); \
		Insert##CType3 (2, ndx, CName3); \
		Insert##CType4 (3, ndx, CName4); \
		Insert##CType5 (4, ndx, CName5); \
		Insert##CType6 (5, ndx, CName6); \
		Insert##CType7 (6, ndx, CName7); \
		Insert##CType8 (7, ndx, CName8); \
		Insert##CType9 (8, ndx, CName9); \
		Insert##CType10 (9, ndx, CName10); \
		Insert##CType11 (10, ndx, CName11); \
		Insert##CType12 (11, ndx, CName12); \
		Insert##CType13 (12, ndx, CName13); \
		Insert##CType14 (13, ndx, CName14); \
		Insert##CType15 (14, ndx, CName15); \
		Insert##CType16 (15, ndx, CName16); \
		Insert##CType17 (16, ndx, CName17); \
		Insert##CType18 (17, ndx, CName18); \
		Insert##CType19 (18, ndx, CName19); \
		Insert##CType20 (19, ndx, CName20); \
		Insert##CType21 (20, ndx, CName21); \
		Insert##CType22 (21, ndx, CName22); \
		Insert##CType23 (22, ndx, CName23); \
		Insert##CType24 (23, ndx, CName24); \
		Insert##CType25 (24, ndx, CName25); \
		Insert##CType26 (25, ndx, CName26); \
		Insert##CType27 (26, ndx, CName27); \
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
	ColumnProxy##CType3 CName3; \
	ColumnProxy##CType4 CName4; \
	ColumnProxy##CType5 CName5; \
	ColumnProxy##CType6 CName6; \
	ColumnProxy##CType7 CName7; \
	ColumnProxy##CType8 CName8; \
	ColumnProxy##CType9 CName9; \
	ColumnProxy##CType10 CName10; \
	ColumnProxy##CType11 CName11; \
	ColumnProxy##CType12 CName12; \
	ColumnProxy##CType13 CName13; \
	ColumnProxy##CType14 CName14; \
	ColumnProxy##CType15 CName15; \
	ColumnProxy##CType16 CName16; \
	ColumnProxy##CType17 CName17; \
	ColumnProxy##CType18 CName18; \
	ColumnProxy##CType19 CName19; \
	ColumnProxy##CType20 CName20; \
	ColumnProxy##CType21 CName21; \
	ColumnProxy##CType22 CName22; \
	ColumnProxy##CType23 CName23; \
	ColumnProxy##CType24 CName24; \
	ColumnProxy##CType25 CName25; \
	ColumnProxy##CType26 CName26; \
	ColumnProxy##CType27 CName27; \
\
protected: \
	friend class Group; \
	TableName(Allocator& alloc, size_t ref, Array* parent, size_t pndx) : TopLevelTable(alloc, ref, parent, pndx) {}; \
\
private: \
	TableName(const TableName&) {} \
	TableName& operator=(const TableName&) {return *this;} \
};



#define TDB_TABLE_28(TableName, CType1, CName1, CType2, CName2, CType3, CName3, CType4, CName4, CType5, CName5, CType6, CName6, CType7, CName7, CType8, CName8, CType9, CName9, CType10, CName10, CType11, CName11, CType12, CName12, CType13, CName13, CType14, CName14, CType15, CName15, CType16, CName16, CType17, CName17, CType18, CName18, CType19, CName19, CType20, CName20, CType21, CName21, CType22, CName22, CType23, CName23, CType24, CName24, CType25, CName25, CType26, CName26, CType27, CName27, CType28, CName28) \
class TableName##Query { \
protected: \
	QueryAccessor##CType1 CName1; \
	QueryAccessor##CType2 CName2; \
	QueryAccessor##CType3 CName3; \
	QueryAccessor##CType4 CName4; \
	QueryAccessor##CType5 CName5; \
	QueryAccessor##CType6 CName6; \
	QueryAccessor##CType7 CName7; \
	QueryAccessor##CType8 CName8; \
	QueryAccessor##CType9 CName9; \
	QueryAccessor##CType10 CName10; \
	QueryAccessor##CType11 CName11; \
	QueryAccessor##CType12 CName12; \
	QueryAccessor##CType13 CName13; \
	QueryAccessor##CType14 CName14; \
	QueryAccessor##CType15 CName15; \
	QueryAccessor##CType16 CName16; \
	QueryAccessor##CType17 CName17; \
	QueryAccessor##CType18 CName18; \
	QueryAccessor##CType19 CName19; \
	QueryAccessor##CType20 CName20; \
	QueryAccessor##CType21 CName21; \
	QueryAccessor##CType22 CName22; \
	QueryAccessor##CType23 CName23; \
	QueryAccessor##CType24 CName24; \
	QueryAccessor##CType25 CName25; \
	QueryAccessor##CType26 CName26; \
	QueryAccessor##CType27 CName27; \
	QueryAccessor##CType28 CName28; \
}; \
\
class TableName : public TopLevelTable { \
public: \
	TableName(Allocator& alloc=GetDefaultAllocator()) : TopLevelTable(alloc) { \
		RegisterColumn(Accessor##CType1::type, #CName1); \
		RegisterColumn(Accessor##CType2::type, #CName2); \
		RegisterColumn(Accessor##CType3::type, #CName3); \
		RegisterColumn(Accessor##CType4::type, #CName4); \
		RegisterColumn(Accessor##CType5::type, #CName5); \
		RegisterColumn(Accessor##CType6::type, #CName6); \
		RegisterColumn(Accessor##CType7::type, #CName7); \
		RegisterColumn(Accessor##CType8::type, #CName8); \
		RegisterColumn(Accessor##CType9::type, #CName9); \
		RegisterColumn(Accessor##CType10::type, #CName10); \
		RegisterColumn(Accessor##CType11::type, #CName11); \
		RegisterColumn(Accessor##CType12::type, #CName12); \
		RegisterColumn(Accessor##CType13::type, #CName13); \
		RegisterColumn(Accessor##CType14::type, #CName14); \
		RegisterColumn(Accessor##CType15::type, #CName15); \
		RegisterColumn(Accessor##CType16::type, #CName16); \
		RegisterColumn(Accessor##CType17::type, #CName17); \
		RegisterColumn(Accessor##CType18::type, #CName18); \
		RegisterColumn(Accessor##CType19::type, #CName19); \
		RegisterColumn(Accessor##CType20::type, #CName20); \
		RegisterColumn(Accessor##CType21::type, #CName21); \
		RegisterColumn(Accessor##CType22::type, #CName22); \
		RegisterColumn(Accessor##CType23::type, #CName23); \
		RegisterColumn(Accessor##CType24::type, #CName24); \
		RegisterColumn(Accessor##CType25::type, #CName25); \
		RegisterColumn(Accessor##CType26::type, #CName26); \
		RegisterColumn(Accessor##CType27::type, #CName27); \
		RegisterColumn(Accessor##CType28::type, #CName28); \
\
		CName1.Create(this, 0); \
		CName2.Create(this, 1); \
		CName3.Create(this, 2); \
		CName4.Create(this, 3); \
		CName5.Create(this, 4); \
		CName6.Create(this, 5); \
		CName7.Create(this, 6); \
		CName8.Create(this, 7); \
		CName9.Create(this, 8); \
		CName10.Create(this, 9); \
		CName11.Create(this, 10); \
		CName12.Create(this, 11); \
		CName13.Create(this, 12); \
		CName14.Create(this, 13); \
		CName15.Create(this, 14); \
		CName16.Create(this, 15); \
		CName17.Create(this, 16); \
		CName18.Create(this, 17); \
		CName19.Create(this, 18); \
		CName20.Create(this, 19); \
		CName21.Create(this, 20); \
		CName22.Create(this, 21); \
		CName23.Create(this, 22); \
		CName24.Create(this, 23); \
		CName25.Create(this, 24); \
		CName26.Create(this, 25); \
		CName27.Create(this, 26); \
		CName28.Create(this, 27); \
	}; \
\
	class TestQuery : public Query { \
	public: \
		TestQuery() : CName1(0), CName2(1), CName3(2), CName4(3), CName5(4), CName6(5), CName7(6), CName8(7), CName9(8), CName10(9), CName11(10), CName12(11), CName13(12), CName14(13), CName15(14), CName16(15), CName17(16), CName18(17), CName19(18), CName20(19), CName21(20), CName22(21), CName23(22), CName24(23), CName25(24), CName26(25), CName27(26), CName28(27) { \
			CName1.SetQuery(this); \
			CName2.SetQuery(this); \
			CName3.SetQuery(this); \
			CName4.SetQuery(this); \
			CName5.SetQuery(this); \
			CName6.SetQuery(this); \
			CName7.SetQuery(this); \
			CName8.SetQuery(this); \
			CName9.SetQuery(this); \
			CName10.SetQuery(this); \
			CName11.SetQuery(this); \
			CName12.SetQuery(this); \
			CName13.SetQuery(this); \
			CName14.SetQuery(this); \
			CName15.SetQuery(this); \
			CName16.SetQuery(this); \
			CName17.SetQuery(this); \
			CName18.SetQuery(this); \
			CName19.SetQuery(this); \
			CName20.SetQuery(this); \
			CName21.SetQuery(this); \
			CName22.SetQuery(this); \
			CName23.SetQuery(this); \
			CName24.SetQuery(this); \
			CName25.SetQuery(this); \
			CName26.SetQuery(this); \
			CName27.SetQuery(this); \
			CName28.SetQuery(this); \
		} \
\
		TestQuery(const TestQuery& copy) : Query(copy), CName1(0), CName2(1), CName3(2), CName4(3), CName5(4), CName6(5), CName7(6), CName8(7), CName9(8), CName10(9), CName11(10), CName12(11), CName13(12), CName14(13), CName15(14), CName16(15), CName17(16), CName18(17), CName19(18), CName20(19), CName21(20), CName22(21), CName23(22), CName24(23), CName25(24), CName26(25), CName27(26), CName28(27) { \
			CName1.SetQuery(this); \
			CName2.SetQuery(this); \
			CName3.SetQuery(this); \
			CName4.SetQuery(this); \
			CName5.SetQuery(this); \
			CName6.SetQuery(this); \
			CName7.SetQuery(this); \
			CName8.SetQuery(this); \
			CName9.SetQuery(this); \
			CName10.SetQuery(this); \
			CName11.SetQuery(this); \
			CName12.SetQuery(this); \
			CName13.SetQuery(this); \
			CName14.SetQuery(this); \
			CName15.SetQuery(this); \
			CName16.SetQuery(this); \
			CName17.SetQuery(this); \
			CName18.SetQuery(this); \
			CName19.SetQuery(this); \
			CName20.SetQuery(this); \
			CName21.SetQuery(this); \
			CName22.SetQuery(this); \
			CName23.SetQuery(this); \
			CName24.SetQuery(this); \
			CName25.SetQuery(this); \
			CName26.SetQuery(this); \
			CName27.SetQuery(this); \
			CName28.SetQuery(this); \
		} \
\
		class TestQueryQueryAccessorInt : private XQueryAccessorInt { \
		public: \
			TestQueryQueryAccessorInt(size_t column_id) : XQueryAccessorInt(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(int64_t value) {return (TestQuery &)XQueryAccessorInt::Equal(value);} \
			TestQuery& NotEqual(int64_t value) {return (TestQuery &)XQueryAccessorInt::NotEqual(value);} \
			TestQuery& Greater(int64_t value) {return (TestQuery &)XQueryAccessorInt::Greater(value);} \
			TestQuery& Less(int64_t value) {return (TestQuery &)XQueryAccessorInt::Less(value);} \
			TestQuery& Between(int64_t from, int64_t to) {return (TestQuery &)XQueryAccessorInt::Between(from, to);} \
		}; \
\
		template <class T> class TestQueryQueryAccessorEnum : public TestQueryQueryAccessorInt { \
		public: \
			TestQueryQueryAccessorEnum<T>(size_t column_id) : TestQueryQueryAccessorInt(column_id) {} \
		}; \
\
		class TestQueryQueryAccessorString : private XQueryAccessorString { \
		public: \
			TestQueryQueryAccessorString(size_t column_id) : XQueryAccessorString(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::Equal(value, CaseSensitive);} \
			TestQuery& NotEqual(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::NotEqual(value, CaseSensitive);} \
			TestQuery& BeginsWith(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::BeginsWith(value, CaseSensitive);} \
			TestQuery& EndsWith(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::EndsWith(value, CaseSensitive);} \
			TestQuery& Contains(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::Contains(value, CaseSensitive);} \
		}; \
\
		class TestQueryQueryAccessorBool : private XQueryAccessorBool { \
		public: \
			TestQueryQueryAccessorBool(size_t column_id) : XQueryAccessorBool(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(bool value) {return (TestQuery &)XQueryAccessorBool::Equal(value);} \
		}; \
\
		TestQueryQueryAccessor##CType1 CName1; \
		TestQueryQueryAccessor##CType2 CName2; \
		TestQueryQueryAccessor##CType3 CName3; \
		TestQueryQueryAccessor##CType4 CName4; \
		TestQueryQueryAccessor##CType5 CName5; \
		TestQueryQueryAccessor##CType6 CName6; \
		TestQueryQueryAccessor##CType7 CName7; \
		TestQueryQueryAccessor##CType8 CName8; \
		TestQueryQueryAccessor##CType9 CName9; \
		TestQueryQueryAccessor##CType10 CName10; \
		TestQueryQueryAccessor##CType11 CName11; \
		TestQueryQueryAccessor##CType12 CName12; \
		TestQueryQueryAccessor##CType13 CName13; \
		TestQueryQueryAccessor##CType14 CName14; \
		TestQueryQueryAccessor##CType15 CName15; \
		TestQueryQueryAccessor##CType16 CName16; \
		TestQueryQueryAccessor##CType17 CName17; \
		TestQueryQueryAccessor##CType18 CName18; \
		TestQueryQueryAccessor##CType19 CName19; \
		TestQueryQueryAccessor##CType20 CName20; \
		TestQueryQueryAccessor##CType21 CName21; \
		TestQueryQueryAccessor##CType22 CName22; \
		TestQueryQueryAccessor##CType23 CName23; \
		TestQueryQueryAccessor##CType24 CName24; \
		TestQueryQueryAccessor##CType25 CName25; \
		TestQueryQueryAccessor##CType26 CName26; \
		TestQueryQueryAccessor##CType27 CName27; \
		TestQueryQueryAccessor##CType28 CName28; \
\
		TestQuery& LeftParan(void) {Query::LeftParan(); return *this;}; \
		TestQuery& Or(void) {Query::Or(); return *this;}; \
		TestQuery& RightParan(void) {Query::RightParan(); return *this;}; \
		TestQuery& Subtable(size_t column) {Query::Subtable(column); return *this;}; \
		TestQuery& Parent() {Query::Parent(); return *this;}; \
	}; \
\
	TestQuery GetQuery() {return TestQuery();} \
\
	class Cursor : public CursorBase { \
	public: \
		Cursor(TableName& table, size_t ndx) : CursorBase(table, ndx) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
			CName17.Create(this, 16); \
			CName18.Create(this, 17); \
			CName19.Create(this, 18); \
			CName20.Create(this, 19); \
			CName21.Create(this, 20); \
			CName22.Create(this, 21); \
			CName23.Create(this, 22); \
			CName24.Create(this, 23); \
			CName25.Create(this, 24); \
			CName26.Create(this, 25); \
			CName27.Create(this, 26); \
			CName28.Create(this, 27); \
		} \
		Cursor(const TableName& table, size_t ndx) : CursorBase(const_cast<TableName&>(table), ndx) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
			CName17.Create(this, 16); \
			CName18.Create(this, 17); \
			CName19.Create(this, 18); \
			CName20.Create(this, 19); \
			CName21.Create(this, 20); \
			CName22.Create(this, 21); \
			CName23.Create(this, 22); \
			CName24.Create(this, 23); \
			CName25.Create(this, 24); \
			CName26.Create(this, 25); \
			CName27.Create(this, 26); \
			CName28.Create(this, 27); \
		} \
		Cursor(const Cursor& v) : CursorBase(v) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
			CName17.Create(this, 16); \
			CName18.Create(this, 17); \
			CName19.Create(this, 18); \
			CName20.Create(this, 19); \
			CName21.Create(this, 20); \
			CName22.Create(this, 21); \
			CName23.Create(this, 22); \
			CName24.Create(this, 23); \
			CName25.Create(this, 24); \
			CName26.Create(this, 25); \
			CName27.Create(this, 26); \
			CName28.Create(this, 27); \
		} \
		Accessor##CType1 CName1; \
		Accessor##CType2 CName2; \
		Accessor##CType3 CName3; \
		Accessor##CType4 CName4; \
		Accessor##CType5 CName5; \
		Accessor##CType6 CName6; \
		Accessor##CType7 CName7; \
		Accessor##CType8 CName8; \
		Accessor##CType9 CName9; \
		Accessor##CType10 CName10; \
		Accessor##CType11 CName11; \
		Accessor##CType12 CName12; \
		Accessor##CType13 CName13; \
		Accessor##CType14 CName14; \
		Accessor##CType15 CName15; \
		Accessor##CType16 CName16; \
		Accessor##CType17 CName17; \
		Accessor##CType18 CName18; \
		Accessor##CType19 CName19; \
		Accessor##CType20 CName20; \
		Accessor##CType21 CName21; \
		Accessor##CType22 CName22; \
		Accessor##CType23 CName23; \
		Accessor##CType24 CName24; \
		Accessor##CType25 CName25; \
		Accessor##CType26 CName26; \
		Accessor##CType27 CName27; \
		Accessor##CType28 CName28; \
	}; \
\
	void Add(tdbType##CType1 CName1, tdbType##CType2 CName2, tdbType##CType3 CName3, tdbType##CType4 CName4, tdbType##CType5 CName5, tdbType##CType6 CName6, tdbType##CType7 CName7, tdbType##CType8 CName8, tdbType##CType9 CName9, tdbType##CType10 CName10, tdbType##CType11 CName11, tdbType##CType12 CName12, tdbType##CType13 CName13, tdbType##CType14 CName14, tdbType##CType15 CName15, tdbType##CType16 CName16, tdbType##CType17 CName17, tdbType##CType18 CName18, tdbType##CType19 CName19, tdbType##CType20 CName20, tdbType##CType21 CName21, tdbType##CType22 CName22, tdbType##CType23 CName23, tdbType##CType24 CName24, tdbType##CType25 CName25, tdbType##CType26 CName26, tdbType##CType27 CName27, tdbType##CType28 CName28) { \
		const size_t ndx = GetSize(); \
		Insert##CType1 (0, ndx, CName1); \
		Insert##CType2 (1, ndx, CName2); \
		Insert##CType3 (2, ndx, CName3); \
		Insert##CType4 (3, ndx, CName4); \
		Insert##CType5 (4, ndx, CName5); \
		Insert##CType6 (5, ndx, CName6); \
		Insert##CType7 (6, ndx, CName7); \
		Insert##CType8 (7, ndx, CName8); \
		Insert##CType9 (8, ndx, CName9); \
		Insert##CType10 (9, ndx, CName10); \
		Insert##CType11 (10, ndx, CName11); \
		Insert##CType12 (11, ndx, CName12); \
		Insert##CType13 (12, ndx, CName13); \
		Insert##CType14 (13, ndx, CName14); \
		Insert##CType15 (14, ndx, CName15); \
		Insert##CType16 (15, ndx, CName16); \
		Insert##CType17 (16, ndx, CName17); \
		Insert##CType18 (17, ndx, CName18); \
		Insert##CType19 (18, ndx, CName19); \
		Insert##CType20 (19, ndx, CName20); \
		Insert##CType21 (20, ndx, CName21); \
		Insert##CType22 (21, ndx, CName22); \
		Insert##CType23 (22, ndx, CName23); \
		Insert##CType24 (23, ndx, CName24); \
		Insert##CType25 (24, ndx, CName25); \
		Insert##CType26 (25, ndx, CName26); \
		Insert##CType27 (26, ndx, CName27); \
		Insert##CType28 (27, ndx, CName28); \
		InsertDone(); \
	} \
\
	void Insert(size_t ndx, tdbType##CType1 CName1, tdbType##CType2 CName2, tdbType##CType3 CName3, tdbType##CType4 CName4, tdbType##CType5 CName5, tdbType##CType6 CName6, tdbType##CType7 CName7, tdbType##CType8 CName8, tdbType##CType9 CName9, tdbType##CType10 CName10, tdbType##CType11 CName11, tdbType##CType12 CName12, tdbType##CType13 CName13, tdbType##CType14 CName14, tdbType##CType15 CName15, tdbType##CType16 CName16, tdbType##CType17 CName17, tdbType##CType18 CName18, tdbType##CType19 CName19, tdbType##CType20 CName20, tdbType##CType21 CName21, tdbType##CType22 CName22, tdbType##CType23 CName23, tdbType##CType24 CName24, tdbType##CType25 CName25, tdbType##CType26 CName26, tdbType##CType27 CName27, tdbType##CType28 CName28) { \
		Insert##CType1 (0, ndx, CName1); \
		Insert##CType2 (1, ndx, CName2); \
		Insert##CType3 (2, ndx, CName3); \
		Insert##CType4 (3, ndx, CName4); \
		Insert##CType5 (4, ndx, CName5); \
		Insert##CType6 (5, ndx, CName6); \
		Insert##CType7 (6, ndx, CName7); \
		Insert##CType8 (7, ndx, CName8); \
		Insert##CType9 (8, ndx, CName9); \
		Insert##CType10 (9, ndx, CName10); \
		Insert##CType11 (10, ndx, CName11); \
		Insert##CType12 (11, ndx, CName12); \
		Insert##CType13 (12, ndx, CName13); \
		Insert##CType14 (13, ndx, CName14); \
		Insert##CType15 (14, ndx, CName15); \
		Insert##CType16 (15, ndx, CName16); \
		Insert##CType17 (16, ndx, CName17); \
		Insert##CType18 (17, ndx, CName18); \
		Insert##CType19 (18, ndx, CName19); \
		Insert##CType20 (19, ndx, CName20); \
		Insert##CType21 (20, ndx, CName21); \
		Insert##CType22 (21, ndx, CName22); \
		Insert##CType23 (22, ndx, CName23); \
		Insert##CType24 (23, ndx, CName24); \
		Insert##CType25 (24, ndx, CName25); \
		Insert##CType26 (25, ndx, CName26); \
		Insert##CType27 (26, ndx, CName27); \
		Insert##CType28 (27, ndx, CName28); \
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
	ColumnProxy##CType3 CName3; \
	ColumnProxy##CType4 CName4; \
	ColumnProxy##CType5 CName5; \
	ColumnProxy##CType6 CName6; \
	ColumnProxy##CType7 CName7; \
	ColumnProxy##CType8 CName8; \
	ColumnProxy##CType9 CName9; \
	ColumnProxy##CType10 CName10; \
	ColumnProxy##CType11 CName11; \
	ColumnProxy##CType12 CName12; \
	ColumnProxy##CType13 CName13; \
	ColumnProxy##CType14 CName14; \
	ColumnProxy##CType15 CName15; \
	ColumnProxy##CType16 CName16; \
	ColumnProxy##CType17 CName17; \
	ColumnProxy##CType18 CName18; \
	ColumnProxy##CType19 CName19; \
	ColumnProxy##CType20 CName20; \
	ColumnProxy##CType21 CName21; \
	ColumnProxy##CType22 CName22; \
	ColumnProxy##CType23 CName23; \
	ColumnProxy##CType24 CName24; \
	ColumnProxy##CType25 CName25; \
	ColumnProxy##CType26 CName26; \
	ColumnProxy##CType27 CName27; \
	ColumnProxy##CType28 CName28; \
\
protected: \
	friend class Group; \
	TableName(Allocator& alloc, size_t ref, Array* parent, size_t pndx) : TopLevelTable(alloc, ref, parent, pndx) {}; \
\
private: \
	TableName(const TableName&) {} \
	TableName& operator=(const TableName&) {return *this;} \
};



#define TDB_TABLE_29(TableName, CType1, CName1, CType2, CName2, CType3, CName3, CType4, CName4, CType5, CName5, CType6, CName6, CType7, CName7, CType8, CName8, CType9, CName9, CType10, CName10, CType11, CName11, CType12, CName12, CType13, CName13, CType14, CName14, CType15, CName15, CType16, CName16, CType17, CName17, CType18, CName18, CType19, CName19, CType20, CName20, CType21, CName21, CType22, CName22, CType23, CName23, CType24, CName24, CType25, CName25, CType26, CName26, CType27, CName27, CType28, CName28, CType29, CName29) \
class TableName##Query { \
protected: \
	QueryAccessor##CType1 CName1; \
	QueryAccessor##CType2 CName2; \
	QueryAccessor##CType3 CName3; \
	QueryAccessor##CType4 CName4; \
	QueryAccessor##CType5 CName5; \
	QueryAccessor##CType6 CName6; \
	QueryAccessor##CType7 CName7; \
	QueryAccessor##CType8 CName8; \
	QueryAccessor##CType9 CName9; \
	QueryAccessor##CType10 CName10; \
	QueryAccessor##CType11 CName11; \
	QueryAccessor##CType12 CName12; \
	QueryAccessor##CType13 CName13; \
	QueryAccessor##CType14 CName14; \
	QueryAccessor##CType15 CName15; \
	QueryAccessor##CType16 CName16; \
	QueryAccessor##CType17 CName17; \
	QueryAccessor##CType18 CName18; \
	QueryAccessor##CType19 CName19; \
	QueryAccessor##CType20 CName20; \
	QueryAccessor##CType21 CName21; \
	QueryAccessor##CType22 CName22; \
	QueryAccessor##CType23 CName23; \
	QueryAccessor##CType24 CName24; \
	QueryAccessor##CType25 CName25; \
	QueryAccessor##CType26 CName26; \
	QueryAccessor##CType27 CName27; \
	QueryAccessor##CType28 CName28; \
	QueryAccessor##CType29 CName29; \
}; \
\
class TableName : public TopLevelTable { \
public: \
	TableName(Allocator& alloc=GetDefaultAllocator()) : TopLevelTable(alloc) { \
		RegisterColumn(Accessor##CType1::type, #CName1); \
		RegisterColumn(Accessor##CType2::type, #CName2); \
		RegisterColumn(Accessor##CType3::type, #CName3); \
		RegisterColumn(Accessor##CType4::type, #CName4); \
		RegisterColumn(Accessor##CType5::type, #CName5); \
		RegisterColumn(Accessor##CType6::type, #CName6); \
		RegisterColumn(Accessor##CType7::type, #CName7); \
		RegisterColumn(Accessor##CType8::type, #CName8); \
		RegisterColumn(Accessor##CType9::type, #CName9); \
		RegisterColumn(Accessor##CType10::type, #CName10); \
		RegisterColumn(Accessor##CType11::type, #CName11); \
		RegisterColumn(Accessor##CType12::type, #CName12); \
		RegisterColumn(Accessor##CType13::type, #CName13); \
		RegisterColumn(Accessor##CType14::type, #CName14); \
		RegisterColumn(Accessor##CType15::type, #CName15); \
		RegisterColumn(Accessor##CType16::type, #CName16); \
		RegisterColumn(Accessor##CType17::type, #CName17); \
		RegisterColumn(Accessor##CType18::type, #CName18); \
		RegisterColumn(Accessor##CType19::type, #CName19); \
		RegisterColumn(Accessor##CType20::type, #CName20); \
		RegisterColumn(Accessor##CType21::type, #CName21); \
		RegisterColumn(Accessor##CType22::type, #CName22); \
		RegisterColumn(Accessor##CType23::type, #CName23); \
		RegisterColumn(Accessor##CType24::type, #CName24); \
		RegisterColumn(Accessor##CType25::type, #CName25); \
		RegisterColumn(Accessor##CType26::type, #CName26); \
		RegisterColumn(Accessor##CType27::type, #CName27); \
		RegisterColumn(Accessor##CType28::type, #CName28); \
		RegisterColumn(Accessor##CType29::type, #CName29); \
\
		CName1.Create(this, 0); \
		CName2.Create(this, 1); \
		CName3.Create(this, 2); \
		CName4.Create(this, 3); \
		CName5.Create(this, 4); \
		CName6.Create(this, 5); \
		CName7.Create(this, 6); \
		CName8.Create(this, 7); \
		CName9.Create(this, 8); \
		CName10.Create(this, 9); \
		CName11.Create(this, 10); \
		CName12.Create(this, 11); \
		CName13.Create(this, 12); \
		CName14.Create(this, 13); \
		CName15.Create(this, 14); \
		CName16.Create(this, 15); \
		CName17.Create(this, 16); \
		CName18.Create(this, 17); \
		CName19.Create(this, 18); \
		CName20.Create(this, 19); \
		CName21.Create(this, 20); \
		CName22.Create(this, 21); \
		CName23.Create(this, 22); \
		CName24.Create(this, 23); \
		CName25.Create(this, 24); \
		CName26.Create(this, 25); \
		CName27.Create(this, 26); \
		CName28.Create(this, 27); \
		CName29.Create(this, 28); \
	}; \
\
	class TestQuery : public Query { \
	public: \
		TestQuery() : CName1(0), CName2(1), CName3(2), CName4(3), CName5(4), CName6(5), CName7(6), CName8(7), CName9(8), CName10(9), CName11(10), CName12(11), CName13(12), CName14(13), CName15(14), CName16(15), CName17(16), CName18(17), CName19(18), CName20(19), CName21(20), CName22(21), CName23(22), CName24(23), CName25(24), CName26(25), CName27(26), CName28(27), CName29(28) { \
			CName1.SetQuery(this); \
			CName2.SetQuery(this); \
			CName3.SetQuery(this); \
			CName4.SetQuery(this); \
			CName5.SetQuery(this); \
			CName6.SetQuery(this); \
			CName7.SetQuery(this); \
			CName8.SetQuery(this); \
			CName9.SetQuery(this); \
			CName10.SetQuery(this); \
			CName11.SetQuery(this); \
			CName12.SetQuery(this); \
			CName13.SetQuery(this); \
			CName14.SetQuery(this); \
			CName15.SetQuery(this); \
			CName16.SetQuery(this); \
			CName17.SetQuery(this); \
			CName18.SetQuery(this); \
			CName19.SetQuery(this); \
			CName20.SetQuery(this); \
			CName21.SetQuery(this); \
			CName22.SetQuery(this); \
			CName23.SetQuery(this); \
			CName24.SetQuery(this); \
			CName25.SetQuery(this); \
			CName26.SetQuery(this); \
			CName27.SetQuery(this); \
			CName28.SetQuery(this); \
			CName29.SetQuery(this); \
		} \
\
		TestQuery(const TestQuery& copy) : Query(copy), CName1(0), CName2(1), CName3(2), CName4(3), CName5(4), CName6(5), CName7(6), CName8(7), CName9(8), CName10(9), CName11(10), CName12(11), CName13(12), CName14(13), CName15(14), CName16(15), CName17(16), CName18(17), CName19(18), CName20(19), CName21(20), CName22(21), CName23(22), CName24(23), CName25(24), CName26(25), CName27(26), CName28(27), CName29(28) { \
			CName1.SetQuery(this); \
			CName2.SetQuery(this); \
			CName3.SetQuery(this); \
			CName4.SetQuery(this); \
			CName5.SetQuery(this); \
			CName6.SetQuery(this); \
			CName7.SetQuery(this); \
			CName8.SetQuery(this); \
			CName9.SetQuery(this); \
			CName10.SetQuery(this); \
			CName11.SetQuery(this); \
			CName12.SetQuery(this); \
			CName13.SetQuery(this); \
			CName14.SetQuery(this); \
			CName15.SetQuery(this); \
			CName16.SetQuery(this); \
			CName17.SetQuery(this); \
			CName18.SetQuery(this); \
			CName19.SetQuery(this); \
			CName20.SetQuery(this); \
			CName21.SetQuery(this); \
			CName22.SetQuery(this); \
			CName23.SetQuery(this); \
			CName24.SetQuery(this); \
			CName25.SetQuery(this); \
			CName26.SetQuery(this); \
			CName27.SetQuery(this); \
			CName28.SetQuery(this); \
			CName29.SetQuery(this); \
		} \
\
		class TestQueryQueryAccessorInt : private XQueryAccessorInt { \
		public: \
			TestQueryQueryAccessorInt(size_t column_id) : XQueryAccessorInt(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(int64_t value) {return (TestQuery &)XQueryAccessorInt::Equal(value);} \
			TestQuery& NotEqual(int64_t value) {return (TestQuery &)XQueryAccessorInt::NotEqual(value);} \
			TestQuery& Greater(int64_t value) {return (TestQuery &)XQueryAccessorInt::Greater(value);} \
			TestQuery& Less(int64_t value) {return (TestQuery &)XQueryAccessorInt::Less(value);} \
			TestQuery& Between(int64_t from, int64_t to) {return (TestQuery &)XQueryAccessorInt::Between(from, to);} \
		}; \
\
		template <class T> class TestQueryQueryAccessorEnum : public TestQueryQueryAccessorInt { \
		public: \
			TestQueryQueryAccessorEnum<T>(size_t column_id) : TestQueryQueryAccessorInt(column_id) {} \
		}; \
\
		class TestQueryQueryAccessorString : private XQueryAccessorString { \
		public: \
			TestQueryQueryAccessorString(size_t column_id) : XQueryAccessorString(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::Equal(value, CaseSensitive);} \
			TestQuery& NotEqual(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::NotEqual(value, CaseSensitive);} \
			TestQuery& BeginsWith(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::BeginsWith(value, CaseSensitive);} \
			TestQuery& EndsWith(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::EndsWith(value, CaseSensitive);} \
			TestQuery& Contains(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::Contains(value, CaseSensitive);} \
		}; \
\
		class TestQueryQueryAccessorBool : private XQueryAccessorBool { \
		public: \
			TestQueryQueryAccessorBool(size_t column_id) : XQueryAccessorBool(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(bool value) {return (TestQuery &)XQueryAccessorBool::Equal(value);} \
		}; \
\
		TestQueryQueryAccessor##CType1 CName1; \
		TestQueryQueryAccessor##CType2 CName2; \
		TestQueryQueryAccessor##CType3 CName3; \
		TestQueryQueryAccessor##CType4 CName4; \
		TestQueryQueryAccessor##CType5 CName5; \
		TestQueryQueryAccessor##CType6 CName6; \
		TestQueryQueryAccessor##CType7 CName7; \
		TestQueryQueryAccessor##CType8 CName8; \
		TestQueryQueryAccessor##CType9 CName9; \
		TestQueryQueryAccessor##CType10 CName10; \
		TestQueryQueryAccessor##CType11 CName11; \
		TestQueryQueryAccessor##CType12 CName12; \
		TestQueryQueryAccessor##CType13 CName13; \
		TestQueryQueryAccessor##CType14 CName14; \
		TestQueryQueryAccessor##CType15 CName15; \
		TestQueryQueryAccessor##CType16 CName16; \
		TestQueryQueryAccessor##CType17 CName17; \
		TestQueryQueryAccessor##CType18 CName18; \
		TestQueryQueryAccessor##CType19 CName19; \
		TestQueryQueryAccessor##CType20 CName20; \
		TestQueryQueryAccessor##CType21 CName21; \
		TestQueryQueryAccessor##CType22 CName22; \
		TestQueryQueryAccessor##CType23 CName23; \
		TestQueryQueryAccessor##CType24 CName24; \
		TestQueryQueryAccessor##CType25 CName25; \
		TestQueryQueryAccessor##CType26 CName26; \
		TestQueryQueryAccessor##CType27 CName27; \
		TestQueryQueryAccessor##CType28 CName28; \
		TestQueryQueryAccessor##CType29 CName29; \
\
		TestQuery& LeftParan(void) {Query::LeftParan(); return *this;}; \
		TestQuery& Or(void) {Query::Or(); return *this;}; \
		TestQuery& RightParan(void) {Query::RightParan(); return *this;}; \
		TestQuery& Subtable(size_t column) {Query::Subtable(column); return *this;}; \
		TestQuery& Parent() {Query::Parent(); return *this;}; \
	}; \
\
	TestQuery GetQuery() {return TestQuery();} \
\
	class Cursor : public CursorBase { \
	public: \
		Cursor(TableName& table, size_t ndx) : CursorBase(table, ndx) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
			CName17.Create(this, 16); \
			CName18.Create(this, 17); \
			CName19.Create(this, 18); \
			CName20.Create(this, 19); \
			CName21.Create(this, 20); \
			CName22.Create(this, 21); \
			CName23.Create(this, 22); \
			CName24.Create(this, 23); \
			CName25.Create(this, 24); \
			CName26.Create(this, 25); \
			CName27.Create(this, 26); \
			CName28.Create(this, 27); \
			CName29.Create(this, 28); \
		} \
		Cursor(const TableName& table, size_t ndx) : CursorBase(const_cast<TableName&>(table), ndx) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
			CName17.Create(this, 16); \
			CName18.Create(this, 17); \
			CName19.Create(this, 18); \
			CName20.Create(this, 19); \
			CName21.Create(this, 20); \
			CName22.Create(this, 21); \
			CName23.Create(this, 22); \
			CName24.Create(this, 23); \
			CName25.Create(this, 24); \
			CName26.Create(this, 25); \
			CName27.Create(this, 26); \
			CName28.Create(this, 27); \
			CName29.Create(this, 28); \
		} \
		Cursor(const Cursor& v) : CursorBase(v) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
			CName17.Create(this, 16); \
			CName18.Create(this, 17); \
			CName19.Create(this, 18); \
			CName20.Create(this, 19); \
			CName21.Create(this, 20); \
			CName22.Create(this, 21); \
			CName23.Create(this, 22); \
			CName24.Create(this, 23); \
			CName25.Create(this, 24); \
			CName26.Create(this, 25); \
			CName27.Create(this, 26); \
			CName28.Create(this, 27); \
			CName29.Create(this, 28); \
		} \
		Accessor##CType1 CName1; \
		Accessor##CType2 CName2; \
		Accessor##CType3 CName3; \
		Accessor##CType4 CName4; \
		Accessor##CType5 CName5; \
		Accessor##CType6 CName6; \
		Accessor##CType7 CName7; \
		Accessor##CType8 CName8; \
		Accessor##CType9 CName9; \
		Accessor##CType10 CName10; \
		Accessor##CType11 CName11; \
		Accessor##CType12 CName12; \
		Accessor##CType13 CName13; \
		Accessor##CType14 CName14; \
		Accessor##CType15 CName15; \
		Accessor##CType16 CName16; \
		Accessor##CType17 CName17; \
		Accessor##CType18 CName18; \
		Accessor##CType19 CName19; \
		Accessor##CType20 CName20; \
		Accessor##CType21 CName21; \
		Accessor##CType22 CName22; \
		Accessor##CType23 CName23; \
		Accessor##CType24 CName24; \
		Accessor##CType25 CName25; \
		Accessor##CType26 CName26; \
		Accessor##CType27 CName27; \
		Accessor##CType28 CName28; \
		Accessor##CType29 CName29; \
	}; \
\
	void Add(tdbType##CType1 CName1, tdbType##CType2 CName2, tdbType##CType3 CName3, tdbType##CType4 CName4, tdbType##CType5 CName5, tdbType##CType6 CName6, tdbType##CType7 CName7, tdbType##CType8 CName8, tdbType##CType9 CName9, tdbType##CType10 CName10, tdbType##CType11 CName11, tdbType##CType12 CName12, tdbType##CType13 CName13, tdbType##CType14 CName14, tdbType##CType15 CName15, tdbType##CType16 CName16, tdbType##CType17 CName17, tdbType##CType18 CName18, tdbType##CType19 CName19, tdbType##CType20 CName20, tdbType##CType21 CName21, tdbType##CType22 CName22, tdbType##CType23 CName23, tdbType##CType24 CName24, tdbType##CType25 CName25, tdbType##CType26 CName26, tdbType##CType27 CName27, tdbType##CType28 CName28, tdbType##CType29 CName29) { \
		const size_t ndx = GetSize(); \
		Insert##CType1 (0, ndx, CName1); \
		Insert##CType2 (1, ndx, CName2); \
		Insert##CType3 (2, ndx, CName3); \
		Insert##CType4 (3, ndx, CName4); \
		Insert##CType5 (4, ndx, CName5); \
		Insert##CType6 (5, ndx, CName6); \
		Insert##CType7 (6, ndx, CName7); \
		Insert##CType8 (7, ndx, CName8); \
		Insert##CType9 (8, ndx, CName9); \
		Insert##CType10 (9, ndx, CName10); \
		Insert##CType11 (10, ndx, CName11); \
		Insert##CType12 (11, ndx, CName12); \
		Insert##CType13 (12, ndx, CName13); \
		Insert##CType14 (13, ndx, CName14); \
		Insert##CType15 (14, ndx, CName15); \
		Insert##CType16 (15, ndx, CName16); \
		Insert##CType17 (16, ndx, CName17); \
		Insert##CType18 (17, ndx, CName18); \
		Insert##CType19 (18, ndx, CName19); \
		Insert##CType20 (19, ndx, CName20); \
		Insert##CType21 (20, ndx, CName21); \
		Insert##CType22 (21, ndx, CName22); \
		Insert##CType23 (22, ndx, CName23); \
		Insert##CType24 (23, ndx, CName24); \
		Insert##CType25 (24, ndx, CName25); \
		Insert##CType26 (25, ndx, CName26); \
		Insert##CType27 (26, ndx, CName27); \
		Insert##CType28 (27, ndx, CName28); \
		Insert##CType29 (28, ndx, CName29); \
		InsertDone(); \
	} \
\
	void Insert(size_t ndx, tdbType##CType1 CName1, tdbType##CType2 CName2, tdbType##CType3 CName3, tdbType##CType4 CName4, tdbType##CType5 CName5, tdbType##CType6 CName6, tdbType##CType7 CName7, tdbType##CType8 CName8, tdbType##CType9 CName9, tdbType##CType10 CName10, tdbType##CType11 CName11, tdbType##CType12 CName12, tdbType##CType13 CName13, tdbType##CType14 CName14, tdbType##CType15 CName15, tdbType##CType16 CName16, tdbType##CType17 CName17, tdbType##CType18 CName18, tdbType##CType19 CName19, tdbType##CType20 CName20, tdbType##CType21 CName21, tdbType##CType22 CName22, tdbType##CType23 CName23, tdbType##CType24 CName24, tdbType##CType25 CName25, tdbType##CType26 CName26, tdbType##CType27 CName27, tdbType##CType28 CName28, tdbType##CType29 CName29) { \
		Insert##CType1 (0, ndx, CName1); \
		Insert##CType2 (1, ndx, CName2); \
		Insert##CType3 (2, ndx, CName3); \
		Insert##CType4 (3, ndx, CName4); \
		Insert##CType5 (4, ndx, CName5); \
		Insert##CType6 (5, ndx, CName6); \
		Insert##CType7 (6, ndx, CName7); \
		Insert##CType8 (7, ndx, CName8); \
		Insert##CType9 (8, ndx, CName9); \
		Insert##CType10 (9, ndx, CName10); \
		Insert##CType11 (10, ndx, CName11); \
		Insert##CType12 (11, ndx, CName12); \
		Insert##CType13 (12, ndx, CName13); \
		Insert##CType14 (13, ndx, CName14); \
		Insert##CType15 (14, ndx, CName15); \
		Insert##CType16 (15, ndx, CName16); \
		Insert##CType17 (16, ndx, CName17); \
		Insert##CType18 (17, ndx, CName18); \
		Insert##CType19 (18, ndx, CName19); \
		Insert##CType20 (19, ndx, CName20); \
		Insert##CType21 (20, ndx, CName21); \
		Insert##CType22 (21, ndx, CName22); \
		Insert##CType23 (22, ndx, CName23); \
		Insert##CType24 (23, ndx, CName24); \
		Insert##CType25 (24, ndx, CName25); \
		Insert##CType26 (25, ndx, CName26); \
		Insert##CType27 (26, ndx, CName27); \
		Insert##CType28 (27, ndx, CName28); \
		Insert##CType29 (28, ndx, CName29); \
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
	ColumnProxy##CType3 CName3; \
	ColumnProxy##CType4 CName4; \
	ColumnProxy##CType5 CName5; \
	ColumnProxy##CType6 CName6; \
	ColumnProxy##CType7 CName7; \
	ColumnProxy##CType8 CName8; \
	ColumnProxy##CType9 CName9; \
	ColumnProxy##CType10 CName10; \
	ColumnProxy##CType11 CName11; \
	ColumnProxy##CType12 CName12; \
	ColumnProxy##CType13 CName13; \
	ColumnProxy##CType14 CName14; \
	ColumnProxy##CType15 CName15; \
	ColumnProxy##CType16 CName16; \
	ColumnProxy##CType17 CName17; \
	ColumnProxy##CType18 CName18; \
	ColumnProxy##CType19 CName19; \
	ColumnProxy##CType20 CName20; \
	ColumnProxy##CType21 CName21; \
	ColumnProxy##CType22 CName22; \
	ColumnProxy##CType23 CName23; \
	ColumnProxy##CType24 CName24; \
	ColumnProxy##CType25 CName25; \
	ColumnProxy##CType26 CName26; \
	ColumnProxy##CType27 CName27; \
	ColumnProxy##CType28 CName28; \
	ColumnProxy##CType29 CName29; \
\
protected: \
	friend class Group; \
	TableName(Allocator& alloc, size_t ref, Array* parent, size_t pndx) : TopLevelTable(alloc, ref, parent, pndx) {}; \
\
private: \
	TableName(const TableName&) {} \
	TableName& operator=(const TableName&) {return *this;} \
};



#define TDB_TABLE_30(TableName, CType1, CName1, CType2, CName2, CType3, CName3, CType4, CName4, CType5, CName5, CType6, CName6, CType7, CName7, CType8, CName8, CType9, CName9, CType10, CName10, CType11, CName11, CType12, CName12, CType13, CName13, CType14, CName14, CType15, CName15, CType16, CName16, CType17, CName17, CType18, CName18, CType19, CName19, CType20, CName20, CType21, CName21, CType22, CName22, CType23, CName23, CType24, CName24, CType25, CName25, CType26, CName26, CType27, CName27, CType28, CName28, CType29, CName29, CType30, CName30) \
class TableName##Query { \
protected: \
	QueryAccessor##CType1 CName1; \
	QueryAccessor##CType2 CName2; \
	QueryAccessor##CType3 CName3; \
	QueryAccessor##CType4 CName4; \
	QueryAccessor##CType5 CName5; \
	QueryAccessor##CType6 CName6; \
	QueryAccessor##CType7 CName7; \
	QueryAccessor##CType8 CName8; \
	QueryAccessor##CType9 CName9; \
	QueryAccessor##CType10 CName10; \
	QueryAccessor##CType11 CName11; \
	QueryAccessor##CType12 CName12; \
	QueryAccessor##CType13 CName13; \
	QueryAccessor##CType14 CName14; \
	QueryAccessor##CType15 CName15; \
	QueryAccessor##CType16 CName16; \
	QueryAccessor##CType17 CName17; \
	QueryAccessor##CType18 CName18; \
	QueryAccessor##CType19 CName19; \
	QueryAccessor##CType20 CName20; \
	QueryAccessor##CType21 CName21; \
	QueryAccessor##CType22 CName22; \
	QueryAccessor##CType23 CName23; \
	QueryAccessor##CType24 CName24; \
	QueryAccessor##CType25 CName25; \
	QueryAccessor##CType26 CName26; \
	QueryAccessor##CType27 CName27; \
	QueryAccessor##CType28 CName28; \
	QueryAccessor##CType29 CName29; \
	QueryAccessor##CType30 CName30; \
}; \
\
class TableName : public TopLevelTable { \
public: \
	TableName(Allocator& alloc=GetDefaultAllocator()) : TopLevelTable(alloc) { \
		RegisterColumn(Accessor##CType1::type, #CName1); \
		RegisterColumn(Accessor##CType2::type, #CName2); \
		RegisterColumn(Accessor##CType3::type, #CName3); \
		RegisterColumn(Accessor##CType4::type, #CName4); \
		RegisterColumn(Accessor##CType5::type, #CName5); \
		RegisterColumn(Accessor##CType6::type, #CName6); \
		RegisterColumn(Accessor##CType7::type, #CName7); \
		RegisterColumn(Accessor##CType8::type, #CName8); \
		RegisterColumn(Accessor##CType9::type, #CName9); \
		RegisterColumn(Accessor##CType10::type, #CName10); \
		RegisterColumn(Accessor##CType11::type, #CName11); \
		RegisterColumn(Accessor##CType12::type, #CName12); \
		RegisterColumn(Accessor##CType13::type, #CName13); \
		RegisterColumn(Accessor##CType14::type, #CName14); \
		RegisterColumn(Accessor##CType15::type, #CName15); \
		RegisterColumn(Accessor##CType16::type, #CName16); \
		RegisterColumn(Accessor##CType17::type, #CName17); \
		RegisterColumn(Accessor##CType18::type, #CName18); \
		RegisterColumn(Accessor##CType19::type, #CName19); \
		RegisterColumn(Accessor##CType20::type, #CName20); \
		RegisterColumn(Accessor##CType21::type, #CName21); \
		RegisterColumn(Accessor##CType22::type, #CName22); \
		RegisterColumn(Accessor##CType23::type, #CName23); \
		RegisterColumn(Accessor##CType24::type, #CName24); \
		RegisterColumn(Accessor##CType25::type, #CName25); \
		RegisterColumn(Accessor##CType26::type, #CName26); \
		RegisterColumn(Accessor##CType27::type, #CName27); \
		RegisterColumn(Accessor##CType28::type, #CName28); \
		RegisterColumn(Accessor##CType29::type, #CName29); \
		RegisterColumn(Accessor##CType30::type, #CName30); \
\
		CName1.Create(this, 0); \
		CName2.Create(this, 1); \
		CName3.Create(this, 2); \
		CName4.Create(this, 3); \
		CName5.Create(this, 4); \
		CName6.Create(this, 5); \
		CName7.Create(this, 6); \
		CName8.Create(this, 7); \
		CName9.Create(this, 8); \
		CName10.Create(this, 9); \
		CName11.Create(this, 10); \
		CName12.Create(this, 11); \
		CName13.Create(this, 12); \
		CName14.Create(this, 13); \
		CName15.Create(this, 14); \
		CName16.Create(this, 15); \
		CName17.Create(this, 16); \
		CName18.Create(this, 17); \
		CName19.Create(this, 18); \
		CName20.Create(this, 19); \
		CName21.Create(this, 20); \
		CName22.Create(this, 21); \
		CName23.Create(this, 22); \
		CName24.Create(this, 23); \
		CName25.Create(this, 24); \
		CName26.Create(this, 25); \
		CName27.Create(this, 26); \
		CName28.Create(this, 27); \
		CName29.Create(this, 28); \
		CName30.Create(this, 29); \
	}; \
\
	class TestQuery : public Query { \
	public: \
		TestQuery() : CName1(0), CName2(1), CName3(2), CName4(3), CName5(4), CName6(5), CName7(6), CName8(7), CName9(8), CName10(9), CName11(10), CName12(11), CName13(12), CName14(13), CName15(14), CName16(15), CName17(16), CName18(17), CName19(18), CName20(19), CName21(20), CName22(21), CName23(22), CName24(23), CName25(24), CName26(25), CName27(26), CName28(27), CName29(28), CName30(29) { \
			CName1.SetQuery(this); \
			CName2.SetQuery(this); \
			CName3.SetQuery(this); \
			CName4.SetQuery(this); \
			CName5.SetQuery(this); \
			CName6.SetQuery(this); \
			CName7.SetQuery(this); \
			CName8.SetQuery(this); \
			CName9.SetQuery(this); \
			CName10.SetQuery(this); \
			CName11.SetQuery(this); \
			CName12.SetQuery(this); \
			CName13.SetQuery(this); \
			CName14.SetQuery(this); \
			CName15.SetQuery(this); \
			CName16.SetQuery(this); \
			CName17.SetQuery(this); \
			CName18.SetQuery(this); \
			CName19.SetQuery(this); \
			CName20.SetQuery(this); \
			CName21.SetQuery(this); \
			CName22.SetQuery(this); \
			CName23.SetQuery(this); \
			CName24.SetQuery(this); \
			CName25.SetQuery(this); \
			CName26.SetQuery(this); \
			CName27.SetQuery(this); \
			CName28.SetQuery(this); \
			CName29.SetQuery(this); \
			CName30.SetQuery(this); \
		} \
\
		TestQuery(const TestQuery& copy) : Query(copy), CName1(0), CName2(1), CName3(2), CName4(3), CName5(4), CName6(5), CName7(6), CName8(7), CName9(8), CName10(9), CName11(10), CName12(11), CName13(12), CName14(13), CName15(14), CName16(15), CName17(16), CName18(17), CName19(18), CName20(19), CName21(20), CName22(21), CName23(22), CName24(23), CName25(24), CName26(25), CName27(26), CName28(27), CName29(28), CName30(29) { \
			CName1.SetQuery(this); \
			CName2.SetQuery(this); \
			CName3.SetQuery(this); \
			CName4.SetQuery(this); \
			CName5.SetQuery(this); \
			CName6.SetQuery(this); \
			CName7.SetQuery(this); \
			CName8.SetQuery(this); \
			CName9.SetQuery(this); \
			CName10.SetQuery(this); \
			CName11.SetQuery(this); \
			CName12.SetQuery(this); \
			CName13.SetQuery(this); \
			CName14.SetQuery(this); \
			CName15.SetQuery(this); \
			CName16.SetQuery(this); \
			CName17.SetQuery(this); \
			CName18.SetQuery(this); \
			CName19.SetQuery(this); \
			CName20.SetQuery(this); \
			CName21.SetQuery(this); \
			CName22.SetQuery(this); \
			CName23.SetQuery(this); \
			CName24.SetQuery(this); \
			CName25.SetQuery(this); \
			CName26.SetQuery(this); \
			CName27.SetQuery(this); \
			CName28.SetQuery(this); \
			CName29.SetQuery(this); \
			CName30.SetQuery(this); \
		} \
\
		class TestQueryQueryAccessorInt : private XQueryAccessorInt { \
		public: \
			TestQueryQueryAccessorInt(size_t column_id) : XQueryAccessorInt(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(int64_t value) {return (TestQuery &)XQueryAccessorInt::Equal(value);} \
			TestQuery& NotEqual(int64_t value) {return (TestQuery &)XQueryAccessorInt::NotEqual(value);} \
			TestQuery& Greater(int64_t value) {return (TestQuery &)XQueryAccessorInt::Greater(value);} \
			TestQuery& Less(int64_t value) {return (TestQuery &)XQueryAccessorInt::Less(value);} \
			TestQuery& Between(int64_t from, int64_t to) {return (TestQuery &)XQueryAccessorInt::Between(from, to);} \
		}; \
\
		template <class T> class TestQueryQueryAccessorEnum : public TestQueryQueryAccessorInt { \
		public: \
			TestQueryQueryAccessorEnum<T>(size_t column_id) : TestQueryQueryAccessorInt(column_id) {} \
		}; \
\
		class TestQueryQueryAccessorString : private XQueryAccessorString { \
		public: \
			TestQueryQueryAccessorString(size_t column_id) : XQueryAccessorString(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::Equal(value, CaseSensitive);} \
			TestQuery& NotEqual(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::NotEqual(value, CaseSensitive);} \
			TestQuery& BeginsWith(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::BeginsWith(value, CaseSensitive);} \
			TestQuery& EndsWith(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::EndsWith(value, CaseSensitive);} \
			TestQuery& Contains(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::Contains(value, CaseSensitive);} \
		}; \
\
		class TestQueryQueryAccessorBool : private XQueryAccessorBool { \
		public: \
			TestQueryQueryAccessorBool(size_t column_id) : XQueryAccessorBool(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(bool value) {return (TestQuery &)XQueryAccessorBool::Equal(value);} \
		}; \
\
		TestQueryQueryAccessor##CType1 CName1; \
		TestQueryQueryAccessor##CType2 CName2; \
		TestQueryQueryAccessor##CType3 CName3; \
		TestQueryQueryAccessor##CType4 CName4; \
		TestQueryQueryAccessor##CType5 CName5; \
		TestQueryQueryAccessor##CType6 CName6; \
		TestQueryQueryAccessor##CType7 CName7; \
		TestQueryQueryAccessor##CType8 CName8; \
		TestQueryQueryAccessor##CType9 CName9; \
		TestQueryQueryAccessor##CType10 CName10; \
		TestQueryQueryAccessor##CType11 CName11; \
		TestQueryQueryAccessor##CType12 CName12; \
		TestQueryQueryAccessor##CType13 CName13; \
		TestQueryQueryAccessor##CType14 CName14; \
		TestQueryQueryAccessor##CType15 CName15; \
		TestQueryQueryAccessor##CType16 CName16; \
		TestQueryQueryAccessor##CType17 CName17; \
		TestQueryQueryAccessor##CType18 CName18; \
		TestQueryQueryAccessor##CType19 CName19; \
		TestQueryQueryAccessor##CType20 CName20; \
		TestQueryQueryAccessor##CType21 CName21; \
		TestQueryQueryAccessor##CType22 CName22; \
		TestQueryQueryAccessor##CType23 CName23; \
		TestQueryQueryAccessor##CType24 CName24; \
		TestQueryQueryAccessor##CType25 CName25; \
		TestQueryQueryAccessor##CType26 CName26; \
		TestQueryQueryAccessor##CType27 CName27; \
		TestQueryQueryAccessor##CType28 CName28; \
		TestQueryQueryAccessor##CType29 CName29; \
		TestQueryQueryAccessor##CType30 CName30; \
\
		TestQuery& LeftParan(void) {Query::LeftParan(); return *this;}; \
		TestQuery& Or(void) {Query::Or(); return *this;}; \
		TestQuery& RightParan(void) {Query::RightParan(); return *this;}; \
		TestQuery& Subtable(size_t column) {Query::Subtable(column); return *this;}; \
		TestQuery& Parent() {Query::Parent(); return *this;}; \
	}; \
\
	TestQuery GetQuery() {return TestQuery();} \
\
	class Cursor : public CursorBase { \
	public: \
		Cursor(TableName& table, size_t ndx) : CursorBase(table, ndx) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
			CName17.Create(this, 16); \
			CName18.Create(this, 17); \
			CName19.Create(this, 18); \
			CName20.Create(this, 19); \
			CName21.Create(this, 20); \
			CName22.Create(this, 21); \
			CName23.Create(this, 22); \
			CName24.Create(this, 23); \
			CName25.Create(this, 24); \
			CName26.Create(this, 25); \
			CName27.Create(this, 26); \
			CName28.Create(this, 27); \
			CName29.Create(this, 28); \
			CName30.Create(this, 29); \
		} \
		Cursor(const TableName& table, size_t ndx) : CursorBase(const_cast<TableName&>(table), ndx) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
			CName17.Create(this, 16); \
			CName18.Create(this, 17); \
			CName19.Create(this, 18); \
			CName20.Create(this, 19); \
			CName21.Create(this, 20); \
			CName22.Create(this, 21); \
			CName23.Create(this, 22); \
			CName24.Create(this, 23); \
			CName25.Create(this, 24); \
			CName26.Create(this, 25); \
			CName27.Create(this, 26); \
			CName28.Create(this, 27); \
			CName29.Create(this, 28); \
			CName30.Create(this, 29); \
		} \
		Cursor(const Cursor& v) : CursorBase(v) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
			CName17.Create(this, 16); \
			CName18.Create(this, 17); \
			CName19.Create(this, 18); \
			CName20.Create(this, 19); \
			CName21.Create(this, 20); \
			CName22.Create(this, 21); \
			CName23.Create(this, 22); \
			CName24.Create(this, 23); \
			CName25.Create(this, 24); \
			CName26.Create(this, 25); \
			CName27.Create(this, 26); \
			CName28.Create(this, 27); \
			CName29.Create(this, 28); \
			CName30.Create(this, 29); \
		} \
		Accessor##CType1 CName1; \
		Accessor##CType2 CName2; \
		Accessor##CType3 CName3; \
		Accessor##CType4 CName4; \
		Accessor##CType5 CName5; \
		Accessor##CType6 CName6; \
		Accessor##CType7 CName7; \
		Accessor##CType8 CName8; \
		Accessor##CType9 CName9; \
		Accessor##CType10 CName10; \
		Accessor##CType11 CName11; \
		Accessor##CType12 CName12; \
		Accessor##CType13 CName13; \
		Accessor##CType14 CName14; \
		Accessor##CType15 CName15; \
		Accessor##CType16 CName16; \
		Accessor##CType17 CName17; \
		Accessor##CType18 CName18; \
		Accessor##CType19 CName19; \
		Accessor##CType20 CName20; \
		Accessor##CType21 CName21; \
		Accessor##CType22 CName22; \
		Accessor##CType23 CName23; \
		Accessor##CType24 CName24; \
		Accessor##CType25 CName25; \
		Accessor##CType26 CName26; \
		Accessor##CType27 CName27; \
		Accessor##CType28 CName28; \
		Accessor##CType29 CName29; \
		Accessor##CType30 CName30; \
	}; \
\
	void Add(tdbType##CType1 CName1, tdbType##CType2 CName2, tdbType##CType3 CName3, tdbType##CType4 CName4, tdbType##CType5 CName5, tdbType##CType6 CName6, tdbType##CType7 CName7, tdbType##CType8 CName8, tdbType##CType9 CName9, tdbType##CType10 CName10, tdbType##CType11 CName11, tdbType##CType12 CName12, tdbType##CType13 CName13, tdbType##CType14 CName14, tdbType##CType15 CName15, tdbType##CType16 CName16, tdbType##CType17 CName17, tdbType##CType18 CName18, tdbType##CType19 CName19, tdbType##CType20 CName20, tdbType##CType21 CName21, tdbType##CType22 CName22, tdbType##CType23 CName23, tdbType##CType24 CName24, tdbType##CType25 CName25, tdbType##CType26 CName26, tdbType##CType27 CName27, tdbType##CType28 CName28, tdbType##CType29 CName29, tdbType##CType30 CName30) { \
		const size_t ndx = GetSize(); \
		Insert##CType1 (0, ndx, CName1); \
		Insert##CType2 (1, ndx, CName2); \
		Insert##CType3 (2, ndx, CName3); \
		Insert##CType4 (3, ndx, CName4); \
		Insert##CType5 (4, ndx, CName5); \
		Insert##CType6 (5, ndx, CName6); \
		Insert##CType7 (6, ndx, CName7); \
		Insert##CType8 (7, ndx, CName8); \
		Insert##CType9 (8, ndx, CName9); \
		Insert##CType10 (9, ndx, CName10); \
		Insert##CType11 (10, ndx, CName11); \
		Insert##CType12 (11, ndx, CName12); \
		Insert##CType13 (12, ndx, CName13); \
		Insert##CType14 (13, ndx, CName14); \
		Insert##CType15 (14, ndx, CName15); \
		Insert##CType16 (15, ndx, CName16); \
		Insert##CType17 (16, ndx, CName17); \
		Insert##CType18 (17, ndx, CName18); \
		Insert##CType19 (18, ndx, CName19); \
		Insert##CType20 (19, ndx, CName20); \
		Insert##CType21 (20, ndx, CName21); \
		Insert##CType22 (21, ndx, CName22); \
		Insert##CType23 (22, ndx, CName23); \
		Insert##CType24 (23, ndx, CName24); \
		Insert##CType25 (24, ndx, CName25); \
		Insert##CType26 (25, ndx, CName26); \
		Insert##CType27 (26, ndx, CName27); \
		Insert##CType28 (27, ndx, CName28); \
		Insert##CType29 (28, ndx, CName29); \
		Insert##CType30 (29, ndx, CName30); \
		InsertDone(); \
	} \
\
	void Insert(size_t ndx, tdbType##CType1 CName1, tdbType##CType2 CName2, tdbType##CType3 CName3, tdbType##CType4 CName4, tdbType##CType5 CName5, tdbType##CType6 CName6, tdbType##CType7 CName7, tdbType##CType8 CName8, tdbType##CType9 CName9, tdbType##CType10 CName10, tdbType##CType11 CName11, tdbType##CType12 CName12, tdbType##CType13 CName13, tdbType##CType14 CName14, tdbType##CType15 CName15, tdbType##CType16 CName16, tdbType##CType17 CName17, tdbType##CType18 CName18, tdbType##CType19 CName19, tdbType##CType20 CName20, tdbType##CType21 CName21, tdbType##CType22 CName22, tdbType##CType23 CName23, tdbType##CType24 CName24, tdbType##CType25 CName25, tdbType##CType26 CName26, tdbType##CType27 CName27, tdbType##CType28 CName28, tdbType##CType29 CName29, tdbType##CType30 CName30) { \
		Insert##CType1 (0, ndx, CName1); \
		Insert##CType2 (1, ndx, CName2); \
		Insert##CType3 (2, ndx, CName3); \
		Insert##CType4 (3, ndx, CName4); \
		Insert##CType5 (4, ndx, CName5); \
		Insert##CType6 (5, ndx, CName6); \
		Insert##CType7 (6, ndx, CName7); \
		Insert##CType8 (7, ndx, CName8); \
		Insert##CType9 (8, ndx, CName9); \
		Insert##CType10 (9, ndx, CName10); \
		Insert##CType11 (10, ndx, CName11); \
		Insert##CType12 (11, ndx, CName12); \
		Insert##CType13 (12, ndx, CName13); \
		Insert##CType14 (13, ndx, CName14); \
		Insert##CType15 (14, ndx, CName15); \
		Insert##CType16 (15, ndx, CName16); \
		Insert##CType17 (16, ndx, CName17); \
		Insert##CType18 (17, ndx, CName18); \
		Insert##CType19 (18, ndx, CName19); \
		Insert##CType20 (19, ndx, CName20); \
		Insert##CType21 (20, ndx, CName21); \
		Insert##CType22 (21, ndx, CName22); \
		Insert##CType23 (22, ndx, CName23); \
		Insert##CType24 (23, ndx, CName24); \
		Insert##CType25 (24, ndx, CName25); \
		Insert##CType26 (25, ndx, CName26); \
		Insert##CType27 (26, ndx, CName27); \
		Insert##CType28 (27, ndx, CName28); \
		Insert##CType29 (28, ndx, CName29); \
		Insert##CType30 (29, ndx, CName30); \
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
	ColumnProxy##CType3 CName3; \
	ColumnProxy##CType4 CName4; \
	ColumnProxy##CType5 CName5; \
	ColumnProxy##CType6 CName6; \
	ColumnProxy##CType7 CName7; \
	ColumnProxy##CType8 CName8; \
	ColumnProxy##CType9 CName9; \
	ColumnProxy##CType10 CName10; \
	ColumnProxy##CType11 CName11; \
	ColumnProxy##CType12 CName12; \
	ColumnProxy##CType13 CName13; \
	ColumnProxy##CType14 CName14; \
	ColumnProxy##CType15 CName15; \
	ColumnProxy##CType16 CName16; \
	ColumnProxy##CType17 CName17; \
	ColumnProxy##CType18 CName18; \
	ColumnProxy##CType19 CName19; \
	ColumnProxy##CType20 CName20; \
	ColumnProxy##CType21 CName21; \
	ColumnProxy##CType22 CName22; \
	ColumnProxy##CType23 CName23; \
	ColumnProxy##CType24 CName24; \
	ColumnProxy##CType25 CName25; \
	ColumnProxy##CType26 CName26; \
	ColumnProxy##CType27 CName27; \
	ColumnProxy##CType28 CName28; \
	ColumnProxy##CType29 CName29; \
	ColumnProxy##CType30 CName30; \
\
protected: \
	friend class Group; \
	TableName(Allocator& alloc, size_t ref, Array* parent, size_t pndx) : TopLevelTable(alloc, ref, parent, pndx) {}; \
\
private: \
	TableName(const TableName&) {} \
	TableName& operator=(const TableName&) {return *this;} \
};



#define TDB_TABLE_31(TableName, CType1, CName1, CType2, CName2, CType3, CName3, CType4, CName4, CType5, CName5, CType6, CName6, CType7, CName7, CType8, CName8, CType9, CName9, CType10, CName10, CType11, CName11, CType12, CName12, CType13, CName13, CType14, CName14, CType15, CName15, CType16, CName16, CType17, CName17, CType18, CName18, CType19, CName19, CType20, CName20, CType21, CName21, CType22, CName22, CType23, CName23, CType24, CName24, CType25, CName25, CType26, CName26, CType27, CName27, CType28, CName28, CType29, CName29, CType30, CName30, CType31, CName31) \
class TableName##Query { \
protected: \
	QueryAccessor##CType1 CName1; \
	QueryAccessor##CType2 CName2; \
	QueryAccessor##CType3 CName3; \
	QueryAccessor##CType4 CName4; \
	QueryAccessor##CType5 CName5; \
	QueryAccessor##CType6 CName6; \
	QueryAccessor##CType7 CName7; \
	QueryAccessor##CType8 CName8; \
	QueryAccessor##CType9 CName9; \
	QueryAccessor##CType10 CName10; \
	QueryAccessor##CType11 CName11; \
	QueryAccessor##CType12 CName12; \
	QueryAccessor##CType13 CName13; \
	QueryAccessor##CType14 CName14; \
	QueryAccessor##CType15 CName15; \
	QueryAccessor##CType16 CName16; \
	QueryAccessor##CType17 CName17; \
	QueryAccessor##CType18 CName18; \
	QueryAccessor##CType19 CName19; \
	QueryAccessor##CType20 CName20; \
	QueryAccessor##CType21 CName21; \
	QueryAccessor##CType22 CName22; \
	QueryAccessor##CType23 CName23; \
	QueryAccessor##CType24 CName24; \
	QueryAccessor##CType25 CName25; \
	QueryAccessor##CType26 CName26; \
	QueryAccessor##CType27 CName27; \
	QueryAccessor##CType28 CName28; \
	QueryAccessor##CType29 CName29; \
	QueryAccessor##CType30 CName30; \
	QueryAccessor##CType31 CName31; \
}; \
\
class TableName : public TopLevelTable { \
public: \
	TableName(Allocator& alloc=GetDefaultAllocator()) : TopLevelTable(alloc) { \
		RegisterColumn(Accessor##CType1::type, #CName1); \
		RegisterColumn(Accessor##CType2::type, #CName2); \
		RegisterColumn(Accessor##CType3::type, #CName3); \
		RegisterColumn(Accessor##CType4::type, #CName4); \
		RegisterColumn(Accessor##CType5::type, #CName5); \
		RegisterColumn(Accessor##CType6::type, #CName6); \
		RegisterColumn(Accessor##CType7::type, #CName7); \
		RegisterColumn(Accessor##CType8::type, #CName8); \
		RegisterColumn(Accessor##CType9::type, #CName9); \
		RegisterColumn(Accessor##CType10::type, #CName10); \
		RegisterColumn(Accessor##CType11::type, #CName11); \
		RegisterColumn(Accessor##CType12::type, #CName12); \
		RegisterColumn(Accessor##CType13::type, #CName13); \
		RegisterColumn(Accessor##CType14::type, #CName14); \
		RegisterColumn(Accessor##CType15::type, #CName15); \
		RegisterColumn(Accessor##CType16::type, #CName16); \
		RegisterColumn(Accessor##CType17::type, #CName17); \
		RegisterColumn(Accessor##CType18::type, #CName18); \
		RegisterColumn(Accessor##CType19::type, #CName19); \
		RegisterColumn(Accessor##CType20::type, #CName20); \
		RegisterColumn(Accessor##CType21::type, #CName21); \
		RegisterColumn(Accessor##CType22::type, #CName22); \
		RegisterColumn(Accessor##CType23::type, #CName23); \
		RegisterColumn(Accessor##CType24::type, #CName24); \
		RegisterColumn(Accessor##CType25::type, #CName25); \
		RegisterColumn(Accessor##CType26::type, #CName26); \
		RegisterColumn(Accessor##CType27::type, #CName27); \
		RegisterColumn(Accessor##CType28::type, #CName28); \
		RegisterColumn(Accessor##CType29::type, #CName29); \
		RegisterColumn(Accessor##CType30::type, #CName30); \
		RegisterColumn(Accessor##CType31::type, #CName31); \
\
		CName1.Create(this, 0); \
		CName2.Create(this, 1); \
		CName3.Create(this, 2); \
		CName4.Create(this, 3); \
		CName5.Create(this, 4); \
		CName6.Create(this, 5); \
		CName7.Create(this, 6); \
		CName8.Create(this, 7); \
		CName9.Create(this, 8); \
		CName10.Create(this, 9); \
		CName11.Create(this, 10); \
		CName12.Create(this, 11); \
		CName13.Create(this, 12); \
		CName14.Create(this, 13); \
		CName15.Create(this, 14); \
		CName16.Create(this, 15); \
		CName17.Create(this, 16); \
		CName18.Create(this, 17); \
		CName19.Create(this, 18); \
		CName20.Create(this, 19); \
		CName21.Create(this, 20); \
		CName22.Create(this, 21); \
		CName23.Create(this, 22); \
		CName24.Create(this, 23); \
		CName25.Create(this, 24); \
		CName26.Create(this, 25); \
		CName27.Create(this, 26); \
		CName28.Create(this, 27); \
		CName29.Create(this, 28); \
		CName30.Create(this, 29); \
		CName31.Create(this, 30); \
	}; \
\
	class TestQuery : public Query { \
	public: \
		TestQuery() : CName1(0), CName2(1), CName3(2), CName4(3), CName5(4), CName6(5), CName7(6), CName8(7), CName9(8), CName10(9), CName11(10), CName12(11), CName13(12), CName14(13), CName15(14), CName16(15), CName17(16), CName18(17), CName19(18), CName20(19), CName21(20), CName22(21), CName23(22), CName24(23), CName25(24), CName26(25), CName27(26), CName28(27), CName29(28), CName30(29), CName31(30) { \
			CName1.SetQuery(this); \
			CName2.SetQuery(this); \
			CName3.SetQuery(this); \
			CName4.SetQuery(this); \
			CName5.SetQuery(this); \
			CName6.SetQuery(this); \
			CName7.SetQuery(this); \
			CName8.SetQuery(this); \
			CName9.SetQuery(this); \
			CName10.SetQuery(this); \
			CName11.SetQuery(this); \
			CName12.SetQuery(this); \
			CName13.SetQuery(this); \
			CName14.SetQuery(this); \
			CName15.SetQuery(this); \
			CName16.SetQuery(this); \
			CName17.SetQuery(this); \
			CName18.SetQuery(this); \
			CName19.SetQuery(this); \
			CName20.SetQuery(this); \
			CName21.SetQuery(this); \
			CName22.SetQuery(this); \
			CName23.SetQuery(this); \
			CName24.SetQuery(this); \
			CName25.SetQuery(this); \
			CName26.SetQuery(this); \
			CName27.SetQuery(this); \
			CName28.SetQuery(this); \
			CName29.SetQuery(this); \
			CName30.SetQuery(this); \
			CName31.SetQuery(this); \
		} \
\
		TestQuery(const TestQuery& copy) : Query(copy), CName1(0), CName2(1), CName3(2), CName4(3), CName5(4), CName6(5), CName7(6), CName8(7), CName9(8), CName10(9), CName11(10), CName12(11), CName13(12), CName14(13), CName15(14), CName16(15), CName17(16), CName18(17), CName19(18), CName20(19), CName21(20), CName22(21), CName23(22), CName24(23), CName25(24), CName26(25), CName27(26), CName28(27), CName29(28), CName30(29), CName31(30) { \
			CName1.SetQuery(this); \
			CName2.SetQuery(this); \
			CName3.SetQuery(this); \
			CName4.SetQuery(this); \
			CName5.SetQuery(this); \
			CName6.SetQuery(this); \
			CName7.SetQuery(this); \
			CName8.SetQuery(this); \
			CName9.SetQuery(this); \
			CName10.SetQuery(this); \
			CName11.SetQuery(this); \
			CName12.SetQuery(this); \
			CName13.SetQuery(this); \
			CName14.SetQuery(this); \
			CName15.SetQuery(this); \
			CName16.SetQuery(this); \
			CName17.SetQuery(this); \
			CName18.SetQuery(this); \
			CName19.SetQuery(this); \
			CName20.SetQuery(this); \
			CName21.SetQuery(this); \
			CName22.SetQuery(this); \
			CName23.SetQuery(this); \
			CName24.SetQuery(this); \
			CName25.SetQuery(this); \
			CName26.SetQuery(this); \
			CName27.SetQuery(this); \
			CName28.SetQuery(this); \
			CName29.SetQuery(this); \
			CName30.SetQuery(this); \
			CName31.SetQuery(this); \
		} \
\
		class TestQueryQueryAccessorInt : private XQueryAccessorInt { \
		public: \
			TestQueryQueryAccessorInt(size_t column_id) : XQueryAccessorInt(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(int64_t value) {return (TestQuery &)XQueryAccessorInt::Equal(value);} \
			TestQuery& NotEqual(int64_t value) {return (TestQuery &)XQueryAccessorInt::NotEqual(value);} \
			TestQuery& Greater(int64_t value) {return (TestQuery &)XQueryAccessorInt::Greater(value);} \
			TestQuery& Less(int64_t value) {return (TestQuery &)XQueryAccessorInt::Less(value);} \
			TestQuery& Between(int64_t from, int64_t to) {return (TestQuery &)XQueryAccessorInt::Between(from, to);} \
		}; \
\
		template <class T> class TestQueryQueryAccessorEnum : public TestQueryQueryAccessorInt { \
		public: \
			TestQueryQueryAccessorEnum<T>(size_t column_id) : TestQueryQueryAccessorInt(column_id) {} \
		}; \
\
		class TestQueryQueryAccessorString : private XQueryAccessorString { \
		public: \
			TestQueryQueryAccessorString(size_t column_id) : XQueryAccessorString(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::Equal(value, CaseSensitive);} \
			TestQuery& NotEqual(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::NotEqual(value, CaseSensitive);} \
			TestQuery& BeginsWith(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::BeginsWith(value, CaseSensitive);} \
			TestQuery& EndsWith(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::EndsWith(value, CaseSensitive);} \
			TestQuery& Contains(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::Contains(value, CaseSensitive);} \
		}; \
\
		class TestQueryQueryAccessorBool : private XQueryAccessorBool { \
		public: \
			TestQueryQueryAccessorBool(size_t column_id) : XQueryAccessorBool(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(bool value) {return (TestQuery &)XQueryAccessorBool::Equal(value);} \
		}; \
\
		TestQueryQueryAccessor##CType1 CName1; \
		TestQueryQueryAccessor##CType2 CName2; \
		TestQueryQueryAccessor##CType3 CName3; \
		TestQueryQueryAccessor##CType4 CName4; \
		TestQueryQueryAccessor##CType5 CName5; \
		TestQueryQueryAccessor##CType6 CName6; \
		TestQueryQueryAccessor##CType7 CName7; \
		TestQueryQueryAccessor##CType8 CName8; \
		TestQueryQueryAccessor##CType9 CName9; \
		TestQueryQueryAccessor##CType10 CName10; \
		TestQueryQueryAccessor##CType11 CName11; \
		TestQueryQueryAccessor##CType12 CName12; \
		TestQueryQueryAccessor##CType13 CName13; \
		TestQueryQueryAccessor##CType14 CName14; \
		TestQueryQueryAccessor##CType15 CName15; \
		TestQueryQueryAccessor##CType16 CName16; \
		TestQueryQueryAccessor##CType17 CName17; \
		TestQueryQueryAccessor##CType18 CName18; \
		TestQueryQueryAccessor##CType19 CName19; \
		TestQueryQueryAccessor##CType20 CName20; \
		TestQueryQueryAccessor##CType21 CName21; \
		TestQueryQueryAccessor##CType22 CName22; \
		TestQueryQueryAccessor##CType23 CName23; \
		TestQueryQueryAccessor##CType24 CName24; \
		TestQueryQueryAccessor##CType25 CName25; \
		TestQueryQueryAccessor##CType26 CName26; \
		TestQueryQueryAccessor##CType27 CName27; \
		TestQueryQueryAccessor##CType28 CName28; \
		TestQueryQueryAccessor##CType29 CName29; \
		TestQueryQueryAccessor##CType30 CName30; \
		TestQueryQueryAccessor##CType31 CName31; \
\
		TestQuery& LeftParan(void) {Query::LeftParan(); return *this;}; \
		TestQuery& Or(void) {Query::Or(); return *this;}; \
		TestQuery& RightParan(void) {Query::RightParan(); return *this;}; \
		TestQuery& Subtable(size_t column) {Query::Subtable(column); return *this;}; \
		TestQuery& Parent() {Query::Parent(); return *this;}; \
	}; \
\
	TestQuery GetQuery() {return TestQuery();} \
\
	class Cursor : public CursorBase { \
	public: \
		Cursor(TableName& table, size_t ndx) : CursorBase(table, ndx) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
			CName17.Create(this, 16); \
			CName18.Create(this, 17); \
			CName19.Create(this, 18); \
			CName20.Create(this, 19); \
			CName21.Create(this, 20); \
			CName22.Create(this, 21); \
			CName23.Create(this, 22); \
			CName24.Create(this, 23); \
			CName25.Create(this, 24); \
			CName26.Create(this, 25); \
			CName27.Create(this, 26); \
			CName28.Create(this, 27); \
			CName29.Create(this, 28); \
			CName30.Create(this, 29); \
			CName31.Create(this, 30); \
		} \
		Cursor(const TableName& table, size_t ndx) : CursorBase(const_cast<TableName&>(table), ndx) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
			CName17.Create(this, 16); \
			CName18.Create(this, 17); \
			CName19.Create(this, 18); \
			CName20.Create(this, 19); \
			CName21.Create(this, 20); \
			CName22.Create(this, 21); \
			CName23.Create(this, 22); \
			CName24.Create(this, 23); \
			CName25.Create(this, 24); \
			CName26.Create(this, 25); \
			CName27.Create(this, 26); \
			CName28.Create(this, 27); \
			CName29.Create(this, 28); \
			CName30.Create(this, 29); \
			CName31.Create(this, 30); \
		} \
		Cursor(const Cursor& v) : CursorBase(v) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
			CName17.Create(this, 16); \
			CName18.Create(this, 17); \
			CName19.Create(this, 18); \
			CName20.Create(this, 19); \
			CName21.Create(this, 20); \
			CName22.Create(this, 21); \
			CName23.Create(this, 22); \
			CName24.Create(this, 23); \
			CName25.Create(this, 24); \
			CName26.Create(this, 25); \
			CName27.Create(this, 26); \
			CName28.Create(this, 27); \
			CName29.Create(this, 28); \
			CName30.Create(this, 29); \
			CName31.Create(this, 30); \
		} \
		Accessor##CType1 CName1; \
		Accessor##CType2 CName2; \
		Accessor##CType3 CName3; \
		Accessor##CType4 CName4; \
		Accessor##CType5 CName5; \
		Accessor##CType6 CName6; \
		Accessor##CType7 CName7; \
		Accessor##CType8 CName8; \
		Accessor##CType9 CName9; \
		Accessor##CType10 CName10; \
		Accessor##CType11 CName11; \
		Accessor##CType12 CName12; \
		Accessor##CType13 CName13; \
		Accessor##CType14 CName14; \
		Accessor##CType15 CName15; \
		Accessor##CType16 CName16; \
		Accessor##CType17 CName17; \
		Accessor##CType18 CName18; \
		Accessor##CType19 CName19; \
		Accessor##CType20 CName20; \
		Accessor##CType21 CName21; \
		Accessor##CType22 CName22; \
		Accessor##CType23 CName23; \
		Accessor##CType24 CName24; \
		Accessor##CType25 CName25; \
		Accessor##CType26 CName26; \
		Accessor##CType27 CName27; \
		Accessor##CType28 CName28; \
		Accessor##CType29 CName29; \
		Accessor##CType30 CName30; \
		Accessor##CType31 CName31; \
	}; \
\
	void Add(tdbType##CType1 CName1, tdbType##CType2 CName2, tdbType##CType3 CName3, tdbType##CType4 CName4, tdbType##CType5 CName5, tdbType##CType6 CName6, tdbType##CType7 CName7, tdbType##CType8 CName8, tdbType##CType9 CName9, tdbType##CType10 CName10, tdbType##CType11 CName11, tdbType##CType12 CName12, tdbType##CType13 CName13, tdbType##CType14 CName14, tdbType##CType15 CName15, tdbType##CType16 CName16, tdbType##CType17 CName17, tdbType##CType18 CName18, tdbType##CType19 CName19, tdbType##CType20 CName20, tdbType##CType21 CName21, tdbType##CType22 CName22, tdbType##CType23 CName23, tdbType##CType24 CName24, tdbType##CType25 CName25, tdbType##CType26 CName26, tdbType##CType27 CName27, tdbType##CType28 CName28, tdbType##CType29 CName29, tdbType##CType30 CName30, tdbType##CType31 CName31) { \
		const size_t ndx = GetSize(); \
		Insert##CType1 (0, ndx, CName1); \
		Insert##CType2 (1, ndx, CName2); \
		Insert##CType3 (2, ndx, CName3); \
		Insert##CType4 (3, ndx, CName4); \
		Insert##CType5 (4, ndx, CName5); \
		Insert##CType6 (5, ndx, CName6); \
		Insert##CType7 (6, ndx, CName7); \
		Insert##CType8 (7, ndx, CName8); \
		Insert##CType9 (8, ndx, CName9); \
		Insert##CType10 (9, ndx, CName10); \
		Insert##CType11 (10, ndx, CName11); \
		Insert##CType12 (11, ndx, CName12); \
		Insert##CType13 (12, ndx, CName13); \
		Insert##CType14 (13, ndx, CName14); \
		Insert##CType15 (14, ndx, CName15); \
		Insert##CType16 (15, ndx, CName16); \
		Insert##CType17 (16, ndx, CName17); \
		Insert##CType18 (17, ndx, CName18); \
		Insert##CType19 (18, ndx, CName19); \
		Insert##CType20 (19, ndx, CName20); \
		Insert##CType21 (20, ndx, CName21); \
		Insert##CType22 (21, ndx, CName22); \
		Insert##CType23 (22, ndx, CName23); \
		Insert##CType24 (23, ndx, CName24); \
		Insert##CType25 (24, ndx, CName25); \
		Insert##CType26 (25, ndx, CName26); \
		Insert##CType27 (26, ndx, CName27); \
		Insert##CType28 (27, ndx, CName28); \
		Insert##CType29 (28, ndx, CName29); \
		Insert##CType30 (29, ndx, CName30); \
		Insert##CType31 (30, ndx, CName31); \
		InsertDone(); \
	} \
\
	void Insert(size_t ndx, tdbType##CType1 CName1, tdbType##CType2 CName2, tdbType##CType3 CName3, tdbType##CType4 CName4, tdbType##CType5 CName5, tdbType##CType6 CName6, tdbType##CType7 CName7, tdbType##CType8 CName8, tdbType##CType9 CName9, tdbType##CType10 CName10, tdbType##CType11 CName11, tdbType##CType12 CName12, tdbType##CType13 CName13, tdbType##CType14 CName14, tdbType##CType15 CName15, tdbType##CType16 CName16, tdbType##CType17 CName17, tdbType##CType18 CName18, tdbType##CType19 CName19, tdbType##CType20 CName20, tdbType##CType21 CName21, tdbType##CType22 CName22, tdbType##CType23 CName23, tdbType##CType24 CName24, tdbType##CType25 CName25, tdbType##CType26 CName26, tdbType##CType27 CName27, tdbType##CType28 CName28, tdbType##CType29 CName29, tdbType##CType30 CName30, tdbType##CType31 CName31) { \
		Insert##CType1 (0, ndx, CName1); \
		Insert##CType2 (1, ndx, CName2); \
		Insert##CType3 (2, ndx, CName3); \
		Insert##CType4 (3, ndx, CName4); \
		Insert##CType5 (4, ndx, CName5); \
		Insert##CType6 (5, ndx, CName6); \
		Insert##CType7 (6, ndx, CName7); \
		Insert##CType8 (7, ndx, CName8); \
		Insert##CType9 (8, ndx, CName9); \
		Insert##CType10 (9, ndx, CName10); \
		Insert##CType11 (10, ndx, CName11); \
		Insert##CType12 (11, ndx, CName12); \
		Insert##CType13 (12, ndx, CName13); \
		Insert##CType14 (13, ndx, CName14); \
		Insert##CType15 (14, ndx, CName15); \
		Insert##CType16 (15, ndx, CName16); \
		Insert##CType17 (16, ndx, CName17); \
		Insert##CType18 (17, ndx, CName18); \
		Insert##CType19 (18, ndx, CName19); \
		Insert##CType20 (19, ndx, CName20); \
		Insert##CType21 (20, ndx, CName21); \
		Insert##CType22 (21, ndx, CName22); \
		Insert##CType23 (22, ndx, CName23); \
		Insert##CType24 (23, ndx, CName24); \
		Insert##CType25 (24, ndx, CName25); \
		Insert##CType26 (25, ndx, CName26); \
		Insert##CType27 (26, ndx, CName27); \
		Insert##CType28 (27, ndx, CName28); \
		Insert##CType29 (28, ndx, CName29); \
		Insert##CType30 (29, ndx, CName30); \
		Insert##CType31 (30, ndx, CName31); \
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
	ColumnProxy##CType3 CName3; \
	ColumnProxy##CType4 CName4; \
	ColumnProxy##CType5 CName5; \
	ColumnProxy##CType6 CName6; \
	ColumnProxy##CType7 CName7; \
	ColumnProxy##CType8 CName8; \
	ColumnProxy##CType9 CName9; \
	ColumnProxy##CType10 CName10; \
	ColumnProxy##CType11 CName11; \
	ColumnProxy##CType12 CName12; \
	ColumnProxy##CType13 CName13; \
	ColumnProxy##CType14 CName14; \
	ColumnProxy##CType15 CName15; \
	ColumnProxy##CType16 CName16; \
	ColumnProxy##CType17 CName17; \
	ColumnProxy##CType18 CName18; \
	ColumnProxy##CType19 CName19; \
	ColumnProxy##CType20 CName20; \
	ColumnProxy##CType21 CName21; \
	ColumnProxy##CType22 CName22; \
	ColumnProxy##CType23 CName23; \
	ColumnProxy##CType24 CName24; \
	ColumnProxy##CType25 CName25; \
	ColumnProxy##CType26 CName26; \
	ColumnProxy##CType27 CName27; \
	ColumnProxy##CType28 CName28; \
	ColumnProxy##CType29 CName29; \
	ColumnProxy##CType30 CName30; \
	ColumnProxy##CType31 CName31; \
\
protected: \
	friend class Group; \
	TableName(Allocator& alloc, size_t ref, Array* parent, size_t pndx) : TopLevelTable(alloc, ref, parent, pndx) {}; \
\
private: \
	TableName(const TableName&) {} \
	TableName& operator=(const TableName&) {return *this;} \
};



#define TDB_TABLE_32(TableName, CType1, CName1, CType2, CName2, CType3, CName3, CType4, CName4, CType5, CName5, CType6, CName6, CType7, CName7, CType8, CName8, CType9, CName9, CType10, CName10, CType11, CName11, CType12, CName12, CType13, CName13, CType14, CName14, CType15, CName15, CType16, CName16, CType17, CName17, CType18, CName18, CType19, CName19, CType20, CName20, CType21, CName21, CType22, CName22, CType23, CName23, CType24, CName24, CType25, CName25, CType26, CName26, CType27, CName27, CType28, CName28, CType29, CName29, CType30, CName30, CType31, CName31, CType32, CName32) \
class TableName##Query { \
protected: \
	QueryAccessor##CType1 CName1; \
	QueryAccessor##CType2 CName2; \
	QueryAccessor##CType3 CName3; \
	QueryAccessor##CType4 CName4; \
	QueryAccessor##CType5 CName5; \
	QueryAccessor##CType6 CName6; \
	QueryAccessor##CType7 CName7; \
	QueryAccessor##CType8 CName8; \
	QueryAccessor##CType9 CName9; \
	QueryAccessor##CType10 CName10; \
	QueryAccessor##CType11 CName11; \
	QueryAccessor##CType12 CName12; \
	QueryAccessor##CType13 CName13; \
	QueryAccessor##CType14 CName14; \
	QueryAccessor##CType15 CName15; \
	QueryAccessor##CType16 CName16; \
	QueryAccessor##CType17 CName17; \
	QueryAccessor##CType18 CName18; \
	QueryAccessor##CType19 CName19; \
	QueryAccessor##CType20 CName20; \
	QueryAccessor##CType21 CName21; \
	QueryAccessor##CType22 CName22; \
	QueryAccessor##CType23 CName23; \
	QueryAccessor##CType24 CName24; \
	QueryAccessor##CType25 CName25; \
	QueryAccessor##CType26 CName26; \
	QueryAccessor##CType27 CName27; \
	QueryAccessor##CType28 CName28; \
	QueryAccessor##CType29 CName29; \
	QueryAccessor##CType30 CName30; \
	QueryAccessor##CType31 CName31; \
	QueryAccessor##CType32 CName32; \
}; \
\
class TableName : public TopLevelTable { \
public: \
	TableName(Allocator& alloc=GetDefaultAllocator()) : TopLevelTable(alloc) { \
		RegisterColumn(Accessor##CType1::type, #CName1); \
		RegisterColumn(Accessor##CType2::type, #CName2); \
		RegisterColumn(Accessor##CType3::type, #CName3); \
		RegisterColumn(Accessor##CType4::type, #CName4); \
		RegisterColumn(Accessor##CType5::type, #CName5); \
		RegisterColumn(Accessor##CType6::type, #CName6); \
		RegisterColumn(Accessor##CType7::type, #CName7); \
		RegisterColumn(Accessor##CType8::type, #CName8); \
		RegisterColumn(Accessor##CType9::type, #CName9); \
		RegisterColumn(Accessor##CType10::type, #CName10); \
		RegisterColumn(Accessor##CType11::type, #CName11); \
		RegisterColumn(Accessor##CType12::type, #CName12); \
		RegisterColumn(Accessor##CType13::type, #CName13); \
		RegisterColumn(Accessor##CType14::type, #CName14); \
		RegisterColumn(Accessor##CType15::type, #CName15); \
		RegisterColumn(Accessor##CType16::type, #CName16); \
		RegisterColumn(Accessor##CType17::type, #CName17); \
		RegisterColumn(Accessor##CType18::type, #CName18); \
		RegisterColumn(Accessor##CType19::type, #CName19); \
		RegisterColumn(Accessor##CType20::type, #CName20); \
		RegisterColumn(Accessor##CType21::type, #CName21); \
		RegisterColumn(Accessor##CType22::type, #CName22); \
		RegisterColumn(Accessor##CType23::type, #CName23); \
		RegisterColumn(Accessor##CType24::type, #CName24); \
		RegisterColumn(Accessor##CType25::type, #CName25); \
		RegisterColumn(Accessor##CType26::type, #CName26); \
		RegisterColumn(Accessor##CType27::type, #CName27); \
		RegisterColumn(Accessor##CType28::type, #CName28); \
		RegisterColumn(Accessor##CType29::type, #CName29); \
		RegisterColumn(Accessor##CType30::type, #CName30); \
		RegisterColumn(Accessor##CType31::type, #CName31); \
		RegisterColumn(Accessor##CType32::type, #CName32); \
\
		CName1.Create(this, 0); \
		CName2.Create(this, 1); \
		CName3.Create(this, 2); \
		CName4.Create(this, 3); \
		CName5.Create(this, 4); \
		CName6.Create(this, 5); \
		CName7.Create(this, 6); \
		CName8.Create(this, 7); \
		CName9.Create(this, 8); \
		CName10.Create(this, 9); \
		CName11.Create(this, 10); \
		CName12.Create(this, 11); \
		CName13.Create(this, 12); \
		CName14.Create(this, 13); \
		CName15.Create(this, 14); \
		CName16.Create(this, 15); \
		CName17.Create(this, 16); \
		CName18.Create(this, 17); \
		CName19.Create(this, 18); \
		CName20.Create(this, 19); \
		CName21.Create(this, 20); \
		CName22.Create(this, 21); \
		CName23.Create(this, 22); \
		CName24.Create(this, 23); \
		CName25.Create(this, 24); \
		CName26.Create(this, 25); \
		CName27.Create(this, 26); \
		CName28.Create(this, 27); \
		CName29.Create(this, 28); \
		CName30.Create(this, 29); \
		CName31.Create(this, 30); \
		CName32.Create(this, 31); \
	}; \
\
	class TestQuery : public Query { \
	public: \
		TestQuery() : CName1(0), CName2(1), CName3(2), CName4(3), CName5(4), CName6(5), CName7(6), CName8(7), CName9(8), CName10(9), CName11(10), CName12(11), CName13(12), CName14(13), CName15(14), CName16(15), CName17(16), CName18(17), CName19(18), CName20(19), CName21(20), CName22(21), CName23(22), CName24(23), CName25(24), CName26(25), CName27(26), CName28(27), CName29(28), CName30(29), CName31(30), CName32(31) { \
			CName1.SetQuery(this); \
			CName2.SetQuery(this); \
			CName3.SetQuery(this); \
			CName4.SetQuery(this); \
			CName5.SetQuery(this); \
			CName6.SetQuery(this); \
			CName7.SetQuery(this); \
			CName8.SetQuery(this); \
			CName9.SetQuery(this); \
			CName10.SetQuery(this); \
			CName11.SetQuery(this); \
			CName12.SetQuery(this); \
			CName13.SetQuery(this); \
			CName14.SetQuery(this); \
			CName15.SetQuery(this); \
			CName16.SetQuery(this); \
			CName17.SetQuery(this); \
			CName18.SetQuery(this); \
			CName19.SetQuery(this); \
			CName20.SetQuery(this); \
			CName21.SetQuery(this); \
			CName22.SetQuery(this); \
			CName23.SetQuery(this); \
			CName24.SetQuery(this); \
			CName25.SetQuery(this); \
			CName26.SetQuery(this); \
			CName27.SetQuery(this); \
			CName28.SetQuery(this); \
			CName29.SetQuery(this); \
			CName30.SetQuery(this); \
			CName31.SetQuery(this); \
			CName32.SetQuery(this); \
		} \
\
		TestQuery(const TestQuery& copy) : Query(copy), CName1(0), CName2(1), CName3(2), CName4(3), CName5(4), CName6(5), CName7(6), CName8(7), CName9(8), CName10(9), CName11(10), CName12(11), CName13(12), CName14(13), CName15(14), CName16(15), CName17(16), CName18(17), CName19(18), CName20(19), CName21(20), CName22(21), CName23(22), CName24(23), CName25(24), CName26(25), CName27(26), CName28(27), CName29(28), CName30(29), CName31(30), CName32(31) { \
			CName1.SetQuery(this); \
			CName2.SetQuery(this); \
			CName3.SetQuery(this); \
			CName4.SetQuery(this); \
			CName5.SetQuery(this); \
			CName6.SetQuery(this); \
			CName7.SetQuery(this); \
			CName8.SetQuery(this); \
			CName9.SetQuery(this); \
			CName10.SetQuery(this); \
			CName11.SetQuery(this); \
			CName12.SetQuery(this); \
			CName13.SetQuery(this); \
			CName14.SetQuery(this); \
			CName15.SetQuery(this); \
			CName16.SetQuery(this); \
			CName17.SetQuery(this); \
			CName18.SetQuery(this); \
			CName19.SetQuery(this); \
			CName20.SetQuery(this); \
			CName21.SetQuery(this); \
			CName22.SetQuery(this); \
			CName23.SetQuery(this); \
			CName24.SetQuery(this); \
			CName25.SetQuery(this); \
			CName26.SetQuery(this); \
			CName27.SetQuery(this); \
			CName28.SetQuery(this); \
			CName29.SetQuery(this); \
			CName30.SetQuery(this); \
			CName31.SetQuery(this); \
			CName32.SetQuery(this); \
		} \
\
		class TestQueryQueryAccessorInt : private XQueryAccessorInt { \
		public: \
			TestQueryQueryAccessorInt(size_t column_id) : XQueryAccessorInt(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(int64_t value) {return (TestQuery &)XQueryAccessorInt::Equal(value);} \
			TestQuery& NotEqual(int64_t value) {return (TestQuery &)XQueryAccessorInt::NotEqual(value);} \
			TestQuery& Greater(int64_t value) {return (TestQuery &)XQueryAccessorInt::Greater(value);} \
			TestQuery& Less(int64_t value) {return (TestQuery &)XQueryAccessorInt::Less(value);} \
			TestQuery& Between(int64_t from, int64_t to) {return (TestQuery &)XQueryAccessorInt::Between(from, to);} \
		}; \
\
		template <class T> class TestQueryQueryAccessorEnum : public TestQueryQueryAccessorInt { \
		public: \
			TestQueryQueryAccessorEnum<T>(size_t column_id) : TestQueryQueryAccessorInt(column_id) {} \
		}; \
\
		class TestQueryQueryAccessorString : private XQueryAccessorString { \
		public: \
			TestQueryQueryAccessorString(size_t column_id) : XQueryAccessorString(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::Equal(value, CaseSensitive);} \
			TestQuery& NotEqual(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::NotEqual(value, CaseSensitive);} \
			TestQuery& BeginsWith(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::BeginsWith(value, CaseSensitive);} \
			TestQuery& EndsWith(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::EndsWith(value, CaseSensitive);} \
			TestQuery& Contains(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::Contains(value, CaseSensitive);} \
		}; \
\
		class TestQueryQueryAccessorBool : private XQueryAccessorBool { \
		public: \
			TestQueryQueryAccessorBool(size_t column_id) : XQueryAccessorBool(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(bool value) {return (TestQuery &)XQueryAccessorBool::Equal(value);} \
		}; \
\
		TestQueryQueryAccessor##CType1 CName1; \
		TestQueryQueryAccessor##CType2 CName2; \
		TestQueryQueryAccessor##CType3 CName3; \
		TestQueryQueryAccessor##CType4 CName4; \
		TestQueryQueryAccessor##CType5 CName5; \
		TestQueryQueryAccessor##CType6 CName6; \
		TestQueryQueryAccessor##CType7 CName7; \
		TestQueryQueryAccessor##CType8 CName8; \
		TestQueryQueryAccessor##CType9 CName9; \
		TestQueryQueryAccessor##CType10 CName10; \
		TestQueryQueryAccessor##CType11 CName11; \
		TestQueryQueryAccessor##CType12 CName12; \
		TestQueryQueryAccessor##CType13 CName13; \
		TestQueryQueryAccessor##CType14 CName14; \
		TestQueryQueryAccessor##CType15 CName15; \
		TestQueryQueryAccessor##CType16 CName16; \
		TestQueryQueryAccessor##CType17 CName17; \
		TestQueryQueryAccessor##CType18 CName18; \
		TestQueryQueryAccessor##CType19 CName19; \
		TestQueryQueryAccessor##CType20 CName20; \
		TestQueryQueryAccessor##CType21 CName21; \
		TestQueryQueryAccessor##CType22 CName22; \
		TestQueryQueryAccessor##CType23 CName23; \
		TestQueryQueryAccessor##CType24 CName24; \
		TestQueryQueryAccessor##CType25 CName25; \
		TestQueryQueryAccessor##CType26 CName26; \
		TestQueryQueryAccessor##CType27 CName27; \
		TestQueryQueryAccessor##CType28 CName28; \
		TestQueryQueryAccessor##CType29 CName29; \
		TestQueryQueryAccessor##CType30 CName30; \
		TestQueryQueryAccessor##CType31 CName31; \
		TestQueryQueryAccessor##CType32 CName32; \
\
		TestQuery& LeftParan(void) {Query::LeftParan(); return *this;}; \
		TestQuery& Or(void) {Query::Or(); return *this;}; \
		TestQuery& RightParan(void) {Query::RightParan(); return *this;}; \
		TestQuery& Subtable(size_t column) {Query::Subtable(column); return *this;}; \
		TestQuery& Parent() {Query::Parent(); return *this;}; \
	}; \
\
	TestQuery GetQuery() {return TestQuery();} \
\
	class Cursor : public CursorBase { \
	public: \
		Cursor(TableName& table, size_t ndx) : CursorBase(table, ndx) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
			CName17.Create(this, 16); \
			CName18.Create(this, 17); \
			CName19.Create(this, 18); \
			CName20.Create(this, 19); \
			CName21.Create(this, 20); \
			CName22.Create(this, 21); \
			CName23.Create(this, 22); \
			CName24.Create(this, 23); \
			CName25.Create(this, 24); \
			CName26.Create(this, 25); \
			CName27.Create(this, 26); \
			CName28.Create(this, 27); \
			CName29.Create(this, 28); \
			CName30.Create(this, 29); \
			CName31.Create(this, 30); \
			CName32.Create(this, 31); \
		} \
		Cursor(const TableName& table, size_t ndx) : CursorBase(const_cast<TableName&>(table), ndx) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
			CName17.Create(this, 16); \
			CName18.Create(this, 17); \
			CName19.Create(this, 18); \
			CName20.Create(this, 19); \
			CName21.Create(this, 20); \
			CName22.Create(this, 21); \
			CName23.Create(this, 22); \
			CName24.Create(this, 23); \
			CName25.Create(this, 24); \
			CName26.Create(this, 25); \
			CName27.Create(this, 26); \
			CName28.Create(this, 27); \
			CName29.Create(this, 28); \
			CName30.Create(this, 29); \
			CName31.Create(this, 30); \
			CName32.Create(this, 31); \
		} \
		Cursor(const Cursor& v) : CursorBase(v) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
			CName17.Create(this, 16); \
			CName18.Create(this, 17); \
			CName19.Create(this, 18); \
			CName20.Create(this, 19); \
			CName21.Create(this, 20); \
			CName22.Create(this, 21); \
			CName23.Create(this, 22); \
			CName24.Create(this, 23); \
			CName25.Create(this, 24); \
			CName26.Create(this, 25); \
			CName27.Create(this, 26); \
			CName28.Create(this, 27); \
			CName29.Create(this, 28); \
			CName30.Create(this, 29); \
			CName31.Create(this, 30); \
			CName32.Create(this, 31); \
		} \
		Accessor##CType1 CName1; \
		Accessor##CType2 CName2; \
		Accessor##CType3 CName3; \
		Accessor##CType4 CName4; \
		Accessor##CType5 CName5; \
		Accessor##CType6 CName6; \
		Accessor##CType7 CName7; \
		Accessor##CType8 CName8; \
		Accessor##CType9 CName9; \
		Accessor##CType10 CName10; \
		Accessor##CType11 CName11; \
		Accessor##CType12 CName12; \
		Accessor##CType13 CName13; \
		Accessor##CType14 CName14; \
		Accessor##CType15 CName15; \
		Accessor##CType16 CName16; \
		Accessor##CType17 CName17; \
		Accessor##CType18 CName18; \
		Accessor##CType19 CName19; \
		Accessor##CType20 CName20; \
		Accessor##CType21 CName21; \
		Accessor##CType22 CName22; \
		Accessor##CType23 CName23; \
		Accessor##CType24 CName24; \
		Accessor##CType25 CName25; \
		Accessor##CType26 CName26; \
		Accessor##CType27 CName27; \
		Accessor##CType28 CName28; \
		Accessor##CType29 CName29; \
		Accessor##CType30 CName30; \
		Accessor##CType31 CName31; \
		Accessor##CType32 CName32; \
	}; \
\
	void Add(tdbType##CType1 CName1, tdbType##CType2 CName2, tdbType##CType3 CName3, tdbType##CType4 CName4, tdbType##CType5 CName5, tdbType##CType6 CName6, tdbType##CType7 CName7, tdbType##CType8 CName8, tdbType##CType9 CName9, tdbType##CType10 CName10, tdbType##CType11 CName11, tdbType##CType12 CName12, tdbType##CType13 CName13, tdbType##CType14 CName14, tdbType##CType15 CName15, tdbType##CType16 CName16, tdbType##CType17 CName17, tdbType##CType18 CName18, tdbType##CType19 CName19, tdbType##CType20 CName20, tdbType##CType21 CName21, tdbType##CType22 CName22, tdbType##CType23 CName23, tdbType##CType24 CName24, tdbType##CType25 CName25, tdbType##CType26 CName26, tdbType##CType27 CName27, tdbType##CType28 CName28, tdbType##CType29 CName29, tdbType##CType30 CName30, tdbType##CType31 CName31, tdbType##CType32 CName32) { \
		const size_t ndx = GetSize(); \
		Insert##CType1 (0, ndx, CName1); \
		Insert##CType2 (1, ndx, CName2); \
		Insert##CType3 (2, ndx, CName3); \
		Insert##CType4 (3, ndx, CName4); \
		Insert##CType5 (4, ndx, CName5); \
		Insert##CType6 (5, ndx, CName6); \
		Insert##CType7 (6, ndx, CName7); \
		Insert##CType8 (7, ndx, CName8); \
		Insert##CType9 (8, ndx, CName9); \
		Insert##CType10 (9, ndx, CName10); \
		Insert##CType11 (10, ndx, CName11); \
		Insert##CType12 (11, ndx, CName12); \
		Insert##CType13 (12, ndx, CName13); \
		Insert##CType14 (13, ndx, CName14); \
		Insert##CType15 (14, ndx, CName15); \
		Insert##CType16 (15, ndx, CName16); \
		Insert##CType17 (16, ndx, CName17); \
		Insert##CType18 (17, ndx, CName18); \
		Insert##CType19 (18, ndx, CName19); \
		Insert##CType20 (19, ndx, CName20); \
		Insert##CType21 (20, ndx, CName21); \
		Insert##CType22 (21, ndx, CName22); \
		Insert##CType23 (22, ndx, CName23); \
		Insert##CType24 (23, ndx, CName24); \
		Insert##CType25 (24, ndx, CName25); \
		Insert##CType26 (25, ndx, CName26); \
		Insert##CType27 (26, ndx, CName27); \
		Insert##CType28 (27, ndx, CName28); \
		Insert##CType29 (28, ndx, CName29); \
		Insert##CType30 (29, ndx, CName30); \
		Insert##CType31 (30, ndx, CName31); \
		Insert##CType32 (31, ndx, CName32); \
		InsertDone(); \
	} \
\
	void Insert(size_t ndx, tdbType##CType1 CName1, tdbType##CType2 CName2, tdbType##CType3 CName3, tdbType##CType4 CName4, tdbType##CType5 CName5, tdbType##CType6 CName6, tdbType##CType7 CName7, tdbType##CType8 CName8, tdbType##CType9 CName9, tdbType##CType10 CName10, tdbType##CType11 CName11, tdbType##CType12 CName12, tdbType##CType13 CName13, tdbType##CType14 CName14, tdbType##CType15 CName15, tdbType##CType16 CName16, tdbType##CType17 CName17, tdbType##CType18 CName18, tdbType##CType19 CName19, tdbType##CType20 CName20, tdbType##CType21 CName21, tdbType##CType22 CName22, tdbType##CType23 CName23, tdbType##CType24 CName24, tdbType##CType25 CName25, tdbType##CType26 CName26, tdbType##CType27 CName27, tdbType##CType28 CName28, tdbType##CType29 CName29, tdbType##CType30 CName30, tdbType##CType31 CName31, tdbType##CType32 CName32) { \
		Insert##CType1 (0, ndx, CName1); \
		Insert##CType2 (1, ndx, CName2); \
		Insert##CType3 (2, ndx, CName3); \
		Insert##CType4 (3, ndx, CName4); \
		Insert##CType5 (4, ndx, CName5); \
		Insert##CType6 (5, ndx, CName6); \
		Insert##CType7 (6, ndx, CName7); \
		Insert##CType8 (7, ndx, CName8); \
		Insert##CType9 (8, ndx, CName9); \
		Insert##CType10 (9, ndx, CName10); \
		Insert##CType11 (10, ndx, CName11); \
		Insert##CType12 (11, ndx, CName12); \
		Insert##CType13 (12, ndx, CName13); \
		Insert##CType14 (13, ndx, CName14); \
		Insert##CType15 (14, ndx, CName15); \
		Insert##CType16 (15, ndx, CName16); \
		Insert##CType17 (16, ndx, CName17); \
		Insert##CType18 (17, ndx, CName18); \
		Insert##CType19 (18, ndx, CName19); \
		Insert##CType20 (19, ndx, CName20); \
		Insert##CType21 (20, ndx, CName21); \
		Insert##CType22 (21, ndx, CName22); \
		Insert##CType23 (22, ndx, CName23); \
		Insert##CType24 (23, ndx, CName24); \
		Insert##CType25 (24, ndx, CName25); \
		Insert##CType26 (25, ndx, CName26); \
		Insert##CType27 (26, ndx, CName27); \
		Insert##CType28 (27, ndx, CName28); \
		Insert##CType29 (28, ndx, CName29); \
		Insert##CType30 (29, ndx, CName30); \
		Insert##CType31 (30, ndx, CName31); \
		Insert##CType32 (31, ndx, CName32); \
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
	ColumnProxy##CType3 CName3; \
	ColumnProxy##CType4 CName4; \
	ColumnProxy##CType5 CName5; \
	ColumnProxy##CType6 CName6; \
	ColumnProxy##CType7 CName7; \
	ColumnProxy##CType8 CName8; \
	ColumnProxy##CType9 CName9; \
	ColumnProxy##CType10 CName10; \
	ColumnProxy##CType11 CName11; \
	ColumnProxy##CType12 CName12; \
	ColumnProxy##CType13 CName13; \
	ColumnProxy##CType14 CName14; \
	ColumnProxy##CType15 CName15; \
	ColumnProxy##CType16 CName16; \
	ColumnProxy##CType17 CName17; \
	ColumnProxy##CType18 CName18; \
	ColumnProxy##CType19 CName19; \
	ColumnProxy##CType20 CName20; \
	ColumnProxy##CType21 CName21; \
	ColumnProxy##CType22 CName22; \
	ColumnProxy##CType23 CName23; \
	ColumnProxy##CType24 CName24; \
	ColumnProxy##CType25 CName25; \
	ColumnProxy##CType26 CName26; \
	ColumnProxy##CType27 CName27; \
	ColumnProxy##CType28 CName28; \
	ColumnProxy##CType29 CName29; \
	ColumnProxy##CType30 CName30; \
	ColumnProxy##CType31 CName31; \
	ColumnProxy##CType32 CName32; \
\
protected: \
	friend class Group; \
	TableName(Allocator& alloc, size_t ref, Array* parent, size_t pndx) : TopLevelTable(alloc, ref, parent, pndx) {}; \
\
private: \
	TableName(const TableName&) {} \
	TableName& operator=(const TableName&) {return *this;} \
};



#define TDB_TABLE_33(TableName, CType1, CName1, CType2, CName2, CType3, CName3, CType4, CName4, CType5, CName5, CType6, CName6, CType7, CName7, CType8, CName8, CType9, CName9, CType10, CName10, CType11, CName11, CType12, CName12, CType13, CName13, CType14, CName14, CType15, CName15, CType16, CName16, CType17, CName17, CType18, CName18, CType19, CName19, CType20, CName20, CType21, CName21, CType22, CName22, CType23, CName23, CType24, CName24, CType25, CName25, CType26, CName26, CType27, CName27, CType28, CName28, CType29, CName29, CType30, CName30, CType31, CName31, CType32, CName32, CType33, CName33) \
class TableName##Query { \
protected: \
	QueryAccessor##CType1 CName1; \
	QueryAccessor##CType2 CName2; \
	QueryAccessor##CType3 CName3; \
	QueryAccessor##CType4 CName4; \
	QueryAccessor##CType5 CName5; \
	QueryAccessor##CType6 CName6; \
	QueryAccessor##CType7 CName7; \
	QueryAccessor##CType8 CName8; \
	QueryAccessor##CType9 CName9; \
	QueryAccessor##CType10 CName10; \
	QueryAccessor##CType11 CName11; \
	QueryAccessor##CType12 CName12; \
	QueryAccessor##CType13 CName13; \
	QueryAccessor##CType14 CName14; \
	QueryAccessor##CType15 CName15; \
	QueryAccessor##CType16 CName16; \
	QueryAccessor##CType17 CName17; \
	QueryAccessor##CType18 CName18; \
	QueryAccessor##CType19 CName19; \
	QueryAccessor##CType20 CName20; \
	QueryAccessor##CType21 CName21; \
	QueryAccessor##CType22 CName22; \
	QueryAccessor##CType23 CName23; \
	QueryAccessor##CType24 CName24; \
	QueryAccessor##CType25 CName25; \
	QueryAccessor##CType26 CName26; \
	QueryAccessor##CType27 CName27; \
	QueryAccessor##CType28 CName28; \
	QueryAccessor##CType29 CName29; \
	QueryAccessor##CType30 CName30; \
	QueryAccessor##CType31 CName31; \
	QueryAccessor##CType32 CName32; \
	QueryAccessor##CType33 CName33; \
}; \
\
class TableName : public TopLevelTable { \
public: \
	TableName(Allocator& alloc=GetDefaultAllocator()) : TopLevelTable(alloc) { \
		RegisterColumn(Accessor##CType1::type, #CName1); \
		RegisterColumn(Accessor##CType2::type, #CName2); \
		RegisterColumn(Accessor##CType3::type, #CName3); \
		RegisterColumn(Accessor##CType4::type, #CName4); \
		RegisterColumn(Accessor##CType5::type, #CName5); \
		RegisterColumn(Accessor##CType6::type, #CName6); \
		RegisterColumn(Accessor##CType7::type, #CName7); \
		RegisterColumn(Accessor##CType8::type, #CName8); \
		RegisterColumn(Accessor##CType9::type, #CName9); \
		RegisterColumn(Accessor##CType10::type, #CName10); \
		RegisterColumn(Accessor##CType11::type, #CName11); \
		RegisterColumn(Accessor##CType12::type, #CName12); \
		RegisterColumn(Accessor##CType13::type, #CName13); \
		RegisterColumn(Accessor##CType14::type, #CName14); \
		RegisterColumn(Accessor##CType15::type, #CName15); \
		RegisterColumn(Accessor##CType16::type, #CName16); \
		RegisterColumn(Accessor##CType17::type, #CName17); \
		RegisterColumn(Accessor##CType18::type, #CName18); \
		RegisterColumn(Accessor##CType19::type, #CName19); \
		RegisterColumn(Accessor##CType20::type, #CName20); \
		RegisterColumn(Accessor##CType21::type, #CName21); \
		RegisterColumn(Accessor##CType22::type, #CName22); \
		RegisterColumn(Accessor##CType23::type, #CName23); \
		RegisterColumn(Accessor##CType24::type, #CName24); \
		RegisterColumn(Accessor##CType25::type, #CName25); \
		RegisterColumn(Accessor##CType26::type, #CName26); \
		RegisterColumn(Accessor##CType27::type, #CName27); \
		RegisterColumn(Accessor##CType28::type, #CName28); \
		RegisterColumn(Accessor##CType29::type, #CName29); \
		RegisterColumn(Accessor##CType30::type, #CName30); \
		RegisterColumn(Accessor##CType31::type, #CName31); \
		RegisterColumn(Accessor##CType32::type, #CName32); \
		RegisterColumn(Accessor##CType33::type, #CName33); \
\
		CName1.Create(this, 0); \
		CName2.Create(this, 1); \
		CName3.Create(this, 2); \
		CName4.Create(this, 3); \
		CName5.Create(this, 4); \
		CName6.Create(this, 5); \
		CName7.Create(this, 6); \
		CName8.Create(this, 7); \
		CName9.Create(this, 8); \
		CName10.Create(this, 9); \
		CName11.Create(this, 10); \
		CName12.Create(this, 11); \
		CName13.Create(this, 12); \
		CName14.Create(this, 13); \
		CName15.Create(this, 14); \
		CName16.Create(this, 15); \
		CName17.Create(this, 16); \
		CName18.Create(this, 17); \
		CName19.Create(this, 18); \
		CName20.Create(this, 19); \
		CName21.Create(this, 20); \
		CName22.Create(this, 21); \
		CName23.Create(this, 22); \
		CName24.Create(this, 23); \
		CName25.Create(this, 24); \
		CName26.Create(this, 25); \
		CName27.Create(this, 26); \
		CName28.Create(this, 27); \
		CName29.Create(this, 28); \
		CName30.Create(this, 29); \
		CName31.Create(this, 30); \
		CName32.Create(this, 31); \
		CName33.Create(this, 32); \
	}; \
\
	class TestQuery : public Query { \
	public: \
		TestQuery() : CName1(0), CName2(1), CName3(2), CName4(3), CName5(4), CName6(5), CName7(6), CName8(7), CName9(8), CName10(9), CName11(10), CName12(11), CName13(12), CName14(13), CName15(14), CName16(15), CName17(16), CName18(17), CName19(18), CName20(19), CName21(20), CName22(21), CName23(22), CName24(23), CName25(24), CName26(25), CName27(26), CName28(27), CName29(28), CName30(29), CName31(30), CName32(31), CName33(32) { \
			CName1.SetQuery(this); \
			CName2.SetQuery(this); \
			CName3.SetQuery(this); \
			CName4.SetQuery(this); \
			CName5.SetQuery(this); \
			CName6.SetQuery(this); \
			CName7.SetQuery(this); \
			CName8.SetQuery(this); \
			CName9.SetQuery(this); \
			CName10.SetQuery(this); \
			CName11.SetQuery(this); \
			CName12.SetQuery(this); \
			CName13.SetQuery(this); \
			CName14.SetQuery(this); \
			CName15.SetQuery(this); \
			CName16.SetQuery(this); \
			CName17.SetQuery(this); \
			CName18.SetQuery(this); \
			CName19.SetQuery(this); \
			CName20.SetQuery(this); \
			CName21.SetQuery(this); \
			CName22.SetQuery(this); \
			CName23.SetQuery(this); \
			CName24.SetQuery(this); \
			CName25.SetQuery(this); \
			CName26.SetQuery(this); \
			CName27.SetQuery(this); \
			CName28.SetQuery(this); \
			CName29.SetQuery(this); \
			CName30.SetQuery(this); \
			CName31.SetQuery(this); \
			CName32.SetQuery(this); \
			CName33.SetQuery(this); \
		} \
\
		TestQuery(const TestQuery& copy) : Query(copy), CName1(0), CName2(1), CName3(2), CName4(3), CName5(4), CName6(5), CName7(6), CName8(7), CName9(8), CName10(9), CName11(10), CName12(11), CName13(12), CName14(13), CName15(14), CName16(15), CName17(16), CName18(17), CName19(18), CName20(19), CName21(20), CName22(21), CName23(22), CName24(23), CName25(24), CName26(25), CName27(26), CName28(27), CName29(28), CName30(29), CName31(30), CName32(31), CName33(32) { \
			CName1.SetQuery(this); \
			CName2.SetQuery(this); \
			CName3.SetQuery(this); \
			CName4.SetQuery(this); \
			CName5.SetQuery(this); \
			CName6.SetQuery(this); \
			CName7.SetQuery(this); \
			CName8.SetQuery(this); \
			CName9.SetQuery(this); \
			CName10.SetQuery(this); \
			CName11.SetQuery(this); \
			CName12.SetQuery(this); \
			CName13.SetQuery(this); \
			CName14.SetQuery(this); \
			CName15.SetQuery(this); \
			CName16.SetQuery(this); \
			CName17.SetQuery(this); \
			CName18.SetQuery(this); \
			CName19.SetQuery(this); \
			CName20.SetQuery(this); \
			CName21.SetQuery(this); \
			CName22.SetQuery(this); \
			CName23.SetQuery(this); \
			CName24.SetQuery(this); \
			CName25.SetQuery(this); \
			CName26.SetQuery(this); \
			CName27.SetQuery(this); \
			CName28.SetQuery(this); \
			CName29.SetQuery(this); \
			CName30.SetQuery(this); \
			CName31.SetQuery(this); \
			CName32.SetQuery(this); \
			CName33.SetQuery(this); \
		} \
\
		class TestQueryQueryAccessorInt : private XQueryAccessorInt { \
		public: \
			TestQueryQueryAccessorInt(size_t column_id) : XQueryAccessorInt(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(int64_t value) {return (TestQuery &)XQueryAccessorInt::Equal(value);} \
			TestQuery& NotEqual(int64_t value) {return (TestQuery &)XQueryAccessorInt::NotEqual(value);} \
			TestQuery& Greater(int64_t value) {return (TestQuery &)XQueryAccessorInt::Greater(value);} \
			TestQuery& Less(int64_t value) {return (TestQuery &)XQueryAccessorInt::Less(value);} \
			TestQuery& Between(int64_t from, int64_t to) {return (TestQuery &)XQueryAccessorInt::Between(from, to);} \
		}; \
\
		template <class T> class TestQueryQueryAccessorEnum : public TestQueryQueryAccessorInt { \
		public: \
			TestQueryQueryAccessorEnum<T>(size_t column_id) : TestQueryQueryAccessorInt(column_id) {} \
		}; \
\
		class TestQueryQueryAccessorString : private XQueryAccessorString { \
		public: \
			TestQueryQueryAccessorString(size_t column_id) : XQueryAccessorString(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::Equal(value, CaseSensitive);} \
			TestQuery& NotEqual(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::NotEqual(value, CaseSensitive);} \
			TestQuery& BeginsWith(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::BeginsWith(value, CaseSensitive);} \
			TestQuery& EndsWith(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::EndsWith(value, CaseSensitive);} \
			TestQuery& Contains(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::Contains(value, CaseSensitive);} \
		}; \
\
		class TestQueryQueryAccessorBool : private XQueryAccessorBool { \
		public: \
			TestQueryQueryAccessorBool(size_t column_id) : XQueryAccessorBool(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(bool value) {return (TestQuery &)XQueryAccessorBool::Equal(value);} \
		}; \
\
		TestQueryQueryAccessor##CType1 CName1; \
		TestQueryQueryAccessor##CType2 CName2; \
		TestQueryQueryAccessor##CType3 CName3; \
		TestQueryQueryAccessor##CType4 CName4; \
		TestQueryQueryAccessor##CType5 CName5; \
		TestQueryQueryAccessor##CType6 CName6; \
		TestQueryQueryAccessor##CType7 CName7; \
		TestQueryQueryAccessor##CType8 CName8; \
		TestQueryQueryAccessor##CType9 CName9; \
		TestQueryQueryAccessor##CType10 CName10; \
		TestQueryQueryAccessor##CType11 CName11; \
		TestQueryQueryAccessor##CType12 CName12; \
		TestQueryQueryAccessor##CType13 CName13; \
		TestQueryQueryAccessor##CType14 CName14; \
		TestQueryQueryAccessor##CType15 CName15; \
		TestQueryQueryAccessor##CType16 CName16; \
		TestQueryQueryAccessor##CType17 CName17; \
		TestQueryQueryAccessor##CType18 CName18; \
		TestQueryQueryAccessor##CType19 CName19; \
		TestQueryQueryAccessor##CType20 CName20; \
		TestQueryQueryAccessor##CType21 CName21; \
		TestQueryQueryAccessor##CType22 CName22; \
		TestQueryQueryAccessor##CType23 CName23; \
		TestQueryQueryAccessor##CType24 CName24; \
		TestQueryQueryAccessor##CType25 CName25; \
		TestQueryQueryAccessor##CType26 CName26; \
		TestQueryQueryAccessor##CType27 CName27; \
		TestQueryQueryAccessor##CType28 CName28; \
		TestQueryQueryAccessor##CType29 CName29; \
		TestQueryQueryAccessor##CType30 CName30; \
		TestQueryQueryAccessor##CType31 CName31; \
		TestQueryQueryAccessor##CType32 CName32; \
		TestQueryQueryAccessor##CType33 CName33; \
\
		TestQuery& LeftParan(void) {Query::LeftParan(); return *this;}; \
		TestQuery& Or(void) {Query::Or(); return *this;}; \
		TestQuery& RightParan(void) {Query::RightParan(); return *this;}; \
		TestQuery& Subtable(size_t column) {Query::Subtable(column); return *this;}; \
		TestQuery& Parent() {Query::Parent(); return *this;}; \
	}; \
\
	TestQuery GetQuery() {return TestQuery();} \
\
	class Cursor : public CursorBase { \
	public: \
		Cursor(TableName& table, size_t ndx) : CursorBase(table, ndx) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
			CName17.Create(this, 16); \
			CName18.Create(this, 17); \
			CName19.Create(this, 18); \
			CName20.Create(this, 19); \
			CName21.Create(this, 20); \
			CName22.Create(this, 21); \
			CName23.Create(this, 22); \
			CName24.Create(this, 23); \
			CName25.Create(this, 24); \
			CName26.Create(this, 25); \
			CName27.Create(this, 26); \
			CName28.Create(this, 27); \
			CName29.Create(this, 28); \
			CName30.Create(this, 29); \
			CName31.Create(this, 30); \
			CName32.Create(this, 31); \
			CName33.Create(this, 32); \
		} \
		Cursor(const TableName& table, size_t ndx) : CursorBase(const_cast<TableName&>(table), ndx) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
			CName17.Create(this, 16); \
			CName18.Create(this, 17); \
			CName19.Create(this, 18); \
			CName20.Create(this, 19); \
			CName21.Create(this, 20); \
			CName22.Create(this, 21); \
			CName23.Create(this, 22); \
			CName24.Create(this, 23); \
			CName25.Create(this, 24); \
			CName26.Create(this, 25); \
			CName27.Create(this, 26); \
			CName28.Create(this, 27); \
			CName29.Create(this, 28); \
			CName30.Create(this, 29); \
			CName31.Create(this, 30); \
			CName32.Create(this, 31); \
			CName33.Create(this, 32); \
		} \
		Cursor(const Cursor& v) : CursorBase(v) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
			CName17.Create(this, 16); \
			CName18.Create(this, 17); \
			CName19.Create(this, 18); \
			CName20.Create(this, 19); \
			CName21.Create(this, 20); \
			CName22.Create(this, 21); \
			CName23.Create(this, 22); \
			CName24.Create(this, 23); \
			CName25.Create(this, 24); \
			CName26.Create(this, 25); \
			CName27.Create(this, 26); \
			CName28.Create(this, 27); \
			CName29.Create(this, 28); \
			CName30.Create(this, 29); \
			CName31.Create(this, 30); \
			CName32.Create(this, 31); \
			CName33.Create(this, 32); \
		} \
		Accessor##CType1 CName1; \
		Accessor##CType2 CName2; \
		Accessor##CType3 CName3; \
		Accessor##CType4 CName4; \
		Accessor##CType5 CName5; \
		Accessor##CType6 CName6; \
		Accessor##CType7 CName7; \
		Accessor##CType8 CName8; \
		Accessor##CType9 CName9; \
		Accessor##CType10 CName10; \
		Accessor##CType11 CName11; \
		Accessor##CType12 CName12; \
		Accessor##CType13 CName13; \
		Accessor##CType14 CName14; \
		Accessor##CType15 CName15; \
		Accessor##CType16 CName16; \
		Accessor##CType17 CName17; \
		Accessor##CType18 CName18; \
		Accessor##CType19 CName19; \
		Accessor##CType20 CName20; \
		Accessor##CType21 CName21; \
		Accessor##CType22 CName22; \
		Accessor##CType23 CName23; \
		Accessor##CType24 CName24; \
		Accessor##CType25 CName25; \
		Accessor##CType26 CName26; \
		Accessor##CType27 CName27; \
		Accessor##CType28 CName28; \
		Accessor##CType29 CName29; \
		Accessor##CType30 CName30; \
		Accessor##CType31 CName31; \
		Accessor##CType32 CName32; \
		Accessor##CType33 CName33; \
	}; \
\
	void Add(tdbType##CType1 CName1, tdbType##CType2 CName2, tdbType##CType3 CName3, tdbType##CType4 CName4, tdbType##CType5 CName5, tdbType##CType6 CName6, tdbType##CType7 CName7, tdbType##CType8 CName8, tdbType##CType9 CName9, tdbType##CType10 CName10, tdbType##CType11 CName11, tdbType##CType12 CName12, tdbType##CType13 CName13, tdbType##CType14 CName14, tdbType##CType15 CName15, tdbType##CType16 CName16, tdbType##CType17 CName17, tdbType##CType18 CName18, tdbType##CType19 CName19, tdbType##CType20 CName20, tdbType##CType21 CName21, tdbType##CType22 CName22, tdbType##CType23 CName23, tdbType##CType24 CName24, tdbType##CType25 CName25, tdbType##CType26 CName26, tdbType##CType27 CName27, tdbType##CType28 CName28, tdbType##CType29 CName29, tdbType##CType30 CName30, tdbType##CType31 CName31, tdbType##CType32 CName32, tdbType##CType33 CName33) { \
		const size_t ndx = GetSize(); \
		Insert##CType1 (0, ndx, CName1); \
		Insert##CType2 (1, ndx, CName2); \
		Insert##CType3 (2, ndx, CName3); \
		Insert##CType4 (3, ndx, CName4); \
		Insert##CType5 (4, ndx, CName5); \
		Insert##CType6 (5, ndx, CName6); \
		Insert##CType7 (6, ndx, CName7); \
		Insert##CType8 (7, ndx, CName8); \
		Insert##CType9 (8, ndx, CName9); \
		Insert##CType10 (9, ndx, CName10); \
		Insert##CType11 (10, ndx, CName11); \
		Insert##CType12 (11, ndx, CName12); \
		Insert##CType13 (12, ndx, CName13); \
		Insert##CType14 (13, ndx, CName14); \
		Insert##CType15 (14, ndx, CName15); \
		Insert##CType16 (15, ndx, CName16); \
		Insert##CType17 (16, ndx, CName17); \
		Insert##CType18 (17, ndx, CName18); \
		Insert##CType19 (18, ndx, CName19); \
		Insert##CType20 (19, ndx, CName20); \
		Insert##CType21 (20, ndx, CName21); \
		Insert##CType22 (21, ndx, CName22); \
		Insert##CType23 (22, ndx, CName23); \
		Insert##CType24 (23, ndx, CName24); \
		Insert##CType25 (24, ndx, CName25); \
		Insert##CType26 (25, ndx, CName26); \
		Insert##CType27 (26, ndx, CName27); \
		Insert##CType28 (27, ndx, CName28); \
		Insert##CType29 (28, ndx, CName29); \
		Insert##CType30 (29, ndx, CName30); \
		Insert##CType31 (30, ndx, CName31); \
		Insert##CType32 (31, ndx, CName32); \
		Insert##CType33 (32, ndx, CName33); \
		InsertDone(); \
	} \
\
	void Insert(size_t ndx, tdbType##CType1 CName1, tdbType##CType2 CName2, tdbType##CType3 CName3, tdbType##CType4 CName4, tdbType##CType5 CName5, tdbType##CType6 CName6, tdbType##CType7 CName7, tdbType##CType8 CName8, tdbType##CType9 CName9, tdbType##CType10 CName10, tdbType##CType11 CName11, tdbType##CType12 CName12, tdbType##CType13 CName13, tdbType##CType14 CName14, tdbType##CType15 CName15, tdbType##CType16 CName16, tdbType##CType17 CName17, tdbType##CType18 CName18, tdbType##CType19 CName19, tdbType##CType20 CName20, tdbType##CType21 CName21, tdbType##CType22 CName22, tdbType##CType23 CName23, tdbType##CType24 CName24, tdbType##CType25 CName25, tdbType##CType26 CName26, tdbType##CType27 CName27, tdbType##CType28 CName28, tdbType##CType29 CName29, tdbType##CType30 CName30, tdbType##CType31 CName31, tdbType##CType32 CName32, tdbType##CType33 CName33) { \
		Insert##CType1 (0, ndx, CName1); \
		Insert##CType2 (1, ndx, CName2); \
		Insert##CType3 (2, ndx, CName3); \
		Insert##CType4 (3, ndx, CName4); \
		Insert##CType5 (4, ndx, CName5); \
		Insert##CType6 (5, ndx, CName6); \
		Insert##CType7 (6, ndx, CName7); \
		Insert##CType8 (7, ndx, CName8); \
		Insert##CType9 (8, ndx, CName9); \
		Insert##CType10 (9, ndx, CName10); \
		Insert##CType11 (10, ndx, CName11); \
		Insert##CType12 (11, ndx, CName12); \
		Insert##CType13 (12, ndx, CName13); \
		Insert##CType14 (13, ndx, CName14); \
		Insert##CType15 (14, ndx, CName15); \
		Insert##CType16 (15, ndx, CName16); \
		Insert##CType17 (16, ndx, CName17); \
		Insert##CType18 (17, ndx, CName18); \
		Insert##CType19 (18, ndx, CName19); \
		Insert##CType20 (19, ndx, CName20); \
		Insert##CType21 (20, ndx, CName21); \
		Insert##CType22 (21, ndx, CName22); \
		Insert##CType23 (22, ndx, CName23); \
		Insert##CType24 (23, ndx, CName24); \
		Insert##CType25 (24, ndx, CName25); \
		Insert##CType26 (25, ndx, CName26); \
		Insert##CType27 (26, ndx, CName27); \
		Insert##CType28 (27, ndx, CName28); \
		Insert##CType29 (28, ndx, CName29); \
		Insert##CType30 (29, ndx, CName30); \
		Insert##CType31 (30, ndx, CName31); \
		Insert##CType32 (31, ndx, CName32); \
		Insert##CType33 (32, ndx, CName33); \
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
	ColumnProxy##CType3 CName3; \
	ColumnProxy##CType4 CName4; \
	ColumnProxy##CType5 CName5; \
	ColumnProxy##CType6 CName6; \
	ColumnProxy##CType7 CName7; \
	ColumnProxy##CType8 CName8; \
	ColumnProxy##CType9 CName9; \
	ColumnProxy##CType10 CName10; \
	ColumnProxy##CType11 CName11; \
	ColumnProxy##CType12 CName12; \
	ColumnProxy##CType13 CName13; \
	ColumnProxy##CType14 CName14; \
	ColumnProxy##CType15 CName15; \
	ColumnProxy##CType16 CName16; \
	ColumnProxy##CType17 CName17; \
	ColumnProxy##CType18 CName18; \
	ColumnProxy##CType19 CName19; \
	ColumnProxy##CType20 CName20; \
	ColumnProxy##CType21 CName21; \
	ColumnProxy##CType22 CName22; \
	ColumnProxy##CType23 CName23; \
	ColumnProxy##CType24 CName24; \
	ColumnProxy##CType25 CName25; \
	ColumnProxy##CType26 CName26; \
	ColumnProxy##CType27 CName27; \
	ColumnProxy##CType28 CName28; \
	ColumnProxy##CType29 CName29; \
	ColumnProxy##CType30 CName30; \
	ColumnProxy##CType31 CName31; \
	ColumnProxy##CType32 CName32; \
	ColumnProxy##CType33 CName33; \
\
protected: \
	friend class Group; \
	TableName(Allocator& alloc, size_t ref, Array* parent, size_t pndx) : TopLevelTable(alloc, ref, parent, pndx) {}; \
\
private: \
	TableName(const TableName&) {} \
	TableName& operator=(const TableName&) {return *this;} \
};



#define TDB_TABLE_34(TableName, CType1, CName1, CType2, CName2, CType3, CName3, CType4, CName4, CType5, CName5, CType6, CName6, CType7, CName7, CType8, CName8, CType9, CName9, CType10, CName10, CType11, CName11, CType12, CName12, CType13, CName13, CType14, CName14, CType15, CName15, CType16, CName16, CType17, CName17, CType18, CName18, CType19, CName19, CType20, CName20, CType21, CName21, CType22, CName22, CType23, CName23, CType24, CName24, CType25, CName25, CType26, CName26, CType27, CName27, CType28, CName28, CType29, CName29, CType30, CName30, CType31, CName31, CType32, CName32, CType33, CName33, CType34, CName34) \
class TableName##Query { \
protected: \
	QueryAccessor##CType1 CName1; \
	QueryAccessor##CType2 CName2; \
	QueryAccessor##CType3 CName3; \
	QueryAccessor##CType4 CName4; \
	QueryAccessor##CType5 CName5; \
	QueryAccessor##CType6 CName6; \
	QueryAccessor##CType7 CName7; \
	QueryAccessor##CType8 CName8; \
	QueryAccessor##CType9 CName9; \
	QueryAccessor##CType10 CName10; \
	QueryAccessor##CType11 CName11; \
	QueryAccessor##CType12 CName12; \
	QueryAccessor##CType13 CName13; \
	QueryAccessor##CType14 CName14; \
	QueryAccessor##CType15 CName15; \
	QueryAccessor##CType16 CName16; \
	QueryAccessor##CType17 CName17; \
	QueryAccessor##CType18 CName18; \
	QueryAccessor##CType19 CName19; \
	QueryAccessor##CType20 CName20; \
	QueryAccessor##CType21 CName21; \
	QueryAccessor##CType22 CName22; \
	QueryAccessor##CType23 CName23; \
	QueryAccessor##CType24 CName24; \
	QueryAccessor##CType25 CName25; \
	QueryAccessor##CType26 CName26; \
	QueryAccessor##CType27 CName27; \
	QueryAccessor##CType28 CName28; \
	QueryAccessor##CType29 CName29; \
	QueryAccessor##CType30 CName30; \
	QueryAccessor##CType31 CName31; \
	QueryAccessor##CType32 CName32; \
	QueryAccessor##CType33 CName33; \
	QueryAccessor##CType34 CName34; \
}; \
\
class TableName : public TopLevelTable { \
public: \
	TableName(Allocator& alloc=GetDefaultAllocator()) : TopLevelTable(alloc) { \
		RegisterColumn(Accessor##CType1::type, #CName1); \
		RegisterColumn(Accessor##CType2::type, #CName2); \
		RegisterColumn(Accessor##CType3::type, #CName3); \
		RegisterColumn(Accessor##CType4::type, #CName4); \
		RegisterColumn(Accessor##CType5::type, #CName5); \
		RegisterColumn(Accessor##CType6::type, #CName6); \
		RegisterColumn(Accessor##CType7::type, #CName7); \
		RegisterColumn(Accessor##CType8::type, #CName8); \
		RegisterColumn(Accessor##CType9::type, #CName9); \
		RegisterColumn(Accessor##CType10::type, #CName10); \
		RegisterColumn(Accessor##CType11::type, #CName11); \
		RegisterColumn(Accessor##CType12::type, #CName12); \
		RegisterColumn(Accessor##CType13::type, #CName13); \
		RegisterColumn(Accessor##CType14::type, #CName14); \
		RegisterColumn(Accessor##CType15::type, #CName15); \
		RegisterColumn(Accessor##CType16::type, #CName16); \
		RegisterColumn(Accessor##CType17::type, #CName17); \
		RegisterColumn(Accessor##CType18::type, #CName18); \
		RegisterColumn(Accessor##CType19::type, #CName19); \
		RegisterColumn(Accessor##CType20::type, #CName20); \
		RegisterColumn(Accessor##CType21::type, #CName21); \
		RegisterColumn(Accessor##CType22::type, #CName22); \
		RegisterColumn(Accessor##CType23::type, #CName23); \
		RegisterColumn(Accessor##CType24::type, #CName24); \
		RegisterColumn(Accessor##CType25::type, #CName25); \
		RegisterColumn(Accessor##CType26::type, #CName26); \
		RegisterColumn(Accessor##CType27::type, #CName27); \
		RegisterColumn(Accessor##CType28::type, #CName28); \
		RegisterColumn(Accessor##CType29::type, #CName29); \
		RegisterColumn(Accessor##CType30::type, #CName30); \
		RegisterColumn(Accessor##CType31::type, #CName31); \
		RegisterColumn(Accessor##CType32::type, #CName32); \
		RegisterColumn(Accessor##CType33::type, #CName33); \
		RegisterColumn(Accessor##CType34::type, #CName34); \
\
		CName1.Create(this, 0); \
		CName2.Create(this, 1); \
		CName3.Create(this, 2); \
		CName4.Create(this, 3); \
		CName5.Create(this, 4); \
		CName6.Create(this, 5); \
		CName7.Create(this, 6); \
		CName8.Create(this, 7); \
		CName9.Create(this, 8); \
		CName10.Create(this, 9); \
		CName11.Create(this, 10); \
		CName12.Create(this, 11); \
		CName13.Create(this, 12); \
		CName14.Create(this, 13); \
		CName15.Create(this, 14); \
		CName16.Create(this, 15); \
		CName17.Create(this, 16); \
		CName18.Create(this, 17); \
		CName19.Create(this, 18); \
		CName20.Create(this, 19); \
		CName21.Create(this, 20); \
		CName22.Create(this, 21); \
		CName23.Create(this, 22); \
		CName24.Create(this, 23); \
		CName25.Create(this, 24); \
		CName26.Create(this, 25); \
		CName27.Create(this, 26); \
		CName28.Create(this, 27); \
		CName29.Create(this, 28); \
		CName30.Create(this, 29); \
		CName31.Create(this, 30); \
		CName32.Create(this, 31); \
		CName33.Create(this, 32); \
		CName34.Create(this, 33); \
	}; \
\
	class TestQuery : public Query { \
	public: \
		TestQuery() : CName1(0), CName2(1), CName3(2), CName4(3), CName5(4), CName6(5), CName7(6), CName8(7), CName9(8), CName10(9), CName11(10), CName12(11), CName13(12), CName14(13), CName15(14), CName16(15), CName17(16), CName18(17), CName19(18), CName20(19), CName21(20), CName22(21), CName23(22), CName24(23), CName25(24), CName26(25), CName27(26), CName28(27), CName29(28), CName30(29), CName31(30), CName32(31), CName33(32), CName34(33) { \
			CName1.SetQuery(this); \
			CName2.SetQuery(this); \
			CName3.SetQuery(this); \
			CName4.SetQuery(this); \
			CName5.SetQuery(this); \
			CName6.SetQuery(this); \
			CName7.SetQuery(this); \
			CName8.SetQuery(this); \
			CName9.SetQuery(this); \
			CName10.SetQuery(this); \
			CName11.SetQuery(this); \
			CName12.SetQuery(this); \
			CName13.SetQuery(this); \
			CName14.SetQuery(this); \
			CName15.SetQuery(this); \
			CName16.SetQuery(this); \
			CName17.SetQuery(this); \
			CName18.SetQuery(this); \
			CName19.SetQuery(this); \
			CName20.SetQuery(this); \
			CName21.SetQuery(this); \
			CName22.SetQuery(this); \
			CName23.SetQuery(this); \
			CName24.SetQuery(this); \
			CName25.SetQuery(this); \
			CName26.SetQuery(this); \
			CName27.SetQuery(this); \
			CName28.SetQuery(this); \
			CName29.SetQuery(this); \
			CName30.SetQuery(this); \
			CName31.SetQuery(this); \
			CName32.SetQuery(this); \
			CName33.SetQuery(this); \
			CName34.SetQuery(this); \
		} \
\
		TestQuery(const TestQuery& copy) : Query(copy), CName1(0), CName2(1), CName3(2), CName4(3), CName5(4), CName6(5), CName7(6), CName8(7), CName9(8), CName10(9), CName11(10), CName12(11), CName13(12), CName14(13), CName15(14), CName16(15), CName17(16), CName18(17), CName19(18), CName20(19), CName21(20), CName22(21), CName23(22), CName24(23), CName25(24), CName26(25), CName27(26), CName28(27), CName29(28), CName30(29), CName31(30), CName32(31), CName33(32), CName34(33) { \
			CName1.SetQuery(this); \
			CName2.SetQuery(this); \
			CName3.SetQuery(this); \
			CName4.SetQuery(this); \
			CName5.SetQuery(this); \
			CName6.SetQuery(this); \
			CName7.SetQuery(this); \
			CName8.SetQuery(this); \
			CName9.SetQuery(this); \
			CName10.SetQuery(this); \
			CName11.SetQuery(this); \
			CName12.SetQuery(this); \
			CName13.SetQuery(this); \
			CName14.SetQuery(this); \
			CName15.SetQuery(this); \
			CName16.SetQuery(this); \
			CName17.SetQuery(this); \
			CName18.SetQuery(this); \
			CName19.SetQuery(this); \
			CName20.SetQuery(this); \
			CName21.SetQuery(this); \
			CName22.SetQuery(this); \
			CName23.SetQuery(this); \
			CName24.SetQuery(this); \
			CName25.SetQuery(this); \
			CName26.SetQuery(this); \
			CName27.SetQuery(this); \
			CName28.SetQuery(this); \
			CName29.SetQuery(this); \
			CName30.SetQuery(this); \
			CName31.SetQuery(this); \
			CName32.SetQuery(this); \
			CName33.SetQuery(this); \
			CName34.SetQuery(this); \
		} \
\
		class TestQueryQueryAccessorInt : private XQueryAccessorInt { \
		public: \
			TestQueryQueryAccessorInt(size_t column_id) : XQueryAccessorInt(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(int64_t value) {return (TestQuery &)XQueryAccessorInt::Equal(value);} \
			TestQuery& NotEqual(int64_t value) {return (TestQuery &)XQueryAccessorInt::NotEqual(value);} \
			TestQuery& Greater(int64_t value) {return (TestQuery &)XQueryAccessorInt::Greater(value);} \
			TestQuery& Less(int64_t value) {return (TestQuery &)XQueryAccessorInt::Less(value);} \
			TestQuery& Between(int64_t from, int64_t to) {return (TestQuery &)XQueryAccessorInt::Between(from, to);} \
		}; \
\
		template <class T> class TestQueryQueryAccessorEnum : public TestQueryQueryAccessorInt { \
		public: \
			TestQueryQueryAccessorEnum<T>(size_t column_id) : TestQueryQueryAccessorInt(column_id) {} \
		}; \
\
		class TestQueryQueryAccessorString : private XQueryAccessorString { \
		public: \
			TestQueryQueryAccessorString(size_t column_id) : XQueryAccessorString(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::Equal(value, CaseSensitive);} \
			TestQuery& NotEqual(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::NotEqual(value, CaseSensitive);} \
			TestQuery& BeginsWith(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::BeginsWith(value, CaseSensitive);} \
			TestQuery& EndsWith(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::EndsWith(value, CaseSensitive);} \
			TestQuery& Contains(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::Contains(value, CaseSensitive);} \
		}; \
\
		class TestQueryQueryAccessorBool : private XQueryAccessorBool { \
		public: \
			TestQueryQueryAccessorBool(size_t column_id) : XQueryAccessorBool(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(bool value) {return (TestQuery &)XQueryAccessorBool::Equal(value);} \
		}; \
\
		TestQueryQueryAccessor##CType1 CName1; \
		TestQueryQueryAccessor##CType2 CName2; \
		TestQueryQueryAccessor##CType3 CName3; \
		TestQueryQueryAccessor##CType4 CName4; \
		TestQueryQueryAccessor##CType5 CName5; \
		TestQueryQueryAccessor##CType6 CName6; \
		TestQueryQueryAccessor##CType7 CName7; \
		TestQueryQueryAccessor##CType8 CName8; \
		TestQueryQueryAccessor##CType9 CName9; \
		TestQueryQueryAccessor##CType10 CName10; \
		TestQueryQueryAccessor##CType11 CName11; \
		TestQueryQueryAccessor##CType12 CName12; \
		TestQueryQueryAccessor##CType13 CName13; \
		TestQueryQueryAccessor##CType14 CName14; \
		TestQueryQueryAccessor##CType15 CName15; \
		TestQueryQueryAccessor##CType16 CName16; \
		TestQueryQueryAccessor##CType17 CName17; \
		TestQueryQueryAccessor##CType18 CName18; \
		TestQueryQueryAccessor##CType19 CName19; \
		TestQueryQueryAccessor##CType20 CName20; \
		TestQueryQueryAccessor##CType21 CName21; \
		TestQueryQueryAccessor##CType22 CName22; \
		TestQueryQueryAccessor##CType23 CName23; \
		TestQueryQueryAccessor##CType24 CName24; \
		TestQueryQueryAccessor##CType25 CName25; \
		TestQueryQueryAccessor##CType26 CName26; \
		TestQueryQueryAccessor##CType27 CName27; \
		TestQueryQueryAccessor##CType28 CName28; \
		TestQueryQueryAccessor##CType29 CName29; \
		TestQueryQueryAccessor##CType30 CName30; \
		TestQueryQueryAccessor##CType31 CName31; \
		TestQueryQueryAccessor##CType32 CName32; \
		TestQueryQueryAccessor##CType33 CName33; \
		TestQueryQueryAccessor##CType34 CName34; \
\
		TestQuery& LeftParan(void) {Query::LeftParan(); return *this;}; \
		TestQuery& Or(void) {Query::Or(); return *this;}; \
		TestQuery& RightParan(void) {Query::RightParan(); return *this;}; \
		TestQuery& Subtable(size_t column) {Query::Subtable(column); return *this;}; \
		TestQuery& Parent() {Query::Parent(); return *this;}; \
	}; \
\
	TestQuery GetQuery() {return TestQuery();} \
\
	class Cursor : public CursorBase { \
	public: \
		Cursor(TableName& table, size_t ndx) : CursorBase(table, ndx) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
			CName17.Create(this, 16); \
			CName18.Create(this, 17); \
			CName19.Create(this, 18); \
			CName20.Create(this, 19); \
			CName21.Create(this, 20); \
			CName22.Create(this, 21); \
			CName23.Create(this, 22); \
			CName24.Create(this, 23); \
			CName25.Create(this, 24); \
			CName26.Create(this, 25); \
			CName27.Create(this, 26); \
			CName28.Create(this, 27); \
			CName29.Create(this, 28); \
			CName30.Create(this, 29); \
			CName31.Create(this, 30); \
			CName32.Create(this, 31); \
			CName33.Create(this, 32); \
			CName34.Create(this, 33); \
		} \
		Cursor(const TableName& table, size_t ndx) : CursorBase(const_cast<TableName&>(table), ndx) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
			CName17.Create(this, 16); \
			CName18.Create(this, 17); \
			CName19.Create(this, 18); \
			CName20.Create(this, 19); \
			CName21.Create(this, 20); \
			CName22.Create(this, 21); \
			CName23.Create(this, 22); \
			CName24.Create(this, 23); \
			CName25.Create(this, 24); \
			CName26.Create(this, 25); \
			CName27.Create(this, 26); \
			CName28.Create(this, 27); \
			CName29.Create(this, 28); \
			CName30.Create(this, 29); \
			CName31.Create(this, 30); \
			CName32.Create(this, 31); \
			CName33.Create(this, 32); \
			CName34.Create(this, 33); \
		} \
		Cursor(const Cursor& v) : CursorBase(v) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
			CName17.Create(this, 16); \
			CName18.Create(this, 17); \
			CName19.Create(this, 18); \
			CName20.Create(this, 19); \
			CName21.Create(this, 20); \
			CName22.Create(this, 21); \
			CName23.Create(this, 22); \
			CName24.Create(this, 23); \
			CName25.Create(this, 24); \
			CName26.Create(this, 25); \
			CName27.Create(this, 26); \
			CName28.Create(this, 27); \
			CName29.Create(this, 28); \
			CName30.Create(this, 29); \
			CName31.Create(this, 30); \
			CName32.Create(this, 31); \
			CName33.Create(this, 32); \
			CName34.Create(this, 33); \
		} \
		Accessor##CType1 CName1; \
		Accessor##CType2 CName2; \
		Accessor##CType3 CName3; \
		Accessor##CType4 CName4; \
		Accessor##CType5 CName5; \
		Accessor##CType6 CName6; \
		Accessor##CType7 CName7; \
		Accessor##CType8 CName8; \
		Accessor##CType9 CName9; \
		Accessor##CType10 CName10; \
		Accessor##CType11 CName11; \
		Accessor##CType12 CName12; \
		Accessor##CType13 CName13; \
		Accessor##CType14 CName14; \
		Accessor##CType15 CName15; \
		Accessor##CType16 CName16; \
		Accessor##CType17 CName17; \
		Accessor##CType18 CName18; \
		Accessor##CType19 CName19; \
		Accessor##CType20 CName20; \
		Accessor##CType21 CName21; \
		Accessor##CType22 CName22; \
		Accessor##CType23 CName23; \
		Accessor##CType24 CName24; \
		Accessor##CType25 CName25; \
		Accessor##CType26 CName26; \
		Accessor##CType27 CName27; \
		Accessor##CType28 CName28; \
		Accessor##CType29 CName29; \
		Accessor##CType30 CName30; \
		Accessor##CType31 CName31; \
		Accessor##CType32 CName32; \
		Accessor##CType33 CName33; \
		Accessor##CType34 CName34; \
	}; \
\
	void Add(tdbType##CType1 CName1, tdbType##CType2 CName2, tdbType##CType3 CName3, tdbType##CType4 CName4, tdbType##CType5 CName5, tdbType##CType6 CName6, tdbType##CType7 CName7, tdbType##CType8 CName8, tdbType##CType9 CName9, tdbType##CType10 CName10, tdbType##CType11 CName11, tdbType##CType12 CName12, tdbType##CType13 CName13, tdbType##CType14 CName14, tdbType##CType15 CName15, tdbType##CType16 CName16, tdbType##CType17 CName17, tdbType##CType18 CName18, tdbType##CType19 CName19, tdbType##CType20 CName20, tdbType##CType21 CName21, tdbType##CType22 CName22, tdbType##CType23 CName23, tdbType##CType24 CName24, tdbType##CType25 CName25, tdbType##CType26 CName26, tdbType##CType27 CName27, tdbType##CType28 CName28, tdbType##CType29 CName29, tdbType##CType30 CName30, tdbType##CType31 CName31, tdbType##CType32 CName32, tdbType##CType33 CName33, tdbType##CType34 CName34) { \
		const size_t ndx = GetSize(); \
		Insert##CType1 (0, ndx, CName1); \
		Insert##CType2 (1, ndx, CName2); \
		Insert##CType3 (2, ndx, CName3); \
		Insert##CType4 (3, ndx, CName4); \
		Insert##CType5 (4, ndx, CName5); \
		Insert##CType6 (5, ndx, CName6); \
		Insert##CType7 (6, ndx, CName7); \
		Insert##CType8 (7, ndx, CName8); \
		Insert##CType9 (8, ndx, CName9); \
		Insert##CType10 (9, ndx, CName10); \
		Insert##CType11 (10, ndx, CName11); \
		Insert##CType12 (11, ndx, CName12); \
		Insert##CType13 (12, ndx, CName13); \
		Insert##CType14 (13, ndx, CName14); \
		Insert##CType15 (14, ndx, CName15); \
		Insert##CType16 (15, ndx, CName16); \
		Insert##CType17 (16, ndx, CName17); \
		Insert##CType18 (17, ndx, CName18); \
		Insert##CType19 (18, ndx, CName19); \
		Insert##CType20 (19, ndx, CName20); \
		Insert##CType21 (20, ndx, CName21); \
		Insert##CType22 (21, ndx, CName22); \
		Insert##CType23 (22, ndx, CName23); \
		Insert##CType24 (23, ndx, CName24); \
		Insert##CType25 (24, ndx, CName25); \
		Insert##CType26 (25, ndx, CName26); \
		Insert##CType27 (26, ndx, CName27); \
		Insert##CType28 (27, ndx, CName28); \
		Insert##CType29 (28, ndx, CName29); \
		Insert##CType30 (29, ndx, CName30); \
		Insert##CType31 (30, ndx, CName31); \
		Insert##CType32 (31, ndx, CName32); \
		Insert##CType33 (32, ndx, CName33); \
		Insert##CType34 (33, ndx, CName34); \
		InsertDone(); \
	} \
\
	void Insert(size_t ndx, tdbType##CType1 CName1, tdbType##CType2 CName2, tdbType##CType3 CName3, tdbType##CType4 CName4, tdbType##CType5 CName5, tdbType##CType6 CName6, tdbType##CType7 CName7, tdbType##CType8 CName8, tdbType##CType9 CName9, tdbType##CType10 CName10, tdbType##CType11 CName11, tdbType##CType12 CName12, tdbType##CType13 CName13, tdbType##CType14 CName14, tdbType##CType15 CName15, tdbType##CType16 CName16, tdbType##CType17 CName17, tdbType##CType18 CName18, tdbType##CType19 CName19, tdbType##CType20 CName20, tdbType##CType21 CName21, tdbType##CType22 CName22, tdbType##CType23 CName23, tdbType##CType24 CName24, tdbType##CType25 CName25, tdbType##CType26 CName26, tdbType##CType27 CName27, tdbType##CType28 CName28, tdbType##CType29 CName29, tdbType##CType30 CName30, tdbType##CType31 CName31, tdbType##CType32 CName32, tdbType##CType33 CName33, tdbType##CType34 CName34) { \
		Insert##CType1 (0, ndx, CName1); \
		Insert##CType2 (1, ndx, CName2); \
		Insert##CType3 (2, ndx, CName3); \
		Insert##CType4 (3, ndx, CName4); \
		Insert##CType5 (4, ndx, CName5); \
		Insert##CType6 (5, ndx, CName6); \
		Insert##CType7 (6, ndx, CName7); \
		Insert##CType8 (7, ndx, CName8); \
		Insert##CType9 (8, ndx, CName9); \
		Insert##CType10 (9, ndx, CName10); \
		Insert##CType11 (10, ndx, CName11); \
		Insert##CType12 (11, ndx, CName12); \
		Insert##CType13 (12, ndx, CName13); \
		Insert##CType14 (13, ndx, CName14); \
		Insert##CType15 (14, ndx, CName15); \
		Insert##CType16 (15, ndx, CName16); \
		Insert##CType17 (16, ndx, CName17); \
		Insert##CType18 (17, ndx, CName18); \
		Insert##CType19 (18, ndx, CName19); \
		Insert##CType20 (19, ndx, CName20); \
		Insert##CType21 (20, ndx, CName21); \
		Insert##CType22 (21, ndx, CName22); \
		Insert##CType23 (22, ndx, CName23); \
		Insert##CType24 (23, ndx, CName24); \
		Insert##CType25 (24, ndx, CName25); \
		Insert##CType26 (25, ndx, CName26); \
		Insert##CType27 (26, ndx, CName27); \
		Insert##CType28 (27, ndx, CName28); \
		Insert##CType29 (28, ndx, CName29); \
		Insert##CType30 (29, ndx, CName30); \
		Insert##CType31 (30, ndx, CName31); \
		Insert##CType32 (31, ndx, CName32); \
		Insert##CType33 (32, ndx, CName33); \
		Insert##CType34 (33, ndx, CName34); \
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
	ColumnProxy##CType3 CName3; \
	ColumnProxy##CType4 CName4; \
	ColumnProxy##CType5 CName5; \
	ColumnProxy##CType6 CName6; \
	ColumnProxy##CType7 CName7; \
	ColumnProxy##CType8 CName8; \
	ColumnProxy##CType9 CName9; \
	ColumnProxy##CType10 CName10; \
	ColumnProxy##CType11 CName11; \
	ColumnProxy##CType12 CName12; \
	ColumnProxy##CType13 CName13; \
	ColumnProxy##CType14 CName14; \
	ColumnProxy##CType15 CName15; \
	ColumnProxy##CType16 CName16; \
	ColumnProxy##CType17 CName17; \
	ColumnProxy##CType18 CName18; \
	ColumnProxy##CType19 CName19; \
	ColumnProxy##CType20 CName20; \
	ColumnProxy##CType21 CName21; \
	ColumnProxy##CType22 CName22; \
	ColumnProxy##CType23 CName23; \
	ColumnProxy##CType24 CName24; \
	ColumnProxy##CType25 CName25; \
	ColumnProxy##CType26 CName26; \
	ColumnProxy##CType27 CName27; \
	ColumnProxy##CType28 CName28; \
	ColumnProxy##CType29 CName29; \
	ColumnProxy##CType30 CName30; \
	ColumnProxy##CType31 CName31; \
	ColumnProxy##CType32 CName32; \
	ColumnProxy##CType33 CName33; \
	ColumnProxy##CType34 CName34; \
\
protected: \
	friend class Group; \
	TableName(Allocator& alloc, size_t ref, Array* parent, size_t pndx) : TopLevelTable(alloc, ref, parent, pndx) {}; \
\
private: \
	TableName(const TableName&) {} \
	TableName& operator=(const TableName&) {return *this;} \
};



#define TDB_TABLE_35(TableName, CType1, CName1, CType2, CName2, CType3, CName3, CType4, CName4, CType5, CName5, CType6, CName6, CType7, CName7, CType8, CName8, CType9, CName9, CType10, CName10, CType11, CName11, CType12, CName12, CType13, CName13, CType14, CName14, CType15, CName15, CType16, CName16, CType17, CName17, CType18, CName18, CType19, CName19, CType20, CName20, CType21, CName21, CType22, CName22, CType23, CName23, CType24, CName24, CType25, CName25, CType26, CName26, CType27, CName27, CType28, CName28, CType29, CName29, CType30, CName30, CType31, CName31, CType32, CName32, CType33, CName33, CType34, CName34, CType35, CName35) \
class TableName##Query { \
protected: \
	QueryAccessor##CType1 CName1; \
	QueryAccessor##CType2 CName2; \
	QueryAccessor##CType3 CName3; \
	QueryAccessor##CType4 CName4; \
	QueryAccessor##CType5 CName5; \
	QueryAccessor##CType6 CName6; \
	QueryAccessor##CType7 CName7; \
	QueryAccessor##CType8 CName8; \
	QueryAccessor##CType9 CName9; \
	QueryAccessor##CType10 CName10; \
	QueryAccessor##CType11 CName11; \
	QueryAccessor##CType12 CName12; \
	QueryAccessor##CType13 CName13; \
	QueryAccessor##CType14 CName14; \
	QueryAccessor##CType15 CName15; \
	QueryAccessor##CType16 CName16; \
	QueryAccessor##CType17 CName17; \
	QueryAccessor##CType18 CName18; \
	QueryAccessor##CType19 CName19; \
	QueryAccessor##CType20 CName20; \
	QueryAccessor##CType21 CName21; \
	QueryAccessor##CType22 CName22; \
	QueryAccessor##CType23 CName23; \
	QueryAccessor##CType24 CName24; \
	QueryAccessor##CType25 CName25; \
	QueryAccessor##CType26 CName26; \
	QueryAccessor##CType27 CName27; \
	QueryAccessor##CType28 CName28; \
	QueryAccessor##CType29 CName29; \
	QueryAccessor##CType30 CName30; \
	QueryAccessor##CType31 CName31; \
	QueryAccessor##CType32 CName32; \
	QueryAccessor##CType33 CName33; \
	QueryAccessor##CType34 CName34; \
	QueryAccessor##CType35 CName35; \
}; \
\
class TableName : public TopLevelTable { \
public: \
	TableName(Allocator& alloc=GetDefaultAllocator()) : TopLevelTable(alloc) { \
		RegisterColumn(Accessor##CType1::type, #CName1); \
		RegisterColumn(Accessor##CType2::type, #CName2); \
		RegisterColumn(Accessor##CType3::type, #CName3); \
		RegisterColumn(Accessor##CType4::type, #CName4); \
		RegisterColumn(Accessor##CType5::type, #CName5); \
		RegisterColumn(Accessor##CType6::type, #CName6); \
		RegisterColumn(Accessor##CType7::type, #CName7); \
		RegisterColumn(Accessor##CType8::type, #CName8); \
		RegisterColumn(Accessor##CType9::type, #CName9); \
		RegisterColumn(Accessor##CType10::type, #CName10); \
		RegisterColumn(Accessor##CType11::type, #CName11); \
		RegisterColumn(Accessor##CType12::type, #CName12); \
		RegisterColumn(Accessor##CType13::type, #CName13); \
		RegisterColumn(Accessor##CType14::type, #CName14); \
		RegisterColumn(Accessor##CType15::type, #CName15); \
		RegisterColumn(Accessor##CType16::type, #CName16); \
		RegisterColumn(Accessor##CType17::type, #CName17); \
		RegisterColumn(Accessor##CType18::type, #CName18); \
		RegisterColumn(Accessor##CType19::type, #CName19); \
		RegisterColumn(Accessor##CType20::type, #CName20); \
		RegisterColumn(Accessor##CType21::type, #CName21); \
		RegisterColumn(Accessor##CType22::type, #CName22); \
		RegisterColumn(Accessor##CType23::type, #CName23); \
		RegisterColumn(Accessor##CType24::type, #CName24); \
		RegisterColumn(Accessor##CType25::type, #CName25); \
		RegisterColumn(Accessor##CType26::type, #CName26); \
		RegisterColumn(Accessor##CType27::type, #CName27); \
		RegisterColumn(Accessor##CType28::type, #CName28); \
		RegisterColumn(Accessor##CType29::type, #CName29); \
		RegisterColumn(Accessor##CType30::type, #CName30); \
		RegisterColumn(Accessor##CType31::type, #CName31); \
		RegisterColumn(Accessor##CType32::type, #CName32); \
		RegisterColumn(Accessor##CType33::type, #CName33); \
		RegisterColumn(Accessor##CType34::type, #CName34); \
		RegisterColumn(Accessor##CType35::type, #CName35); \
\
		CName1.Create(this, 0); \
		CName2.Create(this, 1); \
		CName3.Create(this, 2); \
		CName4.Create(this, 3); \
		CName5.Create(this, 4); \
		CName6.Create(this, 5); \
		CName7.Create(this, 6); \
		CName8.Create(this, 7); \
		CName9.Create(this, 8); \
		CName10.Create(this, 9); \
		CName11.Create(this, 10); \
		CName12.Create(this, 11); \
		CName13.Create(this, 12); \
		CName14.Create(this, 13); \
		CName15.Create(this, 14); \
		CName16.Create(this, 15); \
		CName17.Create(this, 16); \
		CName18.Create(this, 17); \
		CName19.Create(this, 18); \
		CName20.Create(this, 19); \
		CName21.Create(this, 20); \
		CName22.Create(this, 21); \
		CName23.Create(this, 22); \
		CName24.Create(this, 23); \
		CName25.Create(this, 24); \
		CName26.Create(this, 25); \
		CName27.Create(this, 26); \
		CName28.Create(this, 27); \
		CName29.Create(this, 28); \
		CName30.Create(this, 29); \
		CName31.Create(this, 30); \
		CName32.Create(this, 31); \
		CName33.Create(this, 32); \
		CName34.Create(this, 33); \
		CName35.Create(this, 34); \
	}; \
\
	class TestQuery : public Query { \
	public: \
		TestQuery() : CName1(0), CName2(1), CName3(2), CName4(3), CName5(4), CName6(5), CName7(6), CName8(7), CName9(8), CName10(9), CName11(10), CName12(11), CName13(12), CName14(13), CName15(14), CName16(15), CName17(16), CName18(17), CName19(18), CName20(19), CName21(20), CName22(21), CName23(22), CName24(23), CName25(24), CName26(25), CName27(26), CName28(27), CName29(28), CName30(29), CName31(30), CName32(31), CName33(32), CName34(33), CName35(34) { \
			CName1.SetQuery(this); \
			CName2.SetQuery(this); \
			CName3.SetQuery(this); \
			CName4.SetQuery(this); \
			CName5.SetQuery(this); \
			CName6.SetQuery(this); \
			CName7.SetQuery(this); \
			CName8.SetQuery(this); \
			CName9.SetQuery(this); \
			CName10.SetQuery(this); \
			CName11.SetQuery(this); \
			CName12.SetQuery(this); \
			CName13.SetQuery(this); \
			CName14.SetQuery(this); \
			CName15.SetQuery(this); \
			CName16.SetQuery(this); \
			CName17.SetQuery(this); \
			CName18.SetQuery(this); \
			CName19.SetQuery(this); \
			CName20.SetQuery(this); \
			CName21.SetQuery(this); \
			CName22.SetQuery(this); \
			CName23.SetQuery(this); \
			CName24.SetQuery(this); \
			CName25.SetQuery(this); \
			CName26.SetQuery(this); \
			CName27.SetQuery(this); \
			CName28.SetQuery(this); \
			CName29.SetQuery(this); \
			CName30.SetQuery(this); \
			CName31.SetQuery(this); \
			CName32.SetQuery(this); \
			CName33.SetQuery(this); \
			CName34.SetQuery(this); \
			CName35.SetQuery(this); \
		} \
\
		TestQuery(const TestQuery& copy) : Query(copy), CName1(0), CName2(1), CName3(2), CName4(3), CName5(4), CName6(5), CName7(6), CName8(7), CName9(8), CName10(9), CName11(10), CName12(11), CName13(12), CName14(13), CName15(14), CName16(15), CName17(16), CName18(17), CName19(18), CName20(19), CName21(20), CName22(21), CName23(22), CName24(23), CName25(24), CName26(25), CName27(26), CName28(27), CName29(28), CName30(29), CName31(30), CName32(31), CName33(32), CName34(33), CName35(34) { \
			CName1.SetQuery(this); \
			CName2.SetQuery(this); \
			CName3.SetQuery(this); \
			CName4.SetQuery(this); \
			CName5.SetQuery(this); \
			CName6.SetQuery(this); \
			CName7.SetQuery(this); \
			CName8.SetQuery(this); \
			CName9.SetQuery(this); \
			CName10.SetQuery(this); \
			CName11.SetQuery(this); \
			CName12.SetQuery(this); \
			CName13.SetQuery(this); \
			CName14.SetQuery(this); \
			CName15.SetQuery(this); \
			CName16.SetQuery(this); \
			CName17.SetQuery(this); \
			CName18.SetQuery(this); \
			CName19.SetQuery(this); \
			CName20.SetQuery(this); \
			CName21.SetQuery(this); \
			CName22.SetQuery(this); \
			CName23.SetQuery(this); \
			CName24.SetQuery(this); \
			CName25.SetQuery(this); \
			CName26.SetQuery(this); \
			CName27.SetQuery(this); \
			CName28.SetQuery(this); \
			CName29.SetQuery(this); \
			CName30.SetQuery(this); \
			CName31.SetQuery(this); \
			CName32.SetQuery(this); \
			CName33.SetQuery(this); \
			CName34.SetQuery(this); \
			CName35.SetQuery(this); \
		} \
\
		class TestQueryQueryAccessorInt : private XQueryAccessorInt { \
		public: \
			TestQueryQueryAccessorInt(size_t column_id) : XQueryAccessorInt(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(int64_t value) {return (TestQuery &)XQueryAccessorInt::Equal(value);} \
			TestQuery& NotEqual(int64_t value) {return (TestQuery &)XQueryAccessorInt::NotEqual(value);} \
			TestQuery& Greater(int64_t value) {return (TestQuery &)XQueryAccessorInt::Greater(value);} \
			TestQuery& Less(int64_t value) {return (TestQuery &)XQueryAccessorInt::Less(value);} \
			TestQuery& Between(int64_t from, int64_t to) {return (TestQuery &)XQueryAccessorInt::Between(from, to);} \
		}; \
\
		template <class T> class TestQueryQueryAccessorEnum : public TestQueryQueryAccessorInt { \
		public: \
			TestQueryQueryAccessorEnum<T>(size_t column_id) : TestQueryQueryAccessorInt(column_id) {} \
		}; \
\
		class TestQueryQueryAccessorString : private XQueryAccessorString { \
		public: \
			TestQueryQueryAccessorString(size_t column_id) : XQueryAccessorString(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::Equal(value, CaseSensitive);} \
			TestQuery& NotEqual(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::NotEqual(value, CaseSensitive);} \
			TestQuery& BeginsWith(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::BeginsWith(value, CaseSensitive);} \
			TestQuery& EndsWith(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::EndsWith(value, CaseSensitive);} \
			TestQuery& Contains(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::Contains(value, CaseSensitive);} \
		}; \
\
		class TestQueryQueryAccessorBool : private XQueryAccessorBool { \
		public: \
			TestQueryQueryAccessorBool(size_t column_id) : XQueryAccessorBool(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(bool value) {return (TestQuery &)XQueryAccessorBool::Equal(value);} \
		}; \
\
		TestQueryQueryAccessor##CType1 CName1; \
		TestQueryQueryAccessor##CType2 CName2; \
		TestQueryQueryAccessor##CType3 CName3; \
		TestQueryQueryAccessor##CType4 CName4; \
		TestQueryQueryAccessor##CType5 CName5; \
		TestQueryQueryAccessor##CType6 CName6; \
		TestQueryQueryAccessor##CType7 CName7; \
		TestQueryQueryAccessor##CType8 CName8; \
		TestQueryQueryAccessor##CType9 CName9; \
		TestQueryQueryAccessor##CType10 CName10; \
		TestQueryQueryAccessor##CType11 CName11; \
		TestQueryQueryAccessor##CType12 CName12; \
		TestQueryQueryAccessor##CType13 CName13; \
		TestQueryQueryAccessor##CType14 CName14; \
		TestQueryQueryAccessor##CType15 CName15; \
		TestQueryQueryAccessor##CType16 CName16; \
		TestQueryQueryAccessor##CType17 CName17; \
		TestQueryQueryAccessor##CType18 CName18; \
		TestQueryQueryAccessor##CType19 CName19; \
		TestQueryQueryAccessor##CType20 CName20; \
		TestQueryQueryAccessor##CType21 CName21; \
		TestQueryQueryAccessor##CType22 CName22; \
		TestQueryQueryAccessor##CType23 CName23; \
		TestQueryQueryAccessor##CType24 CName24; \
		TestQueryQueryAccessor##CType25 CName25; \
		TestQueryQueryAccessor##CType26 CName26; \
		TestQueryQueryAccessor##CType27 CName27; \
		TestQueryQueryAccessor##CType28 CName28; \
		TestQueryQueryAccessor##CType29 CName29; \
		TestQueryQueryAccessor##CType30 CName30; \
		TestQueryQueryAccessor##CType31 CName31; \
		TestQueryQueryAccessor##CType32 CName32; \
		TestQueryQueryAccessor##CType33 CName33; \
		TestQueryQueryAccessor##CType34 CName34; \
		TestQueryQueryAccessor##CType35 CName35; \
\
		TestQuery& LeftParan(void) {Query::LeftParan(); return *this;}; \
		TestQuery& Or(void) {Query::Or(); return *this;}; \
		TestQuery& RightParan(void) {Query::RightParan(); return *this;}; \
		TestQuery& Subtable(size_t column) {Query::Subtable(column); return *this;}; \
		TestQuery& Parent() {Query::Parent(); return *this;}; \
	}; \
\
	TestQuery GetQuery() {return TestQuery();} \
\
	class Cursor : public CursorBase { \
	public: \
		Cursor(TableName& table, size_t ndx) : CursorBase(table, ndx) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
			CName17.Create(this, 16); \
			CName18.Create(this, 17); \
			CName19.Create(this, 18); \
			CName20.Create(this, 19); \
			CName21.Create(this, 20); \
			CName22.Create(this, 21); \
			CName23.Create(this, 22); \
			CName24.Create(this, 23); \
			CName25.Create(this, 24); \
			CName26.Create(this, 25); \
			CName27.Create(this, 26); \
			CName28.Create(this, 27); \
			CName29.Create(this, 28); \
			CName30.Create(this, 29); \
			CName31.Create(this, 30); \
			CName32.Create(this, 31); \
			CName33.Create(this, 32); \
			CName34.Create(this, 33); \
			CName35.Create(this, 34); \
		} \
		Cursor(const TableName& table, size_t ndx) : CursorBase(const_cast<TableName&>(table), ndx) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
			CName17.Create(this, 16); \
			CName18.Create(this, 17); \
			CName19.Create(this, 18); \
			CName20.Create(this, 19); \
			CName21.Create(this, 20); \
			CName22.Create(this, 21); \
			CName23.Create(this, 22); \
			CName24.Create(this, 23); \
			CName25.Create(this, 24); \
			CName26.Create(this, 25); \
			CName27.Create(this, 26); \
			CName28.Create(this, 27); \
			CName29.Create(this, 28); \
			CName30.Create(this, 29); \
			CName31.Create(this, 30); \
			CName32.Create(this, 31); \
			CName33.Create(this, 32); \
			CName34.Create(this, 33); \
			CName35.Create(this, 34); \
		} \
		Cursor(const Cursor& v) : CursorBase(v) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
			CName17.Create(this, 16); \
			CName18.Create(this, 17); \
			CName19.Create(this, 18); \
			CName20.Create(this, 19); \
			CName21.Create(this, 20); \
			CName22.Create(this, 21); \
			CName23.Create(this, 22); \
			CName24.Create(this, 23); \
			CName25.Create(this, 24); \
			CName26.Create(this, 25); \
			CName27.Create(this, 26); \
			CName28.Create(this, 27); \
			CName29.Create(this, 28); \
			CName30.Create(this, 29); \
			CName31.Create(this, 30); \
			CName32.Create(this, 31); \
			CName33.Create(this, 32); \
			CName34.Create(this, 33); \
			CName35.Create(this, 34); \
		} \
		Accessor##CType1 CName1; \
		Accessor##CType2 CName2; \
		Accessor##CType3 CName3; \
		Accessor##CType4 CName4; \
		Accessor##CType5 CName5; \
		Accessor##CType6 CName6; \
		Accessor##CType7 CName7; \
		Accessor##CType8 CName8; \
		Accessor##CType9 CName9; \
		Accessor##CType10 CName10; \
		Accessor##CType11 CName11; \
		Accessor##CType12 CName12; \
		Accessor##CType13 CName13; \
		Accessor##CType14 CName14; \
		Accessor##CType15 CName15; \
		Accessor##CType16 CName16; \
		Accessor##CType17 CName17; \
		Accessor##CType18 CName18; \
		Accessor##CType19 CName19; \
		Accessor##CType20 CName20; \
		Accessor##CType21 CName21; \
		Accessor##CType22 CName22; \
		Accessor##CType23 CName23; \
		Accessor##CType24 CName24; \
		Accessor##CType25 CName25; \
		Accessor##CType26 CName26; \
		Accessor##CType27 CName27; \
		Accessor##CType28 CName28; \
		Accessor##CType29 CName29; \
		Accessor##CType30 CName30; \
		Accessor##CType31 CName31; \
		Accessor##CType32 CName32; \
		Accessor##CType33 CName33; \
		Accessor##CType34 CName34; \
		Accessor##CType35 CName35; \
	}; \
\
	void Add(tdbType##CType1 CName1, tdbType##CType2 CName2, tdbType##CType3 CName3, tdbType##CType4 CName4, tdbType##CType5 CName5, tdbType##CType6 CName6, tdbType##CType7 CName7, tdbType##CType8 CName8, tdbType##CType9 CName9, tdbType##CType10 CName10, tdbType##CType11 CName11, tdbType##CType12 CName12, tdbType##CType13 CName13, tdbType##CType14 CName14, tdbType##CType15 CName15, tdbType##CType16 CName16, tdbType##CType17 CName17, tdbType##CType18 CName18, tdbType##CType19 CName19, tdbType##CType20 CName20, tdbType##CType21 CName21, tdbType##CType22 CName22, tdbType##CType23 CName23, tdbType##CType24 CName24, tdbType##CType25 CName25, tdbType##CType26 CName26, tdbType##CType27 CName27, tdbType##CType28 CName28, tdbType##CType29 CName29, tdbType##CType30 CName30, tdbType##CType31 CName31, tdbType##CType32 CName32, tdbType##CType33 CName33, tdbType##CType34 CName34, tdbType##CType35 CName35) { \
		const size_t ndx = GetSize(); \
		Insert##CType1 (0, ndx, CName1); \
		Insert##CType2 (1, ndx, CName2); \
		Insert##CType3 (2, ndx, CName3); \
		Insert##CType4 (3, ndx, CName4); \
		Insert##CType5 (4, ndx, CName5); \
		Insert##CType6 (5, ndx, CName6); \
		Insert##CType7 (6, ndx, CName7); \
		Insert##CType8 (7, ndx, CName8); \
		Insert##CType9 (8, ndx, CName9); \
		Insert##CType10 (9, ndx, CName10); \
		Insert##CType11 (10, ndx, CName11); \
		Insert##CType12 (11, ndx, CName12); \
		Insert##CType13 (12, ndx, CName13); \
		Insert##CType14 (13, ndx, CName14); \
		Insert##CType15 (14, ndx, CName15); \
		Insert##CType16 (15, ndx, CName16); \
		Insert##CType17 (16, ndx, CName17); \
		Insert##CType18 (17, ndx, CName18); \
		Insert##CType19 (18, ndx, CName19); \
		Insert##CType20 (19, ndx, CName20); \
		Insert##CType21 (20, ndx, CName21); \
		Insert##CType22 (21, ndx, CName22); \
		Insert##CType23 (22, ndx, CName23); \
		Insert##CType24 (23, ndx, CName24); \
		Insert##CType25 (24, ndx, CName25); \
		Insert##CType26 (25, ndx, CName26); \
		Insert##CType27 (26, ndx, CName27); \
		Insert##CType28 (27, ndx, CName28); \
		Insert##CType29 (28, ndx, CName29); \
		Insert##CType30 (29, ndx, CName30); \
		Insert##CType31 (30, ndx, CName31); \
		Insert##CType32 (31, ndx, CName32); \
		Insert##CType33 (32, ndx, CName33); \
		Insert##CType34 (33, ndx, CName34); \
		Insert##CType35 (34, ndx, CName35); \
		InsertDone(); \
	} \
\
	void Insert(size_t ndx, tdbType##CType1 CName1, tdbType##CType2 CName2, tdbType##CType3 CName3, tdbType##CType4 CName4, tdbType##CType5 CName5, tdbType##CType6 CName6, tdbType##CType7 CName7, tdbType##CType8 CName8, tdbType##CType9 CName9, tdbType##CType10 CName10, tdbType##CType11 CName11, tdbType##CType12 CName12, tdbType##CType13 CName13, tdbType##CType14 CName14, tdbType##CType15 CName15, tdbType##CType16 CName16, tdbType##CType17 CName17, tdbType##CType18 CName18, tdbType##CType19 CName19, tdbType##CType20 CName20, tdbType##CType21 CName21, tdbType##CType22 CName22, tdbType##CType23 CName23, tdbType##CType24 CName24, tdbType##CType25 CName25, tdbType##CType26 CName26, tdbType##CType27 CName27, tdbType##CType28 CName28, tdbType##CType29 CName29, tdbType##CType30 CName30, tdbType##CType31 CName31, tdbType##CType32 CName32, tdbType##CType33 CName33, tdbType##CType34 CName34, tdbType##CType35 CName35) { \
		Insert##CType1 (0, ndx, CName1); \
		Insert##CType2 (1, ndx, CName2); \
		Insert##CType3 (2, ndx, CName3); \
		Insert##CType4 (3, ndx, CName4); \
		Insert##CType5 (4, ndx, CName5); \
		Insert##CType6 (5, ndx, CName6); \
		Insert##CType7 (6, ndx, CName7); \
		Insert##CType8 (7, ndx, CName8); \
		Insert##CType9 (8, ndx, CName9); \
		Insert##CType10 (9, ndx, CName10); \
		Insert##CType11 (10, ndx, CName11); \
		Insert##CType12 (11, ndx, CName12); \
		Insert##CType13 (12, ndx, CName13); \
		Insert##CType14 (13, ndx, CName14); \
		Insert##CType15 (14, ndx, CName15); \
		Insert##CType16 (15, ndx, CName16); \
		Insert##CType17 (16, ndx, CName17); \
		Insert##CType18 (17, ndx, CName18); \
		Insert##CType19 (18, ndx, CName19); \
		Insert##CType20 (19, ndx, CName20); \
		Insert##CType21 (20, ndx, CName21); \
		Insert##CType22 (21, ndx, CName22); \
		Insert##CType23 (22, ndx, CName23); \
		Insert##CType24 (23, ndx, CName24); \
		Insert##CType25 (24, ndx, CName25); \
		Insert##CType26 (25, ndx, CName26); \
		Insert##CType27 (26, ndx, CName27); \
		Insert##CType28 (27, ndx, CName28); \
		Insert##CType29 (28, ndx, CName29); \
		Insert##CType30 (29, ndx, CName30); \
		Insert##CType31 (30, ndx, CName31); \
		Insert##CType32 (31, ndx, CName32); \
		Insert##CType33 (32, ndx, CName33); \
		Insert##CType34 (33, ndx, CName34); \
		Insert##CType35 (34, ndx, CName35); \
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
	ColumnProxy##CType3 CName3; \
	ColumnProxy##CType4 CName4; \
	ColumnProxy##CType5 CName5; \
	ColumnProxy##CType6 CName6; \
	ColumnProxy##CType7 CName7; \
	ColumnProxy##CType8 CName8; \
	ColumnProxy##CType9 CName9; \
	ColumnProxy##CType10 CName10; \
	ColumnProxy##CType11 CName11; \
	ColumnProxy##CType12 CName12; \
	ColumnProxy##CType13 CName13; \
	ColumnProxy##CType14 CName14; \
	ColumnProxy##CType15 CName15; \
	ColumnProxy##CType16 CName16; \
	ColumnProxy##CType17 CName17; \
	ColumnProxy##CType18 CName18; \
	ColumnProxy##CType19 CName19; \
	ColumnProxy##CType20 CName20; \
	ColumnProxy##CType21 CName21; \
	ColumnProxy##CType22 CName22; \
	ColumnProxy##CType23 CName23; \
	ColumnProxy##CType24 CName24; \
	ColumnProxy##CType25 CName25; \
	ColumnProxy##CType26 CName26; \
	ColumnProxy##CType27 CName27; \
	ColumnProxy##CType28 CName28; \
	ColumnProxy##CType29 CName29; \
	ColumnProxy##CType30 CName30; \
	ColumnProxy##CType31 CName31; \
	ColumnProxy##CType32 CName32; \
	ColumnProxy##CType33 CName33; \
	ColumnProxy##CType34 CName34; \
	ColumnProxy##CType35 CName35; \
\
protected: \
	friend class Group; \
	TableName(Allocator& alloc, size_t ref, Array* parent, size_t pndx) : TopLevelTable(alloc, ref, parent, pndx) {}; \
\
private: \
	TableName(const TableName&) {} \
	TableName& operator=(const TableName&) {return *this;} \
};



#define TDB_TABLE_36(TableName, CType1, CName1, CType2, CName2, CType3, CName3, CType4, CName4, CType5, CName5, CType6, CName6, CType7, CName7, CType8, CName8, CType9, CName9, CType10, CName10, CType11, CName11, CType12, CName12, CType13, CName13, CType14, CName14, CType15, CName15, CType16, CName16, CType17, CName17, CType18, CName18, CType19, CName19, CType20, CName20, CType21, CName21, CType22, CName22, CType23, CName23, CType24, CName24, CType25, CName25, CType26, CName26, CType27, CName27, CType28, CName28, CType29, CName29, CType30, CName30, CType31, CName31, CType32, CName32, CType33, CName33, CType34, CName34, CType35, CName35, CType36, CName36) \
class TableName##Query { \
protected: \
	QueryAccessor##CType1 CName1; \
	QueryAccessor##CType2 CName2; \
	QueryAccessor##CType3 CName3; \
	QueryAccessor##CType4 CName4; \
	QueryAccessor##CType5 CName5; \
	QueryAccessor##CType6 CName6; \
	QueryAccessor##CType7 CName7; \
	QueryAccessor##CType8 CName8; \
	QueryAccessor##CType9 CName9; \
	QueryAccessor##CType10 CName10; \
	QueryAccessor##CType11 CName11; \
	QueryAccessor##CType12 CName12; \
	QueryAccessor##CType13 CName13; \
	QueryAccessor##CType14 CName14; \
	QueryAccessor##CType15 CName15; \
	QueryAccessor##CType16 CName16; \
	QueryAccessor##CType17 CName17; \
	QueryAccessor##CType18 CName18; \
	QueryAccessor##CType19 CName19; \
	QueryAccessor##CType20 CName20; \
	QueryAccessor##CType21 CName21; \
	QueryAccessor##CType22 CName22; \
	QueryAccessor##CType23 CName23; \
	QueryAccessor##CType24 CName24; \
	QueryAccessor##CType25 CName25; \
	QueryAccessor##CType26 CName26; \
	QueryAccessor##CType27 CName27; \
	QueryAccessor##CType28 CName28; \
	QueryAccessor##CType29 CName29; \
	QueryAccessor##CType30 CName30; \
	QueryAccessor##CType31 CName31; \
	QueryAccessor##CType32 CName32; \
	QueryAccessor##CType33 CName33; \
	QueryAccessor##CType34 CName34; \
	QueryAccessor##CType35 CName35; \
	QueryAccessor##CType36 CName36; \
}; \
\
class TableName : public TopLevelTable { \
public: \
	TableName(Allocator& alloc=GetDefaultAllocator()) : TopLevelTable(alloc) { \
		RegisterColumn(Accessor##CType1::type, #CName1); \
		RegisterColumn(Accessor##CType2::type, #CName2); \
		RegisterColumn(Accessor##CType3::type, #CName3); \
		RegisterColumn(Accessor##CType4::type, #CName4); \
		RegisterColumn(Accessor##CType5::type, #CName5); \
		RegisterColumn(Accessor##CType6::type, #CName6); \
		RegisterColumn(Accessor##CType7::type, #CName7); \
		RegisterColumn(Accessor##CType8::type, #CName8); \
		RegisterColumn(Accessor##CType9::type, #CName9); \
		RegisterColumn(Accessor##CType10::type, #CName10); \
		RegisterColumn(Accessor##CType11::type, #CName11); \
		RegisterColumn(Accessor##CType12::type, #CName12); \
		RegisterColumn(Accessor##CType13::type, #CName13); \
		RegisterColumn(Accessor##CType14::type, #CName14); \
		RegisterColumn(Accessor##CType15::type, #CName15); \
		RegisterColumn(Accessor##CType16::type, #CName16); \
		RegisterColumn(Accessor##CType17::type, #CName17); \
		RegisterColumn(Accessor##CType18::type, #CName18); \
		RegisterColumn(Accessor##CType19::type, #CName19); \
		RegisterColumn(Accessor##CType20::type, #CName20); \
		RegisterColumn(Accessor##CType21::type, #CName21); \
		RegisterColumn(Accessor##CType22::type, #CName22); \
		RegisterColumn(Accessor##CType23::type, #CName23); \
		RegisterColumn(Accessor##CType24::type, #CName24); \
		RegisterColumn(Accessor##CType25::type, #CName25); \
		RegisterColumn(Accessor##CType26::type, #CName26); \
		RegisterColumn(Accessor##CType27::type, #CName27); \
		RegisterColumn(Accessor##CType28::type, #CName28); \
		RegisterColumn(Accessor##CType29::type, #CName29); \
		RegisterColumn(Accessor##CType30::type, #CName30); \
		RegisterColumn(Accessor##CType31::type, #CName31); \
		RegisterColumn(Accessor##CType32::type, #CName32); \
		RegisterColumn(Accessor##CType33::type, #CName33); \
		RegisterColumn(Accessor##CType34::type, #CName34); \
		RegisterColumn(Accessor##CType35::type, #CName35); \
		RegisterColumn(Accessor##CType36::type, #CName36); \
\
		CName1.Create(this, 0); \
		CName2.Create(this, 1); \
		CName3.Create(this, 2); \
		CName4.Create(this, 3); \
		CName5.Create(this, 4); \
		CName6.Create(this, 5); \
		CName7.Create(this, 6); \
		CName8.Create(this, 7); \
		CName9.Create(this, 8); \
		CName10.Create(this, 9); \
		CName11.Create(this, 10); \
		CName12.Create(this, 11); \
		CName13.Create(this, 12); \
		CName14.Create(this, 13); \
		CName15.Create(this, 14); \
		CName16.Create(this, 15); \
		CName17.Create(this, 16); \
		CName18.Create(this, 17); \
		CName19.Create(this, 18); \
		CName20.Create(this, 19); \
		CName21.Create(this, 20); \
		CName22.Create(this, 21); \
		CName23.Create(this, 22); \
		CName24.Create(this, 23); \
		CName25.Create(this, 24); \
		CName26.Create(this, 25); \
		CName27.Create(this, 26); \
		CName28.Create(this, 27); \
		CName29.Create(this, 28); \
		CName30.Create(this, 29); \
		CName31.Create(this, 30); \
		CName32.Create(this, 31); \
		CName33.Create(this, 32); \
		CName34.Create(this, 33); \
		CName35.Create(this, 34); \
		CName36.Create(this, 35); \
	}; \
\
	class TestQuery : public Query { \
	public: \
		TestQuery() : CName1(0), CName2(1), CName3(2), CName4(3), CName5(4), CName6(5), CName7(6), CName8(7), CName9(8), CName10(9), CName11(10), CName12(11), CName13(12), CName14(13), CName15(14), CName16(15), CName17(16), CName18(17), CName19(18), CName20(19), CName21(20), CName22(21), CName23(22), CName24(23), CName25(24), CName26(25), CName27(26), CName28(27), CName29(28), CName30(29), CName31(30), CName32(31), CName33(32), CName34(33), CName35(34), CName36(35) { \
			CName1.SetQuery(this); \
			CName2.SetQuery(this); \
			CName3.SetQuery(this); \
			CName4.SetQuery(this); \
			CName5.SetQuery(this); \
			CName6.SetQuery(this); \
			CName7.SetQuery(this); \
			CName8.SetQuery(this); \
			CName9.SetQuery(this); \
			CName10.SetQuery(this); \
			CName11.SetQuery(this); \
			CName12.SetQuery(this); \
			CName13.SetQuery(this); \
			CName14.SetQuery(this); \
			CName15.SetQuery(this); \
			CName16.SetQuery(this); \
			CName17.SetQuery(this); \
			CName18.SetQuery(this); \
			CName19.SetQuery(this); \
			CName20.SetQuery(this); \
			CName21.SetQuery(this); \
			CName22.SetQuery(this); \
			CName23.SetQuery(this); \
			CName24.SetQuery(this); \
			CName25.SetQuery(this); \
			CName26.SetQuery(this); \
			CName27.SetQuery(this); \
			CName28.SetQuery(this); \
			CName29.SetQuery(this); \
			CName30.SetQuery(this); \
			CName31.SetQuery(this); \
			CName32.SetQuery(this); \
			CName33.SetQuery(this); \
			CName34.SetQuery(this); \
			CName35.SetQuery(this); \
			CName36.SetQuery(this); \
		} \
\
		TestQuery(const TestQuery& copy) : Query(copy), CName1(0), CName2(1), CName3(2), CName4(3), CName5(4), CName6(5), CName7(6), CName8(7), CName9(8), CName10(9), CName11(10), CName12(11), CName13(12), CName14(13), CName15(14), CName16(15), CName17(16), CName18(17), CName19(18), CName20(19), CName21(20), CName22(21), CName23(22), CName24(23), CName25(24), CName26(25), CName27(26), CName28(27), CName29(28), CName30(29), CName31(30), CName32(31), CName33(32), CName34(33), CName35(34), CName36(35) { \
			CName1.SetQuery(this); \
			CName2.SetQuery(this); \
			CName3.SetQuery(this); \
			CName4.SetQuery(this); \
			CName5.SetQuery(this); \
			CName6.SetQuery(this); \
			CName7.SetQuery(this); \
			CName8.SetQuery(this); \
			CName9.SetQuery(this); \
			CName10.SetQuery(this); \
			CName11.SetQuery(this); \
			CName12.SetQuery(this); \
			CName13.SetQuery(this); \
			CName14.SetQuery(this); \
			CName15.SetQuery(this); \
			CName16.SetQuery(this); \
			CName17.SetQuery(this); \
			CName18.SetQuery(this); \
			CName19.SetQuery(this); \
			CName20.SetQuery(this); \
			CName21.SetQuery(this); \
			CName22.SetQuery(this); \
			CName23.SetQuery(this); \
			CName24.SetQuery(this); \
			CName25.SetQuery(this); \
			CName26.SetQuery(this); \
			CName27.SetQuery(this); \
			CName28.SetQuery(this); \
			CName29.SetQuery(this); \
			CName30.SetQuery(this); \
			CName31.SetQuery(this); \
			CName32.SetQuery(this); \
			CName33.SetQuery(this); \
			CName34.SetQuery(this); \
			CName35.SetQuery(this); \
			CName36.SetQuery(this); \
		} \
\
		class TestQueryQueryAccessorInt : private XQueryAccessorInt { \
		public: \
			TestQueryQueryAccessorInt(size_t column_id) : XQueryAccessorInt(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(int64_t value) {return (TestQuery &)XQueryAccessorInt::Equal(value);} \
			TestQuery& NotEqual(int64_t value) {return (TestQuery &)XQueryAccessorInt::NotEqual(value);} \
			TestQuery& Greater(int64_t value) {return (TestQuery &)XQueryAccessorInt::Greater(value);} \
			TestQuery& Less(int64_t value) {return (TestQuery &)XQueryAccessorInt::Less(value);} \
			TestQuery& Between(int64_t from, int64_t to) {return (TestQuery &)XQueryAccessorInt::Between(from, to);} \
		}; \
\
		template <class T> class TestQueryQueryAccessorEnum : public TestQueryQueryAccessorInt { \
		public: \
			TestQueryQueryAccessorEnum<T>(size_t column_id) : TestQueryQueryAccessorInt(column_id) {} \
		}; \
\
		class TestQueryQueryAccessorString : private XQueryAccessorString { \
		public: \
			TestQueryQueryAccessorString(size_t column_id) : XQueryAccessorString(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::Equal(value, CaseSensitive);} \
			TestQuery& NotEqual(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::NotEqual(value, CaseSensitive);} \
			TestQuery& BeginsWith(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::BeginsWith(value, CaseSensitive);} \
			TestQuery& EndsWith(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::EndsWith(value, CaseSensitive);} \
			TestQuery& Contains(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::Contains(value, CaseSensitive);} \
		}; \
\
		class TestQueryQueryAccessorBool : private XQueryAccessorBool { \
		public: \
			TestQueryQueryAccessorBool(size_t column_id) : XQueryAccessorBool(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(bool value) {return (TestQuery &)XQueryAccessorBool::Equal(value);} \
		}; \
\
		TestQueryQueryAccessor##CType1 CName1; \
		TestQueryQueryAccessor##CType2 CName2; \
		TestQueryQueryAccessor##CType3 CName3; \
		TestQueryQueryAccessor##CType4 CName4; \
		TestQueryQueryAccessor##CType5 CName5; \
		TestQueryQueryAccessor##CType6 CName6; \
		TestQueryQueryAccessor##CType7 CName7; \
		TestQueryQueryAccessor##CType8 CName8; \
		TestQueryQueryAccessor##CType9 CName9; \
		TestQueryQueryAccessor##CType10 CName10; \
		TestQueryQueryAccessor##CType11 CName11; \
		TestQueryQueryAccessor##CType12 CName12; \
		TestQueryQueryAccessor##CType13 CName13; \
		TestQueryQueryAccessor##CType14 CName14; \
		TestQueryQueryAccessor##CType15 CName15; \
		TestQueryQueryAccessor##CType16 CName16; \
		TestQueryQueryAccessor##CType17 CName17; \
		TestQueryQueryAccessor##CType18 CName18; \
		TestQueryQueryAccessor##CType19 CName19; \
		TestQueryQueryAccessor##CType20 CName20; \
		TestQueryQueryAccessor##CType21 CName21; \
		TestQueryQueryAccessor##CType22 CName22; \
		TestQueryQueryAccessor##CType23 CName23; \
		TestQueryQueryAccessor##CType24 CName24; \
		TestQueryQueryAccessor##CType25 CName25; \
		TestQueryQueryAccessor##CType26 CName26; \
		TestQueryQueryAccessor##CType27 CName27; \
		TestQueryQueryAccessor##CType28 CName28; \
		TestQueryQueryAccessor##CType29 CName29; \
		TestQueryQueryAccessor##CType30 CName30; \
		TestQueryQueryAccessor##CType31 CName31; \
		TestQueryQueryAccessor##CType32 CName32; \
		TestQueryQueryAccessor##CType33 CName33; \
		TestQueryQueryAccessor##CType34 CName34; \
		TestQueryQueryAccessor##CType35 CName35; \
		TestQueryQueryAccessor##CType36 CName36; \
\
		TestQuery& LeftParan(void) {Query::LeftParan(); return *this;}; \
		TestQuery& Or(void) {Query::Or(); return *this;}; \
		TestQuery& RightParan(void) {Query::RightParan(); return *this;}; \
		TestQuery& Subtable(size_t column) {Query::Subtable(column); return *this;}; \
		TestQuery& Parent() {Query::Parent(); return *this;}; \
	}; \
\
	TestQuery GetQuery() {return TestQuery();} \
\
	class Cursor : public CursorBase { \
	public: \
		Cursor(TableName& table, size_t ndx) : CursorBase(table, ndx) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
			CName17.Create(this, 16); \
			CName18.Create(this, 17); \
			CName19.Create(this, 18); \
			CName20.Create(this, 19); \
			CName21.Create(this, 20); \
			CName22.Create(this, 21); \
			CName23.Create(this, 22); \
			CName24.Create(this, 23); \
			CName25.Create(this, 24); \
			CName26.Create(this, 25); \
			CName27.Create(this, 26); \
			CName28.Create(this, 27); \
			CName29.Create(this, 28); \
			CName30.Create(this, 29); \
			CName31.Create(this, 30); \
			CName32.Create(this, 31); \
			CName33.Create(this, 32); \
			CName34.Create(this, 33); \
			CName35.Create(this, 34); \
			CName36.Create(this, 35); \
		} \
		Cursor(const TableName& table, size_t ndx) : CursorBase(const_cast<TableName&>(table), ndx) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
			CName17.Create(this, 16); \
			CName18.Create(this, 17); \
			CName19.Create(this, 18); \
			CName20.Create(this, 19); \
			CName21.Create(this, 20); \
			CName22.Create(this, 21); \
			CName23.Create(this, 22); \
			CName24.Create(this, 23); \
			CName25.Create(this, 24); \
			CName26.Create(this, 25); \
			CName27.Create(this, 26); \
			CName28.Create(this, 27); \
			CName29.Create(this, 28); \
			CName30.Create(this, 29); \
			CName31.Create(this, 30); \
			CName32.Create(this, 31); \
			CName33.Create(this, 32); \
			CName34.Create(this, 33); \
			CName35.Create(this, 34); \
			CName36.Create(this, 35); \
		} \
		Cursor(const Cursor& v) : CursorBase(v) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
			CName17.Create(this, 16); \
			CName18.Create(this, 17); \
			CName19.Create(this, 18); \
			CName20.Create(this, 19); \
			CName21.Create(this, 20); \
			CName22.Create(this, 21); \
			CName23.Create(this, 22); \
			CName24.Create(this, 23); \
			CName25.Create(this, 24); \
			CName26.Create(this, 25); \
			CName27.Create(this, 26); \
			CName28.Create(this, 27); \
			CName29.Create(this, 28); \
			CName30.Create(this, 29); \
			CName31.Create(this, 30); \
			CName32.Create(this, 31); \
			CName33.Create(this, 32); \
			CName34.Create(this, 33); \
			CName35.Create(this, 34); \
			CName36.Create(this, 35); \
		} \
		Accessor##CType1 CName1; \
		Accessor##CType2 CName2; \
		Accessor##CType3 CName3; \
		Accessor##CType4 CName4; \
		Accessor##CType5 CName5; \
		Accessor##CType6 CName6; \
		Accessor##CType7 CName7; \
		Accessor##CType8 CName8; \
		Accessor##CType9 CName9; \
		Accessor##CType10 CName10; \
		Accessor##CType11 CName11; \
		Accessor##CType12 CName12; \
		Accessor##CType13 CName13; \
		Accessor##CType14 CName14; \
		Accessor##CType15 CName15; \
		Accessor##CType16 CName16; \
		Accessor##CType17 CName17; \
		Accessor##CType18 CName18; \
		Accessor##CType19 CName19; \
		Accessor##CType20 CName20; \
		Accessor##CType21 CName21; \
		Accessor##CType22 CName22; \
		Accessor##CType23 CName23; \
		Accessor##CType24 CName24; \
		Accessor##CType25 CName25; \
		Accessor##CType26 CName26; \
		Accessor##CType27 CName27; \
		Accessor##CType28 CName28; \
		Accessor##CType29 CName29; \
		Accessor##CType30 CName30; \
		Accessor##CType31 CName31; \
		Accessor##CType32 CName32; \
		Accessor##CType33 CName33; \
		Accessor##CType34 CName34; \
		Accessor##CType35 CName35; \
		Accessor##CType36 CName36; \
	}; \
\
	void Add(tdbType##CType1 CName1, tdbType##CType2 CName2, tdbType##CType3 CName3, tdbType##CType4 CName4, tdbType##CType5 CName5, tdbType##CType6 CName6, tdbType##CType7 CName7, tdbType##CType8 CName8, tdbType##CType9 CName9, tdbType##CType10 CName10, tdbType##CType11 CName11, tdbType##CType12 CName12, tdbType##CType13 CName13, tdbType##CType14 CName14, tdbType##CType15 CName15, tdbType##CType16 CName16, tdbType##CType17 CName17, tdbType##CType18 CName18, tdbType##CType19 CName19, tdbType##CType20 CName20, tdbType##CType21 CName21, tdbType##CType22 CName22, tdbType##CType23 CName23, tdbType##CType24 CName24, tdbType##CType25 CName25, tdbType##CType26 CName26, tdbType##CType27 CName27, tdbType##CType28 CName28, tdbType##CType29 CName29, tdbType##CType30 CName30, tdbType##CType31 CName31, tdbType##CType32 CName32, tdbType##CType33 CName33, tdbType##CType34 CName34, tdbType##CType35 CName35, tdbType##CType36 CName36) { \
		const size_t ndx = GetSize(); \
		Insert##CType1 (0, ndx, CName1); \
		Insert##CType2 (1, ndx, CName2); \
		Insert##CType3 (2, ndx, CName3); \
		Insert##CType4 (3, ndx, CName4); \
		Insert##CType5 (4, ndx, CName5); \
		Insert##CType6 (5, ndx, CName6); \
		Insert##CType7 (6, ndx, CName7); \
		Insert##CType8 (7, ndx, CName8); \
		Insert##CType9 (8, ndx, CName9); \
		Insert##CType10 (9, ndx, CName10); \
		Insert##CType11 (10, ndx, CName11); \
		Insert##CType12 (11, ndx, CName12); \
		Insert##CType13 (12, ndx, CName13); \
		Insert##CType14 (13, ndx, CName14); \
		Insert##CType15 (14, ndx, CName15); \
		Insert##CType16 (15, ndx, CName16); \
		Insert##CType17 (16, ndx, CName17); \
		Insert##CType18 (17, ndx, CName18); \
		Insert##CType19 (18, ndx, CName19); \
		Insert##CType20 (19, ndx, CName20); \
		Insert##CType21 (20, ndx, CName21); \
		Insert##CType22 (21, ndx, CName22); \
		Insert##CType23 (22, ndx, CName23); \
		Insert##CType24 (23, ndx, CName24); \
		Insert##CType25 (24, ndx, CName25); \
		Insert##CType26 (25, ndx, CName26); \
		Insert##CType27 (26, ndx, CName27); \
		Insert##CType28 (27, ndx, CName28); \
		Insert##CType29 (28, ndx, CName29); \
		Insert##CType30 (29, ndx, CName30); \
		Insert##CType31 (30, ndx, CName31); \
		Insert##CType32 (31, ndx, CName32); \
		Insert##CType33 (32, ndx, CName33); \
		Insert##CType34 (33, ndx, CName34); \
		Insert##CType35 (34, ndx, CName35); \
		Insert##CType36 (35, ndx, CName36); \
		InsertDone(); \
	} \
\
	void Insert(size_t ndx, tdbType##CType1 CName1, tdbType##CType2 CName2, tdbType##CType3 CName3, tdbType##CType4 CName4, tdbType##CType5 CName5, tdbType##CType6 CName6, tdbType##CType7 CName7, tdbType##CType8 CName8, tdbType##CType9 CName9, tdbType##CType10 CName10, tdbType##CType11 CName11, tdbType##CType12 CName12, tdbType##CType13 CName13, tdbType##CType14 CName14, tdbType##CType15 CName15, tdbType##CType16 CName16, tdbType##CType17 CName17, tdbType##CType18 CName18, tdbType##CType19 CName19, tdbType##CType20 CName20, tdbType##CType21 CName21, tdbType##CType22 CName22, tdbType##CType23 CName23, tdbType##CType24 CName24, tdbType##CType25 CName25, tdbType##CType26 CName26, tdbType##CType27 CName27, tdbType##CType28 CName28, tdbType##CType29 CName29, tdbType##CType30 CName30, tdbType##CType31 CName31, tdbType##CType32 CName32, tdbType##CType33 CName33, tdbType##CType34 CName34, tdbType##CType35 CName35, tdbType##CType36 CName36) { \
		Insert##CType1 (0, ndx, CName1); \
		Insert##CType2 (1, ndx, CName2); \
		Insert##CType3 (2, ndx, CName3); \
		Insert##CType4 (3, ndx, CName4); \
		Insert##CType5 (4, ndx, CName5); \
		Insert##CType6 (5, ndx, CName6); \
		Insert##CType7 (6, ndx, CName7); \
		Insert##CType8 (7, ndx, CName8); \
		Insert##CType9 (8, ndx, CName9); \
		Insert##CType10 (9, ndx, CName10); \
		Insert##CType11 (10, ndx, CName11); \
		Insert##CType12 (11, ndx, CName12); \
		Insert##CType13 (12, ndx, CName13); \
		Insert##CType14 (13, ndx, CName14); \
		Insert##CType15 (14, ndx, CName15); \
		Insert##CType16 (15, ndx, CName16); \
		Insert##CType17 (16, ndx, CName17); \
		Insert##CType18 (17, ndx, CName18); \
		Insert##CType19 (18, ndx, CName19); \
		Insert##CType20 (19, ndx, CName20); \
		Insert##CType21 (20, ndx, CName21); \
		Insert##CType22 (21, ndx, CName22); \
		Insert##CType23 (22, ndx, CName23); \
		Insert##CType24 (23, ndx, CName24); \
		Insert##CType25 (24, ndx, CName25); \
		Insert##CType26 (25, ndx, CName26); \
		Insert##CType27 (26, ndx, CName27); \
		Insert##CType28 (27, ndx, CName28); \
		Insert##CType29 (28, ndx, CName29); \
		Insert##CType30 (29, ndx, CName30); \
		Insert##CType31 (30, ndx, CName31); \
		Insert##CType32 (31, ndx, CName32); \
		Insert##CType33 (32, ndx, CName33); \
		Insert##CType34 (33, ndx, CName34); \
		Insert##CType35 (34, ndx, CName35); \
		Insert##CType36 (35, ndx, CName36); \
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
	ColumnProxy##CType3 CName3; \
	ColumnProxy##CType4 CName4; \
	ColumnProxy##CType5 CName5; \
	ColumnProxy##CType6 CName6; \
	ColumnProxy##CType7 CName7; \
	ColumnProxy##CType8 CName8; \
	ColumnProxy##CType9 CName9; \
	ColumnProxy##CType10 CName10; \
	ColumnProxy##CType11 CName11; \
	ColumnProxy##CType12 CName12; \
	ColumnProxy##CType13 CName13; \
	ColumnProxy##CType14 CName14; \
	ColumnProxy##CType15 CName15; \
	ColumnProxy##CType16 CName16; \
	ColumnProxy##CType17 CName17; \
	ColumnProxy##CType18 CName18; \
	ColumnProxy##CType19 CName19; \
	ColumnProxy##CType20 CName20; \
	ColumnProxy##CType21 CName21; \
	ColumnProxy##CType22 CName22; \
	ColumnProxy##CType23 CName23; \
	ColumnProxy##CType24 CName24; \
	ColumnProxy##CType25 CName25; \
	ColumnProxy##CType26 CName26; \
	ColumnProxy##CType27 CName27; \
	ColumnProxy##CType28 CName28; \
	ColumnProxy##CType29 CName29; \
	ColumnProxy##CType30 CName30; \
	ColumnProxy##CType31 CName31; \
	ColumnProxy##CType32 CName32; \
	ColumnProxy##CType33 CName33; \
	ColumnProxy##CType34 CName34; \
	ColumnProxy##CType35 CName35; \
	ColumnProxy##CType36 CName36; \
\
protected: \
	friend class Group; \
	TableName(Allocator& alloc, size_t ref, Array* parent, size_t pndx) : TopLevelTable(alloc, ref, parent, pndx) {}; \
\
private: \
	TableName(const TableName&) {} \
	TableName& operator=(const TableName&) {return *this;} \
};



#define TDB_TABLE_37(TableName, CType1, CName1, CType2, CName2, CType3, CName3, CType4, CName4, CType5, CName5, CType6, CName6, CType7, CName7, CType8, CName8, CType9, CName9, CType10, CName10, CType11, CName11, CType12, CName12, CType13, CName13, CType14, CName14, CType15, CName15, CType16, CName16, CType17, CName17, CType18, CName18, CType19, CName19, CType20, CName20, CType21, CName21, CType22, CName22, CType23, CName23, CType24, CName24, CType25, CName25, CType26, CName26, CType27, CName27, CType28, CName28, CType29, CName29, CType30, CName30, CType31, CName31, CType32, CName32, CType33, CName33, CType34, CName34, CType35, CName35, CType36, CName36, CType37, CName37) \
class TableName##Query { \
protected: \
	QueryAccessor##CType1 CName1; \
	QueryAccessor##CType2 CName2; \
	QueryAccessor##CType3 CName3; \
	QueryAccessor##CType4 CName4; \
	QueryAccessor##CType5 CName5; \
	QueryAccessor##CType6 CName6; \
	QueryAccessor##CType7 CName7; \
	QueryAccessor##CType8 CName8; \
	QueryAccessor##CType9 CName9; \
	QueryAccessor##CType10 CName10; \
	QueryAccessor##CType11 CName11; \
	QueryAccessor##CType12 CName12; \
	QueryAccessor##CType13 CName13; \
	QueryAccessor##CType14 CName14; \
	QueryAccessor##CType15 CName15; \
	QueryAccessor##CType16 CName16; \
	QueryAccessor##CType17 CName17; \
	QueryAccessor##CType18 CName18; \
	QueryAccessor##CType19 CName19; \
	QueryAccessor##CType20 CName20; \
	QueryAccessor##CType21 CName21; \
	QueryAccessor##CType22 CName22; \
	QueryAccessor##CType23 CName23; \
	QueryAccessor##CType24 CName24; \
	QueryAccessor##CType25 CName25; \
	QueryAccessor##CType26 CName26; \
	QueryAccessor##CType27 CName27; \
	QueryAccessor##CType28 CName28; \
	QueryAccessor##CType29 CName29; \
	QueryAccessor##CType30 CName30; \
	QueryAccessor##CType31 CName31; \
	QueryAccessor##CType32 CName32; \
	QueryAccessor##CType33 CName33; \
	QueryAccessor##CType34 CName34; \
	QueryAccessor##CType35 CName35; \
	QueryAccessor##CType36 CName36; \
	QueryAccessor##CType37 CName37; \
}; \
\
class TableName : public TopLevelTable { \
public: \
	TableName(Allocator& alloc=GetDefaultAllocator()) : TopLevelTable(alloc) { \
		RegisterColumn(Accessor##CType1::type, #CName1); \
		RegisterColumn(Accessor##CType2::type, #CName2); \
		RegisterColumn(Accessor##CType3::type, #CName3); \
		RegisterColumn(Accessor##CType4::type, #CName4); \
		RegisterColumn(Accessor##CType5::type, #CName5); \
		RegisterColumn(Accessor##CType6::type, #CName6); \
		RegisterColumn(Accessor##CType7::type, #CName7); \
		RegisterColumn(Accessor##CType8::type, #CName8); \
		RegisterColumn(Accessor##CType9::type, #CName9); \
		RegisterColumn(Accessor##CType10::type, #CName10); \
		RegisterColumn(Accessor##CType11::type, #CName11); \
		RegisterColumn(Accessor##CType12::type, #CName12); \
		RegisterColumn(Accessor##CType13::type, #CName13); \
		RegisterColumn(Accessor##CType14::type, #CName14); \
		RegisterColumn(Accessor##CType15::type, #CName15); \
		RegisterColumn(Accessor##CType16::type, #CName16); \
		RegisterColumn(Accessor##CType17::type, #CName17); \
		RegisterColumn(Accessor##CType18::type, #CName18); \
		RegisterColumn(Accessor##CType19::type, #CName19); \
		RegisterColumn(Accessor##CType20::type, #CName20); \
		RegisterColumn(Accessor##CType21::type, #CName21); \
		RegisterColumn(Accessor##CType22::type, #CName22); \
		RegisterColumn(Accessor##CType23::type, #CName23); \
		RegisterColumn(Accessor##CType24::type, #CName24); \
		RegisterColumn(Accessor##CType25::type, #CName25); \
		RegisterColumn(Accessor##CType26::type, #CName26); \
		RegisterColumn(Accessor##CType27::type, #CName27); \
		RegisterColumn(Accessor##CType28::type, #CName28); \
		RegisterColumn(Accessor##CType29::type, #CName29); \
		RegisterColumn(Accessor##CType30::type, #CName30); \
		RegisterColumn(Accessor##CType31::type, #CName31); \
		RegisterColumn(Accessor##CType32::type, #CName32); \
		RegisterColumn(Accessor##CType33::type, #CName33); \
		RegisterColumn(Accessor##CType34::type, #CName34); \
		RegisterColumn(Accessor##CType35::type, #CName35); \
		RegisterColumn(Accessor##CType36::type, #CName36); \
		RegisterColumn(Accessor##CType37::type, #CName37); \
\
		CName1.Create(this, 0); \
		CName2.Create(this, 1); \
		CName3.Create(this, 2); \
		CName4.Create(this, 3); \
		CName5.Create(this, 4); \
		CName6.Create(this, 5); \
		CName7.Create(this, 6); \
		CName8.Create(this, 7); \
		CName9.Create(this, 8); \
		CName10.Create(this, 9); \
		CName11.Create(this, 10); \
		CName12.Create(this, 11); \
		CName13.Create(this, 12); \
		CName14.Create(this, 13); \
		CName15.Create(this, 14); \
		CName16.Create(this, 15); \
		CName17.Create(this, 16); \
		CName18.Create(this, 17); \
		CName19.Create(this, 18); \
		CName20.Create(this, 19); \
		CName21.Create(this, 20); \
		CName22.Create(this, 21); \
		CName23.Create(this, 22); \
		CName24.Create(this, 23); \
		CName25.Create(this, 24); \
		CName26.Create(this, 25); \
		CName27.Create(this, 26); \
		CName28.Create(this, 27); \
		CName29.Create(this, 28); \
		CName30.Create(this, 29); \
		CName31.Create(this, 30); \
		CName32.Create(this, 31); \
		CName33.Create(this, 32); \
		CName34.Create(this, 33); \
		CName35.Create(this, 34); \
		CName36.Create(this, 35); \
		CName37.Create(this, 36); \
	}; \
\
	class TestQuery : public Query { \
	public: \
		TestQuery() : CName1(0), CName2(1), CName3(2), CName4(3), CName5(4), CName6(5), CName7(6), CName8(7), CName9(8), CName10(9), CName11(10), CName12(11), CName13(12), CName14(13), CName15(14), CName16(15), CName17(16), CName18(17), CName19(18), CName20(19), CName21(20), CName22(21), CName23(22), CName24(23), CName25(24), CName26(25), CName27(26), CName28(27), CName29(28), CName30(29), CName31(30), CName32(31), CName33(32), CName34(33), CName35(34), CName36(35), CName37(36) { \
			CName1.SetQuery(this); \
			CName2.SetQuery(this); \
			CName3.SetQuery(this); \
			CName4.SetQuery(this); \
			CName5.SetQuery(this); \
			CName6.SetQuery(this); \
			CName7.SetQuery(this); \
			CName8.SetQuery(this); \
			CName9.SetQuery(this); \
			CName10.SetQuery(this); \
			CName11.SetQuery(this); \
			CName12.SetQuery(this); \
			CName13.SetQuery(this); \
			CName14.SetQuery(this); \
			CName15.SetQuery(this); \
			CName16.SetQuery(this); \
			CName17.SetQuery(this); \
			CName18.SetQuery(this); \
			CName19.SetQuery(this); \
			CName20.SetQuery(this); \
			CName21.SetQuery(this); \
			CName22.SetQuery(this); \
			CName23.SetQuery(this); \
			CName24.SetQuery(this); \
			CName25.SetQuery(this); \
			CName26.SetQuery(this); \
			CName27.SetQuery(this); \
			CName28.SetQuery(this); \
			CName29.SetQuery(this); \
			CName30.SetQuery(this); \
			CName31.SetQuery(this); \
			CName32.SetQuery(this); \
			CName33.SetQuery(this); \
			CName34.SetQuery(this); \
			CName35.SetQuery(this); \
			CName36.SetQuery(this); \
			CName37.SetQuery(this); \
		} \
\
		TestQuery(const TestQuery& copy) : Query(copy), CName1(0), CName2(1), CName3(2), CName4(3), CName5(4), CName6(5), CName7(6), CName8(7), CName9(8), CName10(9), CName11(10), CName12(11), CName13(12), CName14(13), CName15(14), CName16(15), CName17(16), CName18(17), CName19(18), CName20(19), CName21(20), CName22(21), CName23(22), CName24(23), CName25(24), CName26(25), CName27(26), CName28(27), CName29(28), CName30(29), CName31(30), CName32(31), CName33(32), CName34(33), CName35(34), CName36(35), CName37(36) { \
			CName1.SetQuery(this); \
			CName2.SetQuery(this); \
			CName3.SetQuery(this); \
			CName4.SetQuery(this); \
			CName5.SetQuery(this); \
			CName6.SetQuery(this); \
			CName7.SetQuery(this); \
			CName8.SetQuery(this); \
			CName9.SetQuery(this); \
			CName10.SetQuery(this); \
			CName11.SetQuery(this); \
			CName12.SetQuery(this); \
			CName13.SetQuery(this); \
			CName14.SetQuery(this); \
			CName15.SetQuery(this); \
			CName16.SetQuery(this); \
			CName17.SetQuery(this); \
			CName18.SetQuery(this); \
			CName19.SetQuery(this); \
			CName20.SetQuery(this); \
			CName21.SetQuery(this); \
			CName22.SetQuery(this); \
			CName23.SetQuery(this); \
			CName24.SetQuery(this); \
			CName25.SetQuery(this); \
			CName26.SetQuery(this); \
			CName27.SetQuery(this); \
			CName28.SetQuery(this); \
			CName29.SetQuery(this); \
			CName30.SetQuery(this); \
			CName31.SetQuery(this); \
			CName32.SetQuery(this); \
			CName33.SetQuery(this); \
			CName34.SetQuery(this); \
			CName35.SetQuery(this); \
			CName36.SetQuery(this); \
			CName37.SetQuery(this); \
		} \
\
		class TestQueryQueryAccessorInt : private XQueryAccessorInt { \
		public: \
			TestQueryQueryAccessorInt(size_t column_id) : XQueryAccessorInt(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(int64_t value) {return (TestQuery &)XQueryAccessorInt::Equal(value);} \
			TestQuery& NotEqual(int64_t value) {return (TestQuery &)XQueryAccessorInt::NotEqual(value);} \
			TestQuery& Greater(int64_t value) {return (TestQuery &)XQueryAccessorInt::Greater(value);} \
			TestQuery& Less(int64_t value) {return (TestQuery &)XQueryAccessorInt::Less(value);} \
			TestQuery& Between(int64_t from, int64_t to) {return (TestQuery &)XQueryAccessorInt::Between(from, to);} \
		}; \
\
		template <class T> class TestQueryQueryAccessorEnum : public TestQueryQueryAccessorInt { \
		public: \
			TestQueryQueryAccessorEnum<T>(size_t column_id) : TestQueryQueryAccessorInt(column_id) {} \
		}; \
\
		class TestQueryQueryAccessorString : private XQueryAccessorString { \
		public: \
			TestQueryQueryAccessorString(size_t column_id) : XQueryAccessorString(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::Equal(value, CaseSensitive);} \
			TestQuery& NotEqual(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::NotEqual(value, CaseSensitive);} \
			TestQuery& BeginsWith(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::BeginsWith(value, CaseSensitive);} \
			TestQuery& EndsWith(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::EndsWith(value, CaseSensitive);} \
			TestQuery& Contains(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::Contains(value, CaseSensitive);} \
		}; \
\
		class TestQueryQueryAccessorBool : private XQueryAccessorBool { \
		public: \
			TestQueryQueryAccessorBool(size_t column_id) : XQueryAccessorBool(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(bool value) {return (TestQuery &)XQueryAccessorBool::Equal(value);} \
		}; \
\
		TestQueryQueryAccessor##CType1 CName1; \
		TestQueryQueryAccessor##CType2 CName2; \
		TestQueryQueryAccessor##CType3 CName3; \
		TestQueryQueryAccessor##CType4 CName4; \
		TestQueryQueryAccessor##CType5 CName5; \
		TestQueryQueryAccessor##CType6 CName6; \
		TestQueryQueryAccessor##CType7 CName7; \
		TestQueryQueryAccessor##CType8 CName8; \
		TestQueryQueryAccessor##CType9 CName9; \
		TestQueryQueryAccessor##CType10 CName10; \
		TestQueryQueryAccessor##CType11 CName11; \
		TestQueryQueryAccessor##CType12 CName12; \
		TestQueryQueryAccessor##CType13 CName13; \
		TestQueryQueryAccessor##CType14 CName14; \
		TestQueryQueryAccessor##CType15 CName15; \
		TestQueryQueryAccessor##CType16 CName16; \
		TestQueryQueryAccessor##CType17 CName17; \
		TestQueryQueryAccessor##CType18 CName18; \
		TestQueryQueryAccessor##CType19 CName19; \
		TestQueryQueryAccessor##CType20 CName20; \
		TestQueryQueryAccessor##CType21 CName21; \
		TestQueryQueryAccessor##CType22 CName22; \
		TestQueryQueryAccessor##CType23 CName23; \
		TestQueryQueryAccessor##CType24 CName24; \
		TestQueryQueryAccessor##CType25 CName25; \
		TestQueryQueryAccessor##CType26 CName26; \
		TestQueryQueryAccessor##CType27 CName27; \
		TestQueryQueryAccessor##CType28 CName28; \
		TestQueryQueryAccessor##CType29 CName29; \
		TestQueryQueryAccessor##CType30 CName30; \
		TestQueryQueryAccessor##CType31 CName31; \
		TestQueryQueryAccessor##CType32 CName32; \
		TestQueryQueryAccessor##CType33 CName33; \
		TestQueryQueryAccessor##CType34 CName34; \
		TestQueryQueryAccessor##CType35 CName35; \
		TestQueryQueryAccessor##CType36 CName36; \
		TestQueryQueryAccessor##CType37 CName37; \
\
		TestQuery& LeftParan(void) {Query::LeftParan(); return *this;}; \
		TestQuery& Or(void) {Query::Or(); return *this;}; \
		TestQuery& RightParan(void) {Query::RightParan(); return *this;}; \
		TestQuery& Subtable(size_t column) {Query::Subtable(column); return *this;}; \
		TestQuery& Parent() {Query::Parent(); return *this;}; \
	}; \
\
	TestQuery GetQuery() {return TestQuery();} \
\
	class Cursor : public CursorBase { \
	public: \
		Cursor(TableName& table, size_t ndx) : CursorBase(table, ndx) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
			CName17.Create(this, 16); \
			CName18.Create(this, 17); \
			CName19.Create(this, 18); \
			CName20.Create(this, 19); \
			CName21.Create(this, 20); \
			CName22.Create(this, 21); \
			CName23.Create(this, 22); \
			CName24.Create(this, 23); \
			CName25.Create(this, 24); \
			CName26.Create(this, 25); \
			CName27.Create(this, 26); \
			CName28.Create(this, 27); \
			CName29.Create(this, 28); \
			CName30.Create(this, 29); \
			CName31.Create(this, 30); \
			CName32.Create(this, 31); \
			CName33.Create(this, 32); \
			CName34.Create(this, 33); \
			CName35.Create(this, 34); \
			CName36.Create(this, 35); \
			CName37.Create(this, 36); \
		} \
		Cursor(const TableName& table, size_t ndx) : CursorBase(const_cast<TableName&>(table), ndx) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
			CName17.Create(this, 16); \
			CName18.Create(this, 17); \
			CName19.Create(this, 18); \
			CName20.Create(this, 19); \
			CName21.Create(this, 20); \
			CName22.Create(this, 21); \
			CName23.Create(this, 22); \
			CName24.Create(this, 23); \
			CName25.Create(this, 24); \
			CName26.Create(this, 25); \
			CName27.Create(this, 26); \
			CName28.Create(this, 27); \
			CName29.Create(this, 28); \
			CName30.Create(this, 29); \
			CName31.Create(this, 30); \
			CName32.Create(this, 31); \
			CName33.Create(this, 32); \
			CName34.Create(this, 33); \
			CName35.Create(this, 34); \
			CName36.Create(this, 35); \
			CName37.Create(this, 36); \
		} \
		Cursor(const Cursor& v) : CursorBase(v) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
			CName17.Create(this, 16); \
			CName18.Create(this, 17); \
			CName19.Create(this, 18); \
			CName20.Create(this, 19); \
			CName21.Create(this, 20); \
			CName22.Create(this, 21); \
			CName23.Create(this, 22); \
			CName24.Create(this, 23); \
			CName25.Create(this, 24); \
			CName26.Create(this, 25); \
			CName27.Create(this, 26); \
			CName28.Create(this, 27); \
			CName29.Create(this, 28); \
			CName30.Create(this, 29); \
			CName31.Create(this, 30); \
			CName32.Create(this, 31); \
			CName33.Create(this, 32); \
			CName34.Create(this, 33); \
			CName35.Create(this, 34); \
			CName36.Create(this, 35); \
			CName37.Create(this, 36); \
		} \
		Accessor##CType1 CName1; \
		Accessor##CType2 CName2; \
		Accessor##CType3 CName3; \
		Accessor##CType4 CName4; \
		Accessor##CType5 CName5; \
		Accessor##CType6 CName6; \
		Accessor##CType7 CName7; \
		Accessor##CType8 CName8; \
		Accessor##CType9 CName9; \
		Accessor##CType10 CName10; \
		Accessor##CType11 CName11; \
		Accessor##CType12 CName12; \
		Accessor##CType13 CName13; \
		Accessor##CType14 CName14; \
		Accessor##CType15 CName15; \
		Accessor##CType16 CName16; \
		Accessor##CType17 CName17; \
		Accessor##CType18 CName18; \
		Accessor##CType19 CName19; \
		Accessor##CType20 CName20; \
		Accessor##CType21 CName21; \
		Accessor##CType22 CName22; \
		Accessor##CType23 CName23; \
		Accessor##CType24 CName24; \
		Accessor##CType25 CName25; \
		Accessor##CType26 CName26; \
		Accessor##CType27 CName27; \
		Accessor##CType28 CName28; \
		Accessor##CType29 CName29; \
		Accessor##CType30 CName30; \
		Accessor##CType31 CName31; \
		Accessor##CType32 CName32; \
		Accessor##CType33 CName33; \
		Accessor##CType34 CName34; \
		Accessor##CType35 CName35; \
		Accessor##CType36 CName36; \
		Accessor##CType37 CName37; \
	}; \
\
	void Add(tdbType##CType1 CName1, tdbType##CType2 CName2, tdbType##CType3 CName3, tdbType##CType4 CName4, tdbType##CType5 CName5, tdbType##CType6 CName6, tdbType##CType7 CName7, tdbType##CType8 CName8, tdbType##CType9 CName9, tdbType##CType10 CName10, tdbType##CType11 CName11, tdbType##CType12 CName12, tdbType##CType13 CName13, tdbType##CType14 CName14, tdbType##CType15 CName15, tdbType##CType16 CName16, tdbType##CType17 CName17, tdbType##CType18 CName18, tdbType##CType19 CName19, tdbType##CType20 CName20, tdbType##CType21 CName21, tdbType##CType22 CName22, tdbType##CType23 CName23, tdbType##CType24 CName24, tdbType##CType25 CName25, tdbType##CType26 CName26, tdbType##CType27 CName27, tdbType##CType28 CName28, tdbType##CType29 CName29, tdbType##CType30 CName30, tdbType##CType31 CName31, tdbType##CType32 CName32, tdbType##CType33 CName33, tdbType##CType34 CName34, tdbType##CType35 CName35, tdbType##CType36 CName36, tdbType##CType37 CName37) { \
		const size_t ndx = GetSize(); \
		Insert##CType1 (0, ndx, CName1); \
		Insert##CType2 (1, ndx, CName2); \
		Insert##CType3 (2, ndx, CName3); \
		Insert##CType4 (3, ndx, CName4); \
		Insert##CType5 (4, ndx, CName5); \
		Insert##CType6 (5, ndx, CName6); \
		Insert##CType7 (6, ndx, CName7); \
		Insert##CType8 (7, ndx, CName8); \
		Insert##CType9 (8, ndx, CName9); \
		Insert##CType10 (9, ndx, CName10); \
		Insert##CType11 (10, ndx, CName11); \
		Insert##CType12 (11, ndx, CName12); \
		Insert##CType13 (12, ndx, CName13); \
		Insert##CType14 (13, ndx, CName14); \
		Insert##CType15 (14, ndx, CName15); \
		Insert##CType16 (15, ndx, CName16); \
		Insert##CType17 (16, ndx, CName17); \
		Insert##CType18 (17, ndx, CName18); \
		Insert##CType19 (18, ndx, CName19); \
		Insert##CType20 (19, ndx, CName20); \
		Insert##CType21 (20, ndx, CName21); \
		Insert##CType22 (21, ndx, CName22); \
		Insert##CType23 (22, ndx, CName23); \
		Insert##CType24 (23, ndx, CName24); \
		Insert##CType25 (24, ndx, CName25); \
		Insert##CType26 (25, ndx, CName26); \
		Insert##CType27 (26, ndx, CName27); \
		Insert##CType28 (27, ndx, CName28); \
		Insert##CType29 (28, ndx, CName29); \
		Insert##CType30 (29, ndx, CName30); \
		Insert##CType31 (30, ndx, CName31); \
		Insert##CType32 (31, ndx, CName32); \
		Insert##CType33 (32, ndx, CName33); \
		Insert##CType34 (33, ndx, CName34); \
		Insert##CType35 (34, ndx, CName35); \
		Insert##CType36 (35, ndx, CName36); \
		Insert##CType37 (36, ndx, CName37); \
		InsertDone(); \
	} \
\
	void Insert(size_t ndx, tdbType##CType1 CName1, tdbType##CType2 CName2, tdbType##CType3 CName3, tdbType##CType4 CName4, tdbType##CType5 CName5, tdbType##CType6 CName6, tdbType##CType7 CName7, tdbType##CType8 CName8, tdbType##CType9 CName9, tdbType##CType10 CName10, tdbType##CType11 CName11, tdbType##CType12 CName12, tdbType##CType13 CName13, tdbType##CType14 CName14, tdbType##CType15 CName15, tdbType##CType16 CName16, tdbType##CType17 CName17, tdbType##CType18 CName18, tdbType##CType19 CName19, tdbType##CType20 CName20, tdbType##CType21 CName21, tdbType##CType22 CName22, tdbType##CType23 CName23, tdbType##CType24 CName24, tdbType##CType25 CName25, tdbType##CType26 CName26, tdbType##CType27 CName27, tdbType##CType28 CName28, tdbType##CType29 CName29, tdbType##CType30 CName30, tdbType##CType31 CName31, tdbType##CType32 CName32, tdbType##CType33 CName33, tdbType##CType34 CName34, tdbType##CType35 CName35, tdbType##CType36 CName36, tdbType##CType37 CName37) { \
		Insert##CType1 (0, ndx, CName1); \
		Insert##CType2 (1, ndx, CName2); \
		Insert##CType3 (2, ndx, CName3); \
		Insert##CType4 (3, ndx, CName4); \
		Insert##CType5 (4, ndx, CName5); \
		Insert##CType6 (5, ndx, CName6); \
		Insert##CType7 (6, ndx, CName7); \
		Insert##CType8 (7, ndx, CName8); \
		Insert##CType9 (8, ndx, CName9); \
		Insert##CType10 (9, ndx, CName10); \
		Insert##CType11 (10, ndx, CName11); \
		Insert##CType12 (11, ndx, CName12); \
		Insert##CType13 (12, ndx, CName13); \
		Insert##CType14 (13, ndx, CName14); \
		Insert##CType15 (14, ndx, CName15); \
		Insert##CType16 (15, ndx, CName16); \
		Insert##CType17 (16, ndx, CName17); \
		Insert##CType18 (17, ndx, CName18); \
		Insert##CType19 (18, ndx, CName19); \
		Insert##CType20 (19, ndx, CName20); \
		Insert##CType21 (20, ndx, CName21); \
		Insert##CType22 (21, ndx, CName22); \
		Insert##CType23 (22, ndx, CName23); \
		Insert##CType24 (23, ndx, CName24); \
		Insert##CType25 (24, ndx, CName25); \
		Insert##CType26 (25, ndx, CName26); \
		Insert##CType27 (26, ndx, CName27); \
		Insert##CType28 (27, ndx, CName28); \
		Insert##CType29 (28, ndx, CName29); \
		Insert##CType30 (29, ndx, CName30); \
		Insert##CType31 (30, ndx, CName31); \
		Insert##CType32 (31, ndx, CName32); \
		Insert##CType33 (32, ndx, CName33); \
		Insert##CType34 (33, ndx, CName34); \
		Insert##CType35 (34, ndx, CName35); \
		Insert##CType36 (35, ndx, CName36); \
		Insert##CType37 (36, ndx, CName37); \
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
	ColumnProxy##CType3 CName3; \
	ColumnProxy##CType4 CName4; \
	ColumnProxy##CType5 CName5; \
	ColumnProxy##CType6 CName6; \
	ColumnProxy##CType7 CName7; \
	ColumnProxy##CType8 CName8; \
	ColumnProxy##CType9 CName9; \
	ColumnProxy##CType10 CName10; \
	ColumnProxy##CType11 CName11; \
	ColumnProxy##CType12 CName12; \
	ColumnProxy##CType13 CName13; \
	ColumnProxy##CType14 CName14; \
	ColumnProxy##CType15 CName15; \
	ColumnProxy##CType16 CName16; \
	ColumnProxy##CType17 CName17; \
	ColumnProxy##CType18 CName18; \
	ColumnProxy##CType19 CName19; \
	ColumnProxy##CType20 CName20; \
	ColumnProxy##CType21 CName21; \
	ColumnProxy##CType22 CName22; \
	ColumnProxy##CType23 CName23; \
	ColumnProxy##CType24 CName24; \
	ColumnProxy##CType25 CName25; \
	ColumnProxy##CType26 CName26; \
	ColumnProxy##CType27 CName27; \
	ColumnProxy##CType28 CName28; \
	ColumnProxy##CType29 CName29; \
	ColumnProxy##CType30 CName30; \
	ColumnProxy##CType31 CName31; \
	ColumnProxy##CType32 CName32; \
	ColumnProxy##CType33 CName33; \
	ColumnProxy##CType34 CName34; \
	ColumnProxy##CType35 CName35; \
	ColumnProxy##CType36 CName36; \
	ColumnProxy##CType37 CName37; \
\
protected: \
	friend class Group; \
	TableName(Allocator& alloc, size_t ref, Array* parent, size_t pndx) : TopLevelTable(alloc, ref, parent, pndx) {}; \
\
private: \
	TableName(const TableName&) {} \
	TableName& operator=(const TableName&) {return *this;} \
};



#define TDB_TABLE_38(TableName, CType1, CName1, CType2, CName2, CType3, CName3, CType4, CName4, CType5, CName5, CType6, CName6, CType7, CName7, CType8, CName8, CType9, CName9, CType10, CName10, CType11, CName11, CType12, CName12, CType13, CName13, CType14, CName14, CType15, CName15, CType16, CName16, CType17, CName17, CType18, CName18, CType19, CName19, CType20, CName20, CType21, CName21, CType22, CName22, CType23, CName23, CType24, CName24, CType25, CName25, CType26, CName26, CType27, CName27, CType28, CName28, CType29, CName29, CType30, CName30, CType31, CName31, CType32, CName32, CType33, CName33, CType34, CName34, CType35, CName35, CType36, CName36, CType37, CName37, CType38, CName38) \
class TableName##Query { \
protected: \
	QueryAccessor##CType1 CName1; \
	QueryAccessor##CType2 CName2; \
	QueryAccessor##CType3 CName3; \
	QueryAccessor##CType4 CName4; \
	QueryAccessor##CType5 CName5; \
	QueryAccessor##CType6 CName6; \
	QueryAccessor##CType7 CName7; \
	QueryAccessor##CType8 CName8; \
	QueryAccessor##CType9 CName9; \
	QueryAccessor##CType10 CName10; \
	QueryAccessor##CType11 CName11; \
	QueryAccessor##CType12 CName12; \
	QueryAccessor##CType13 CName13; \
	QueryAccessor##CType14 CName14; \
	QueryAccessor##CType15 CName15; \
	QueryAccessor##CType16 CName16; \
	QueryAccessor##CType17 CName17; \
	QueryAccessor##CType18 CName18; \
	QueryAccessor##CType19 CName19; \
	QueryAccessor##CType20 CName20; \
	QueryAccessor##CType21 CName21; \
	QueryAccessor##CType22 CName22; \
	QueryAccessor##CType23 CName23; \
	QueryAccessor##CType24 CName24; \
	QueryAccessor##CType25 CName25; \
	QueryAccessor##CType26 CName26; \
	QueryAccessor##CType27 CName27; \
	QueryAccessor##CType28 CName28; \
	QueryAccessor##CType29 CName29; \
	QueryAccessor##CType30 CName30; \
	QueryAccessor##CType31 CName31; \
	QueryAccessor##CType32 CName32; \
	QueryAccessor##CType33 CName33; \
	QueryAccessor##CType34 CName34; \
	QueryAccessor##CType35 CName35; \
	QueryAccessor##CType36 CName36; \
	QueryAccessor##CType37 CName37; \
	QueryAccessor##CType38 CName38; \
}; \
\
class TableName : public TopLevelTable { \
public: \
	TableName(Allocator& alloc=GetDefaultAllocator()) : TopLevelTable(alloc) { \
		RegisterColumn(Accessor##CType1::type, #CName1); \
		RegisterColumn(Accessor##CType2::type, #CName2); \
		RegisterColumn(Accessor##CType3::type, #CName3); \
		RegisterColumn(Accessor##CType4::type, #CName4); \
		RegisterColumn(Accessor##CType5::type, #CName5); \
		RegisterColumn(Accessor##CType6::type, #CName6); \
		RegisterColumn(Accessor##CType7::type, #CName7); \
		RegisterColumn(Accessor##CType8::type, #CName8); \
		RegisterColumn(Accessor##CType9::type, #CName9); \
		RegisterColumn(Accessor##CType10::type, #CName10); \
		RegisterColumn(Accessor##CType11::type, #CName11); \
		RegisterColumn(Accessor##CType12::type, #CName12); \
		RegisterColumn(Accessor##CType13::type, #CName13); \
		RegisterColumn(Accessor##CType14::type, #CName14); \
		RegisterColumn(Accessor##CType15::type, #CName15); \
		RegisterColumn(Accessor##CType16::type, #CName16); \
		RegisterColumn(Accessor##CType17::type, #CName17); \
		RegisterColumn(Accessor##CType18::type, #CName18); \
		RegisterColumn(Accessor##CType19::type, #CName19); \
		RegisterColumn(Accessor##CType20::type, #CName20); \
		RegisterColumn(Accessor##CType21::type, #CName21); \
		RegisterColumn(Accessor##CType22::type, #CName22); \
		RegisterColumn(Accessor##CType23::type, #CName23); \
		RegisterColumn(Accessor##CType24::type, #CName24); \
		RegisterColumn(Accessor##CType25::type, #CName25); \
		RegisterColumn(Accessor##CType26::type, #CName26); \
		RegisterColumn(Accessor##CType27::type, #CName27); \
		RegisterColumn(Accessor##CType28::type, #CName28); \
		RegisterColumn(Accessor##CType29::type, #CName29); \
		RegisterColumn(Accessor##CType30::type, #CName30); \
		RegisterColumn(Accessor##CType31::type, #CName31); \
		RegisterColumn(Accessor##CType32::type, #CName32); \
		RegisterColumn(Accessor##CType33::type, #CName33); \
		RegisterColumn(Accessor##CType34::type, #CName34); \
		RegisterColumn(Accessor##CType35::type, #CName35); \
		RegisterColumn(Accessor##CType36::type, #CName36); \
		RegisterColumn(Accessor##CType37::type, #CName37); \
		RegisterColumn(Accessor##CType38::type, #CName38); \
\
		CName1.Create(this, 0); \
		CName2.Create(this, 1); \
		CName3.Create(this, 2); \
		CName4.Create(this, 3); \
		CName5.Create(this, 4); \
		CName6.Create(this, 5); \
		CName7.Create(this, 6); \
		CName8.Create(this, 7); \
		CName9.Create(this, 8); \
		CName10.Create(this, 9); \
		CName11.Create(this, 10); \
		CName12.Create(this, 11); \
		CName13.Create(this, 12); \
		CName14.Create(this, 13); \
		CName15.Create(this, 14); \
		CName16.Create(this, 15); \
		CName17.Create(this, 16); \
		CName18.Create(this, 17); \
		CName19.Create(this, 18); \
		CName20.Create(this, 19); \
		CName21.Create(this, 20); \
		CName22.Create(this, 21); \
		CName23.Create(this, 22); \
		CName24.Create(this, 23); \
		CName25.Create(this, 24); \
		CName26.Create(this, 25); \
		CName27.Create(this, 26); \
		CName28.Create(this, 27); \
		CName29.Create(this, 28); \
		CName30.Create(this, 29); \
		CName31.Create(this, 30); \
		CName32.Create(this, 31); \
		CName33.Create(this, 32); \
		CName34.Create(this, 33); \
		CName35.Create(this, 34); \
		CName36.Create(this, 35); \
		CName37.Create(this, 36); \
		CName38.Create(this, 37); \
	}; \
\
	class TestQuery : public Query { \
	public: \
		TestQuery() : CName1(0), CName2(1), CName3(2), CName4(3), CName5(4), CName6(5), CName7(6), CName8(7), CName9(8), CName10(9), CName11(10), CName12(11), CName13(12), CName14(13), CName15(14), CName16(15), CName17(16), CName18(17), CName19(18), CName20(19), CName21(20), CName22(21), CName23(22), CName24(23), CName25(24), CName26(25), CName27(26), CName28(27), CName29(28), CName30(29), CName31(30), CName32(31), CName33(32), CName34(33), CName35(34), CName36(35), CName37(36), CName38(37) { \
			CName1.SetQuery(this); \
			CName2.SetQuery(this); \
			CName3.SetQuery(this); \
			CName4.SetQuery(this); \
			CName5.SetQuery(this); \
			CName6.SetQuery(this); \
			CName7.SetQuery(this); \
			CName8.SetQuery(this); \
			CName9.SetQuery(this); \
			CName10.SetQuery(this); \
			CName11.SetQuery(this); \
			CName12.SetQuery(this); \
			CName13.SetQuery(this); \
			CName14.SetQuery(this); \
			CName15.SetQuery(this); \
			CName16.SetQuery(this); \
			CName17.SetQuery(this); \
			CName18.SetQuery(this); \
			CName19.SetQuery(this); \
			CName20.SetQuery(this); \
			CName21.SetQuery(this); \
			CName22.SetQuery(this); \
			CName23.SetQuery(this); \
			CName24.SetQuery(this); \
			CName25.SetQuery(this); \
			CName26.SetQuery(this); \
			CName27.SetQuery(this); \
			CName28.SetQuery(this); \
			CName29.SetQuery(this); \
			CName30.SetQuery(this); \
			CName31.SetQuery(this); \
			CName32.SetQuery(this); \
			CName33.SetQuery(this); \
			CName34.SetQuery(this); \
			CName35.SetQuery(this); \
			CName36.SetQuery(this); \
			CName37.SetQuery(this); \
			CName38.SetQuery(this); \
		} \
\
		TestQuery(const TestQuery& copy) : Query(copy), CName1(0), CName2(1), CName3(2), CName4(3), CName5(4), CName6(5), CName7(6), CName8(7), CName9(8), CName10(9), CName11(10), CName12(11), CName13(12), CName14(13), CName15(14), CName16(15), CName17(16), CName18(17), CName19(18), CName20(19), CName21(20), CName22(21), CName23(22), CName24(23), CName25(24), CName26(25), CName27(26), CName28(27), CName29(28), CName30(29), CName31(30), CName32(31), CName33(32), CName34(33), CName35(34), CName36(35), CName37(36), CName38(37) { \
			CName1.SetQuery(this); \
			CName2.SetQuery(this); \
			CName3.SetQuery(this); \
			CName4.SetQuery(this); \
			CName5.SetQuery(this); \
			CName6.SetQuery(this); \
			CName7.SetQuery(this); \
			CName8.SetQuery(this); \
			CName9.SetQuery(this); \
			CName10.SetQuery(this); \
			CName11.SetQuery(this); \
			CName12.SetQuery(this); \
			CName13.SetQuery(this); \
			CName14.SetQuery(this); \
			CName15.SetQuery(this); \
			CName16.SetQuery(this); \
			CName17.SetQuery(this); \
			CName18.SetQuery(this); \
			CName19.SetQuery(this); \
			CName20.SetQuery(this); \
			CName21.SetQuery(this); \
			CName22.SetQuery(this); \
			CName23.SetQuery(this); \
			CName24.SetQuery(this); \
			CName25.SetQuery(this); \
			CName26.SetQuery(this); \
			CName27.SetQuery(this); \
			CName28.SetQuery(this); \
			CName29.SetQuery(this); \
			CName30.SetQuery(this); \
			CName31.SetQuery(this); \
			CName32.SetQuery(this); \
			CName33.SetQuery(this); \
			CName34.SetQuery(this); \
			CName35.SetQuery(this); \
			CName36.SetQuery(this); \
			CName37.SetQuery(this); \
			CName38.SetQuery(this); \
		} \
\
		class TestQueryQueryAccessorInt : private XQueryAccessorInt { \
		public: \
			TestQueryQueryAccessorInt(size_t column_id) : XQueryAccessorInt(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(int64_t value) {return (TestQuery &)XQueryAccessorInt::Equal(value);} \
			TestQuery& NotEqual(int64_t value) {return (TestQuery &)XQueryAccessorInt::NotEqual(value);} \
			TestQuery& Greater(int64_t value) {return (TestQuery &)XQueryAccessorInt::Greater(value);} \
			TestQuery& Less(int64_t value) {return (TestQuery &)XQueryAccessorInt::Less(value);} \
			TestQuery& Between(int64_t from, int64_t to) {return (TestQuery &)XQueryAccessorInt::Between(from, to);} \
		}; \
\
		template <class T> class TestQueryQueryAccessorEnum : public TestQueryQueryAccessorInt { \
		public: \
			TestQueryQueryAccessorEnum<T>(size_t column_id) : TestQueryQueryAccessorInt(column_id) {} \
		}; \
\
		class TestQueryQueryAccessorString : private XQueryAccessorString { \
		public: \
			TestQueryQueryAccessorString(size_t column_id) : XQueryAccessorString(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::Equal(value, CaseSensitive);} \
			TestQuery& NotEqual(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::NotEqual(value, CaseSensitive);} \
			TestQuery& BeginsWith(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::BeginsWith(value, CaseSensitive);} \
			TestQuery& EndsWith(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::EndsWith(value, CaseSensitive);} \
			TestQuery& Contains(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::Contains(value, CaseSensitive);} \
		}; \
\
		class TestQueryQueryAccessorBool : private XQueryAccessorBool { \
		public: \
			TestQueryQueryAccessorBool(size_t column_id) : XQueryAccessorBool(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(bool value) {return (TestQuery &)XQueryAccessorBool::Equal(value);} \
		}; \
\
		TestQueryQueryAccessor##CType1 CName1; \
		TestQueryQueryAccessor##CType2 CName2; \
		TestQueryQueryAccessor##CType3 CName3; \
		TestQueryQueryAccessor##CType4 CName4; \
		TestQueryQueryAccessor##CType5 CName5; \
		TestQueryQueryAccessor##CType6 CName6; \
		TestQueryQueryAccessor##CType7 CName7; \
		TestQueryQueryAccessor##CType8 CName8; \
		TestQueryQueryAccessor##CType9 CName9; \
		TestQueryQueryAccessor##CType10 CName10; \
		TestQueryQueryAccessor##CType11 CName11; \
		TestQueryQueryAccessor##CType12 CName12; \
		TestQueryQueryAccessor##CType13 CName13; \
		TestQueryQueryAccessor##CType14 CName14; \
		TestQueryQueryAccessor##CType15 CName15; \
		TestQueryQueryAccessor##CType16 CName16; \
		TestQueryQueryAccessor##CType17 CName17; \
		TestQueryQueryAccessor##CType18 CName18; \
		TestQueryQueryAccessor##CType19 CName19; \
		TestQueryQueryAccessor##CType20 CName20; \
		TestQueryQueryAccessor##CType21 CName21; \
		TestQueryQueryAccessor##CType22 CName22; \
		TestQueryQueryAccessor##CType23 CName23; \
		TestQueryQueryAccessor##CType24 CName24; \
		TestQueryQueryAccessor##CType25 CName25; \
		TestQueryQueryAccessor##CType26 CName26; \
		TestQueryQueryAccessor##CType27 CName27; \
		TestQueryQueryAccessor##CType28 CName28; \
		TestQueryQueryAccessor##CType29 CName29; \
		TestQueryQueryAccessor##CType30 CName30; \
		TestQueryQueryAccessor##CType31 CName31; \
		TestQueryQueryAccessor##CType32 CName32; \
		TestQueryQueryAccessor##CType33 CName33; \
		TestQueryQueryAccessor##CType34 CName34; \
		TestQueryQueryAccessor##CType35 CName35; \
		TestQueryQueryAccessor##CType36 CName36; \
		TestQueryQueryAccessor##CType37 CName37; \
		TestQueryQueryAccessor##CType38 CName38; \
\
		TestQuery& LeftParan(void) {Query::LeftParan(); return *this;}; \
		TestQuery& Or(void) {Query::Or(); return *this;}; \
		TestQuery& RightParan(void) {Query::RightParan(); return *this;}; \
		TestQuery& Subtable(size_t column) {Query::Subtable(column); return *this;}; \
		TestQuery& Parent() {Query::Parent(); return *this;}; \
	}; \
\
	TestQuery GetQuery() {return TestQuery();} \
\
	class Cursor : public CursorBase { \
	public: \
		Cursor(TableName& table, size_t ndx) : CursorBase(table, ndx) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
			CName17.Create(this, 16); \
			CName18.Create(this, 17); \
			CName19.Create(this, 18); \
			CName20.Create(this, 19); \
			CName21.Create(this, 20); \
			CName22.Create(this, 21); \
			CName23.Create(this, 22); \
			CName24.Create(this, 23); \
			CName25.Create(this, 24); \
			CName26.Create(this, 25); \
			CName27.Create(this, 26); \
			CName28.Create(this, 27); \
			CName29.Create(this, 28); \
			CName30.Create(this, 29); \
			CName31.Create(this, 30); \
			CName32.Create(this, 31); \
			CName33.Create(this, 32); \
			CName34.Create(this, 33); \
			CName35.Create(this, 34); \
			CName36.Create(this, 35); \
			CName37.Create(this, 36); \
			CName38.Create(this, 37); \
		} \
		Cursor(const TableName& table, size_t ndx) : CursorBase(const_cast<TableName&>(table), ndx) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
			CName17.Create(this, 16); \
			CName18.Create(this, 17); \
			CName19.Create(this, 18); \
			CName20.Create(this, 19); \
			CName21.Create(this, 20); \
			CName22.Create(this, 21); \
			CName23.Create(this, 22); \
			CName24.Create(this, 23); \
			CName25.Create(this, 24); \
			CName26.Create(this, 25); \
			CName27.Create(this, 26); \
			CName28.Create(this, 27); \
			CName29.Create(this, 28); \
			CName30.Create(this, 29); \
			CName31.Create(this, 30); \
			CName32.Create(this, 31); \
			CName33.Create(this, 32); \
			CName34.Create(this, 33); \
			CName35.Create(this, 34); \
			CName36.Create(this, 35); \
			CName37.Create(this, 36); \
			CName38.Create(this, 37); \
		} \
		Cursor(const Cursor& v) : CursorBase(v) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
			CName17.Create(this, 16); \
			CName18.Create(this, 17); \
			CName19.Create(this, 18); \
			CName20.Create(this, 19); \
			CName21.Create(this, 20); \
			CName22.Create(this, 21); \
			CName23.Create(this, 22); \
			CName24.Create(this, 23); \
			CName25.Create(this, 24); \
			CName26.Create(this, 25); \
			CName27.Create(this, 26); \
			CName28.Create(this, 27); \
			CName29.Create(this, 28); \
			CName30.Create(this, 29); \
			CName31.Create(this, 30); \
			CName32.Create(this, 31); \
			CName33.Create(this, 32); \
			CName34.Create(this, 33); \
			CName35.Create(this, 34); \
			CName36.Create(this, 35); \
			CName37.Create(this, 36); \
			CName38.Create(this, 37); \
		} \
		Accessor##CType1 CName1; \
		Accessor##CType2 CName2; \
		Accessor##CType3 CName3; \
		Accessor##CType4 CName4; \
		Accessor##CType5 CName5; \
		Accessor##CType6 CName6; \
		Accessor##CType7 CName7; \
		Accessor##CType8 CName8; \
		Accessor##CType9 CName9; \
		Accessor##CType10 CName10; \
		Accessor##CType11 CName11; \
		Accessor##CType12 CName12; \
		Accessor##CType13 CName13; \
		Accessor##CType14 CName14; \
		Accessor##CType15 CName15; \
		Accessor##CType16 CName16; \
		Accessor##CType17 CName17; \
		Accessor##CType18 CName18; \
		Accessor##CType19 CName19; \
		Accessor##CType20 CName20; \
		Accessor##CType21 CName21; \
		Accessor##CType22 CName22; \
		Accessor##CType23 CName23; \
		Accessor##CType24 CName24; \
		Accessor##CType25 CName25; \
		Accessor##CType26 CName26; \
		Accessor##CType27 CName27; \
		Accessor##CType28 CName28; \
		Accessor##CType29 CName29; \
		Accessor##CType30 CName30; \
		Accessor##CType31 CName31; \
		Accessor##CType32 CName32; \
		Accessor##CType33 CName33; \
		Accessor##CType34 CName34; \
		Accessor##CType35 CName35; \
		Accessor##CType36 CName36; \
		Accessor##CType37 CName37; \
		Accessor##CType38 CName38; \
	}; \
\
	void Add(tdbType##CType1 CName1, tdbType##CType2 CName2, tdbType##CType3 CName3, tdbType##CType4 CName4, tdbType##CType5 CName5, tdbType##CType6 CName6, tdbType##CType7 CName7, tdbType##CType8 CName8, tdbType##CType9 CName9, tdbType##CType10 CName10, tdbType##CType11 CName11, tdbType##CType12 CName12, tdbType##CType13 CName13, tdbType##CType14 CName14, tdbType##CType15 CName15, tdbType##CType16 CName16, tdbType##CType17 CName17, tdbType##CType18 CName18, tdbType##CType19 CName19, tdbType##CType20 CName20, tdbType##CType21 CName21, tdbType##CType22 CName22, tdbType##CType23 CName23, tdbType##CType24 CName24, tdbType##CType25 CName25, tdbType##CType26 CName26, tdbType##CType27 CName27, tdbType##CType28 CName28, tdbType##CType29 CName29, tdbType##CType30 CName30, tdbType##CType31 CName31, tdbType##CType32 CName32, tdbType##CType33 CName33, tdbType##CType34 CName34, tdbType##CType35 CName35, tdbType##CType36 CName36, tdbType##CType37 CName37, tdbType##CType38 CName38) { \
		const size_t ndx = GetSize(); \
		Insert##CType1 (0, ndx, CName1); \
		Insert##CType2 (1, ndx, CName2); \
		Insert##CType3 (2, ndx, CName3); \
		Insert##CType4 (3, ndx, CName4); \
		Insert##CType5 (4, ndx, CName5); \
		Insert##CType6 (5, ndx, CName6); \
		Insert##CType7 (6, ndx, CName7); \
		Insert##CType8 (7, ndx, CName8); \
		Insert##CType9 (8, ndx, CName9); \
		Insert##CType10 (9, ndx, CName10); \
		Insert##CType11 (10, ndx, CName11); \
		Insert##CType12 (11, ndx, CName12); \
		Insert##CType13 (12, ndx, CName13); \
		Insert##CType14 (13, ndx, CName14); \
		Insert##CType15 (14, ndx, CName15); \
		Insert##CType16 (15, ndx, CName16); \
		Insert##CType17 (16, ndx, CName17); \
		Insert##CType18 (17, ndx, CName18); \
		Insert##CType19 (18, ndx, CName19); \
		Insert##CType20 (19, ndx, CName20); \
		Insert##CType21 (20, ndx, CName21); \
		Insert##CType22 (21, ndx, CName22); \
		Insert##CType23 (22, ndx, CName23); \
		Insert##CType24 (23, ndx, CName24); \
		Insert##CType25 (24, ndx, CName25); \
		Insert##CType26 (25, ndx, CName26); \
		Insert##CType27 (26, ndx, CName27); \
		Insert##CType28 (27, ndx, CName28); \
		Insert##CType29 (28, ndx, CName29); \
		Insert##CType30 (29, ndx, CName30); \
		Insert##CType31 (30, ndx, CName31); \
		Insert##CType32 (31, ndx, CName32); \
		Insert##CType33 (32, ndx, CName33); \
		Insert##CType34 (33, ndx, CName34); \
		Insert##CType35 (34, ndx, CName35); \
		Insert##CType36 (35, ndx, CName36); \
		Insert##CType37 (36, ndx, CName37); \
		Insert##CType38 (37, ndx, CName38); \
		InsertDone(); \
	} \
\
	void Insert(size_t ndx, tdbType##CType1 CName1, tdbType##CType2 CName2, tdbType##CType3 CName3, tdbType##CType4 CName4, tdbType##CType5 CName5, tdbType##CType6 CName6, tdbType##CType7 CName7, tdbType##CType8 CName8, tdbType##CType9 CName9, tdbType##CType10 CName10, tdbType##CType11 CName11, tdbType##CType12 CName12, tdbType##CType13 CName13, tdbType##CType14 CName14, tdbType##CType15 CName15, tdbType##CType16 CName16, tdbType##CType17 CName17, tdbType##CType18 CName18, tdbType##CType19 CName19, tdbType##CType20 CName20, tdbType##CType21 CName21, tdbType##CType22 CName22, tdbType##CType23 CName23, tdbType##CType24 CName24, tdbType##CType25 CName25, tdbType##CType26 CName26, tdbType##CType27 CName27, tdbType##CType28 CName28, tdbType##CType29 CName29, tdbType##CType30 CName30, tdbType##CType31 CName31, tdbType##CType32 CName32, tdbType##CType33 CName33, tdbType##CType34 CName34, tdbType##CType35 CName35, tdbType##CType36 CName36, tdbType##CType37 CName37, tdbType##CType38 CName38) { \
		Insert##CType1 (0, ndx, CName1); \
		Insert##CType2 (1, ndx, CName2); \
		Insert##CType3 (2, ndx, CName3); \
		Insert##CType4 (3, ndx, CName4); \
		Insert##CType5 (4, ndx, CName5); \
		Insert##CType6 (5, ndx, CName6); \
		Insert##CType7 (6, ndx, CName7); \
		Insert##CType8 (7, ndx, CName8); \
		Insert##CType9 (8, ndx, CName9); \
		Insert##CType10 (9, ndx, CName10); \
		Insert##CType11 (10, ndx, CName11); \
		Insert##CType12 (11, ndx, CName12); \
		Insert##CType13 (12, ndx, CName13); \
		Insert##CType14 (13, ndx, CName14); \
		Insert##CType15 (14, ndx, CName15); \
		Insert##CType16 (15, ndx, CName16); \
		Insert##CType17 (16, ndx, CName17); \
		Insert##CType18 (17, ndx, CName18); \
		Insert##CType19 (18, ndx, CName19); \
		Insert##CType20 (19, ndx, CName20); \
		Insert##CType21 (20, ndx, CName21); \
		Insert##CType22 (21, ndx, CName22); \
		Insert##CType23 (22, ndx, CName23); \
		Insert##CType24 (23, ndx, CName24); \
		Insert##CType25 (24, ndx, CName25); \
		Insert##CType26 (25, ndx, CName26); \
		Insert##CType27 (26, ndx, CName27); \
		Insert##CType28 (27, ndx, CName28); \
		Insert##CType29 (28, ndx, CName29); \
		Insert##CType30 (29, ndx, CName30); \
		Insert##CType31 (30, ndx, CName31); \
		Insert##CType32 (31, ndx, CName32); \
		Insert##CType33 (32, ndx, CName33); \
		Insert##CType34 (33, ndx, CName34); \
		Insert##CType35 (34, ndx, CName35); \
		Insert##CType36 (35, ndx, CName36); \
		Insert##CType37 (36, ndx, CName37); \
		Insert##CType38 (37, ndx, CName38); \
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
	ColumnProxy##CType3 CName3; \
	ColumnProxy##CType4 CName4; \
	ColumnProxy##CType5 CName5; \
	ColumnProxy##CType6 CName6; \
	ColumnProxy##CType7 CName7; \
	ColumnProxy##CType8 CName8; \
	ColumnProxy##CType9 CName9; \
	ColumnProxy##CType10 CName10; \
	ColumnProxy##CType11 CName11; \
	ColumnProxy##CType12 CName12; \
	ColumnProxy##CType13 CName13; \
	ColumnProxy##CType14 CName14; \
	ColumnProxy##CType15 CName15; \
	ColumnProxy##CType16 CName16; \
	ColumnProxy##CType17 CName17; \
	ColumnProxy##CType18 CName18; \
	ColumnProxy##CType19 CName19; \
	ColumnProxy##CType20 CName20; \
	ColumnProxy##CType21 CName21; \
	ColumnProxy##CType22 CName22; \
	ColumnProxy##CType23 CName23; \
	ColumnProxy##CType24 CName24; \
	ColumnProxy##CType25 CName25; \
	ColumnProxy##CType26 CName26; \
	ColumnProxy##CType27 CName27; \
	ColumnProxy##CType28 CName28; \
	ColumnProxy##CType29 CName29; \
	ColumnProxy##CType30 CName30; \
	ColumnProxy##CType31 CName31; \
	ColumnProxy##CType32 CName32; \
	ColumnProxy##CType33 CName33; \
	ColumnProxy##CType34 CName34; \
	ColumnProxy##CType35 CName35; \
	ColumnProxy##CType36 CName36; \
	ColumnProxy##CType37 CName37; \
	ColumnProxy##CType38 CName38; \
\
protected: \
	friend class Group; \
	TableName(Allocator& alloc, size_t ref, Array* parent, size_t pndx) : TopLevelTable(alloc, ref, parent, pndx) {}; \
\
private: \
	TableName(const TableName&) {} \
	TableName& operator=(const TableName&) {return *this;} \
};



#define TDB_TABLE_39(TableName, CType1, CName1, CType2, CName2, CType3, CName3, CType4, CName4, CType5, CName5, CType6, CName6, CType7, CName7, CType8, CName8, CType9, CName9, CType10, CName10, CType11, CName11, CType12, CName12, CType13, CName13, CType14, CName14, CType15, CName15, CType16, CName16, CType17, CName17, CType18, CName18, CType19, CName19, CType20, CName20, CType21, CName21, CType22, CName22, CType23, CName23, CType24, CName24, CType25, CName25, CType26, CName26, CType27, CName27, CType28, CName28, CType29, CName29, CType30, CName30, CType31, CName31, CType32, CName32, CType33, CName33, CType34, CName34, CType35, CName35, CType36, CName36, CType37, CName37, CType38, CName38, CType39, CName39) \
class TableName##Query { \
protected: \
	QueryAccessor##CType1 CName1; \
	QueryAccessor##CType2 CName2; \
	QueryAccessor##CType3 CName3; \
	QueryAccessor##CType4 CName4; \
	QueryAccessor##CType5 CName5; \
	QueryAccessor##CType6 CName6; \
	QueryAccessor##CType7 CName7; \
	QueryAccessor##CType8 CName8; \
	QueryAccessor##CType9 CName9; \
	QueryAccessor##CType10 CName10; \
	QueryAccessor##CType11 CName11; \
	QueryAccessor##CType12 CName12; \
	QueryAccessor##CType13 CName13; \
	QueryAccessor##CType14 CName14; \
	QueryAccessor##CType15 CName15; \
	QueryAccessor##CType16 CName16; \
	QueryAccessor##CType17 CName17; \
	QueryAccessor##CType18 CName18; \
	QueryAccessor##CType19 CName19; \
	QueryAccessor##CType20 CName20; \
	QueryAccessor##CType21 CName21; \
	QueryAccessor##CType22 CName22; \
	QueryAccessor##CType23 CName23; \
	QueryAccessor##CType24 CName24; \
	QueryAccessor##CType25 CName25; \
	QueryAccessor##CType26 CName26; \
	QueryAccessor##CType27 CName27; \
	QueryAccessor##CType28 CName28; \
	QueryAccessor##CType29 CName29; \
	QueryAccessor##CType30 CName30; \
	QueryAccessor##CType31 CName31; \
	QueryAccessor##CType32 CName32; \
	QueryAccessor##CType33 CName33; \
	QueryAccessor##CType34 CName34; \
	QueryAccessor##CType35 CName35; \
	QueryAccessor##CType36 CName36; \
	QueryAccessor##CType37 CName37; \
	QueryAccessor##CType38 CName38; \
	QueryAccessor##CType39 CName39; \
}; \
\
class TableName : public TopLevelTable { \
public: \
	TableName(Allocator& alloc=GetDefaultAllocator()) : TopLevelTable(alloc) { \
		RegisterColumn(Accessor##CType1::type, #CName1); \
		RegisterColumn(Accessor##CType2::type, #CName2); \
		RegisterColumn(Accessor##CType3::type, #CName3); \
		RegisterColumn(Accessor##CType4::type, #CName4); \
		RegisterColumn(Accessor##CType5::type, #CName5); \
		RegisterColumn(Accessor##CType6::type, #CName6); \
		RegisterColumn(Accessor##CType7::type, #CName7); \
		RegisterColumn(Accessor##CType8::type, #CName8); \
		RegisterColumn(Accessor##CType9::type, #CName9); \
		RegisterColumn(Accessor##CType10::type, #CName10); \
		RegisterColumn(Accessor##CType11::type, #CName11); \
		RegisterColumn(Accessor##CType12::type, #CName12); \
		RegisterColumn(Accessor##CType13::type, #CName13); \
		RegisterColumn(Accessor##CType14::type, #CName14); \
		RegisterColumn(Accessor##CType15::type, #CName15); \
		RegisterColumn(Accessor##CType16::type, #CName16); \
		RegisterColumn(Accessor##CType17::type, #CName17); \
		RegisterColumn(Accessor##CType18::type, #CName18); \
		RegisterColumn(Accessor##CType19::type, #CName19); \
		RegisterColumn(Accessor##CType20::type, #CName20); \
		RegisterColumn(Accessor##CType21::type, #CName21); \
		RegisterColumn(Accessor##CType22::type, #CName22); \
		RegisterColumn(Accessor##CType23::type, #CName23); \
		RegisterColumn(Accessor##CType24::type, #CName24); \
		RegisterColumn(Accessor##CType25::type, #CName25); \
		RegisterColumn(Accessor##CType26::type, #CName26); \
		RegisterColumn(Accessor##CType27::type, #CName27); \
		RegisterColumn(Accessor##CType28::type, #CName28); \
		RegisterColumn(Accessor##CType29::type, #CName29); \
		RegisterColumn(Accessor##CType30::type, #CName30); \
		RegisterColumn(Accessor##CType31::type, #CName31); \
		RegisterColumn(Accessor##CType32::type, #CName32); \
		RegisterColumn(Accessor##CType33::type, #CName33); \
		RegisterColumn(Accessor##CType34::type, #CName34); \
		RegisterColumn(Accessor##CType35::type, #CName35); \
		RegisterColumn(Accessor##CType36::type, #CName36); \
		RegisterColumn(Accessor##CType37::type, #CName37); \
		RegisterColumn(Accessor##CType38::type, #CName38); \
		RegisterColumn(Accessor##CType39::type, #CName39); \
\
		CName1.Create(this, 0); \
		CName2.Create(this, 1); \
		CName3.Create(this, 2); \
		CName4.Create(this, 3); \
		CName5.Create(this, 4); \
		CName6.Create(this, 5); \
		CName7.Create(this, 6); \
		CName8.Create(this, 7); \
		CName9.Create(this, 8); \
		CName10.Create(this, 9); \
		CName11.Create(this, 10); \
		CName12.Create(this, 11); \
		CName13.Create(this, 12); \
		CName14.Create(this, 13); \
		CName15.Create(this, 14); \
		CName16.Create(this, 15); \
		CName17.Create(this, 16); \
		CName18.Create(this, 17); \
		CName19.Create(this, 18); \
		CName20.Create(this, 19); \
		CName21.Create(this, 20); \
		CName22.Create(this, 21); \
		CName23.Create(this, 22); \
		CName24.Create(this, 23); \
		CName25.Create(this, 24); \
		CName26.Create(this, 25); \
		CName27.Create(this, 26); \
		CName28.Create(this, 27); \
		CName29.Create(this, 28); \
		CName30.Create(this, 29); \
		CName31.Create(this, 30); \
		CName32.Create(this, 31); \
		CName33.Create(this, 32); \
		CName34.Create(this, 33); \
		CName35.Create(this, 34); \
		CName36.Create(this, 35); \
		CName37.Create(this, 36); \
		CName38.Create(this, 37); \
		CName39.Create(this, 38); \
	}; \
\
	class TestQuery : public Query { \
	public: \
		TestQuery() : CName1(0), CName2(1), CName3(2), CName4(3), CName5(4), CName6(5), CName7(6), CName8(7), CName9(8), CName10(9), CName11(10), CName12(11), CName13(12), CName14(13), CName15(14), CName16(15), CName17(16), CName18(17), CName19(18), CName20(19), CName21(20), CName22(21), CName23(22), CName24(23), CName25(24), CName26(25), CName27(26), CName28(27), CName29(28), CName30(29), CName31(30), CName32(31), CName33(32), CName34(33), CName35(34), CName36(35), CName37(36), CName38(37), CName39(38) { \
			CName1.SetQuery(this); \
			CName2.SetQuery(this); \
			CName3.SetQuery(this); \
			CName4.SetQuery(this); \
			CName5.SetQuery(this); \
			CName6.SetQuery(this); \
			CName7.SetQuery(this); \
			CName8.SetQuery(this); \
			CName9.SetQuery(this); \
			CName10.SetQuery(this); \
			CName11.SetQuery(this); \
			CName12.SetQuery(this); \
			CName13.SetQuery(this); \
			CName14.SetQuery(this); \
			CName15.SetQuery(this); \
			CName16.SetQuery(this); \
			CName17.SetQuery(this); \
			CName18.SetQuery(this); \
			CName19.SetQuery(this); \
			CName20.SetQuery(this); \
			CName21.SetQuery(this); \
			CName22.SetQuery(this); \
			CName23.SetQuery(this); \
			CName24.SetQuery(this); \
			CName25.SetQuery(this); \
			CName26.SetQuery(this); \
			CName27.SetQuery(this); \
			CName28.SetQuery(this); \
			CName29.SetQuery(this); \
			CName30.SetQuery(this); \
			CName31.SetQuery(this); \
			CName32.SetQuery(this); \
			CName33.SetQuery(this); \
			CName34.SetQuery(this); \
			CName35.SetQuery(this); \
			CName36.SetQuery(this); \
			CName37.SetQuery(this); \
			CName38.SetQuery(this); \
			CName39.SetQuery(this); \
		} \
\
		TestQuery(const TestQuery& copy) : Query(copy), CName1(0), CName2(1), CName3(2), CName4(3), CName5(4), CName6(5), CName7(6), CName8(7), CName9(8), CName10(9), CName11(10), CName12(11), CName13(12), CName14(13), CName15(14), CName16(15), CName17(16), CName18(17), CName19(18), CName20(19), CName21(20), CName22(21), CName23(22), CName24(23), CName25(24), CName26(25), CName27(26), CName28(27), CName29(28), CName30(29), CName31(30), CName32(31), CName33(32), CName34(33), CName35(34), CName36(35), CName37(36), CName38(37), CName39(38) { \
			CName1.SetQuery(this); \
			CName2.SetQuery(this); \
			CName3.SetQuery(this); \
			CName4.SetQuery(this); \
			CName5.SetQuery(this); \
			CName6.SetQuery(this); \
			CName7.SetQuery(this); \
			CName8.SetQuery(this); \
			CName9.SetQuery(this); \
			CName10.SetQuery(this); \
			CName11.SetQuery(this); \
			CName12.SetQuery(this); \
			CName13.SetQuery(this); \
			CName14.SetQuery(this); \
			CName15.SetQuery(this); \
			CName16.SetQuery(this); \
			CName17.SetQuery(this); \
			CName18.SetQuery(this); \
			CName19.SetQuery(this); \
			CName20.SetQuery(this); \
			CName21.SetQuery(this); \
			CName22.SetQuery(this); \
			CName23.SetQuery(this); \
			CName24.SetQuery(this); \
			CName25.SetQuery(this); \
			CName26.SetQuery(this); \
			CName27.SetQuery(this); \
			CName28.SetQuery(this); \
			CName29.SetQuery(this); \
			CName30.SetQuery(this); \
			CName31.SetQuery(this); \
			CName32.SetQuery(this); \
			CName33.SetQuery(this); \
			CName34.SetQuery(this); \
			CName35.SetQuery(this); \
			CName36.SetQuery(this); \
			CName37.SetQuery(this); \
			CName38.SetQuery(this); \
			CName39.SetQuery(this); \
		} \
\
		class TestQueryQueryAccessorInt : private XQueryAccessorInt { \
		public: \
			TestQueryQueryAccessorInt(size_t column_id) : XQueryAccessorInt(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(int64_t value) {return (TestQuery &)XQueryAccessorInt::Equal(value);} \
			TestQuery& NotEqual(int64_t value) {return (TestQuery &)XQueryAccessorInt::NotEqual(value);} \
			TestQuery& Greater(int64_t value) {return (TestQuery &)XQueryAccessorInt::Greater(value);} \
			TestQuery& Less(int64_t value) {return (TestQuery &)XQueryAccessorInt::Less(value);} \
			TestQuery& Between(int64_t from, int64_t to) {return (TestQuery &)XQueryAccessorInt::Between(from, to);} \
		}; \
\
		template <class T> class TestQueryQueryAccessorEnum : public TestQueryQueryAccessorInt { \
		public: \
			TestQueryQueryAccessorEnum<T>(size_t column_id) : TestQueryQueryAccessorInt(column_id) {} \
		}; \
\
		class TestQueryQueryAccessorString : private XQueryAccessorString { \
		public: \
			TestQueryQueryAccessorString(size_t column_id) : XQueryAccessorString(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::Equal(value, CaseSensitive);} \
			TestQuery& NotEqual(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::NotEqual(value, CaseSensitive);} \
			TestQuery& BeginsWith(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::BeginsWith(value, CaseSensitive);} \
			TestQuery& EndsWith(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::EndsWith(value, CaseSensitive);} \
			TestQuery& Contains(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::Contains(value, CaseSensitive);} \
		}; \
\
		class TestQueryQueryAccessorBool : private XQueryAccessorBool { \
		public: \
			TestQueryQueryAccessorBool(size_t column_id) : XQueryAccessorBool(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(bool value) {return (TestQuery &)XQueryAccessorBool::Equal(value);} \
		}; \
\
		TestQueryQueryAccessor##CType1 CName1; \
		TestQueryQueryAccessor##CType2 CName2; \
		TestQueryQueryAccessor##CType3 CName3; \
		TestQueryQueryAccessor##CType4 CName4; \
		TestQueryQueryAccessor##CType5 CName5; \
		TestQueryQueryAccessor##CType6 CName6; \
		TestQueryQueryAccessor##CType7 CName7; \
		TestQueryQueryAccessor##CType8 CName8; \
		TestQueryQueryAccessor##CType9 CName9; \
		TestQueryQueryAccessor##CType10 CName10; \
		TestQueryQueryAccessor##CType11 CName11; \
		TestQueryQueryAccessor##CType12 CName12; \
		TestQueryQueryAccessor##CType13 CName13; \
		TestQueryQueryAccessor##CType14 CName14; \
		TestQueryQueryAccessor##CType15 CName15; \
		TestQueryQueryAccessor##CType16 CName16; \
		TestQueryQueryAccessor##CType17 CName17; \
		TestQueryQueryAccessor##CType18 CName18; \
		TestQueryQueryAccessor##CType19 CName19; \
		TestQueryQueryAccessor##CType20 CName20; \
		TestQueryQueryAccessor##CType21 CName21; \
		TestQueryQueryAccessor##CType22 CName22; \
		TestQueryQueryAccessor##CType23 CName23; \
		TestQueryQueryAccessor##CType24 CName24; \
		TestQueryQueryAccessor##CType25 CName25; \
		TestQueryQueryAccessor##CType26 CName26; \
		TestQueryQueryAccessor##CType27 CName27; \
		TestQueryQueryAccessor##CType28 CName28; \
		TestQueryQueryAccessor##CType29 CName29; \
		TestQueryQueryAccessor##CType30 CName30; \
		TestQueryQueryAccessor##CType31 CName31; \
		TestQueryQueryAccessor##CType32 CName32; \
		TestQueryQueryAccessor##CType33 CName33; \
		TestQueryQueryAccessor##CType34 CName34; \
		TestQueryQueryAccessor##CType35 CName35; \
		TestQueryQueryAccessor##CType36 CName36; \
		TestQueryQueryAccessor##CType37 CName37; \
		TestQueryQueryAccessor##CType38 CName38; \
		TestQueryQueryAccessor##CType39 CName39; \
\
		TestQuery& LeftParan(void) {Query::LeftParan(); return *this;}; \
		TestQuery& Or(void) {Query::Or(); return *this;}; \
		TestQuery& RightParan(void) {Query::RightParan(); return *this;}; \
		TestQuery& Subtable(size_t column) {Query::Subtable(column); return *this;}; \
		TestQuery& Parent() {Query::Parent(); return *this;}; \
	}; \
\
	TestQuery GetQuery() {return TestQuery();} \
\
	class Cursor : public CursorBase { \
	public: \
		Cursor(TableName& table, size_t ndx) : CursorBase(table, ndx) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
			CName17.Create(this, 16); \
			CName18.Create(this, 17); \
			CName19.Create(this, 18); \
			CName20.Create(this, 19); \
			CName21.Create(this, 20); \
			CName22.Create(this, 21); \
			CName23.Create(this, 22); \
			CName24.Create(this, 23); \
			CName25.Create(this, 24); \
			CName26.Create(this, 25); \
			CName27.Create(this, 26); \
			CName28.Create(this, 27); \
			CName29.Create(this, 28); \
			CName30.Create(this, 29); \
			CName31.Create(this, 30); \
			CName32.Create(this, 31); \
			CName33.Create(this, 32); \
			CName34.Create(this, 33); \
			CName35.Create(this, 34); \
			CName36.Create(this, 35); \
			CName37.Create(this, 36); \
			CName38.Create(this, 37); \
			CName39.Create(this, 38); \
		} \
		Cursor(const TableName& table, size_t ndx) : CursorBase(const_cast<TableName&>(table), ndx) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
			CName17.Create(this, 16); \
			CName18.Create(this, 17); \
			CName19.Create(this, 18); \
			CName20.Create(this, 19); \
			CName21.Create(this, 20); \
			CName22.Create(this, 21); \
			CName23.Create(this, 22); \
			CName24.Create(this, 23); \
			CName25.Create(this, 24); \
			CName26.Create(this, 25); \
			CName27.Create(this, 26); \
			CName28.Create(this, 27); \
			CName29.Create(this, 28); \
			CName30.Create(this, 29); \
			CName31.Create(this, 30); \
			CName32.Create(this, 31); \
			CName33.Create(this, 32); \
			CName34.Create(this, 33); \
			CName35.Create(this, 34); \
			CName36.Create(this, 35); \
			CName37.Create(this, 36); \
			CName38.Create(this, 37); \
			CName39.Create(this, 38); \
		} \
		Cursor(const Cursor& v) : CursorBase(v) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
			CName17.Create(this, 16); \
			CName18.Create(this, 17); \
			CName19.Create(this, 18); \
			CName20.Create(this, 19); \
			CName21.Create(this, 20); \
			CName22.Create(this, 21); \
			CName23.Create(this, 22); \
			CName24.Create(this, 23); \
			CName25.Create(this, 24); \
			CName26.Create(this, 25); \
			CName27.Create(this, 26); \
			CName28.Create(this, 27); \
			CName29.Create(this, 28); \
			CName30.Create(this, 29); \
			CName31.Create(this, 30); \
			CName32.Create(this, 31); \
			CName33.Create(this, 32); \
			CName34.Create(this, 33); \
			CName35.Create(this, 34); \
			CName36.Create(this, 35); \
			CName37.Create(this, 36); \
			CName38.Create(this, 37); \
			CName39.Create(this, 38); \
		} \
		Accessor##CType1 CName1; \
		Accessor##CType2 CName2; \
		Accessor##CType3 CName3; \
		Accessor##CType4 CName4; \
		Accessor##CType5 CName5; \
		Accessor##CType6 CName6; \
		Accessor##CType7 CName7; \
		Accessor##CType8 CName8; \
		Accessor##CType9 CName9; \
		Accessor##CType10 CName10; \
		Accessor##CType11 CName11; \
		Accessor##CType12 CName12; \
		Accessor##CType13 CName13; \
		Accessor##CType14 CName14; \
		Accessor##CType15 CName15; \
		Accessor##CType16 CName16; \
		Accessor##CType17 CName17; \
		Accessor##CType18 CName18; \
		Accessor##CType19 CName19; \
		Accessor##CType20 CName20; \
		Accessor##CType21 CName21; \
		Accessor##CType22 CName22; \
		Accessor##CType23 CName23; \
		Accessor##CType24 CName24; \
		Accessor##CType25 CName25; \
		Accessor##CType26 CName26; \
		Accessor##CType27 CName27; \
		Accessor##CType28 CName28; \
		Accessor##CType29 CName29; \
		Accessor##CType30 CName30; \
		Accessor##CType31 CName31; \
		Accessor##CType32 CName32; \
		Accessor##CType33 CName33; \
		Accessor##CType34 CName34; \
		Accessor##CType35 CName35; \
		Accessor##CType36 CName36; \
		Accessor##CType37 CName37; \
		Accessor##CType38 CName38; \
		Accessor##CType39 CName39; \
	}; \
\
	void Add(tdbType##CType1 CName1, tdbType##CType2 CName2, tdbType##CType3 CName3, tdbType##CType4 CName4, tdbType##CType5 CName5, tdbType##CType6 CName6, tdbType##CType7 CName7, tdbType##CType8 CName8, tdbType##CType9 CName9, tdbType##CType10 CName10, tdbType##CType11 CName11, tdbType##CType12 CName12, tdbType##CType13 CName13, tdbType##CType14 CName14, tdbType##CType15 CName15, tdbType##CType16 CName16, tdbType##CType17 CName17, tdbType##CType18 CName18, tdbType##CType19 CName19, tdbType##CType20 CName20, tdbType##CType21 CName21, tdbType##CType22 CName22, tdbType##CType23 CName23, tdbType##CType24 CName24, tdbType##CType25 CName25, tdbType##CType26 CName26, tdbType##CType27 CName27, tdbType##CType28 CName28, tdbType##CType29 CName29, tdbType##CType30 CName30, tdbType##CType31 CName31, tdbType##CType32 CName32, tdbType##CType33 CName33, tdbType##CType34 CName34, tdbType##CType35 CName35, tdbType##CType36 CName36, tdbType##CType37 CName37, tdbType##CType38 CName38, tdbType##CType39 CName39) { \
		const size_t ndx = GetSize(); \
		Insert##CType1 (0, ndx, CName1); \
		Insert##CType2 (1, ndx, CName2); \
		Insert##CType3 (2, ndx, CName3); \
		Insert##CType4 (3, ndx, CName4); \
		Insert##CType5 (4, ndx, CName5); \
		Insert##CType6 (5, ndx, CName6); \
		Insert##CType7 (6, ndx, CName7); \
		Insert##CType8 (7, ndx, CName8); \
		Insert##CType9 (8, ndx, CName9); \
		Insert##CType10 (9, ndx, CName10); \
		Insert##CType11 (10, ndx, CName11); \
		Insert##CType12 (11, ndx, CName12); \
		Insert##CType13 (12, ndx, CName13); \
		Insert##CType14 (13, ndx, CName14); \
		Insert##CType15 (14, ndx, CName15); \
		Insert##CType16 (15, ndx, CName16); \
		Insert##CType17 (16, ndx, CName17); \
		Insert##CType18 (17, ndx, CName18); \
		Insert##CType19 (18, ndx, CName19); \
		Insert##CType20 (19, ndx, CName20); \
		Insert##CType21 (20, ndx, CName21); \
		Insert##CType22 (21, ndx, CName22); \
		Insert##CType23 (22, ndx, CName23); \
		Insert##CType24 (23, ndx, CName24); \
		Insert##CType25 (24, ndx, CName25); \
		Insert##CType26 (25, ndx, CName26); \
		Insert##CType27 (26, ndx, CName27); \
		Insert##CType28 (27, ndx, CName28); \
		Insert##CType29 (28, ndx, CName29); \
		Insert##CType30 (29, ndx, CName30); \
		Insert##CType31 (30, ndx, CName31); \
		Insert##CType32 (31, ndx, CName32); \
		Insert##CType33 (32, ndx, CName33); \
		Insert##CType34 (33, ndx, CName34); \
		Insert##CType35 (34, ndx, CName35); \
		Insert##CType36 (35, ndx, CName36); \
		Insert##CType37 (36, ndx, CName37); \
		Insert##CType38 (37, ndx, CName38); \
		Insert##CType39 (38, ndx, CName39); \
		InsertDone(); \
	} \
\
	void Insert(size_t ndx, tdbType##CType1 CName1, tdbType##CType2 CName2, tdbType##CType3 CName3, tdbType##CType4 CName4, tdbType##CType5 CName5, tdbType##CType6 CName6, tdbType##CType7 CName7, tdbType##CType8 CName8, tdbType##CType9 CName9, tdbType##CType10 CName10, tdbType##CType11 CName11, tdbType##CType12 CName12, tdbType##CType13 CName13, tdbType##CType14 CName14, tdbType##CType15 CName15, tdbType##CType16 CName16, tdbType##CType17 CName17, tdbType##CType18 CName18, tdbType##CType19 CName19, tdbType##CType20 CName20, tdbType##CType21 CName21, tdbType##CType22 CName22, tdbType##CType23 CName23, tdbType##CType24 CName24, tdbType##CType25 CName25, tdbType##CType26 CName26, tdbType##CType27 CName27, tdbType##CType28 CName28, tdbType##CType29 CName29, tdbType##CType30 CName30, tdbType##CType31 CName31, tdbType##CType32 CName32, tdbType##CType33 CName33, tdbType##CType34 CName34, tdbType##CType35 CName35, tdbType##CType36 CName36, tdbType##CType37 CName37, tdbType##CType38 CName38, tdbType##CType39 CName39) { \
		Insert##CType1 (0, ndx, CName1); \
		Insert##CType2 (1, ndx, CName2); \
		Insert##CType3 (2, ndx, CName3); \
		Insert##CType4 (3, ndx, CName4); \
		Insert##CType5 (4, ndx, CName5); \
		Insert##CType6 (5, ndx, CName6); \
		Insert##CType7 (6, ndx, CName7); \
		Insert##CType8 (7, ndx, CName8); \
		Insert##CType9 (8, ndx, CName9); \
		Insert##CType10 (9, ndx, CName10); \
		Insert##CType11 (10, ndx, CName11); \
		Insert##CType12 (11, ndx, CName12); \
		Insert##CType13 (12, ndx, CName13); \
		Insert##CType14 (13, ndx, CName14); \
		Insert##CType15 (14, ndx, CName15); \
		Insert##CType16 (15, ndx, CName16); \
		Insert##CType17 (16, ndx, CName17); \
		Insert##CType18 (17, ndx, CName18); \
		Insert##CType19 (18, ndx, CName19); \
		Insert##CType20 (19, ndx, CName20); \
		Insert##CType21 (20, ndx, CName21); \
		Insert##CType22 (21, ndx, CName22); \
		Insert##CType23 (22, ndx, CName23); \
		Insert##CType24 (23, ndx, CName24); \
		Insert##CType25 (24, ndx, CName25); \
		Insert##CType26 (25, ndx, CName26); \
		Insert##CType27 (26, ndx, CName27); \
		Insert##CType28 (27, ndx, CName28); \
		Insert##CType29 (28, ndx, CName29); \
		Insert##CType30 (29, ndx, CName30); \
		Insert##CType31 (30, ndx, CName31); \
		Insert##CType32 (31, ndx, CName32); \
		Insert##CType33 (32, ndx, CName33); \
		Insert##CType34 (33, ndx, CName34); \
		Insert##CType35 (34, ndx, CName35); \
		Insert##CType36 (35, ndx, CName36); \
		Insert##CType37 (36, ndx, CName37); \
		Insert##CType38 (37, ndx, CName38); \
		Insert##CType39 (38, ndx, CName39); \
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
	ColumnProxy##CType3 CName3; \
	ColumnProxy##CType4 CName4; \
	ColumnProxy##CType5 CName5; \
	ColumnProxy##CType6 CName6; \
	ColumnProxy##CType7 CName7; \
	ColumnProxy##CType8 CName8; \
	ColumnProxy##CType9 CName9; \
	ColumnProxy##CType10 CName10; \
	ColumnProxy##CType11 CName11; \
	ColumnProxy##CType12 CName12; \
	ColumnProxy##CType13 CName13; \
	ColumnProxy##CType14 CName14; \
	ColumnProxy##CType15 CName15; \
	ColumnProxy##CType16 CName16; \
	ColumnProxy##CType17 CName17; \
	ColumnProxy##CType18 CName18; \
	ColumnProxy##CType19 CName19; \
	ColumnProxy##CType20 CName20; \
	ColumnProxy##CType21 CName21; \
	ColumnProxy##CType22 CName22; \
	ColumnProxy##CType23 CName23; \
	ColumnProxy##CType24 CName24; \
	ColumnProxy##CType25 CName25; \
	ColumnProxy##CType26 CName26; \
	ColumnProxy##CType27 CName27; \
	ColumnProxy##CType28 CName28; \
	ColumnProxy##CType29 CName29; \
	ColumnProxy##CType30 CName30; \
	ColumnProxy##CType31 CName31; \
	ColumnProxy##CType32 CName32; \
	ColumnProxy##CType33 CName33; \
	ColumnProxy##CType34 CName34; \
	ColumnProxy##CType35 CName35; \
	ColumnProxy##CType36 CName36; \
	ColumnProxy##CType37 CName37; \
	ColumnProxy##CType38 CName38; \
	ColumnProxy##CType39 CName39; \
\
protected: \
	friend class Group; \
	TableName(Allocator& alloc, size_t ref, Array* parent, size_t pndx) : TopLevelTable(alloc, ref, parent, pndx) {}; \
\
private: \
	TableName(const TableName&) {} \
	TableName& operator=(const TableName&) {return *this;} \
};



#define TDB_TABLE_40(TableName, CType1, CName1, CType2, CName2, CType3, CName3, CType4, CName4, CType5, CName5, CType6, CName6, CType7, CName7, CType8, CName8, CType9, CName9, CType10, CName10, CType11, CName11, CType12, CName12, CType13, CName13, CType14, CName14, CType15, CName15, CType16, CName16, CType17, CName17, CType18, CName18, CType19, CName19, CType20, CName20, CType21, CName21, CType22, CName22, CType23, CName23, CType24, CName24, CType25, CName25, CType26, CName26, CType27, CName27, CType28, CName28, CType29, CName29, CType30, CName30, CType31, CName31, CType32, CName32, CType33, CName33, CType34, CName34, CType35, CName35, CType36, CName36, CType37, CName37, CType38, CName38, CType39, CName39, CType40, CName40) \
class TableName##Query { \
protected: \
	QueryAccessor##CType1 CName1; \
	QueryAccessor##CType2 CName2; \
	QueryAccessor##CType3 CName3; \
	QueryAccessor##CType4 CName4; \
	QueryAccessor##CType5 CName5; \
	QueryAccessor##CType6 CName6; \
	QueryAccessor##CType7 CName7; \
	QueryAccessor##CType8 CName8; \
	QueryAccessor##CType9 CName9; \
	QueryAccessor##CType10 CName10; \
	QueryAccessor##CType11 CName11; \
	QueryAccessor##CType12 CName12; \
	QueryAccessor##CType13 CName13; \
	QueryAccessor##CType14 CName14; \
	QueryAccessor##CType15 CName15; \
	QueryAccessor##CType16 CName16; \
	QueryAccessor##CType17 CName17; \
	QueryAccessor##CType18 CName18; \
	QueryAccessor##CType19 CName19; \
	QueryAccessor##CType20 CName20; \
	QueryAccessor##CType21 CName21; \
	QueryAccessor##CType22 CName22; \
	QueryAccessor##CType23 CName23; \
	QueryAccessor##CType24 CName24; \
	QueryAccessor##CType25 CName25; \
	QueryAccessor##CType26 CName26; \
	QueryAccessor##CType27 CName27; \
	QueryAccessor##CType28 CName28; \
	QueryAccessor##CType29 CName29; \
	QueryAccessor##CType30 CName30; \
	QueryAccessor##CType31 CName31; \
	QueryAccessor##CType32 CName32; \
	QueryAccessor##CType33 CName33; \
	QueryAccessor##CType34 CName34; \
	QueryAccessor##CType35 CName35; \
	QueryAccessor##CType36 CName36; \
	QueryAccessor##CType37 CName37; \
	QueryAccessor##CType38 CName38; \
	QueryAccessor##CType39 CName39; \
	QueryAccessor##CType40 CName40; \
}; \
\
class TableName : public TopLevelTable { \
public: \
	TableName(Allocator& alloc=GetDefaultAllocator()) : TopLevelTable(alloc) { \
		RegisterColumn(Accessor##CType1::type, #CName1); \
		RegisterColumn(Accessor##CType2::type, #CName2); \
		RegisterColumn(Accessor##CType3::type, #CName3); \
		RegisterColumn(Accessor##CType4::type, #CName4); \
		RegisterColumn(Accessor##CType5::type, #CName5); \
		RegisterColumn(Accessor##CType6::type, #CName6); \
		RegisterColumn(Accessor##CType7::type, #CName7); \
		RegisterColumn(Accessor##CType8::type, #CName8); \
		RegisterColumn(Accessor##CType9::type, #CName9); \
		RegisterColumn(Accessor##CType10::type, #CName10); \
		RegisterColumn(Accessor##CType11::type, #CName11); \
		RegisterColumn(Accessor##CType12::type, #CName12); \
		RegisterColumn(Accessor##CType13::type, #CName13); \
		RegisterColumn(Accessor##CType14::type, #CName14); \
		RegisterColumn(Accessor##CType15::type, #CName15); \
		RegisterColumn(Accessor##CType16::type, #CName16); \
		RegisterColumn(Accessor##CType17::type, #CName17); \
		RegisterColumn(Accessor##CType18::type, #CName18); \
		RegisterColumn(Accessor##CType19::type, #CName19); \
		RegisterColumn(Accessor##CType20::type, #CName20); \
		RegisterColumn(Accessor##CType21::type, #CName21); \
		RegisterColumn(Accessor##CType22::type, #CName22); \
		RegisterColumn(Accessor##CType23::type, #CName23); \
		RegisterColumn(Accessor##CType24::type, #CName24); \
		RegisterColumn(Accessor##CType25::type, #CName25); \
		RegisterColumn(Accessor##CType26::type, #CName26); \
		RegisterColumn(Accessor##CType27::type, #CName27); \
		RegisterColumn(Accessor##CType28::type, #CName28); \
		RegisterColumn(Accessor##CType29::type, #CName29); \
		RegisterColumn(Accessor##CType30::type, #CName30); \
		RegisterColumn(Accessor##CType31::type, #CName31); \
		RegisterColumn(Accessor##CType32::type, #CName32); \
		RegisterColumn(Accessor##CType33::type, #CName33); \
		RegisterColumn(Accessor##CType34::type, #CName34); \
		RegisterColumn(Accessor##CType35::type, #CName35); \
		RegisterColumn(Accessor##CType36::type, #CName36); \
		RegisterColumn(Accessor##CType37::type, #CName37); \
		RegisterColumn(Accessor##CType38::type, #CName38); \
		RegisterColumn(Accessor##CType39::type, #CName39); \
		RegisterColumn(Accessor##CType40::type, #CName40); \
\
		CName1.Create(this, 0); \
		CName2.Create(this, 1); \
		CName3.Create(this, 2); \
		CName4.Create(this, 3); \
		CName5.Create(this, 4); \
		CName6.Create(this, 5); \
		CName7.Create(this, 6); \
		CName8.Create(this, 7); \
		CName9.Create(this, 8); \
		CName10.Create(this, 9); \
		CName11.Create(this, 10); \
		CName12.Create(this, 11); \
		CName13.Create(this, 12); \
		CName14.Create(this, 13); \
		CName15.Create(this, 14); \
		CName16.Create(this, 15); \
		CName17.Create(this, 16); \
		CName18.Create(this, 17); \
		CName19.Create(this, 18); \
		CName20.Create(this, 19); \
		CName21.Create(this, 20); \
		CName22.Create(this, 21); \
		CName23.Create(this, 22); \
		CName24.Create(this, 23); \
		CName25.Create(this, 24); \
		CName26.Create(this, 25); \
		CName27.Create(this, 26); \
		CName28.Create(this, 27); \
		CName29.Create(this, 28); \
		CName30.Create(this, 29); \
		CName31.Create(this, 30); \
		CName32.Create(this, 31); \
		CName33.Create(this, 32); \
		CName34.Create(this, 33); \
		CName35.Create(this, 34); \
		CName36.Create(this, 35); \
		CName37.Create(this, 36); \
		CName38.Create(this, 37); \
		CName39.Create(this, 38); \
		CName40.Create(this, 39); \
	}; \
\
	class TestQuery : public Query { \
	public: \
		TestQuery() : CName1(0), CName2(1), CName3(2), CName4(3), CName5(4), CName6(5), CName7(6), CName8(7), CName9(8), CName10(9), CName11(10), CName12(11), CName13(12), CName14(13), CName15(14), CName16(15), CName17(16), CName18(17), CName19(18), CName20(19), CName21(20), CName22(21), CName23(22), CName24(23), CName25(24), CName26(25), CName27(26), CName28(27), CName29(28), CName30(29), CName31(30), CName32(31), CName33(32), CName34(33), CName35(34), CName36(35), CName37(36), CName38(37), CName39(38), CName40(39) { \
			CName1.SetQuery(this); \
			CName2.SetQuery(this); \
			CName3.SetQuery(this); \
			CName4.SetQuery(this); \
			CName5.SetQuery(this); \
			CName6.SetQuery(this); \
			CName7.SetQuery(this); \
			CName8.SetQuery(this); \
			CName9.SetQuery(this); \
			CName10.SetQuery(this); \
			CName11.SetQuery(this); \
			CName12.SetQuery(this); \
			CName13.SetQuery(this); \
			CName14.SetQuery(this); \
			CName15.SetQuery(this); \
			CName16.SetQuery(this); \
			CName17.SetQuery(this); \
			CName18.SetQuery(this); \
			CName19.SetQuery(this); \
			CName20.SetQuery(this); \
			CName21.SetQuery(this); \
			CName22.SetQuery(this); \
			CName23.SetQuery(this); \
			CName24.SetQuery(this); \
			CName25.SetQuery(this); \
			CName26.SetQuery(this); \
			CName27.SetQuery(this); \
			CName28.SetQuery(this); \
			CName29.SetQuery(this); \
			CName30.SetQuery(this); \
			CName31.SetQuery(this); \
			CName32.SetQuery(this); \
			CName33.SetQuery(this); \
			CName34.SetQuery(this); \
			CName35.SetQuery(this); \
			CName36.SetQuery(this); \
			CName37.SetQuery(this); \
			CName38.SetQuery(this); \
			CName39.SetQuery(this); \
			CName40.SetQuery(this); \
		} \
\
		TestQuery(const TestQuery& copy) : Query(copy), CName1(0), CName2(1), CName3(2), CName4(3), CName5(4), CName6(5), CName7(6), CName8(7), CName9(8), CName10(9), CName11(10), CName12(11), CName13(12), CName14(13), CName15(14), CName16(15), CName17(16), CName18(17), CName19(18), CName20(19), CName21(20), CName22(21), CName23(22), CName24(23), CName25(24), CName26(25), CName27(26), CName28(27), CName29(28), CName30(29), CName31(30), CName32(31), CName33(32), CName34(33), CName35(34), CName36(35), CName37(36), CName38(37), CName39(38), CName40(39) { \
			CName1.SetQuery(this); \
			CName2.SetQuery(this); \
			CName3.SetQuery(this); \
			CName4.SetQuery(this); \
			CName5.SetQuery(this); \
			CName6.SetQuery(this); \
			CName7.SetQuery(this); \
			CName8.SetQuery(this); \
			CName9.SetQuery(this); \
			CName10.SetQuery(this); \
			CName11.SetQuery(this); \
			CName12.SetQuery(this); \
			CName13.SetQuery(this); \
			CName14.SetQuery(this); \
			CName15.SetQuery(this); \
			CName16.SetQuery(this); \
			CName17.SetQuery(this); \
			CName18.SetQuery(this); \
			CName19.SetQuery(this); \
			CName20.SetQuery(this); \
			CName21.SetQuery(this); \
			CName22.SetQuery(this); \
			CName23.SetQuery(this); \
			CName24.SetQuery(this); \
			CName25.SetQuery(this); \
			CName26.SetQuery(this); \
			CName27.SetQuery(this); \
			CName28.SetQuery(this); \
			CName29.SetQuery(this); \
			CName30.SetQuery(this); \
			CName31.SetQuery(this); \
			CName32.SetQuery(this); \
			CName33.SetQuery(this); \
			CName34.SetQuery(this); \
			CName35.SetQuery(this); \
			CName36.SetQuery(this); \
			CName37.SetQuery(this); \
			CName38.SetQuery(this); \
			CName39.SetQuery(this); \
			CName40.SetQuery(this); \
		} \
\
		class TestQueryQueryAccessorInt : private XQueryAccessorInt { \
		public: \
			TestQueryQueryAccessorInt(size_t column_id) : XQueryAccessorInt(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(int64_t value) {return (TestQuery &)XQueryAccessorInt::Equal(value);} \
			TestQuery& NotEqual(int64_t value) {return (TestQuery &)XQueryAccessorInt::NotEqual(value);} \
			TestQuery& Greater(int64_t value) {return (TestQuery &)XQueryAccessorInt::Greater(value);} \
			TestQuery& Less(int64_t value) {return (TestQuery &)XQueryAccessorInt::Less(value);} \
			TestQuery& Between(int64_t from, int64_t to) {return (TestQuery &)XQueryAccessorInt::Between(from, to);} \
		}; \
\
		template <class T> class TestQueryQueryAccessorEnum : public TestQueryQueryAccessorInt { \
		public: \
			TestQueryQueryAccessorEnum<T>(size_t column_id) : TestQueryQueryAccessorInt(column_id) {} \
		}; \
\
		class TestQueryQueryAccessorString : private XQueryAccessorString { \
		public: \
			TestQueryQueryAccessorString(size_t column_id) : XQueryAccessorString(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::Equal(value, CaseSensitive);} \
			TestQuery& NotEqual(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::NotEqual(value, CaseSensitive);} \
			TestQuery& BeginsWith(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::BeginsWith(value, CaseSensitive);} \
			TestQuery& EndsWith(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::EndsWith(value, CaseSensitive);} \
			TestQuery& Contains(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::Contains(value, CaseSensitive);} \
		}; \
\
		class TestQueryQueryAccessorBool : private XQueryAccessorBool { \
		public: \
			TestQueryQueryAccessorBool(size_t column_id) : XQueryAccessorBool(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(bool value) {return (TestQuery &)XQueryAccessorBool::Equal(value);} \
		}; \
\
		TestQueryQueryAccessor##CType1 CName1; \
		TestQueryQueryAccessor##CType2 CName2; \
		TestQueryQueryAccessor##CType3 CName3; \
		TestQueryQueryAccessor##CType4 CName4; \
		TestQueryQueryAccessor##CType5 CName5; \
		TestQueryQueryAccessor##CType6 CName6; \
		TestQueryQueryAccessor##CType7 CName7; \
		TestQueryQueryAccessor##CType8 CName8; \
		TestQueryQueryAccessor##CType9 CName9; \
		TestQueryQueryAccessor##CType10 CName10; \
		TestQueryQueryAccessor##CType11 CName11; \
		TestQueryQueryAccessor##CType12 CName12; \
		TestQueryQueryAccessor##CType13 CName13; \
		TestQueryQueryAccessor##CType14 CName14; \
		TestQueryQueryAccessor##CType15 CName15; \
		TestQueryQueryAccessor##CType16 CName16; \
		TestQueryQueryAccessor##CType17 CName17; \
		TestQueryQueryAccessor##CType18 CName18; \
		TestQueryQueryAccessor##CType19 CName19; \
		TestQueryQueryAccessor##CType20 CName20; \
		TestQueryQueryAccessor##CType21 CName21; \
		TestQueryQueryAccessor##CType22 CName22; \
		TestQueryQueryAccessor##CType23 CName23; \
		TestQueryQueryAccessor##CType24 CName24; \
		TestQueryQueryAccessor##CType25 CName25; \
		TestQueryQueryAccessor##CType26 CName26; \
		TestQueryQueryAccessor##CType27 CName27; \
		TestQueryQueryAccessor##CType28 CName28; \
		TestQueryQueryAccessor##CType29 CName29; \
		TestQueryQueryAccessor##CType30 CName30; \
		TestQueryQueryAccessor##CType31 CName31; \
		TestQueryQueryAccessor##CType32 CName32; \
		TestQueryQueryAccessor##CType33 CName33; \
		TestQueryQueryAccessor##CType34 CName34; \
		TestQueryQueryAccessor##CType35 CName35; \
		TestQueryQueryAccessor##CType36 CName36; \
		TestQueryQueryAccessor##CType37 CName37; \
		TestQueryQueryAccessor##CType38 CName38; \
		TestQueryQueryAccessor##CType39 CName39; \
		TestQueryQueryAccessor##CType40 CName40; \
\
		TestQuery& LeftParan(void) {Query::LeftParan(); return *this;}; \
		TestQuery& Or(void) {Query::Or(); return *this;}; \
		TestQuery& RightParan(void) {Query::RightParan(); return *this;}; \
		TestQuery& Subtable(size_t column) {Query::Subtable(column); return *this;}; \
		TestQuery& Parent() {Query::Parent(); return *this;}; \
	}; \
\
	TestQuery GetQuery() {return TestQuery();} \
\
	class Cursor : public CursorBase { \
	public: \
		Cursor(TableName& table, size_t ndx) : CursorBase(table, ndx) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
			CName17.Create(this, 16); \
			CName18.Create(this, 17); \
			CName19.Create(this, 18); \
			CName20.Create(this, 19); \
			CName21.Create(this, 20); \
			CName22.Create(this, 21); \
			CName23.Create(this, 22); \
			CName24.Create(this, 23); \
			CName25.Create(this, 24); \
			CName26.Create(this, 25); \
			CName27.Create(this, 26); \
			CName28.Create(this, 27); \
			CName29.Create(this, 28); \
			CName30.Create(this, 29); \
			CName31.Create(this, 30); \
			CName32.Create(this, 31); \
			CName33.Create(this, 32); \
			CName34.Create(this, 33); \
			CName35.Create(this, 34); \
			CName36.Create(this, 35); \
			CName37.Create(this, 36); \
			CName38.Create(this, 37); \
			CName39.Create(this, 38); \
			CName40.Create(this, 39); \
		} \
		Cursor(const TableName& table, size_t ndx) : CursorBase(const_cast<TableName&>(table), ndx) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
			CName17.Create(this, 16); \
			CName18.Create(this, 17); \
			CName19.Create(this, 18); \
			CName20.Create(this, 19); \
			CName21.Create(this, 20); \
			CName22.Create(this, 21); \
			CName23.Create(this, 22); \
			CName24.Create(this, 23); \
			CName25.Create(this, 24); \
			CName26.Create(this, 25); \
			CName27.Create(this, 26); \
			CName28.Create(this, 27); \
			CName29.Create(this, 28); \
			CName30.Create(this, 29); \
			CName31.Create(this, 30); \
			CName32.Create(this, 31); \
			CName33.Create(this, 32); \
			CName34.Create(this, 33); \
			CName35.Create(this, 34); \
			CName36.Create(this, 35); \
			CName37.Create(this, 36); \
			CName38.Create(this, 37); \
			CName39.Create(this, 38); \
			CName40.Create(this, 39); \
		} \
		Cursor(const Cursor& v) : CursorBase(v) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
			CName17.Create(this, 16); \
			CName18.Create(this, 17); \
			CName19.Create(this, 18); \
			CName20.Create(this, 19); \
			CName21.Create(this, 20); \
			CName22.Create(this, 21); \
			CName23.Create(this, 22); \
			CName24.Create(this, 23); \
			CName25.Create(this, 24); \
			CName26.Create(this, 25); \
			CName27.Create(this, 26); \
			CName28.Create(this, 27); \
			CName29.Create(this, 28); \
			CName30.Create(this, 29); \
			CName31.Create(this, 30); \
			CName32.Create(this, 31); \
			CName33.Create(this, 32); \
			CName34.Create(this, 33); \
			CName35.Create(this, 34); \
			CName36.Create(this, 35); \
			CName37.Create(this, 36); \
			CName38.Create(this, 37); \
			CName39.Create(this, 38); \
			CName40.Create(this, 39); \
		} \
		Accessor##CType1 CName1; \
		Accessor##CType2 CName2; \
		Accessor##CType3 CName3; \
		Accessor##CType4 CName4; \
		Accessor##CType5 CName5; \
		Accessor##CType6 CName6; \
		Accessor##CType7 CName7; \
		Accessor##CType8 CName8; \
		Accessor##CType9 CName9; \
		Accessor##CType10 CName10; \
		Accessor##CType11 CName11; \
		Accessor##CType12 CName12; \
		Accessor##CType13 CName13; \
		Accessor##CType14 CName14; \
		Accessor##CType15 CName15; \
		Accessor##CType16 CName16; \
		Accessor##CType17 CName17; \
		Accessor##CType18 CName18; \
		Accessor##CType19 CName19; \
		Accessor##CType20 CName20; \
		Accessor##CType21 CName21; \
		Accessor##CType22 CName22; \
		Accessor##CType23 CName23; \
		Accessor##CType24 CName24; \
		Accessor##CType25 CName25; \
		Accessor##CType26 CName26; \
		Accessor##CType27 CName27; \
		Accessor##CType28 CName28; \
		Accessor##CType29 CName29; \
		Accessor##CType30 CName30; \
		Accessor##CType31 CName31; \
		Accessor##CType32 CName32; \
		Accessor##CType33 CName33; \
		Accessor##CType34 CName34; \
		Accessor##CType35 CName35; \
		Accessor##CType36 CName36; \
		Accessor##CType37 CName37; \
		Accessor##CType38 CName38; \
		Accessor##CType39 CName39; \
		Accessor##CType40 CName40; \
	}; \
\
	void Add(tdbType##CType1 CName1, tdbType##CType2 CName2, tdbType##CType3 CName3, tdbType##CType4 CName4, tdbType##CType5 CName5, tdbType##CType6 CName6, tdbType##CType7 CName7, tdbType##CType8 CName8, tdbType##CType9 CName9, tdbType##CType10 CName10, tdbType##CType11 CName11, tdbType##CType12 CName12, tdbType##CType13 CName13, tdbType##CType14 CName14, tdbType##CType15 CName15, tdbType##CType16 CName16, tdbType##CType17 CName17, tdbType##CType18 CName18, tdbType##CType19 CName19, tdbType##CType20 CName20, tdbType##CType21 CName21, tdbType##CType22 CName22, tdbType##CType23 CName23, tdbType##CType24 CName24, tdbType##CType25 CName25, tdbType##CType26 CName26, tdbType##CType27 CName27, tdbType##CType28 CName28, tdbType##CType29 CName29, tdbType##CType30 CName30, tdbType##CType31 CName31, tdbType##CType32 CName32, tdbType##CType33 CName33, tdbType##CType34 CName34, tdbType##CType35 CName35, tdbType##CType36 CName36, tdbType##CType37 CName37, tdbType##CType38 CName38, tdbType##CType39 CName39, tdbType##CType40 CName40) { \
		const size_t ndx = GetSize(); \
		Insert##CType1 (0, ndx, CName1); \
		Insert##CType2 (1, ndx, CName2); \
		Insert##CType3 (2, ndx, CName3); \
		Insert##CType4 (3, ndx, CName4); \
		Insert##CType5 (4, ndx, CName5); \
		Insert##CType6 (5, ndx, CName6); \
		Insert##CType7 (6, ndx, CName7); \
		Insert##CType8 (7, ndx, CName8); \
		Insert##CType9 (8, ndx, CName9); \
		Insert##CType10 (9, ndx, CName10); \
		Insert##CType11 (10, ndx, CName11); \
		Insert##CType12 (11, ndx, CName12); \
		Insert##CType13 (12, ndx, CName13); \
		Insert##CType14 (13, ndx, CName14); \
		Insert##CType15 (14, ndx, CName15); \
		Insert##CType16 (15, ndx, CName16); \
		Insert##CType17 (16, ndx, CName17); \
		Insert##CType18 (17, ndx, CName18); \
		Insert##CType19 (18, ndx, CName19); \
		Insert##CType20 (19, ndx, CName20); \
		Insert##CType21 (20, ndx, CName21); \
		Insert##CType22 (21, ndx, CName22); \
		Insert##CType23 (22, ndx, CName23); \
		Insert##CType24 (23, ndx, CName24); \
		Insert##CType25 (24, ndx, CName25); \
		Insert##CType26 (25, ndx, CName26); \
		Insert##CType27 (26, ndx, CName27); \
		Insert##CType28 (27, ndx, CName28); \
		Insert##CType29 (28, ndx, CName29); \
		Insert##CType30 (29, ndx, CName30); \
		Insert##CType31 (30, ndx, CName31); \
		Insert##CType32 (31, ndx, CName32); \
		Insert##CType33 (32, ndx, CName33); \
		Insert##CType34 (33, ndx, CName34); \
		Insert##CType35 (34, ndx, CName35); \
		Insert##CType36 (35, ndx, CName36); \
		Insert##CType37 (36, ndx, CName37); \
		Insert##CType38 (37, ndx, CName38); \
		Insert##CType39 (38, ndx, CName39); \
		Insert##CType40 (39, ndx, CName40); \
		InsertDone(); \
	} \
\
	void Insert(size_t ndx, tdbType##CType1 CName1, tdbType##CType2 CName2, tdbType##CType3 CName3, tdbType##CType4 CName4, tdbType##CType5 CName5, tdbType##CType6 CName6, tdbType##CType7 CName7, tdbType##CType8 CName8, tdbType##CType9 CName9, tdbType##CType10 CName10, tdbType##CType11 CName11, tdbType##CType12 CName12, tdbType##CType13 CName13, tdbType##CType14 CName14, tdbType##CType15 CName15, tdbType##CType16 CName16, tdbType##CType17 CName17, tdbType##CType18 CName18, tdbType##CType19 CName19, tdbType##CType20 CName20, tdbType##CType21 CName21, tdbType##CType22 CName22, tdbType##CType23 CName23, tdbType##CType24 CName24, tdbType##CType25 CName25, tdbType##CType26 CName26, tdbType##CType27 CName27, tdbType##CType28 CName28, tdbType##CType29 CName29, tdbType##CType30 CName30, tdbType##CType31 CName31, tdbType##CType32 CName32, tdbType##CType33 CName33, tdbType##CType34 CName34, tdbType##CType35 CName35, tdbType##CType36 CName36, tdbType##CType37 CName37, tdbType##CType38 CName38, tdbType##CType39 CName39, tdbType##CType40 CName40) { \
		Insert##CType1 (0, ndx, CName1); \
		Insert##CType2 (1, ndx, CName2); \
		Insert##CType3 (2, ndx, CName3); \
		Insert##CType4 (3, ndx, CName4); \
		Insert##CType5 (4, ndx, CName5); \
		Insert##CType6 (5, ndx, CName6); \
		Insert##CType7 (6, ndx, CName7); \
		Insert##CType8 (7, ndx, CName8); \
		Insert##CType9 (8, ndx, CName9); \
		Insert##CType10 (9, ndx, CName10); \
		Insert##CType11 (10, ndx, CName11); \
		Insert##CType12 (11, ndx, CName12); \
		Insert##CType13 (12, ndx, CName13); \
		Insert##CType14 (13, ndx, CName14); \
		Insert##CType15 (14, ndx, CName15); \
		Insert##CType16 (15, ndx, CName16); \
		Insert##CType17 (16, ndx, CName17); \
		Insert##CType18 (17, ndx, CName18); \
		Insert##CType19 (18, ndx, CName19); \
		Insert##CType20 (19, ndx, CName20); \
		Insert##CType21 (20, ndx, CName21); \
		Insert##CType22 (21, ndx, CName22); \
		Insert##CType23 (22, ndx, CName23); \
		Insert##CType24 (23, ndx, CName24); \
		Insert##CType25 (24, ndx, CName25); \
		Insert##CType26 (25, ndx, CName26); \
		Insert##CType27 (26, ndx, CName27); \
		Insert##CType28 (27, ndx, CName28); \
		Insert##CType29 (28, ndx, CName29); \
		Insert##CType30 (29, ndx, CName30); \
		Insert##CType31 (30, ndx, CName31); \
		Insert##CType32 (31, ndx, CName32); \
		Insert##CType33 (32, ndx, CName33); \
		Insert##CType34 (33, ndx, CName34); \
		Insert##CType35 (34, ndx, CName35); \
		Insert##CType36 (35, ndx, CName36); \
		Insert##CType37 (36, ndx, CName37); \
		Insert##CType38 (37, ndx, CName38); \
		Insert##CType39 (38, ndx, CName39); \
		Insert##CType40 (39, ndx, CName40); \
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
	ColumnProxy##CType3 CName3; \
	ColumnProxy##CType4 CName4; \
	ColumnProxy##CType5 CName5; \
	ColumnProxy##CType6 CName6; \
	ColumnProxy##CType7 CName7; \
	ColumnProxy##CType8 CName8; \
	ColumnProxy##CType9 CName9; \
	ColumnProxy##CType10 CName10; \
	ColumnProxy##CType11 CName11; \
	ColumnProxy##CType12 CName12; \
	ColumnProxy##CType13 CName13; \
	ColumnProxy##CType14 CName14; \
	ColumnProxy##CType15 CName15; \
	ColumnProxy##CType16 CName16; \
	ColumnProxy##CType17 CName17; \
	ColumnProxy##CType18 CName18; \
	ColumnProxy##CType19 CName19; \
	ColumnProxy##CType20 CName20; \
	ColumnProxy##CType21 CName21; \
	ColumnProxy##CType22 CName22; \
	ColumnProxy##CType23 CName23; \
	ColumnProxy##CType24 CName24; \
	ColumnProxy##CType25 CName25; \
	ColumnProxy##CType26 CName26; \
	ColumnProxy##CType27 CName27; \
	ColumnProxy##CType28 CName28; \
	ColumnProxy##CType29 CName29; \
	ColumnProxy##CType30 CName30; \
	ColumnProxy##CType31 CName31; \
	ColumnProxy##CType32 CName32; \
	ColumnProxy##CType33 CName33; \
	ColumnProxy##CType34 CName34; \
	ColumnProxy##CType35 CName35; \
	ColumnProxy##CType36 CName36; \
	ColumnProxy##CType37 CName37; \
	ColumnProxy##CType38 CName38; \
	ColumnProxy##CType39 CName39; \
	ColumnProxy##CType40 CName40; \
\
protected: \
	friend class Group; \
	TableName(Allocator& alloc, size_t ref, Array* parent, size_t pndx) : TopLevelTable(alloc, ref, parent, pndx) {}; \
\
private: \
	TableName(const TableName&) {} \
	TableName& operator=(const TableName&) {return *this;} \
};



#define TDB_TABLE_41(TableName, CType1, CName1, CType2, CName2, CType3, CName3, CType4, CName4, CType5, CName5, CType6, CName6, CType7, CName7, CType8, CName8, CType9, CName9, CType10, CName10, CType11, CName11, CType12, CName12, CType13, CName13, CType14, CName14, CType15, CName15, CType16, CName16, CType17, CName17, CType18, CName18, CType19, CName19, CType20, CName20, CType21, CName21, CType22, CName22, CType23, CName23, CType24, CName24, CType25, CName25, CType26, CName26, CType27, CName27, CType28, CName28, CType29, CName29, CType30, CName30, CType31, CName31, CType32, CName32, CType33, CName33, CType34, CName34, CType35, CName35, CType36, CName36, CType37, CName37, CType38, CName38, CType39, CName39, CType40, CName40, CType41, CName41) \
class TableName##Query { \
protected: \
	QueryAccessor##CType1 CName1; \
	QueryAccessor##CType2 CName2; \
	QueryAccessor##CType3 CName3; \
	QueryAccessor##CType4 CName4; \
	QueryAccessor##CType5 CName5; \
	QueryAccessor##CType6 CName6; \
	QueryAccessor##CType7 CName7; \
	QueryAccessor##CType8 CName8; \
	QueryAccessor##CType9 CName9; \
	QueryAccessor##CType10 CName10; \
	QueryAccessor##CType11 CName11; \
	QueryAccessor##CType12 CName12; \
	QueryAccessor##CType13 CName13; \
	QueryAccessor##CType14 CName14; \
	QueryAccessor##CType15 CName15; \
	QueryAccessor##CType16 CName16; \
	QueryAccessor##CType17 CName17; \
	QueryAccessor##CType18 CName18; \
	QueryAccessor##CType19 CName19; \
	QueryAccessor##CType20 CName20; \
	QueryAccessor##CType21 CName21; \
	QueryAccessor##CType22 CName22; \
	QueryAccessor##CType23 CName23; \
	QueryAccessor##CType24 CName24; \
	QueryAccessor##CType25 CName25; \
	QueryAccessor##CType26 CName26; \
	QueryAccessor##CType27 CName27; \
	QueryAccessor##CType28 CName28; \
	QueryAccessor##CType29 CName29; \
	QueryAccessor##CType30 CName30; \
	QueryAccessor##CType31 CName31; \
	QueryAccessor##CType32 CName32; \
	QueryAccessor##CType33 CName33; \
	QueryAccessor##CType34 CName34; \
	QueryAccessor##CType35 CName35; \
	QueryAccessor##CType36 CName36; \
	QueryAccessor##CType37 CName37; \
	QueryAccessor##CType38 CName38; \
	QueryAccessor##CType39 CName39; \
	QueryAccessor##CType40 CName40; \
	QueryAccessor##CType41 CName41; \
}; \
\
class TableName : public TopLevelTable { \
public: \
	TableName(Allocator& alloc=GetDefaultAllocator()) : TopLevelTable(alloc) { \
		RegisterColumn(Accessor##CType1::type, #CName1); \
		RegisterColumn(Accessor##CType2::type, #CName2); \
		RegisterColumn(Accessor##CType3::type, #CName3); \
		RegisterColumn(Accessor##CType4::type, #CName4); \
		RegisterColumn(Accessor##CType5::type, #CName5); \
		RegisterColumn(Accessor##CType6::type, #CName6); \
		RegisterColumn(Accessor##CType7::type, #CName7); \
		RegisterColumn(Accessor##CType8::type, #CName8); \
		RegisterColumn(Accessor##CType9::type, #CName9); \
		RegisterColumn(Accessor##CType10::type, #CName10); \
		RegisterColumn(Accessor##CType11::type, #CName11); \
		RegisterColumn(Accessor##CType12::type, #CName12); \
		RegisterColumn(Accessor##CType13::type, #CName13); \
		RegisterColumn(Accessor##CType14::type, #CName14); \
		RegisterColumn(Accessor##CType15::type, #CName15); \
		RegisterColumn(Accessor##CType16::type, #CName16); \
		RegisterColumn(Accessor##CType17::type, #CName17); \
		RegisterColumn(Accessor##CType18::type, #CName18); \
		RegisterColumn(Accessor##CType19::type, #CName19); \
		RegisterColumn(Accessor##CType20::type, #CName20); \
		RegisterColumn(Accessor##CType21::type, #CName21); \
		RegisterColumn(Accessor##CType22::type, #CName22); \
		RegisterColumn(Accessor##CType23::type, #CName23); \
		RegisterColumn(Accessor##CType24::type, #CName24); \
		RegisterColumn(Accessor##CType25::type, #CName25); \
		RegisterColumn(Accessor##CType26::type, #CName26); \
		RegisterColumn(Accessor##CType27::type, #CName27); \
		RegisterColumn(Accessor##CType28::type, #CName28); \
		RegisterColumn(Accessor##CType29::type, #CName29); \
		RegisterColumn(Accessor##CType30::type, #CName30); \
		RegisterColumn(Accessor##CType31::type, #CName31); \
		RegisterColumn(Accessor##CType32::type, #CName32); \
		RegisterColumn(Accessor##CType33::type, #CName33); \
		RegisterColumn(Accessor##CType34::type, #CName34); \
		RegisterColumn(Accessor##CType35::type, #CName35); \
		RegisterColumn(Accessor##CType36::type, #CName36); \
		RegisterColumn(Accessor##CType37::type, #CName37); \
		RegisterColumn(Accessor##CType38::type, #CName38); \
		RegisterColumn(Accessor##CType39::type, #CName39); \
		RegisterColumn(Accessor##CType40::type, #CName40); \
		RegisterColumn(Accessor##CType41::type, #CName41); \
\
		CName1.Create(this, 0); \
		CName2.Create(this, 1); \
		CName3.Create(this, 2); \
		CName4.Create(this, 3); \
		CName5.Create(this, 4); \
		CName6.Create(this, 5); \
		CName7.Create(this, 6); \
		CName8.Create(this, 7); \
		CName9.Create(this, 8); \
		CName10.Create(this, 9); \
		CName11.Create(this, 10); \
		CName12.Create(this, 11); \
		CName13.Create(this, 12); \
		CName14.Create(this, 13); \
		CName15.Create(this, 14); \
		CName16.Create(this, 15); \
		CName17.Create(this, 16); \
		CName18.Create(this, 17); \
		CName19.Create(this, 18); \
		CName20.Create(this, 19); \
		CName21.Create(this, 20); \
		CName22.Create(this, 21); \
		CName23.Create(this, 22); \
		CName24.Create(this, 23); \
		CName25.Create(this, 24); \
		CName26.Create(this, 25); \
		CName27.Create(this, 26); \
		CName28.Create(this, 27); \
		CName29.Create(this, 28); \
		CName30.Create(this, 29); \
		CName31.Create(this, 30); \
		CName32.Create(this, 31); \
		CName33.Create(this, 32); \
		CName34.Create(this, 33); \
		CName35.Create(this, 34); \
		CName36.Create(this, 35); \
		CName37.Create(this, 36); \
		CName38.Create(this, 37); \
		CName39.Create(this, 38); \
		CName40.Create(this, 39); \
		CName41.Create(this, 40); \
	}; \
\
	class TestQuery : public Query { \
	public: \
		TestQuery() : CName1(0), CName2(1), CName3(2), CName4(3), CName5(4), CName6(5), CName7(6), CName8(7), CName9(8), CName10(9), CName11(10), CName12(11), CName13(12), CName14(13), CName15(14), CName16(15), CName17(16), CName18(17), CName19(18), CName20(19), CName21(20), CName22(21), CName23(22), CName24(23), CName25(24), CName26(25), CName27(26), CName28(27), CName29(28), CName30(29), CName31(30), CName32(31), CName33(32), CName34(33), CName35(34), CName36(35), CName37(36), CName38(37), CName39(38), CName40(39), CName41(40) { \
			CName1.SetQuery(this); \
			CName2.SetQuery(this); \
			CName3.SetQuery(this); \
			CName4.SetQuery(this); \
			CName5.SetQuery(this); \
			CName6.SetQuery(this); \
			CName7.SetQuery(this); \
			CName8.SetQuery(this); \
			CName9.SetQuery(this); \
			CName10.SetQuery(this); \
			CName11.SetQuery(this); \
			CName12.SetQuery(this); \
			CName13.SetQuery(this); \
			CName14.SetQuery(this); \
			CName15.SetQuery(this); \
			CName16.SetQuery(this); \
			CName17.SetQuery(this); \
			CName18.SetQuery(this); \
			CName19.SetQuery(this); \
			CName20.SetQuery(this); \
			CName21.SetQuery(this); \
			CName22.SetQuery(this); \
			CName23.SetQuery(this); \
			CName24.SetQuery(this); \
			CName25.SetQuery(this); \
			CName26.SetQuery(this); \
			CName27.SetQuery(this); \
			CName28.SetQuery(this); \
			CName29.SetQuery(this); \
			CName30.SetQuery(this); \
			CName31.SetQuery(this); \
			CName32.SetQuery(this); \
			CName33.SetQuery(this); \
			CName34.SetQuery(this); \
			CName35.SetQuery(this); \
			CName36.SetQuery(this); \
			CName37.SetQuery(this); \
			CName38.SetQuery(this); \
			CName39.SetQuery(this); \
			CName40.SetQuery(this); \
			CName41.SetQuery(this); \
		} \
\
		TestQuery(const TestQuery& copy) : Query(copy), CName1(0), CName2(1), CName3(2), CName4(3), CName5(4), CName6(5), CName7(6), CName8(7), CName9(8), CName10(9), CName11(10), CName12(11), CName13(12), CName14(13), CName15(14), CName16(15), CName17(16), CName18(17), CName19(18), CName20(19), CName21(20), CName22(21), CName23(22), CName24(23), CName25(24), CName26(25), CName27(26), CName28(27), CName29(28), CName30(29), CName31(30), CName32(31), CName33(32), CName34(33), CName35(34), CName36(35), CName37(36), CName38(37), CName39(38), CName40(39), CName41(40) { \
			CName1.SetQuery(this); \
			CName2.SetQuery(this); \
			CName3.SetQuery(this); \
			CName4.SetQuery(this); \
			CName5.SetQuery(this); \
			CName6.SetQuery(this); \
			CName7.SetQuery(this); \
			CName8.SetQuery(this); \
			CName9.SetQuery(this); \
			CName10.SetQuery(this); \
			CName11.SetQuery(this); \
			CName12.SetQuery(this); \
			CName13.SetQuery(this); \
			CName14.SetQuery(this); \
			CName15.SetQuery(this); \
			CName16.SetQuery(this); \
			CName17.SetQuery(this); \
			CName18.SetQuery(this); \
			CName19.SetQuery(this); \
			CName20.SetQuery(this); \
			CName21.SetQuery(this); \
			CName22.SetQuery(this); \
			CName23.SetQuery(this); \
			CName24.SetQuery(this); \
			CName25.SetQuery(this); \
			CName26.SetQuery(this); \
			CName27.SetQuery(this); \
			CName28.SetQuery(this); \
			CName29.SetQuery(this); \
			CName30.SetQuery(this); \
			CName31.SetQuery(this); \
			CName32.SetQuery(this); \
			CName33.SetQuery(this); \
			CName34.SetQuery(this); \
			CName35.SetQuery(this); \
			CName36.SetQuery(this); \
			CName37.SetQuery(this); \
			CName38.SetQuery(this); \
			CName39.SetQuery(this); \
			CName40.SetQuery(this); \
			CName41.SetQuery(this); \
		} \
\
		class TestQueryQueryAccessorInt : private XQueryAccessorInt { \
		public: \
			TestQueryQueryAccessorInt(size_t column_id) : XQueryAccessorInt(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(int64_t value) {return (TestQuery &)XQueryAccessorInt::Equal(value);} \
			TestQuery& NotEqual(int64_t value) {return (TestQuery &)XQueryAccessorInt::NotEqual(value);} \
			TestQuery& Greater(int64_t value) {return (TestQuery &)XQueryAccessorInt::Greater(value);} \
			TestQuery& Less(int64_t value) {return (TestQuery &)XQueryAccessorInt::Less(value);} \
			TestQuery& Between(int64_t from, int64_t to) {return (TestQuery &)XQueryAccessorInt::Between(from, to);} \
		}; \
\
		template <class T> class TestQueryQueryAccessorEnum : public TestQueryQueryAccessorInt { \
		public: \
			TestQueryQueryAccessorEnum<T>(size_t column_id) : TestQueryQueryAccessorInt(column_id) {} \
		}; \
\
		class TestQueryQueryAccessorString : private XQueryAccessorString { \
		public: \
			TestQueryQueryAccessorString(size_t column_id) : XQueryAccessorString(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::Equal(value, CaseSensitive);} \
			TestQuery& NotEqual(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::NotEqual(value, CaseSensitive);} \
			TestQuery& BeginsWith(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::BeginsWith(value, CaseSensitive);} \
			TestQuery& EndsWith(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::EndsWith(value, CaseSensitive);} \
			TestQuery& Contains(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::Contains(value, CaseSensitive);} \
		}; \
\
		class TestQueryQueryAccessorBool : private XQueryAccessorBool { \
		public: \
			TestQueryQueryAccessorBool(size_t column_id) : XQueryAccessorBool(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(bool value) {return (TestQuery &)XQueryAccessorBool::Equal(value);} \
		}; \
\
		TestQueryQueryAccessor##CType1 CName1; \
		TestQueryQueryAccessor##CType2 CName2; \
		TestQueryQueryAccessor##CType3 CName3; \
		TestQueryQueryAccessor##CType4 CName4; \
		TestQueryQueryAccessor##CType5 CName5; \
		TestQueryQueryAccessor##CType6 CName6; \
		TestQueryQueryAccessor##CType7 CName7; \
		TestQueryQueryAccessor##CType8 CName8; \
		TestQueryQueryAccessor##CType9 CName9; \
		TestQueryQueryAccessor##CType10 CName10; \
		TestQueryQueryAccessor##CType11 CName11; \
		TestQueryQueryAccessor##CType12 CName12; \
		TestQueryQueryAccessor##CType13 CName13; \
		TestQueryQueryAccessor##CType14 CName14; \
		TestQueryQueryAccessor##CType15 CName15; \
		TestQueryQueryAccessor##CType16 CName16; \
		TestQueryQueryAccessor##CType17 CName17; \
		TestQueryQueryAccessor##CType18 CName18; \
		TestQueryQueryAccessor##CType19 CName19; \
		TestQueryQueryAccessor##CType20 CName20; \
		TestQueryQueryAccessor##CType21 CName21; \
		TestQueryQueryAccessor##CType22 CName22; \
		TestQueryQueryAccessor##CType23 CName23; \
		TestQueryQueryAccessor##CType24 CName24; \
		TestQueryQueryAccessor##CType25 CName25; \
		TestQueryQueryAccessor##CType26 CName26; \
		TestQueryQueryAccessor##CType27 CName27; \
		TestQueryQueryAccessor##CType28 CName28; \
		TestQueryQueryAccessor##CType29 CName29; \
		TestQueryQueryAccessor##CType30 CName30; \
		TestQueryQueryAccessor##CType31 CName31; \
		TestQueryQueryAccessor##CType32 CName32; \
		TestQueryQueryAccessor##CType33 CName33; \
		TestQueryQueryAccessor##CType34 CName34; \
		TestQueryQueryAccessor##CType35 CName35; \
		TestQueryQueryAccessor##CType36 CName36; \
		TestQueryQueryAccessor##CType37 CName37; \
		TestQueryQueryAccessor##CType38 CName38; \
		TestQueryQueryAccessor##CType39 CName39; \
		TestQueryQueryAccessor##CType40 CName40; \
		TestQueryQueryAccessor##CType41 CName41; \
\
		TestQuery& LeftParan(void) {Query::LeftParan(); return *this;}; \
		TestQuery& Or(void) {Query::Or(); return *this;}; \
		TestQuery& RightParan(void) {Query::RightParan(); return *this;}; \
		TestQuery& Subtable(size_t column) {Query::Subtable(column); return *this;}; \
		TestQuery& Parent() {Query::Parent(); return *this;}; \
	}; \
\
	TestQuery GetQuery() {return TestQuery();} \
\
	class Cursor : public CursorBase { \
	public: \
		Cursor(TableName& table, size_t ndx) : CursorBase(table, ndx) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
			CName17.Create(this, 16); \
			CName18.Create(this, 17); \
			CName19.Create(this, 18); \
			CName20.Create(this, 19); \
			CName21.Create(this, 20); \
			CName22.Create(this, 21); \
			CName23.Create(this, 22); \
			CName24.Create(this, 23); \
			CName25.Create(this, 24); \
			CName26.Create(this, 25); \
			CName27.Create(this, 26); \
			CName28.Create(this, 27); \
			CName29.Create(this, 28); \
			CName30.Create(this, 29); \
			CName31.Create(this, 30); \
			CName32.Create(this, 31); \
			CName33.Create(this, 32); \
			CName34.Create(this, 33); \
			CName35.Create(this, 34); \
			CName36.Create(this, 35); \
			CName37.Create(this, 36); \
			CName38.Create(this, 37); \
			CName39.Create(this, 38); \
			CName40.Create(this, 39); \
			CName41.Create(this, 40); \
		} \
		Cursor(const TableName& table, size_t ndx) : CursorBase(const_cast<TableName&>(table), ndx) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
			CName17.Create(this, 16); \
			CName18.Create(this, 17); \
			CName19.Create(this, 18); \
			CName20.Create(this, 19); \
			CName21.Create(this, 20); \
			CName22.Create(this, 21); \
			CName23.Create(this, 22); \
			CName24.Create(this, 23); \
			CName25.Create(this, 24); \
			CName26.Create(this, 25); \
			CName27.Create(this, 26); \
			CName28.Create(this, 27); \
			CName29.Create(this, 28); \
			CName30.Create(this, 29); \
			CName31.Create(this, 30); \
			CName32.Create(this, 31); \
			CName33.Create(this, 32); \
			CName34.Create(this, 33); \
			CName35.Create(this, 34); \
			CName36.Create(this, 35); \
			CName37.Create(this, 36); \
			CName38.Create(this, 37); \
			CName39.Create(this, 38); \
			CName40.Create(this, 39); \
			CName41.Create(this, 40); \
		} \
		Cursor(const Cursor& v) : CursorBase(v) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
			CName17.Create(this, 16); \
			CName18.Create(this, 17); \
			CName19.Create(this, 18); \
			CName20.Create(this, 19); \
			CName21.Create(this, 20); \
			CName22.Create(this, 21); \
			CName23.Create(this, 22); \
			CName24.Create(this, 23); \
			CName25.Create(this, 24); \
			CName26.Create(this, 25); \
			CName27.Create(this, 26); \
			CName28.Create(this, 27); \
			CName29.Create(this, 28); \
			CName30.Create(this, 29); \
			CName31.Create(this, 30); \
			CName32.Create(this, 31); \
			CName33.Create(this, 32); \
			CName34.Create(this, 33); \
			CName35.Create(this, 34); \
			CName36.Create(this, 35); \
			CName37.Create(this, 36); \
			CName38.Create(this, 37); \
			CName39.Create(this, 38); \
			CName40.Create(this, 39); \
			CName41.Create(this, 40); \
		} \
		Accessor##CType1 CName1; \
		Accessor##CType2 CName2; \
		Accessor##CType3 CName3; \
		Accessor##CType4 CName4; \
		Accessor##CType5 CName5; \
		Accessor##CType6 CName6; \
		Accessor##CType7 CName7; \
		Accessor##CType8 CName8; \
		Accessor##CType9 CName9; \
		Accessor##CType10 CName10; \
		Accessor##CType11 CName11; \
		Accessor##CType12 CName12; \
		Accessor##CType13 CName13; \
		Accessor##CType14 CName14; \
		Accessor##CType15 CName15; \
		Accessor##CType16 CName16; \
		Accessor##CType17 CName17; \
		Accessor##CType18 CName18; \
		Accessor##CType19 CName19; \
		Accessor##CType20 CName20; \
		Accessor##CType21 CName21; \
		Accessor##CType22 CName22; \
		Accessor##CType23 CName23; \
		Accessor##CType24 CName24; \
		Accessor##CType25 CName25; \
		Accessor##CType26 CName26; \
		Accessor##CType27 CName27; \
		Accessor##CType28 CName28; \
		Accessor##CType29 CName29; \
		Accessor##CType30 CName30; \
		Accessor##CType31 CName31; \
		Accessor##CType32 CName32; \
		Accessor##CType33 CName33; \
		Accessor##CType34 CName34; \
		Accessor##CType35 CName35; \
		Accessor##CType36 CName36; \
		Accessor##CType37 CName37; \
		Accessor##CType38 CName38; \
		Accessor##CType39 CName39; \
		Accessor##CType40 CName40; \
		Accessor##CType41 CName41; \
	}; \
\
	void Add(tdbType##CType1 CName1, tdbType##CType2 CName2, tdbType##CType3 CName3, tdbType##CType4 CName4, tdbType##CType5 CName5, tdbType##CType6 CName6, tdbType##CType7 CName7, tdbType##CType8 CName8, tdbType##CType9 CName9, tdbType##CType10 CName10, tdbType##CType11 CName11, tdbType##CType12 CName12, tdbType##CType13 CName13, tdbType##CType14 CName14, tdbType##CType15 CName15, tdbType##CType16 CName16, tdbType##CType17 CName17, tdbType##CType18 CName18, tdbType##CType19 CName19, tdbType##CType20 CName20, tdbType##CType21 CName21, tdbType##CType22 CName22, tdbType##CType23 CName23, tdbType##CType24 CName24, tdbType##CType25 CName25, tdbType##CType26 CName26, tdbType##CType27 CName27, tdbType##CType28 CName28, tdbType##CType29 CName29, tdbType##CType30 CName30, tdbType##CType31 CName31, tdbType##CType32 CName32, tdbType##CType33 CName33, tdbType##CType34 CName34, tdbType##CType35 CName35, tdbType##CType36 CName36, tdbType##CType37 CName37, tdbType##CType38 CName38, tdbType##CType39 CName39, tdbType##CType40 CName40, tdbType##CType41 CName41) { \
		const size_t ndx = GetSize(); \
		Insert##CType1 (0, ndx, CName1); \
		Insert##CType2 (1, ndx, CName2); \
		Insert##CType3 (2, ndx, CName3); \
		Insert##CType4 (3, ndx, CName4); \
		Insert##CType5 (4, ndx, CName5); \
		Insert##CType6 (5, ndx, CName6); \
		Insert##CType7 (6, ndx, CName7); \
		Insert##CType8 (7, ndx, CName8); \
		Insert##CType9 (8, ndx, CName9); \
		Insert##CType10 (9, ndx, CName10); \
		Insert##CType11 (10, ndx, CName11); \
		Insert##CType12 (11, ndx, CName12); \
		Insert##CType13 (12, ndx, CName13); \
		Insert##CType14 (13, ndx, CName14); \
		Insert##CType15 (14, ndx, CName15); \
		Insert##CType16 (15, ndx, CName16); \
		Insert##CType17 (16, ndx, CName17); \
		Insert##CType18 (17, ndx, CName18); \
		Insert##CType19 (18, ndx, CName19); \
		Insert##CType20 (19, ndx, CName20); \
		Insert##CType21 (20, ndx, CName21); \
		Insert##CType22 (21, ndx, CName22); \
		Insert##CType23 (22, ndx, CName23); \
		Insert##CType24 (23, ndx, CName24); \
		Insert##CType25 (24, ndx, CName25); \
		Insert##CType26 (25, ndx, CName26); \
		Insert##CType27 (26, ndx, CName27); \
		Insert##CType28 (27, ndx, CName28); \
		Insert##CType29 (28, ndx, CName29); \
		Insert##CType30 (29, ndx, CName30); \
		Insert##CType31 (30, ndx, CName31); \
		Insert##CType32 (31, ndx, CName32); \
		Insert##CType33 (32, ndx, CName33); \
		Insert##CType34 (33, ndx, CName34); \
		Insert##CType35 (34, ndx, CName35); \
		Insert##CType36 (35, ndx, CName36); \
		Insert##CType37 (36, ndx, CName37); \
		Insert##CType38 (37, ndx, CName38); \
		Insert##CType39 (38, ndx, CName39); \
		Insert##CType40 (39, ndx, CName40); \
		Insert##CType41 (40, ndx, CName41); \
		InsertDone(); \
	} \
\
	void Insert(size_t ndx, tdbType##CType1 CName1, tdbType##CType2 CName2, tdbType##CType3 CName3, tdbType##CType4 CName4, tdbType##CType5 CName5, tdbType##CType6 CName6, tdbType##CType7 CName7, tdbType##CType8 CName8, tdbType##CType9 CName9, tdbType##CType10 CName10, tdbType##CType11 CName11, tdbType##CType12 CName12, tdbType##CType13 CName13, tdbType##CType14 CName14, tdbType##CType15 CName15, tdbType##CType16 CName16, tdbType##CType17 CName17, tdbType##CType18 CName18, tdbType##CType19 CName19, tdbType##CType20 CName20, tdbType##CType21 CName21, tdbType##CType22 CName22, tdbType##CType23 CName23, tdbType##CType24 CName24, tdbType##CType25 CName25, tdbType##CType26 CName26, tdbType##CType27 CName27, tdbType##CType28 CName28, tdbType##CType29 CName29, tdbType##CType30 CName30, tdbType##CType31 CName31, tdbType##CType32 CName32, tdbType##CType33 CName33, tdbType##CType34 CName34, tdbType##CType35 CName35, tdbType##CType36 CName36, tdbType##CType37 CName37, tdbType##CType38 CName38, tdbType##CType39 CName39, tdbType##CType40 CName40, tdbType##CType41 CName41) { \
		Insert##CType1 (0, ndx, CName1); \
		Insert##CType2 (1, ndx, CName2); \
		Insert##CType3 (2, ndx, CName3); \
		Insert##CType4 (3, ndx, CName4); \
		Insert##CType5 (4, ndx, CName5); \
		Insert##CType6 (5, ndx, CName6); \
		Insert##CType7 (6, ndx, CName7); \
		Insert##CType8 (7, ndx, CName8); \
		Insert##CType9 (8, ndx, CName9); \
		Insert##CType10 (9, ndx, CName10); \
		Insert##CType11 (10, ndx, CName11); \
		Insert##CType12 (11, ndx, CName12); \
		Insert##CType13 (12, ndx, CName13); \
		Insert##CType14 (13, ndx, CName14); \
		Insert##CType15 (14, ndx, CName15); \
		Insert##CType16 (15, ndx, CName16); \
		Insert##CType17 (16, ndx, CName17); \
		Insert##CType18 (17, ndx, CName18); \
		Insert##CType19 (18, ndx, CName19); \
		Insert##CType20 (19, ndx, CName20); \
		Insert##CType21 (20, ndx, CName21); \
		Insert##CType22 (21, ndx, CName22); \
		Insert##CType23 (22, ndx, CName23); \
		Insert##CType24 (23, ndx, CName24); \
		Insert##CType25 (24, ndx, CName25); \
		Insert##CType26 (25, ndx, CName26); \
		Insert##CType27 (26, ndx, CName27); \
		Insert##CType28 (27, ndx, CName28); \
		Insert##CType29 (28, ndx, CName29); \
		Insert##CType30 (29, ndx, CName30); \
		Insert##CType31 (30, ndx, CName31); \
		Insert##CType32 (31, ndx, CName32); \
		Insert##CType33 (32, ndx, CName33); \
		Insert##CType34 (33, ndx, CName34); \
		Insert##CType35 (34, ndx, CName35); \
		Insert##CType36 (35, ndx, CName36); \
		Insert##CType37 (36, ndx, CName37); \
		Insert##CType38 (37, ndx, CName38); \
		Insert##CType39 (38, ndx, CName39); \
		Insert##CType40 (39, ndx, CName40); \
		Insert##CType41 (40, ndx, CName41); \
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
	ColumnProxy##CType3 CName3; \
	ColumnProxy##CType4 CName4; \
	ColumnProxy##CType5 CName5; \
	ColumnProxy##CType6 CName6; \
	ColumnProxy##CType7 CName7; \
	ColumnProxy##CType8 CName8; \
	ColumnProxy##CType9 CName9; \
	ColumnProxy##CType10 CName10; \
	ColumnProxy##CType11 CName11; \
	ColumnProxy##CType12 CName12; \
	ColumnProxy##CType13 CName13; \
	ColumnProxy##CType14 CName14; \
	ColumnProxy##CType15 CName15; \
	ColumnProxy##CType16 CName16; \
	ColumnProxy##CType17 CName17; \
	ColumnProxy##CType18 CName18; \
	ColumnProxy##CType19 CName19; \
	ColumnProxy##CType20 CName20; \
	ColumnProxy##CType21 CName21; \
	ColumnProxy##CType22 CName22; \
	ColumnProxy##CType23 CName23; \
	ColumnProxy##CType24 CName24; \
	ColumnProxy##CType25 CName25; \
	ColumnProxy##CType26 CName26; \
	ColumnProxy##CType27 CName27; \
	ColumnProxy##CType28 CName28; \
	ColumnProxy##CType29 CName29; \
	ColumnProxy##CType30 CName30; \
	ColumnProxy##CType31 CName31; \
	ColumnProxy##CType32 CName32; \
	ColumnProxy##CType33 CName33; \
	ColumnProxy##CType34 CName34; \
	ColumnProxy##CType35 CName35; \
	ColumnProxy##CType36 CName36; \
	ColumnProxy##CType37 CName37; \
	ColumnProxy##CType38 CName38; \
	ColumnProxy##CType39 CName39; \
	ColumnProxy##CType40 CName40; \
	ColumnProxy##CType41 CName41; \
\
protected: \
	friend class Group; \
	TableName(Allocator& alloc, size_t ref, Array* parent, size_t pndx) : TopLevelTable(alloc, ref, parent, pndx) {}; \
\
private: \
	TableName(const TableName&) {} \
	TableName& operator=(const TableName&) {return *this;} \
};



#define TDB_TABLE_42(TableName, CType1, CName1, CType2, CName2, CType3, CName3, CType4, CName4, CType5, CName5, CType6, CName6, CType7, CName7, CType8, CName8, CType9, CName9, CType10, CName10, CType11, CName11, CType12, CName12, CType13, CName13, CType14, CName14, CType15, CName15, CType16, CName16, CType17, CName17, CType18, CName18, CType19, CName19, CType20, CName20, CType21, CName21, CType22, CName22, CType23, CName23, CType24, CName24, CType25, CName25, CType26, CName26, CType27, CName27, CType28, CName28, CType29, CName29, CType30, CName30, CType31, CName31, CType32, CName32, CType33, CName33, CType34, CName34, CType35, CName35, CType36, CName36, CType37, CName37, CType38, CName38, CType39, CName39, CType40, CName40, CType41, CName41, CType42, CName42) \
class TableName##Query { \
protected: \
	QueryAccessor##CType1 CName1; \
	QueryAccessor##CType2 CName2; \
	QueryAccessor##CType3 CName3; \
	QueryAccessor##CType4 CName4; \
	QueryAccessor##CType5 CName5; \
	QueryAccessor##CType6 CName6; \
	QueryAccessor##CType7 CName7; \
	QueryAccessor##CType8 CName8; \
	QueryAccessor##CType9 CName9; \
	QueryAccessor##CType10 CName10; \
	QueryAccessor##CType11 CName11; \
	QueryAccessor##CType12 CName12; \
	QueryAccessor##CType13 CName13; \
	QueryAccessor##CType14 CName14; \
	QueryAccessor##CType15 CName15; \
	QueryAccessor##CType16 CName16; \
	QueryAccessor##CType17 CName17; \
	QueryAccessor##CType18 CName18; \
	QueryAccessor##CType19 CName19; \
	QueryAccessor##CType20 CName20; \
	QueryAccessor##CType21 CName21; \
	QueryAccessor##CType22 CName22; \
	QueryAccessor##CType23 CName23; \
	QueryAccessor##CType24 CName24; \
	QueryAccessor##CType25 CName25; \
	QueryAccessor##CType26 CName26; \
	QueryAccessor##CType27 CName27; \
	QueryAccessor##CType28 CName28; \
	QueryAccessor##CType29 CName29; \
	QueryAccessor##CType30 CName30; \
	QueryAccessor##CType31 CName31; \
	QueryAccessor##CType32 CName32; \
	QueryAccessor##CType33 CName33; \
	QueryAccessor##CType34 CName34; \
	QueryAccessor##CType35 CName35; \
	QueryAccessor##CType36 CName36; \
	QueryAccessor##CType37 CName37; \
	QueryAccessor##CType38 CName38; \
	QueryAccessor##CType39 CName39; \
	QueryAccessor##CType40 CName40; \
	QueryAccessor##CType41 CName41; \
	QueryAccessor##CType42 CName42; \
}; \
\
class TableName : public TopLevelTable { \
public: \
	TableName(Allocator& alloc=GetDefaultAllocator()) : TopLevelTable(alloc) { \
		RegisterColumn(Accessor##CType1::type, #CName1); \
		RegisterColumn(Accessor##CType2::type, #CName2); \
		RegisterColumn(Accessor##CType3::type, #CName3); \
		RegisterColumn(Accessor##CType4::type, #CName4); \
		RegisterColumn(Accessor##CType5::type, #CName5); \
		RegisterColumn(Accessor##CType6::type, #CName6); \
		RegisterColumn(Accessor##CType7::type, #CName7); \
		RegisterColumn(Accessor##CType8::type, #CName8); \
		RegisterColumn(Accessor##CType9::type, #CName9); \
		RegisterColumn(Accessor##CType10::type, #CName10); \
		RegisterColumn(Accessor##CType11::type, #CName11); \
		RegisterColumn(Accessor##CType12::type, #CName12); \
		RegisterColumn(Accessor##CType13::type, #CName13); \
		RegisterColumn(Accessor##CType14::type, #CName14); \
		RegisterColumn(Accessor##CType15::type, #CName15); \
		RegisterColumn(Accessor##CType16::type, #CName16); \
		RegisterColumn(Accessor##CType17::type, #CName17); \
		RegisterColumn(Accessor##CType18::type, #CName18); \
		RegisterColumn(Accessor##CType19::type, #CName19); \
		RegisterColumn(Accessor##CType20::type, #CName20); \
		RegisterColumn(Accessor##CType21::type, #CName21); \
		RegisterColumn(Accessor##CType22::type, #CName22); \
		RegisterColumn(Accessor##CType23::type, #CName23); \
		RegisterColumn(Accessor##CType24::type, #CName24); \
		RegisterColumn(Accessor##CType25::type, #CName25); \
		RegisterColumn(Accessor##CType26::type, #CName26); \
		RegisterColumn(Accessor##CType27::type, #CName27); \
		RegisterColumn(Accessor##CType28::type, #CName28); \
		RegisterColumn(Accessor##CType29::type, #CName29); \
		RegisterColumn(Accessor##CType30::type, #CName30); \
		RegisterColumn(Accessor##CType31::type, #CName31); \
		RegisterColumn(Accessor##CType32::type, #CName32); \
		RegisterColumn(Accessor##CType33::type, #CName33); \
		RegisterColumn(Accessor##CType34::type, #CName34); \
		RegisterColumn(Accessor##CType35::type, #CName35); \
		RegisterColumn(Accessor##CType36::type, #CName36); \
		RegisterColumn(Accessor##CType37::type, #CName37); \
		RegisterColumn(Accessor##CType38::type, #CName38); \
		RegisterColumn(Accessor##CType39::type, #CName39); \
		RegisterColumn(Accessor##CType40::type, #CName40); \
		RegisterColumn(Accessor##CType41::type, #CName41); \
		RegisterColumn(Accessor##CType42::type, #CName42); \
\
		CName1.Create(this, 0); \
		CName2.Create(this, 1); \
		CName3.Create(this, 2); \
		CName4.Create(this, 3); \
		CName5.Create(this, 4); \
		CName6.Create(this, 5); \
		CName7.Create(this, 6); \
		CName8.Create(this, 7); \
		CName9.Create(this, 8); \
		CName10.Create(this, 9); \
		CName11.Create(this, 10); \
		CName12.Create(this, 11); \
		CName13.Create(this, 12); \
		CName14.Create(this, 13); \
		CName15.Create(this, 14); \
		CName16.Create(this, 15); \
		CName17.Create(this, 16); \
		CName18.Create(this, 17); \
		CName19.Create(this, 18); \
		CName20.Create(this, 19); \
		CName21.Create(this, 20); \
		CName22.Create(this, 21); \
		CName23.Create(this, 22); \
		CName24.Create(this, 23); \
		CName25.Create(this, 24); \
		CName26.Create(this, 25); \
		CName27.Create(this, 26); \
		CName28.Create(this, 27); \
		CName29.Create(this, 28); \
		CName30.Create(this, 29); \
		CName31.Create(this, 30); \
		CName32.Create(this, 31); \
		CName33.Create(this, 32); \
		CName34.Create(this, 33); \
		CName35.Create(this, 34); \
		CName36.Create(this, 35); \
		CName37.Create(this, 36); \
		CName38.Create(this, 37); \
		CName39.Create(this, 38); \
		CName40.Create(this, 39); \
		CName41.Create(this, 40); \
		CName42.Create(this, 41); \
	}; \
\
	class TestQuery : public Query { \
	public: \
		TestQuery() : CName1(0), CName2(1), CName3(2), CName4(3), CName5(4), CName6(5), CName7(6), CName8(7), CName9(8), CName10(9), CName11(10), CName12(11), CName13(12), CName14(13), CName15(14), CName16(15), CName17(16), CName18(17), CName19(18), CName20(19), CName21(20), CName22(21), CName23(22), CName24(23), CName25(24), CName26(25), CName27(26), CName28(27), CName29(28), CName30(29), CName31(30), CName32(31), CName33(32), CName34(33), CName35(34), CName36(35), CName37(36), CName38(37), CName39(38), CName40(39), CName41(40), CName42(41) { \
			CName1.SetQuery(this); \
			CName2.SetQuery(this); \
			CName3.SetQuery(this); \
			CName4.SetQuery(this); \
			CName5.SetQuery(this); \
			CName6.SetQuery(this); \
			CName7.SetQuery(this); \
			CName8.SetQuery(this); \
			CName9.SetQuery(this); \
			CName10.SetQuery(this); \
			CName11.SetQuery(this); \
			CName12.SetQuery(this); \
			CName13.SetQuery(this); \
			CName14.SetQuery(this); \
			CName15.SetQuery(this); \
			CName16.SetQuery(this); \
			CName17.SetQuery(this); \
			CName18.SetQuery(this); \
			CName19.SetQuery(this); \
			CName20.SetQuery(this); \
			CName21.SetQuery(this); \
			CName22.SetQuery(this); \
			CName23.SetQuery(this); \
			CName24.SetQuery(this); \
			CName25.SetQuery(this); \
			CName26.SetQuery(this); \
			CName27.SetQuery(this); \
			CName28.SetQuery(this); \
			CName29.SetQuery(this); \
			CName30.SetQuery(this); \
			CName31.SetQuery(this); \
			CName32.SetQuery(this); \
			CName33.SetQuery(this); \
			CName34.SetQuery(this); \
			CName35.SetQuery(this); \
			CName36.SetQuery(this); \
			CName37.SetQuery(this); \
			CName38.SetQuery(this); \
			CName39.SetQuery(this); \
			CName40.SetQuery(this); \
			CName41.SetQuery(this); \
			CName42.SetQuery(this); \
		} \
\
		TestQuery(const TestQuery& copy) : Query(copy), CName1(0), CName2(1), CName3(2), CName4(3), CName5(4), CName6(5), CName7(6), CName8(7), CName9(8), CName10(9), CName11(10), CName12(11), CName13(12), CName14(13), CName15(14), CName16(15), CName17(16), CName18(17), CName19(18), CName20(19), CName21(20), CName22(21), CName23(22), CName24(23), CName25(24), CName26(25), CName27(26), CName28(27), CName29(28), CName30(29), CName31(30), CName32(31), CName33(32), CName34(33), CName35(34), CName36(35), CName37(36), CName38(37), CName39(38), CName40(39), CName41(40), CName42(41) { \
			CName1.SetQuery(this); \
			CName2.SetQuery(this); \
			CName3.SetQuery(this); \
			CName4.SetQuery(this); \
			CName5.SetQuery(this); \
			CName6.SetQuery(this); \
			CName7.SetQuery(this); \
			CName8.SetQuery(this); \
			CName9.SetQuery(this); \
			CName10.SetQuery(this); \
			CName11.SetQuery(this); \
			CName12.SetQuery(this); \
			CName13.SetQuery(this); \
			CName14.SetQuery(this); \
			CName15.SetQuery(this); \
			CName16.SetQuery(this); \
			CName17.SetQuery(this); \
			CName18.SetQuery(this); \
			CName19.SetQuery(this); \
			CName20.SetQuery(this); \
			CName21.SetQuery(this); \
			CName22.SetQuery(this); \
			CName23.SetQuery(this); \
			CName24.SetQuery(this); \
			CName25.SetQuery(this); \
			CName26.SetQuery(this); \
			CName27.SetQuery(this); \
			CName28.SetQuery(this); \
			CName29.SetQuery(this); \
			CName30.SetQuery(this); \
			CName31.SetQuery(this); \
			CName32.SetQuery(this); \
			CName33.SetQuery(this); \
			CName34.SetQuery(this); \
			CName35.SetQuery(this); \
			CName36.SetQuery(this); \
			CName37.SetQuery(this); \
			CName38.SetQuery(this); \
			CName39.SetQuery(this); \
			CName40.SetQuery(this); \
			CName41.SetQuery(this); \
			CName42.SetQuery(this); \
		} \
\
		class TestQueryQueryAccessorInt : private XQueryAccessorInt { \
		public: \
			TestQueryQueryAccessorInt(size_t column_id) : XQueryAccessorInt(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(int64_t value) {return (TestQuery &)XQueryAccessorInt::Equal(value);} \
			TestQuery& NotEqual(int64_t value) {return (TestQuery &)XQueryAccessorInt::NotEqual(value);} \
			TestQuery& Greater(int64_t value) {return (TestQuery &)XQueryAccessorInt::Greater(value);} \
			TestQuery& Less(int64_t value) {return (TestQuery &)XQueryAccessorInt::Less(value);} \
			TestQuery& Between(int64_t from, int64_t to) {return (TestQuery &)XQueryAccessorInt::Between(from, to);} \
		}; \
\
		template <class T> class TestQueryQueryAccessorEnum : public TestQueryQueryAccessorInt { \
		public: \
			TestQueryQueryAccessorEnum<T>(size_t column_id) : TestQueryQueryAccessorInt(column_id) {} \
		}; \
\
		class TestQueryQueryAccessorString : private XQueryAccessorString { \
		public: \
			TestQueryQueryAccessorString(size_t column_id) : XQueryAccessorString(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::Equal(value, CaseSensitive);} \
			TestQuery& NotEqual(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::NotEqual(value, CaseSensitive);} \
			TestQuery& BeginsWith(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::BeginsWith(value, CaseSensitive);} \
			TestQuery& EndsWith(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::EndsWith(value, CaseSensitive);} \
			TestQuery& Contains(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::Contains(value, CaseSensitive);} \
		}; \
\
		class TestQueryQueryAccessorBool : private XQueryAccessorBool { \
		public: \
			TestQueryQueryAccessorBool(size_t column_id) : XQueryAccessorBool(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(bool value) {return (TestQuery &)XQueryAccessorBool::Equal(value);} \
		}; \
\
		TestQueryQueryAccessor##CType1 CName1; \
		TestQueryQueryAccessor##CType2 CName2; \
		TestQueryQueryAccessor##CType3 CName3; \
		TestQueryQueryAccessor##CType4 CName4; \
		TestQueryQueryAccessor##CType5 CName5; \
		TestQueryQueryAccessor##CType6 CName6; \
		TestQueryQueryAccessor##CType7 CName7; \
		TestQueryQueryAccessor##CType8 CName8; \
		TestQueryQueryAccessor##CType9 CName9; \
		TestQueryQueryAccessor##CType10 CName10; \
		TestQueryQueryAccessor##CType11 CName11; \
		TestQueryQueryAccessor##CType12 CName12; \
		TestQueryQueryAccessor##CType13 CName13; \
		TestQueryQueryAccessor##CType14 CName14; \
		TestQueryQueryAccessor##CType15 CName15; \
		TestQueryQueryAccessor##CType16 CName16; \
		TestQueryQueryAccessor##CType17 CName17; \
		TestQueryQueryAccessor##CType18 CName18; \
		TestQueryQueryAccessor##CType19 CName19; \
		TestQueryQueryAccessor##CType20 CName20; \
		TestQueryQueryAccessor##CType21 CName21; \
		TestQueryQueryAccessor##CType22 CName22; \
		TestQueryQueryAccessor##CType23 CName23; \
		TestQueryQueryAccessor##CType24 CName24; \
		TestQueryQueryAccessor##CType25 CName25; \
		TestQueryQueryAccessor##CType26 CName26; \
		TestQueryQueryAccessor##CType27 CName27; \
		TestQueryQueryAccessor##CType28 CName28; \
		TestQueryQueryAccessor##CType29 CName29; \
		TestQueryQueryAccessor##CType30 CName30; \
		TestQueryQueryAccessor##CType31 CName31; \
		TestQueryQueryAccessor##CType32 CName32; \
		TestQueryQueryAccessor##CType33 CName33; \
		TestQueryQueryAccessor##CType34 CName34; \
		TestQueryQueryAccessor##CType35 CName35; \
		TestQueryQueryAccessor##CType36 CName36; \
		TestQueryQueryAccessor##CType37 CName37; \
		TestQueryQueryAccessor##CType38 CName38; \
		TestQueryQueryAccessor##CType39 CName39; \
		TestQueryQueryAccessor##CType40 CName40; \
		TestQueryQueryAccessor##CType41 CName41; \
		TestQueryQueryAccessor##CType42 CName42; \
\
		TestQuery& LeftParan(void) {Query::LeftParan(); return *this;}; \
		TestQuery& Or(void) {Query::Or(); return *this;}; \
		TestQuery& RightParan(void) {Query::RightParan(); return *this;}; \
		TestQuery& Subtable(size_t column) {Query::Subtable(column); return *this;}; \
		TestQuery& Parent() {Query::Parent(); return *this;}; \
	}; \
\
	TestQuery GetQuery() {return TestQuery();} \
\
	class Cursor : public CursorBase { \
	public: \
		Cursor(TableName& table, size_t ndx) : CursorBase(table, ndx) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
			CName17.Create(this, 16); \
			CName18.Create(this, 17); \
			CName19.Create(this, 18); \
			CName20.Create(this, 19); \
			CName21.Create(this, 20); \
			CName22.Create(this, 21); \
			CName23.Create(this, 22); \
			CName24.Create(this, 23); \
			CName25.Create(this, 24); \
			CName26.Create(this, 25); \
			CName27.Create(this, 26); \
			CName28.Create(this, 27); \
			CName29.Create(this, 28); \
			CName30.Create(this, 29); \
			CName31.Create(this, 30); \
			CName32.Create(this, 31); \
			CName33.Create(this, 32); \
			CName34.Create(this, 33); \
			CName35.Create(this, 34); \
			CName36.Create(this, 35); \
			CName37.Create(this, 36); \
			CName38.Create(this, 37); \
			CName39.Create(this, 38); \
			CName40.Create(this, 39); \
			CName41.Create(this, 40); \
			CName42.Create(this, 41); \
		} \
		Cursor(const TableName& table, size_t ndx) : CursorBase(const_cast<TableName&>(table), ndx) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
			CName17.Create(this, 16); \
			CName18.Create(this, 17); \
			CName19.Create(this, 18); \
			CName20.Create(this, 19); \
			CName21.Create(this, 20); \
			CName22.Create(this, 21); \
			CName23.Create(this, 22); \
			CName24.Create(this, 23); \
			CName25.Create(this, 24); \
			CName26.Create(this, 25); \
			CName27.Create(this, 26); \
			CName28.Create(this, 27); \
			CName29.Create(this, 28); \
			CName30.Create(this, 29); \
			CName31.Create(this, 30); \
			CName32.Create(this, 31); \
			CName33.Create(this, 32); \
			CName34.Create(this, 33); \
			CName35.Create(this, 34); \
			CName36.Create(this, 35); \
			CName37.Create(this, 36); \
			CName38.Create(this, 37); \
			CName39.Create(this, 38); \
			CName40.Create(this, 39); \
			CName41.Create(this, 40); \
			CName42.Create(this, 41); \
		} \
		Cursor(const Cursor& v) : CursorBase(v) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
			CName17.Create(this, 16); \
			CName18.Create(this, 17); \
			CName19.Create(this, 18); \
			CName20.Create(this, 19); \
			CName21.Create(this, 20); \
			CName22.Create(this, 21); \
			CName23.Create(this, 22); \
			CName24.Create(this, 23); \
			CName25.Create(this, 24); \
			CName26.Create(this, 25); \
			CName27.Create(this, 26); \
			CName28.Create(this, 27); \
			CName29.Create(this, 28); \
			CName30.Create(this, 29); \
			CName31.Create(this, 30); \
			CName32.Create(this, 31); \
			CName33.Create(this, 32); \
			CName34.Create(this, 33); \
			CName35.Create(this, 34); \
			CName36.Create(this, 35); \
			CName37.Create(this, 36); \
			CName38.Create(this, 37); \
			CName39.Create(this, 38); \
			CName40.Create(this, 39); \
			CName41.Create(this, 40); \
			CName42.Create(this, 41); \
		} \
		Accessor##CType1 CName1; \
		Accessor##CType2 CName2; \
		Accessor##CType3 CName3; \
		Accessor##CType4 CName4; \
		Accessor##CType5 CName5; \
		Accessor##CType6 CName6; \
		Accessor##CType7 CName7; \
		Accessor##CType8 CName8; \
		Accessor##CType9 CName9; \
		Accessor##CType10 CName10; \
		Accessor##CType11 CName11; \
		Accessor##CType12 CName12; \
		Accessor##CType13 CName13; \
		Accessor##CType14 CName14; \
		Accessor##CType15 CName15; \
		Accessor##CType16 CName16; \
		Accessor##CType17 CName17; \
		Accessor##CType18 CName18; \
		Accessor##CType19 CName19; \
		Accessor##CType20 CName20; \
		Accessor##CType21 CName21; \
		Accessor##CType22 CName22; \
		Accessor##CType23 CName23; \
		Accessor##CType24 CName24; \
		Accessor##CType25 CName25; \
		Accessor##CType26 CName26; \
		Accessor##CType27 CName27; \
		Accessor##CType28 CName28; \
		Accessor##CType29 CName29; \
		Accessor##CType30 CName30; \
		Accessor##CType31 CName31; \
		Accessor##CType32 CName32; \
		Accessor##CType33 CName33; \
		Accessor##CType34 CName34; \
		Accessor##CType35 CName35; \
		Accessor##CType36 CName36; \
		Accessor##CType37 CName37; \
		Accessor##CType38 CName38; \
		Accessor##CType39 CName39; \
		Accessor##CType40 CName40; \
		Accessor##CType41 CName41; \
		Accessor##CType42 CName42; \
	}; \
\
	void Add(tdbType##CType1 CName1, tdbType##CType2 CName2, tdbType##CType3 CName3, tdbType##CType4 CName4, tdbType##CType5 CName5, tdbType##CType6 CName6, tdbType##CType7 CName7, tdbType##CType8 CName8, tdbType##CType9 CName9, tdbType##CType10 CName10, tdbType##CType11 CName11, tdbType##CType12 CName12, tdbType##CType13 CName13, tdbType##CType14 CName14, tdbType##CType15 CName15, tdbType##CType16 CName16, tdbType##CType17 CName17, tdbType##CType18 CName18, tdbType##CType19 CName19, tdbType##CType20 CName20, tdbType##CType21 CName21, tdbType##CType22 CName22, tdbType##CType23 CName23, tdbType##CType24 CName24, tdbType##CType25 CName25, tdbType##CType26 CName26, tdbType##CType27 CName27, tdbType##CType28 CName28, tdbType##CType29 CName29, tdbType##CType30 CName30, tdbType##CType31 CName31, tdbType##CType32 CName32, tdbType##CType33 CName33, tdbType##CType34 CName34, tdbType##CType35 CName35, tdbType##CType36 CName36, tdbType##CType37 CName37, tdbType##CType38 CName38, tdbType##CType39 CName39, tdbType##CType40 CName40, tdbType##CType41 CName41, tdbType##CType42 CName42) { \
		const size_t ndx = GetSize(); \
		Insert##CType1 (0, ndx, CName1); \
		Insert##CType2 (1, ndx, CName2); \
		Insert##CType3 (2, ndx, CName3); \
		Insert##CType4 (3, ndx, CName4); \
		Insert##CType5 (4, ndx, CName5); \
		Insert##CType6 (5, ndx, CName6); \
		Insert##CType7 (6, ndx, CName7); \
		Insert##CType8 (7, ndx, CName8); \
		Insert##CType9 (8, ndx, CName9); \
		Insert##CType10 (9, ndx, CName10); \
		Insert##CType11 (10, ndx, CName11); \
		Insert##CType12 (11, ndx, CName12); \
		Insert##CType13 (12, ndx, CName13); \
		Insert##CType14 (13, ndx, CName14); \
		Insert##CType15 (14, ndx, CName15); \
		Insert##CType16 (15, ndx, CName16); \
		Insert##CType17 (16, ndx, CName17); \
		Insert##CType18 (17, ndx, CName18); \
		Insert##CType19 (18, ndx, CName19); \
		Insert##CType20 (19, ndx, CName20); \
		Insert##CType21 (20, ndx, CName21); \
		Insert##CType22 (21, ndx, CName22); \
		Insert##CType23 (22, ndx, CName23); \
		Insert##CType24 (23, ndx, CName24); \
		Insert##CType25 (24, ndx, CName25); \
		Insert##CType26 (25, ndx, CName26); \
		Insert##CType27 (26, ndx, CName27); \
		Insert##CType28 (27, ndx, CName28); \
		Insert##CType29 (28, ndx, CName29); \
		Insert##CType30 (29, ndx, CName30); \
		Insert##CType31 (30, ndx, CName31); \
		Insert##CType32 (31, ndx, CName32); \
		Insert##CType33 (32, ndx, CName33); \
		Insert##CType34 (33, ndx, CName34); \
		Insert##CType35 (34, ndx, CName35); \
		Insert##CType36 (35, ndx, CName36); \
		Insert##CType37 (36, ndx, CName37); \
		Insert##CType38 (37, ndx, CName38); \
		Insert##CType39 (38, ndx, CName39); \
		Insert##CType40 (39, ndx, CName40); \
		Insert##CType41 (40, ndx, CName41); \
		Insert##CType42 (41, ndx, CName42); \
		InsertDone(); \
	} \
\
	void Insert(size_t ndx, tdbType##CType1 CName1, tdbType##CType2 CName2, tdbType##CType3 CName3, tdbType##CType4 CName4, tdbType##CType5 CName5, tdbType##CType6 CName6, tdbType##CType7 CName7, tdbType##CType8 CName8, tdbType##CType9 CName9, tdbType##CType10 CName10, tdbType##CType11 CName11, tdbType##CType12 CName12, tdbType##CType13 CName13, tdbType##CType14 CName14, tdbType##CType15 CName15, tdbType##CType16 CName16, tdbType##CType17 CName17, tdbType##CType18 CName18, tdbType##CType19 CName19, tdbType##CType20 CName20, tdbType##CType21 CName21, tdbType##CType22 CName22, tdbType##CType23 CName23, tdbType##CType24 CName24, tdbType##CType25 CName25, tdbType##CType26 CName26, tdbType##CType27 CName27, tdbType##CType28 CName28, tdbType##CType29 CName29, tdbType##CType30 CName30, tdbType##CType31 CName31, tdbType##CType32 CName32, tdbType##CType33 CName33, tdbType##CType34 CName34, tdbType##CType35 CName35, tdbType##CType36 CName36, tdbType##CType37 CName37, tdbType##CType38 CName38, tdbType##CType39 CName39, tdbType##CType40 CName40, tdbType##CType41 CName41, tdbType##CType42 CName42) { \
		Insert##CType1 (0, ndx, CName1); \
		Insert##CType2 (1, ndx, CName2); \
		Insert##CType3 (2, ndx, CName3); \
		Insert##CType4 (3, ndx, CName4); \
		Insert##CType5 (4, ndx, CName5); \
		Insert##CType6 (5, ndx, CName6); \
		Insert##CType7 (6, ndx, CName7); \
		Insert##CType8 (7, ndx, CName8); \
		Insert##CType9 (8, ndx, CName9); \
		Insert##CType10 (9, ndx, CName10); \
		Insert##CType11 (10, ndx, CName11); \
		Insert##CType12 (11, ndx, CName12); \
		Insert##CType13 (12, ndx, CName13); \
		Insert##CType14 (13, ndx, CName14); \
		Insert##CType15 (14, ndx, CName15); \
		Insert##CType16 (15, ndx, CName16); \
		Insert##CType17 (16, ndx, CName17); \
		Insert##CType18 (17, ndx, CName18); \
		Insert##CType19 (18, ndx, CName19); \
		Insert##CType20 (19, ndx, CName20); \
		Insert##CType21 (20, ndx, CName21); \
		Insert##CType22 (21, ndx, CName22); \
		Insert##CType23 (22, ndx, CName23); \
		Insert##CType24 (23, ndx, CName24); \
		Insert##CType25 (24, ndx, CName25); \
		Insert##CType26 (25, ndx, CName26); \
		Insert##CType27 (26, ndx, CName27); \
		Insert##CType28 (27, ndx, CName28); \
		Insert##CType29 (28, ndx, CName29); \
		Insert##CType30 (29, ndx, CName30); \
		Insert##CType31 (30, ndx, CName31); \
		Insert##CType32 (31, ndx, CName32); \
		Insert##CType33 (32, ndx, CName33); \
		Insert##CType34 (33, ndx, CName34); \
		Insert##CType35 (34, ndx, CName35); \
		Insert##CType36 (35, ndx, CName36); \
		Insert##CType37 (36, ndx, CName37); \
		Insert##CType38 (37, ndx, CName38); \
		Insert##CType39 (38, ndx, CName39); \
		Insert##CType40 (39, ndx, CName40); \
		Insert##CType41 (40, ndx, CName41); \
		Insert##CType42 (41, ndx, CName42); \
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
	ColumnProxy##CType3 CName3; \
	ColumnProxy##CType4 CName4; \
	ColumnProxy##CType5 CName5; \
	ColumnProxy##CType6 CName6; \
	ColumnProxy##CType7 CName7; \
	ColumnProxy##CType8 CName8; \
	ColumnProxy##CType9 CName9; \
	ColumnProxy##CType10 CName10; \
	ColumnProxy##CType11 CName11; \
	ColumnProxy##CType12 CName12; \
	ColumnProxy##CType13 CName13; \
	ColumnProxy##CType14 CName14; \
	ColumnProxy##CType15 CName15; \
	ColumnProxy##CType16 CName16; \
	ColumnProxy##CType17 CName17; \
	ColumnProxy##CType18 CName18; \
	ColumnProxy##CType19 CName19; \
	ColumnProxy##CType20 CName20; \
	ColumnProxy##CType21 CName21; \
	ColumnProxy##CType22 CName22; \
	ColumnProxy##CType23 CName23; \
	ColumnProxy##CType24 CName24; \
	ColumnProxy##CType25 CName25; \
	ColumnProxy##CType26 CName26; \
	ColumnProxy##CType27 CName27; \
	ColumnProxy##CType28 CName28; \
	ColumnProxy##CType29 CName29; \
	ColumnProxy##CType30 CName30; \
	ColumnProxy##CType31 CName31; \
	ColumnProxy##CType32 CName32; \
	ColumnProxy##CType33 CName33; \
	ColumnProxy##CType34 CName34; \
	ColumnProxy##CType35 CName35; \
	ColumnProxy##CType36 CName36; \
	ColumnProxy##CType37 CName37; \
	ColumnProxy##CType38 CName38; \
	ColumnProxy##CType39 CName39; \
	ColumnProxy##CType40 CName40; \
	ColumnProxy##CType41 CName41; \
	ColumnProxy##CType42 CName42; \
\
protected: \
	friend class Group; \
	TableName(Allocator& alloc, size_t ref, Array* parent, size_t pndx) : TopLevelTable(alloc, ref, parent, pndx) {}; \
\
private: \
	TableName(const TableName&) {} \
	TableName& operator=(const TableName&) {return *this;} \
};



#define TDB_TABLE_43(TableName, CType1, CName1, CType2, CName2, CType3, CName3, CType4, CName4, CType5, CName5, CType6, CName6, CType7, CName7, CType8, CName8, CType9, CName9, CType10, CName10, CType11, CName11, CType12, CName12, CType13, CName13, CType14, CName14, CType15, CName15, CType16, CName16, CType17, CName17, CType18, CName18, CType19, CName19, CType20, CName20, CType21, CName21, CType22, CName22, CType23, CName23, CType24, CName24, CType25, CName25, CType26, CName26, CType27, CName27, CType28, CName28, CType29, CName29, CType30, CName30, CType31, CName31, CType32, CName32, CType33, CName33, CType34, CName34, CType35, CName35, CType36, CName36, CType37, CName37, CType38, CName38, CType39, CName39, CType40, CName40, CType41, CName41, CType42, CName42, CType43, CName43) \
class TableName##Query { \
protected: \
	QueryAccessor##CType1 CName1; \
	QueryAccessor##CType2 CName2; \
	QueryAccessor##CType3 CName3; \
	QueryAccessor##CType4 CName4; \
	QueryAccessor##CType5 CName5; \
	QueryAccessor##CType6 CName6; \
	QueryAccessor##CType7 CName7; \
	QueryAccessor##CType8 CName8; \
	QueryAccessor##CType9 CName9; \
	QueryAccessor##CType10 CName10; \
	QueryAccessor##CType11 CName11; \
	QueryAccessor##CType12 CName12; \
	QueryAccessor##CType13 CName13; \
	QueryAccessor##CType14 CName14; \
	QueryAccessor##CType15 CName15; \
	QueryAccessor##CType16 CName16; \
	QueryAccessor##CType17 CName17; \
	QueryAccessor##CType18 CName18; \
	QueryAccessor##CType19 CName19; \
	QueryAccessor##CType20 CName20; \
	QueryAccessor##CType21 CName21; \
	QueryAccessor##CType22 CName22; \
	QueryAccessor##CType23 CName23; \
	QueryAccessor##CType24 CName24; \
	QueryAccessor##CType25 CName25; \
	QueryAccessor##CType26 CName26; \
	QueryAccessor##CType27 CName27; \
	QueryAccessor##CType28 CName28; \
	QueryAccessor##CType29 CName29; \
	QueryAccessor##CType30 CName30; \
	QueryAccessor##CType31 CName31; \
	QueryAccessor##CType32 CName32; \
	QueryAccessor##CType33 CName33; \
	QueryAccessor##CType34 CName34; \
	QueryAccessor##CType35 CName35; \
	QueryAccessor##CType36 CName36; \
	QueryAccessor##CType37 CName37; \
	QueryAccessor##CType38 CName38; \
	QueryAccessor##CType39 CName39; \
	QueryAccessor##CType40 CName40; \
	QueryAccessor##CType41 CName41; \
	QueryAccessor##CType42 CName42; \
	QueryAccessor##CType43 CName43; \
}; \
\
class TableName : public TopLevelTable { \
public: \
	TableName(Allocator& alloc=GetDefaultAllocator()) : TopLevelTable(alloc) { \
		RegisterColumn(Accessor##CType1::type, #CName1); \
		RegisterColumn(Accessor##CType2::type, #CName2); \
		RegisterColumn(Accessor##CType3::type, #CName3); \
		RegisterColumn(Accessor##CType4::type, #CName4); \
		RegisterColumn(Accessor##CType5::type, #CName5); \
		RegisterColumn(Accessor##CType6::type, #CName6); \
		RegisterColumn(Accessor##CType7::type, #CName7); \
		RegisterColumn(Accessor##CType8::type, #CName8); \
		RegisterColumn(Accessor##CType9::type, #CName9); \
		RegisterColumn(Accessor##CType10::type, #CName10); \
		RegisterColumn(Accessor##CType11::type, #CName11); \
		RegisterColumn(Accessor##CType12::type, #CName12); \
		RegisterColumn(Accessor##CType13::type, #CName13); \
		RegisterColumn(Accessor##CType14::type, #CName14); \
		RegisterColumn(Accessor##CType15::type, #CName15); \
		RegisterColumn(Accessor##CType16::type, #CName16); \
		RegisterColumn(Accessor##CType17::type, #CName17); \
		RegisterColumn(Accessor##CType18::type, #CName18); \
		RegisterColumn(Accessor##CType19::type, #CName19); \
		RegisterColumn(Accessor##CType20::type, #CName20); \
		RegisterColumn(Accessor##CType21::type, #CName21); \
		RegisterColumn(Accessor##CType22::type, #CName22); \
		RegisterColumn(Accessor##CType23::type, #CName23); \
		RegisterColumn(Accessor##CType24::type, #CName24); \
		RegisterColumn(Accessor##CType25::type, #CName25); \
		RegisterColumn(Accessor##CType26::type, #CName26); \
		RegisterColumn(Accessor##CType27::type, #CName27); \
		RegisterColumn(Accessor##CType28::type, #CName28); \
		RegisterColumn(Accessor##CType29::type, #CName29); \
		RegisterColumn(Accessor##CType30::type, #CName30); \
		RegisterColumn(Accessor##CType31::type, #CName31); \
		RegisterColumn(Accessor##CType32::type, #CName32); \
		RegisterColumn(Accessor##CType33::type, #CName33); \
		RegisterColumn(Accessor##CType34::type, #CName34); \
		RegisterColumn(Accessor##CType35::type, #CName35); \
		RegisterColumn(Accessor##CType36::type, #CName36); \
		RegisterColumn(Accessor##CType37::type, #CName37); \
		RegisterColumn(Accessor##CType38::type, #CName38); \
		RegisterColumn(Accessor##CType39::type, #CName39); \
		RegisterColumn(Accessor##CType40::type, #CName40); \
		RegisterColumn(Accessor##CType41::type, #CName41); \
		RegisterColumn(Accessor##CType42::type, #CName42); \
		RegisterColumn(Accessor##CType43::type, #CName43); \
\
		CName1.Create(this, 0); \
		CName2.Create(this, 1); \
		CName3.Create(this, 2); \
		CName4.Create(this, 3); \
		CName5.Create(this, 4); \
		CName6.Create(this, 5); \
		CName7.Create(this, 6); \
		CName8.Create(this, 7); \
		CName9.Create(this, 8); \
		CName10.Create(this, 9); \
		CName11.Create(this, 10); \
		CName12.Create(this, 11); \
		CName13.Create(this, 12); \
		CName14.Create(this, 13); \
		CName15.Create(this, 14); \
		CName16.Create(this, 15); \
		CName17.Create(this, 16); \
		CName18.Create(this, 17); \
		CName19.Create(this, 18); \
		CName20.Create(this, 19); \
		CName21.Create(this, 20); \
		CName22.Create(this, 21); \
		CName23.Create(this, 22); \
		CName24.Create(this, 23); \
		CName25.Create(this, 24); \
		CName26.Create(this, 25); \
		CName27.Create(this, 26); \
		CName28.Create(this, 27); \
		CName29.Create(this, 28); \
		CName30.Create(this, 29); \
		CName31.Create(this, 30); \
		CName32.Create(this, 31); \
		CName33.Create(this, 32); \
		CName34.Create(this, 33); \
		CName35.Create(this, 34); \
		CName36.Create(this, 35); \
		CName37.Create(this, 36); \
		CName38.Create(this, 37); \
		CName39.Create(this, 38); \
		CName40.Create(this, 39); \
		CName41.Create(this, 40); \
		CName42.Create(this, 41); \
		CName43.Create(this, 42); \
	}; \
\
	class TestQuery : public Query { \
	public: \
		TestQuery() : CName1(0), CName2(1), CName3(2), CName4(3), CName5(4), CName6(5), CName7(6), CName8(7), CName9(8), CName10(9), CName11(10), CName12(11), CName13(12), CName14(13), CName15(14), CName16(15), CName17(16), CName18(17), CName19(18), CName20(19), CName21(20), CName22(21), CName23(22), CName24(23), CName25(24), CName26(25), CName27(26), CName28(27), CName29(28), CName30(29), CName31(30), CName32(31), CName33(32), CName34(33), CName35(34), CName36(35), CName37(36), CName38(37), CName39(38), CName40(39), CName41(40), CName42(41), CName43(42) { \
			CName1.SetQuery(this); \
			CName2.SetQuery(this); \
			CName3.SetQuery(this); \
			CName4.SetQuery(this); \
			CName5.SetQuery(this); \
			CName6.SetQuery(this); \
			CName7.SetQuery(this); \
			CName8.SetQuery(this); \
			CName9.SetQuery(this); \
			CName10.SetQuery(this); \
			CName11.SetQuery(this); \
			CName12.SetQuery(this); \
			CName13.SetQuery(this); \
			CName14.SetQuery(this); \
			CName15.SetQuery(this); \
			CName16.SetQuery(this); \
			CName17.SetQuery(this); \
			CName18.SetQuery(this); \
			CName19.SetQuery(this); \
			CName20.SetQuery(this); \
			CName21.SetQuery(this); \
			CName22.SetQuery(this); \
			CName23.SetQuery(this); \
			CName24.SetQuery(this); \
			CName25.SetQuery(this); \
			CName26.SetQuery(this); \
			CName27.SetQuery(this); \
			CName28.SetQuery(this); \
			CName29.SetQuery(this); \
			CName30.SetQuery(this); \
			CName31.SetQuery(this); \
			CName32.SetQuery(this); \
			CName33.SetQuery(this); \
			CName34.SetQuery(this); \
			CName35.SetQuery(this); \
			CName36.SetQuery(this); \
			CName37.SetQuery(this); \
			CName38.SetQuery(this); \
			CName39.SetQuery(this); \
			CName40.SetQuery(this); \
			CName41.SetQuery(this); \
			CName42.SetQuery(this); \
			CName43.SetQuery(this); \
		} \
\
		TestQuery(const TestQuery& copy) : Query(copy), CName1(0), CName2(1), CName3(2), CName4(3), CName5(4), CName6(5), CName7(6), CName8(7), CName9(8), CName10(9), CName11(10), CName12(11), CName13(12), CName14(13), CName15(14), CName16(15), CName17(16), CName18(17), CName19(18), CName20(19), CName21(20), CName22(21), CName23(22), CName24(23), CName25(24), CName26(25), CName27(26), CName28(27), CName29(28), CName30(29), CName31(30), CName32(31), CName33(32), CName34(33), CName35(34), CName36(35), CName37(36), CName38(37), CName39(38), CName40(39), CName41(40), CName42(41), CName43(42) { \
			CName1.SetQuery(this); \
			CName2.SetQuery(this); \
			CName3.SetQuery(this); \
			CName4.SetQuery(this); \
			CName5.SetQuery(this); \
			CName6.SetQuery(this); \
			CName7.SetQuery(this); \
			CName8.SetQuery(this); \
			CName9.SetQuery(this); \
			CName10.SetQuery(this); \
			CName11.SetQuery(this); \
			CName12.SetQuery(this); \
			CName13.SetQuery(this); \
			CName14.SetQuery(this); \
			CName15.SetQuery(this); \
			CName16.SetQuery(this); \
			CName17.SetQuery(this); \
			CName18.SetQuery(this); \
			CName19.SetQuery(this); \
			CName20.SetQuery(this); \
			CName21.SetQuery(this); \
			CName22.SetQuery(this); \
			CName23.SetQuery(this); \
			CName24.SetQuery(this); \
			CName25.SetQuery(this); \
			CName26.SetQuery(this); \
			CName27.SetQuery(this); \
			CName28.SetQuery(this); \
			CName29.SetQuery(this); \
			CName30.SetQuery(this); \
			CName31.SetQuery(this); \
			CName32.SetQuery(this); \
			CName33.SetQuery(this); \
			CName34.SetQuery(this); \
			CName35.SetQuery(this); \
			CName36.SetQuery(this); \
			CName37.SetQuery(this); \
			CName38.SetQuery(this); \
			CName39.SetQuery(this); \
			CName40.SetQuery(this); \
			CName41.SetQuery(this); \
			CName42.SetQuery(this); \
			CName43.SetQuery(this); \
		} \
\
		class TestQueryQueryAccessorInt : private XQueryAccessorInt { \
		public: \
			TestQueryQueryAccessorInt(size_t column_id) : XQueryAccessorInt(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(int64_t value) {return (TestQuery &)XQueryAccessorInt::Equal(value);} \
			TestQuery& NotEqual(int64_t value) {return (TestQuery &)XQueryAccessorInt::NotEqual(value);} \
			TestQuery& Greater(int64_t value) {return (TestQuery &)XQueryAccessorInt::Greater(value);} \
			TestQuery& Less(int64_t value) {return (TestQuery &)XQueryAccessorInt::Less(value);} \
			TestQuery& Between(int64_t from, int64_t to) {return (TestQuery &)XQueryAccessorInt::Between(from, to);} \
		}; \
\
		template <class T> class TestQueryQueryAccessorEnum : public TestQueryQueryAccessorInt { \
		public: \
			TestQueryQueryAccessorEnum<T>(size_t column_id) : TestQueryQueryAccessorInt(column_id) {} \
		}; \
\
		class TestQueryQueryAccessorString : private XQueryAccessorString { \
		public: \
			TestQueryQueryAccessorString(size_t column_id) : XQueryAccessorString(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::Equal(value, CaseSensitive);} \
			TestQuery& NotEqual(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::NotEqual(value, CaseSensitive);} \
			TestQuery& BeginsWith(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::BeginsWith(value, CaseSensitive);} \
			TestQuery& EndsWith(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::EndsWith(value, CaseSensitive);} \
			TestQuery& Contains(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::Contains(value, CaseSensitive);} \
		}; \
\
		class TestQueryQueryAccessorBool : private XQueryAccessorBool { \
		public: \
			TestQueryQueryAccessorBool(size_t column_id) : XQueryAccessorBool(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(bool value) {return (TestQuery &)XQueryAccessorBool::Equal(value);} \
		}; \
\
		TestQueryQueryAccessor##CType1 CName1; \
		TestQueryQueryAccessor##CType2 CName2; \
		TestQueryQueryAccessor##CType3 CName3; \
		TestQueryQueryAccessor##CType4 CName4; \
		TestQueryQueryAccessor##CType5 CName5; \
		TestQueryQueryAccessor##CType6 CName6; \
		TestQueryQueryAccessor##CType7 CName7; \
		TestQueryQueryAccessor##CType8 CName8; \
		TestQueryQueryAccessor##CType9 CName9; \
		TestQueryQueryAccessor##CType10 CName10; \
		TestQueryQueryAccessor##CType11 CName11; \
		TestQueryQueryAccessor##CType12 CName12; \
		TestQueryQueryAccessor##CType13 CName13; \
		TestQueryQueryAccessor##CType14 CName14; \
		TestQueryQueryAccessor##CType15 CName15; \
		TestQueryQueryAccessor##CType16 CName16; \
		TestQueryQueryAccessor##CType17 CName17; \
		TestQueryQueryAccessor##CType18 CName18; \
		TestQueryQueryAccessor##CType19 CName19; \
		TestQueryQueryAccessor##CType20 CName20; \
		TestQueryQueryAccessor##CType21 CName21; \
		TestQueryQueryAccessor##CType22 CName22; \
		TestQueryQueryAccessor##CType23 CName23; \
		TestQueryQueryAccessor##CType24 CName24; \
		TestQueryQueryAccessor##CType25 CName25; \
		TestQueryQueryAccessor##CType26 CName26; \
		TestQueryQueryAccessor##CType27 CName27; \
		TestQueryQueryAccessor##CType28 CName28; \
		TestQueryQueryAccessor##CType29 CName29; \
		TestQueryQueryAccessor##CType30 CName30; \
		TestQueryQueryAccessor##CType31 CName31; \
		TestQueryQueryAccessor##CType32 CName32; \
		TestQueryQueryAccessor##CType33 CName33; \
		TestQueryQueryAccessor##CType34 CName34; \
		TestQueryQueryAccessor##CType35 CName35; \
		TestQueryQueryAccessor##CType36 CName36; \
		TestQueryQueryAccessor##CType37 CName37; \
		TestQueryQueryAccessor##CType38 CName38; \
		TestQueryQueryAccessor##CType39 CName39; \
		TestQueryQueryAccessor##CType40 CName40; \
		TestQueryQueryAccessor##CType41 CName41; \
		TestQueryQueryAccessor##CType42 CName42; \
		TestQueryQueryAccessor##CType43 CName43; \
\
		TestQuery& LeftParan(void) {Query::LeftParan(); return *this;}; \
		TestQuery& Or(void) {Query::Or(); return *this;}; \
		TestQuery& RightParan(void) {Query::RightParan(); return *this;}; \
		TestQuery& Subtable(size_t column) {Query::Subtable(column); return *this;}; \
		TestQuery& Parent() {Query::Parent(); return *this;}; \
	}; \
\
	TestQuery GetQuery() {return TestQuery();} \
\
	class Cursor : public CursorBase { \
	public: \
		Cursor(TableName& table, size_t ndx) : CursorBase(table, ndx) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
			CName17.Create(this, 16); \
			CName18.Create(this, 17); \
			CName19.Create(this, 18); \
			CName20.Create(this, 19); \
			CName21.Create(this, 20); \
			CName22.Create(this, 21); \
			CName23.Create(this, 22); \
			CName24.Create(this, 23); \
			CName25.Create(this, 24); \
			CName26.Create(this, 25); \
			CName27.Create(this, 26); \
			CName28.Create(this, 27); \
			CName29.Create(this, 28); \
			CName30.Create(this, 29); \
			CName31.Create(this, 30); \
			CName32.Create(this, 31); \
			CName33.Create(this, 32); \
			CName34.Create(this, 33); \
			CName35.Create(this, 34); \
			CName36.Create(this, 35); \
			CName37.Create(this, 36); \
			CName38.Create(this, 37); \
			CName39.Create(this, 38); \
			CName40.Create(this, 39); \
			CName41.Create(this, 40); \
			CName42.Create(this, 41); \
			CName43.Create(this, 42); \
		} \
		Cursor(const TableName& table, size_t ndx) : CursorBase(const_cast<TableName&>(table), ndx) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
			CName17.Create(this, 16); \
			CName18.Create(this, 17); \
			CName19.Create(this, 18); \
			CName20.Create(this, 19); \
			CName21.Create(this, 20); \
			CName22.Create(this, 21); \
			CName23.Create(this, 22); \
			CName24.Create(this, 23); \
			CName25.Create(this, 24); \
			CName26.Create(this, 25); \
			CName27.Create(this, 26); \
			CName28.Create(this, 27); \
			CName29.Create(this, 28); \
			CName30.Create(this, 29); \
			CName31.Create(this, 30); \
			CName32.Create(this, 31); \
			CName33.Create(this, 32); \
			CName34.Create(this, 33); \
			CName35.Create(this, 34); \
			CName36.Create(this, 35); \
			CName37.Create(this, 36); \
			CName38.Create(this, 37); \
			CName39.Create(this, 38); \
			CName40.Create(this, 39); \
			CName41.Create(this, 40); \
			CName42.Create(this, 41); \
			CName43.Create(this, 42); \
		} \
		Cursor(const Cursor& v) : CursorBase(v) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
			CName17.Create(this, 16); \
			CName18.Create(this, 17); \
			CName19.Create(this, 18); \
			CName20.Create(this, 19); \
			CName21.Create(this, 20); \
			CName22.Create(this, 21); \
			CName23.Create(this, 22); \
			CName24.Create(this, 23); \
			CName25.Create(this, 24); \
			CName26.Create(this, 25); \
			CName27.Create(this, 26); \
			CName28.Create(this, 27); \
			CName29.Create(this, 28); \
			CName30.Create(this, 29); \
			CName31.Create(this, 30); \
			CName32.Create(this, 31); \
			CName33.Create(this, 32); \
			CName34.Create(this, 33); \
			CName35.Create(this, 34); \
			CName36.Create(this, 35); \
			CName37.Create(this, 36); \
			CName38.Create(this, 37); \
			CName39.Create(this, 38); \
			CName40.Create(this, 39); \
			CName41.Create(this, 40); \
			CName42.Create(this, 41); \
			CName43.Create(this, 42); \
		} \
		Accessor##CType1 CName1; \
		Accessor##CType2 CName2; \
		Accessor##CType3 CName3; \
		Accessor##CType4 CName4; \
		Accessor##CType5 CName5; \
		Accessor##CType6 CName6; \
		Accessor##CType7 CName7; \
		Accessor##CType8 CName8; \
		Accessor##CType9 CName9; \
		Accessor##CType10 CName10; \
		Accessor##CType11 CName11; \
		Accessor##CType12 CName12; \
		Accessor##CType13 CName13; \
		Accessor##CType14 CName14; \
		Accessor##CType15 CName15; \
		Accessor##CType16 CName16; \
		Accessor##CType17 CName17; \
		Accessor##CType18 CName18; \
		Accessor##CType19 CName19; \
		Accessor##CType20 CName20; \
		Accessor##CType21 CName21; \
		Accessor##CType22 CName22; \
		Accessor##CType23 CName23; \
		Accessor##CType24 CName24; \
		Accessor##CType25 CName25; \
		Accessor##CType26 CName26; \
		Accessor##CType27 CName27; \
		Accessor##CType28 CName28; \
		Accessor##CType29 CName29; \
		Accessor##CType30 CName30; \
		Accessor##CType31 CName31; \
		Accessor##CType32 CName32; \
		Accessor##CType33 CName33; \
		Accessor##CType34 CName34; \
		Accessor##CType35 CName35; \
		Accessor##CType36 CName36; \
		Accessor##CType37 CName37; \
		Accessor##CType38 CName38; \
		Accessor##CType39 CName39; \
		Accessor##CType40 CName40; \
		Accessor##CType41 CName41; \
		Accessor##CType42 CName42; \
		Accessor##CType43 CName43; \
	}; \
\
	void Add(tdbType##CType1 CName1, tdbType##CType2 CName2, tdbType##CType3 CName3, tdbType##CType4 CName4, tdbType##CType5 CName5, tdbType##CType6 CName6, tdbType##CType7 CName7, tdbType##CType8 CName8, tdbType##CType9 CName9, tdbType##CType10 CName10, tdbType##CType11 CName11, tdbType##CType12 CName12, tdbType##CType13 CName13, tdbType##CType14 CName14, tdbType##CType15 CName15, tdbType##CType16 CName16, tdbType##CType17 CName17, tdbType##CType18 CName18, tdbType##CType19 CName19, tdbType##CType20 CName20, tdbType##CType21 CName21, tdbType##CType22 CName22, tdbType##CType23 CName23, tdbType##CType24 CName24, tdbType##CType25 CName25, tdbType##CType26 CName26, tdbType##CType27 CName27, tdbType##CType28 CName28, tdbType##CType29 CName29, tdbType##CType30 CName30, tdbType##CType31 CName31, tdbType##CType32 CName32, tdbType##CType33 CName33, tdbType##CType34 CName34, tdbType##CType35 CName35, tdbType##CType36 CName36, tdbType##CType37 CName37, tdbType##CType38 CName38, tdbType##CType39 CName39, tdbType##CType40 CName40, tdbType##CType41 CName41, tdbType##CType42 CName42, tdbType##CType43 CName43) { \
		const size_t ndx = GetSize(); \
		Insert##CType1 (0, ndx, CName1); \
		Insert##CType2 (1, ndx, CName2); \
		Insert##CType3 (2, ndx, CName3); \
		Insert##CType4 (3, ndx, CName4); \
		Insert##CType5 (4, ndx, CName5); \
		Insert##CType6 (5, ndx, CName6); \
		Insert##CType7 (6, ndx, CName7); \
		Insert##CType8 (7, ndx, CName8); \
		Insert##CType9 (8, ndx, CName9); \
		Insert##CType10 (9, ndx, CName10); \
		Insert##CType11 (10, ndx, CName11); \
		Insert##CType12 (11, ndx, CName12); \
		Insert##CType13 (12, ndx, CName13); \
		Insert##CType14 (13, ndx, CName14); \
		Insert##CType15 (14, ndx, CName15); \
		Insert##CType16 (15, ndx, CName16); \
		Insert##CType17 (16, ndx, CName17); \
		Insert##CType18 (17, ndx, CName18); \
		Insert##CType19 (18, ndx, CName19); \
		Insert##CType20 (19, ndx, CName20); \
		Insert##CType21 (20, ndx, CName21); \
		Insert##CType22 (21, ndx, CName22); \
		Insert##CType23 (22, ndx, CName23); \
		Insert##CType24 (23, ndx, CName24); \
		Insert##CType25 (24, ndx, CName25); \
		Insert##CType26 (25, ndx, CName26); \
		Insert##CType27 (26, ndx, CName27); \
		Insert##CType28 (27, ndx, CName28); \
		Insert##CType29 (28, ndx, CName29); \
		Insert##CType30 (29, ndx, CName30); \
		Insert##CType31 (30, ndx, CName31); \
		Insert##CType32 (31, ndx, CName32); \
		Insert##CType33 (32, ndx, CName33); \
		Insert##CType34 (33, ndx, CName34); \
		Insert##CType35 (34, ndx, CName35); \
		Insert##CType36 (35, ndx, CName36); \
		Insert##CType37 (36, ndx, CName37); \
		Insert##CType38 (37, ndx, CName38); \
		Insert##CType39 (38, ndx, CName39); \
		Insert##CType40 (39, ndx, CName40); \
		Insert##CType41 (40, ndx, CName41); \
		Insert##CType42 (41, ndx, CName42); \
		Insert##CType43 (42, ndx, CName43); \
		InsertDone(); \
	} \
\
	void Insert(size_t ndx, tdbType##CType1 CName1, tdbType##CType2 CName2, tdbType##CType3 CName3, tdbType##CType4 CName4, tdbType##CType5 CName5, tdbType##CType6 CName6, tdbType##CType7 CName7, tdbType##CType8 CName8, tdbType##CType9 CName9, tdbType##CType10 CName10, tdbType##CType11 CName11, tdbType##CType12 CName12, tdbType##CType13 CName13, tdbType##CType14 CName14, tdbType##CType15 CName15, tdbType##CType16 CName16, tdbType##CType17 CName17, tdbType##CType18 CName18, tdbType##CType19 CName19, tdbType##CType20 CName20, tdbType##CType21 CName21, tdbType##CType22 CName22, tdbType##CType23 CName23, tdbType##CType24 CName24, tdbType##CType25 CName25, tdbType##CType26 CName26, tdbType##CType27 CName27, tdbType##CType28 CName28, tdbType##CType29 CName29, tdbType##CType30 CName30, tdbType##CType31 CName31, tdbType##CType32 CName32, tdbType##CType33 CName33, tdbType##CType34 CName34, tdbType##CType35 CName35, tdbType##CType36 CName36, tdbType##CType37 CName37, tdbType##CType38 CName38, tdbType##CType39 CName39, tdbType##CType40 CName40, tdbType##CType41 CName41, tdbType##CType42 CName42, tdbType##CType43 CName43) { \
		Insert##CType1 (0, ndx, CName1); \
		Insert##CType2 (1, ndx, CName2); \
		Insert##CType3 (2, ndx, CName3); \
		Insert##CType4 (3, ndx, CName4); \
		Insert##CType5 (4, ndx, CName5); \
		Insert##CType6 (5, ndx, CName6); \
		Insert##CType7 (6, ndx, CName7); \
		Insert##CType8 (7, ndx, CName8); \
		Insert##CType9 (8, ndx, CName9); \
		Insert##CType10 (9, ndx, CName10); \
		Insert##CType11 (10, ndx, CName11); \
		Insert##CType12 (11, ndx, CName12); \
		Insert##CType13 (12, ndx, CName13); \
		Insert##CType14 (13, ndx, CName14); \
		Insert##CType15 (14, ndx, CName15); \
		Insert##CType16 (15, ndx, CName16); \
		Insert##CType17 (16, ndx, CName17); \
		Insert##CType18 (17, ndx, CName18); \
		Insert##CType19 (18, ndx, CName19); \
		Insert##CType20 (19, ndx, CName20); \
		Insert##CType21 (20, ndx, CName21); \
		Insert##CType22 (21, ndx, CName22); \
		Insert##CType23 (22, ndx, CName23); \
		Insert##CType24 (23, ndx, CName24); \
		Insert##CType25 (24, ndx, CName25); \
		Insert##CType26 (25, ndx, CName26); \
		Insert##CType27 (26, ndx, CName27); \
		Insert##CType28 (27, ndx, CName28); \
		Insert##CType29 (28, ndx, CName29); \
		Insert##CType30 (29, ndx, CName30); \
		Insert##CType31 (30, ndx, CName31); \
		Insert##CType32 (31, ndx, CName32); \
		Insert##CType33 (32, ndx, CName33); \
		Insert##CType34 (33, ndx, CName34); \
		Insert##CType35 (34, ndx, CName35); \
		Insert##CType36 (35, ndx, CName36); \
		Insert##CType37 (36, ndx, CName37); \
		Insert##CType38 (37, ndx, CName38); \
		Insert##CType39 (38, ndx, CName39); \
		Insert##CType40 (39, ndx, CName40); \
		Insert##CType41 (40, ndx, CName41); \
		Insert##CType42 (41, ndx, CName42); \
		Insert##CType43 (42, ndx, CName43); \
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
	ColumnProxy##CType3 CName3; \
	ColumnProxy##CType4 CName4; \
	ColumnProxy##CType5 CName5; \
	ColumnProxy##CType6 CName6; \
	ColumnProxy##CType7 CName7; \
	ColumnProxy##CType8 CName8; \
	ColumnProxy##CType9 CName9; \
	ColumnProxy##CType10 CName10; \
	ColumnProxy##CType11 CName11; \
	ColumnProxy##CType12 CName12; \
	ColumnProxy##CType13 CName13; \
	ColumnProxy##CType14 CName14; \
	ColumnProxy##CType15 CName15; \
	ColumnProxy##CType16 CName16; \
	ColumnProxy##CType17 CName17; \
	ColumnProxy##CType18 CName18; \
	ColumnProxy##CType19 CName19; \
	ColumnProxy##CType20 CName20; \
	ColumnProxy##CType21 CName21; \
	ColumnProxy##CType22 CName22; \
	ColumnProxy##CType23 CName23; \
	ColumnProxy##CType24 CName24; \
	ColumnProxy##CType25 CName25; \
	ColumnProxy##CType26 CName26; \
	ColumnProxy##CType27 CName27; \
	ColumnProxy##CType28 CName28; \
	ColumnProxy##CType29 CName29; \
	ColumnProxy##CType30 CName30; \
	ColumnProxy##CType31 CName31; \
	ColumnProxy##CType32 CName32; \
	ColumnProxy##CType33 CName33; \
	ColumnProxy##CType34 CName34; \
	ColumnProxy##CType35 CName35; \
	ColumnProxy##CType36 CName36; \
	ColumnProxy##CType37 CName37; \
	ColumnProxy##CType38 CName38; \
	ColumnProxy##CType39 CName39; \
	ColumnProxy##CType40 CName40; \
	ColumnProxy##CType41 CName41; \
	ColumnProxy##CType42 CName42; \
	ColumnProxy##CType43 CName43; \
\
protected: \
	friend class Group; \
	TableName(Allocator& alloc, size_t ref, Array* parent, size_t pndx) : TopLevelTable(alloc, ref, parent, pndx) {}; \
\
private: \
	TableName(const TableName&) {} \
	TableName& operator=(const TableName&) {return *this;} \
};



#define TDB_TABLE_44(TableName, CType1, CName1, CType2, CName2, CType3, CName3, CType4, CName4, CType5, CName5, CType6, CName6, CType7, CName7, CType8, CName8, CType9, CName9, CType10, CName10, CType11, CName11, CType12, CName12, CType13, CName13, CType14, CName14, CType15, CName15, CType16, CName16, CType17, CName17, CType18, CName18, CType19, CName19, CType20, CName20, CType21, CName21, CType22, CName22, CType23, CName23, CType24, CName24, CType25, CName25, CType26, CName26, CType27, CName27, CType28, CName28, CType29, CName29, CType30, CName30, CType31, CName31, CType32, CName32, CType33, CName33, CType34, CName34, CType35, CName35, CType36, CName36, CType37, CName37, CType38, CName38, CType39, CName39, CType40, CName40, CType41, CName41, CType42, CName42, CType43, CName43, CType44, CName44) \
class TableName##Query { \
protected: \
	QueryAccessor##CType1 CName1; \
	QueryAccessor##CType2 CName2; \
	QueryAccessor##CType3 CName3; \
	QueryAccessor##CType4 CName4; \
	QueryAccessor##CType5 CName5; \
	QueryAccessor##CType6 CName6; \
	QueryAccessor##CType7 CName7; \
	QueryAccessor##CType8 CName8; \
	QueryAccessor##CType9 CName9; \
	QueryAccessor##CType10 CName10; \
	QueryAccessor##CType11 CName11; \
	QueryAccessor##CType12 CName12; \
	QueryAccessor##CType13 CName13; \
	QueryAccessor##CType14 CName14; \
	QueryAccessor##CType15 CName15; \
	QueryAccessor##CType16 CName16; \
	QueryAccessor##CType17 CName17; \
	QueryAccessor##CType18 CName18; \
	QueryAccessor##CType19 CName19; \
	QueryAccessor##CType20 CName20; \
	QueryAccessor##CType21 CName21; \
	QueryAccessor##CType22 CName22; \
	QueryAccessor##CType23 CName23; \
	QueryAccessor##CType24 CName24; \
	QueryAccessor##CType25 CName25; \
	QueryAccessor##CType26 CName26; \
	QueryAccessor##CType27 CName27; \
	QueryAccessor##CType28 CName28; \
	QueryAccessor##CType29 CName29; \
	QueryAccessor##CType30 CName30; \
	QueryAccessor##CType31 CName31; \
	QueryAccessor##CType32 CName32; \
	QueryAccessor##CType33 CName33; \
	QueryAccessor##CType34 CName34; \
	QueryAccessor##CType35 CName35; \
	QueryAccessor##CType36 CName36; \
	QueryAccessor##CType37 CName37; \
	QueryAccessor##CType38 CName38; \
	QueryAccessor##CType39 CName39; \
	QueryAccessor##CType40 CName40; \
	QueryAccessor##CType41 CName41; \
	QueryAccessor##CType42 CName42; \
	QueryAccessor##CType43 CName43; \
	QueryAccessor##CType44 CName44; \
}; \
\
class TableName : public TopLevelTable { \
public: \
	TableName(Allocator& alloc=GetDefaultAllocator()) : TopLevelTable(alloc) { \
		RegisterColumn(Accessor##CType1::type, #CName1); \
		RegisterColumn(Accessor##CType2::type, #CName2); \
		RegisterColumn(Accessor##CType3::type, #CName3); \
		RegisterColumn(Accessor##CType4::type, #CName4); \
		RegisterColumn(Accessor##CType5::type, #CName5); \
		RegisterColumn(Accessor##CType6::type, #CName6); \
		RegisterColumn(Accessor##CType7::type, #CName7); \
		RegisterColumn(Accessor##CType8::type, #CName8); \
		RegisterColumn(Accessor##CType9::type, #CName9); \
		RegisterColumn(Accessor##CType10::type, #CName10); \
		RegisterColumn(Accessor##CType11::type, #CName11); \
		RegisterColumn(Accessor##CType12::type, #CName12); \
		RegisterColumn(Accessor##CType13::type, #CName13); \
		RegisterColumn(Accessor##CType14::type, #CName14); \
		RegisterColumn(Accessor##CType15::type, #CName15); \
		RegisterColumn(Accessor##CType16::type, #CName16); \
		RegisterColumn(Accessor##CType17::type, #CName17); \
		RegisterColumn(Accessor##CType18::type, #CName18); \
		RegisterColumn(Accessor##CType19::type, #CName19); \
		RegisterColumn(Accessor##CType20::type, #CName20); \
		RegisterColumn(Accessor##CType21::type, #CName21); \
		RegisterColumn(Accessor##CType22::type, #CName22); \
		RegisterColumn(Accessor##CType23::type, #CName23); \
		RegisterColumn(Accessor##CType24::type, #CName24); \
		RegisterColumn(Accessor##CType25::type, #CName25); \
		RegisterColumn(Accessor##CType26::type, #CName26); \
		RegisterColumn(Accessor##CType27::type, #CName27); \
		RegisterColumn(Accessor##CType28::type, #CName28); \
		RegisterColumn(Accessor##CType29::type, #CName29); \
		RegisterColumn(Accessor##CType30::type, #CName30); \
		RegisterColumn(Accessor##CType31::type, #CName31); \
		RegisterColumn(Accessor##CType32::type, #CName32); \
		RegisterColumn(Accessor##CType33::type, #CName33); \
		RegisterColumn(Accessor##CType34::type, #CName34); \
		RegisterColumn(Accessor##CType35::type, #CName35); \
		RegisterColumn(Accessor##CType36::type, #CName36); \
		RegisterColumn(Accessor##CType37::type, #CName37); \
		RegisterColumn(Accessor##CType38::type, #CName38); \
		RegisterColumn(Accessor##CType39::type, #CName39); \
		RegisterColumn(Accessor##CType40::type, #CName40); \
		RegisterColumn(Accessor##CType41::type, #CName41); \
		RegisterColumn(Accessor##CType42::type, #CName42); \
		RegisterColumn(Accessor##CType43::type, #CName43); \
		RegisterColumn(Accessor##CType44::type, #CName44); \
\
		CName1.Create(this, 0); \
		CName2.Create(this, 1); \
		CName3.Create(this, 2); \
		CName4.Create(this, 3); \
		CName5.Create(this, 4); \
		CName6.Create(this, 5); \
		CName7.Create(this, 6); \
		CName8.Create(this, 7); \
		CName9.Create(this, 8); \
		CName10.Create(this, 9); \
		CName11.Create(this, 10); \
		CName12.Create(this, 11); \
		CName13.Create(this, 12); \
		CName14.Create(this, 13); \
		CName15.Create(this, 14); \
		CName16.Create(this, 15); \
		CName17.Create(this, 16); \
		CName18.Create(this, 17); \
		CName19.Create(this, 18); \
		CName20.Create(this, 19); \
		CName21.Create(this, 20); \
		CName22.Create(this, 21); \
		CName23.Create(this, 22); \
		CName24.Create(this, 23); \
		CName25.Create(this, 24); \
		CName26.Create(this, 25); \
		CName27.Create(this, 26); \
		CName28.Create(this, 27); \
		CName29.Create(this, 28); \
		CName30.Create(this, 29); \
		CName31.Create(this, 30); \
		CName32.Create(this, 31); \
		CName33.Create(this, 32); \
		CName34.Create(this, 33); \
		CName35.Create(this, 34); \
		CName36.Create(this, 35); \
		CName37.Create(this, 36); \
		CName38.Create(this, 37); \
		CName39.Create(this, 38); \
		CName40.Create(this, 39); \
		CName41.Create(this, 40); \
		CName42.Create(this, 41); \
		CName43.Create(this, 42); \
		CName44.Create(this, 43); \
	}; \
\
	class TestQuery : public Query { \
	public: \
		TestQuery() : CName1(0), CName2(1), CName3(2), CName4(3), CName5(4), CName6(5), CName7(6), CName8(7), CName9(8), CName10(9), CName11(10), CName12(11), CName13(12), CName14(13), CName15(14), CName16(15), CName17(16), CName18(17), CName19(18), CName20(19), CName21(20), CName22(21), CName23(22), CName24(23), CName25(24), CName26(25), CName27(26), CName28(27), CName29(28), CName30(29), CName31(30), CName32(31), CName33(32), CName34(33), CName35(34), CName36(35), CName37(36), CName38(37), CName39(38), CName40(39), CName41(40), CName42(41), CName43(42), CName44(43) { \
			CName1.SetQuery(this); \
			CName2.SetQuery(this); \
			CName3.SetQuery(this); \
			CName4.SetQuery(this); \
			CName5.SetQuery(this); \
			CName6.SetQuery(this); \
			CName7.SetQuery(this); \
			CName8.SetQuery(this); \
			CName9.SetQuery(this); \
			CName10.SetQuery(this); \
			CName11.SetQuery(this); \
			CName12.SetQuery(this); \
			CName13.SetQuery(this); \
			CName14.SetQuery(this); \
			CName15.SetQuery(this); \
			CName16.SetQuery(this); \
			CName17.SetQuery(this); \
			CName18.SetQuery(this); \
			CName19.SetQuery(this); \
			CName20.SetQuery(this); \
			CName21.SetQuery(this); \
			CName22.SetQuery(this); \
			CName23.SetQuery(this); \
			CName24.SetQuery(this); \
			CName25.SetQuery(this); \
			CName26.SetQuery(this); \
			CName27.SetQuery(this); \
			CName28.SetQuery(this); \
			CName29.SetQuery(this); \
			CName30.SetQuery(this); \
			CName31.SetQuery(this); \
			CName32.SetQuery(this); \
			CName33.SetQuery(this); \
			CName34.SetQuery(this); \
			CName35.SetQuery(this); \
			CName36.SetQuery(this); \
			CName37.SetQuery(this); \
			CName38.SetQuery(this); \
			CName39.SetQuery(this); \
			CName40.SetQuery(this); \
			CName41.SetQuery(this); \
			CName42.SetQuery(this); \
			CName43.SetQuery(this); \
			CName44.SetQuery(this); \
		} \
\
		TestQuery(const TestQuery& copy) : Query(copy), CName1(0), CName2(1), CName3(2), CName4(3), CName5(4), CName6(5), CName7(6), CName8(7), CName9(8), CName10(9), CName11(10), CName12(11), CName13(12), CName14(13), CName15(14), CName16(15), CName17(16), CName18(17), CName19(18), CName20(19), CName21(20), CName22(21), CName23(22), CName24(23), CName25(24), CName26(25), CName27(26), CName28(27), CName29(28), CName30(29), CName31(30), CName32(31), CName33(32), CName34(33), CName35(34), CName36(35), CName37(36), CName38(37), CName39(38), CName40(39), CName41(40), CName42(41), CName43(42), CName44(43) { \
			CName1.SetQuery(this); \
			CName2.SetQuery(this); \
			CName3.SetQuery(this); \
			CName4.SetQuery(this); \
			CName5.SetQuery(this); \
			CName6.SetQuery(this); \
			CName7.SetQuery(this); \
			CName8.SetQuery(this); \
			CName9.SetQuery(this); \
			CName10.SetQuery(this); \
			CName11.SetQuery(this); \
			CName12.SetQuery(this); \
			CName13.SetQuery(this); \
			CName14.SetQuery(this); \
			CName15.SetQuery(this); \
			CName16.SetQuery(this); \
			CName17.SetQuery(this); \
			CName18.SetQuery(this); \
			CName19.SetQuery(this); \
			CName20.SetQuery(this); \
			CName21.SetQuery(this); \
			CName22.SetQuery(this); \
			CName23.SetQuery(this); \
			CName24.SetQuery(this); \
			CName25.SetQuery(this); \
			CName26.SetQuery(this); \
			CName27.SetQuery(this); \
			CName28.SetQuery(this); \
			CName29.SetQuery(this); \
			CName30.SetQuery(this); \
			CName31.SetQuery(this); \
			CName32.SetQuery(this); \
			CName33.SetQuery(this); \
			CName34.SetQuery(this); \
			CName35.SetQuery(this); \
			CName36.SetQuery(this); \
			CName37.SetQuery(this); \
			CName38.SetQuery(this); \
			CName39.SetQuery(this); \
			CName40.SetQuery(this); \
			CName41.SetQuery(this); \
			CName42.SetQuery(this); \
			CName43.SetQuery(this); \
			CName44.SetQuery(this); \
		} \
\
		class TestQueryQueryAccessorInt : private XQueryAccessorInt { \
		public: \
			TestQueryQueryAccessorInt(size_t column_id) : XQueryAccessorInt(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(int64_t value) {return (TestQuery &)XQueryAccessorInt::Equal(value);} \
			TestQuery& NotEqual(int64_t value) {return (TestQuery &)XQueryAccessorInt::NotEqual(value);} \
			TestQuery& Greater(int64_t value) {return (TestQuery &)XQueryAccessorInt::Greater(value);} \
			TestQuery& Less(int64_t value) {return (TestQuery &)XQueryAccessorInt::Less(value);} \
			TestQuery& Between(int64_t from, int64_t to) {return (TestQuery &)XQueryAccessorInt::Between(from, to);} \
		}; \
\
		template <class T> class TestQueryQueryAccessorEnum : public TestQueryQueryAccessorInt { \
		public: \
			TestQueryQueryAccessorEnum<T>(size_t column_id) : TestQueryQueryAccessorInt(column_id) {} \
		}; \
\
		class TestQueryQueryAccessorString : private XQueryAccessorString { \
		public: \
			TestQueryQueryAccessorString(size_t column_id) : XQueryAccessorString(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::Equal(value, CaseSensitive);} \
			TestQuery& NotEqual(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::NotEqual(value, CaseSensitive);} \
			TestQuery& BeginsWith(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::BeginsWith(value, CaseSensitive);} \
			TestQuery& EndsWith(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::EndsWith(value, CaseSensitive);} \
			TestQuery& Contains(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::Contains(value, CaseSensitive);} \
		}; \
\
		class TestQueryQueryAccessorBool : private XQueryAccessorBool { \
		public: \
			TestQueryQueryAccessorBool(size_t column_id) : XQueryAccessorBool(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(bool value) {return (TestQuery &)XQueryAccessorBool::Equal(value);} \
		}; \
\
		TestQueryQueryAccessor##CType1 CName1; \
		TestQueryQueryAccessor##CType2 CName2; \
		TestQueryQueryAccessor##CType3 CName3; \
		TestQueryQueryAccessor##CType4 CName4; \
		TestQueryQueryAccessor##CType5 CName5; \
		TestQueryQueryAccessor##CType6 CName6; \
		TestQueryQueryAccessor##CType7 CName7; \
		TestQueryQueryAccessor##CType8 CName8; \
		TestQueryQueryAccessor##CType9 CName9; \
		TestQueryQueryAccessor##CType10 CName10; \
		TestQueryQueryAccessor##CType11 CName11; \
		TestQueryQueryAccessor##CType12 CName12; \
		TestQueryQueryAccessor##CType13 CName13; \
		TestQueryQueryAccessor##CType14 CName14; \
		TestQueryQueryAccessor##CType15 CName15; \
		TestQueryQueryAccessor##CType16 CName16; \
		TestQueryQueryAccessor##CType17 CName17; \
		TestQueryQueryAccessor##CType18 CName18; \
		TestQueryQueryAccessor##CType19 CName19; \
		TestQueryQueryAccessor##CType20 CName20; \
		TestQueryQueryAccessor##CType21 CName21; \
		TestQueryQueryAccessor##CType22 CName22; \
		TestQueryQueryAccessor##CType23 CName23; \
		TestQueryQueryAccessor##CType24 CName24; \
		TestQueryQueryAccessor##CType25 CName25; \
		TestQueryQueryAccessor##CType26 CName26; \
		TestQueryQueryAccessor##CType27 CName27; \
		TestQueryQueryAccessor##CType28 CName28; \
		TestQueryQueryAccessor##CType29 CName29; \
		TestQueryQueryAccessor##CType30 CName30; \
		TestQueryQueryAccessor##CType31 CName31; \
		TestQueryQueryAccessor##CType32 CName32; \
		TestQueryQueryAccessor##CType33 CName33; \
		TestQueryQueryAccessor##CType34 CName34; \
		TestQueryQueryAccessor##CType35 CName35; \
		TestQueryQueryAccessor##CType36 CName36; \
		TestQueryQueryAccessor##CType37 CName37; \
		TestQueryQueryAccessor##CType38 CName38; \
		TestQueryQueryAccessor##CType39 CName39; \
		TestQueryQueryAccessor##CType40 CName40; \
		TestQueryQueryAccessor##CType41 CName41; \
		TestQueryQueryAccessor##CType42 CName42; \
		TestQueryQueryAccessor##CType43 CName43; \
		TestQueryQueryAccessor##CType44 CName44; \
\
		TestQuery& LeftParan(void) {Query::LeftParan(); return *this;}; \
		TestQuery& Or(void) {Query::Or(); return *this;}; \
		TestQuery& RightParan(void) {Query::RightParan(); return *this;}; \
		TestQuery& Subtable(size_t column) {Query::Subtable(column); return *this;}; \
		TestQuery& Parent() {Query::Parent(); return *this;}; \
	}; \
\
	TestQuery GetQuery() {return TestQuery();} \
\
	class Cursor : public CursorBase { \
	public: \
		Cursor(TableName& table, size_t ndx) : CursorBase(table, ndx) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
			CName17.Create(this, 16); \
			CName18.Create(this, 17); \
			CName19.Create(this, 18); \
			CName20.Create(this, 19); \
			CName21.Create(this, 20); \
			CName22.Create(this, 21); \
			CName23.Create(this, 22); \
			CName24.Create(this, 23); \
			CName25.Create(this, 24); \
			CName26.Create(this, 25); \
			CName27.Create(this, 26); \
			CName28.Create(this, 27); \
			CName29.Create(this, 28); \
			CName30.Create(this, 29); \
			CName31.Create(this, 30); \
			CName32.Create(this, 31); \
			CName33.Create(this, 32); \
			CName34.Create(this, 33); \
			CName35.Create(this, 34); \
			CName36.Create(this, 35); \
			CName37.Create(this, 36); \
			CName38.Create(this, 37); \
			CName39.Create(this, 38); \
			CName40.Create(this, 39); \
			CName41.Create(this, 40); \
			CName42.Create(this, 41); \
			CName43.Create(this, 42); \
			CName44.Create(this, 43); \
		} \
		Cursor(const TableName& table, size_t ndx) : CursorBase(const_cast<TableName&>(table), ndx) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
			CName17.Create(this, 16); \
			CName18.Create(this, 17); \
			CName19.Create(this, 18); \
			CName20.Create(this, 19); \
			CName21.Create(this, 20); \
			CName22.Create(this, 21); \
			CName23.Create(this, 22); \
			CName24.Create(this, 23); \
			CName25.Create(this, 24); \
			CName26.Create(this, 25); \
			CName27.Create(this, 26); \
			CName28.Create(this, 27); \
			CName29.Create(this, 28); \
			CName30.Create(this, 29); \
			CName31.Create(this, 30); \
			CName32.Create(this, 31); \
			CName33.Create(this, 32); \
			CName34.Create(this, 33); \
			CName35.Create(this, 34); \
			CName36.Create(this, 35); \
			CName37.Create(this, 36); \
			CName38.Create(this, 37); \
			CName39.Create(this, 38); \
			CName40.Create(this, 39); \
			CName41.Create(this, 40); \
			CName42.Create(this, 41); \
			CName43.Create(this, 42); \
			CName44.Create(this, 43); \
		} \
		Cursor(const Cursor& v) : CursorBase(v) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
			CName17.Create(this, 16); \
			CName18.Create(this, 17); \
			CName19.Create(this, 18); \
			CName20.Create(this, 19); \
			CName21.Create(this, 20); \
			CName22.Create(this, 21); \
			CName23.Create(this, 22); \
			CName24.Create(this, 23); \
			CName25.Create(this, 24); \
			CName26.Create(this, 25); \
			CName27.Create(this, 26); \
			CName28.Create(this, 27); \
			CName29.Create(this, 28); \
			CName30.Create(this, 29); \
			CName31.Create(this, 30); \
			CName32.Create(this, 31); \
			CName33.Create(this, 32); \
			CName34.Create(this, 33); \
			CName35.Create(this, 34); \
			CName36.Create(this, 35); \
			CName37.Create(this, 36); \
			CName38.Create(this, 37); \
			CName39.Create(this, 38); \
			CName40.Create(this, 39); \
			CName41.Create(this, 40); \
			CName42.Create(this, 41); \
			CName43.Create(this, 42); \
			CName44.Create(this, 43); \
		} \
		Accessor##CType1 CName1; \
		Accessor##CType2 CName2; \
		Accessor##CType3 CName3; \
		Accessor##CType4 CName4; \
		Accessor##CType5 CName5; \
		Accessor##CType6 CName6; \
		Accessor##CType7 CName7; \
		Accessor##CType8 CName8; \
		Accessor##CType9 CName9; \
		Accessor##CType10 CName10; \
		Accessor##CType11 CName11; \
		Accessor##CType12 CName12; \
		Accessor##CType13 CName13; \
		Accessor##CType14 CName14; \
		Accessor##CType15 CName15; \
		Accessor##CType16 CName16; \
		Accessor##CType17 CName17; \
		Accessor##CType18 CName18; \
		Accessor##CType19 CName19; \
		Accessor##CType20 CName20; \
		Accessor##CType21 CName21; \
		Accessor##CType22 CName22; \
		Accessor##CType23 CName23; \
		Accessor##CType24 CName24; \
		Accessor##CType25 CName25; \
		Accessor##CType26 CName26; \
		Accessor##CType27 CName27; \
		Accessor##CType28 CName28; \
		Accessor##CType29 CName29; \
		Accessor##CType30 CName30; \
		Accessor##CType31 CName31; \
		Accessor##CType32 CName32; \
		Accessor##CType33 CName33; \
		Accessor##CType34 CName34; \
		Accessor##CType35 CName35; \
		Accessor##CType36 CName36; \
		Accessor##CType37 CName37; \
		Accessor##CType38 CName38; \
		Accessor##CType39 CName39; \
		Accessor##CType40 CName40; \
		Accessor##CType41 CName41; \
		Accessor##CType42 CName42; \
		Accessor##CType43 CName43; \
		Accessor##CType44 CName44; \
	}; \
\
	void Add(tdbType##CType1 CName1, tdbType##CType2 CName2, tdbType##CType3 CName3, tdbType##CType4 CName4, tdbType##CType5 CName5, tdbType##CType6 CName6, tdbType##CType7 CName7, tdbType##CType8 CName8, tdbType##CType9 CName9, tdbType##CType10 CName10, tdbType##CType11 CName11, tdbType##CType12 CName12, tdbType##CType13 CName13, tdbType##CType14 CName14, tdbType##CType15 CName15, tdbType##CType16 CName16, tdbType##CType17 CName17, tdbType##CType18 CName18, tdbType##CType19 CName19, tdbType##CType20 CName20, tdbType##CType21 CName21, tdbType##CType22 CName22, tdbType##CType23 CName23, tdbType##CType24 CName24, tdbType##CType25 CName25, tdbType##CType26 CName26, tdbType##CType27 CName27, tdbType##CType28 CName28, tdbType##CType29 CName29, tdbType##CType30 CName30, tdbType##CType31 CName31, tdbType##CType32 CName32, tdbType##CType33 CName33, tdbType##CType34 CName34, tdbType##CType35 CName35, tdbType##CType36 CName36, tdbType##CType37 CName37, tdbType##CType38 CName38, tdbType##CType39 CName39, tdbType##CType40 CName40, tdbType##CType41 CName41, tdbType##CType42 CName42, tdbType##CType43 CName43, tdbType##CType44 CName44) { \
		const size_t ndx = GetSize(); \
		Insert##CType1 (0, ndx, CName1); \
		Insert##CType2 (1, ndx, CName2); \
		Insert##CType3 (2, ndx, CName3); \
		Insert##CType4 (3, ndx, CName4); \
		Insert##CType5 (4, ndx, CName5); \
		Insert##CType6 (5, ndx, CName6); \
		Insert##CType7 (6, ndx, CName7); \
		Insert##CType8 (7, ndx, CName8); \
		Insert##CType9 (8, ndx, CName9); \
		Insert##CType10 (9, ndx, CName10); \
		Insert##CType11 (10, ndx, CName11); \
		Insert##CType12 (11, ndx, CName12); \
		Insert##CType13 (12, ndx, CName13); \
		Insert##CType14 (13, ndx, CName14); \
		Insert##CType15 (14, ndx, CName15); \
		Insert##CType16 (15, ndx, CName16); \
		Insert##CType17 (16, ndx, CName17); \
		Insert##CType18 (17, ndx, CName18); \
		Insert##CType19 (18, ndx, CName19); \
		Insert##CType20 (19, ndx, CName20); \
		Insert##CType21 (20, ndx, CName21); \
		Insert##CType22 (21, ndx, CName22); \
		Insert##CType23 (22, ndx, CName23); \
		Insert##CType24 (23, ndx, CName24); \
		Insert##CType25 (24, ndx, CName25); \
		Insert##CType26 (25, ndx, CName26); \
		Insert##CType27 (26, ndx, CName27); \
		Insert##CType28 (27, ndx, CName28); \
		Insert##CType29 (28, ndx, CName29); \
		Insert##CType30 (29, ndx, CName30); \
		Insert##CType31 (30, ndx, CName31); \
		Insert##CType32 (31, ndx, CName32); \
		Insert##CType33 (32, ndx, CName33); \
		Insert##CType34 (33, ndx, CName34); \
		Insert##CType35 (34, ndx, CName35); \
		Insert##CType36 (35, ndx, CName36); \
		Insert##CType37 (36, ndx, CName37); \
		Insert##CType38 (37, ndx, CName38); \
		Insert##CType39 (38, ndx, CName39); \
		Insert##CType40 (39, ndx, CName40); \
		Insert##CType41 (40, ndx, CName41); \
		Insert##CType42 (41, ndx, CName42); \
		Insert##CType43 (42, ndx, CName43); \
		Insert##CType44 (43, ndx, CName44); \
		InsertDone(); \
	} \
\
	void Insert(size_t ndx, tdbType##CType1 CName1, tdbType##CType2 CName2, tdbType##CType3 CName3, tdbType##CType4 CName4, tdbType##CType5 CName5, tdbType##CType6 CName6, tdbType##CType7 CName7, tdbType##CType8 CName8, tdbType##CType9 CName9, tdbType##CType10 CName10, tdbType##CType11 CName11, tdbType##CType12 CName12, tdbType##CType13 CName13, tdbType##CType14 CName14, tdbType##CType15 CName15, tdbType##CType16 CName16, tdbType##CType17 CName17, tdbType##CType18 CName18, tdbType##CType19 CName19, tdbType##CType20 CName20, tdbType##CType21 CName21, tdbType##CType22 CName22, tdbType##CType23 CName23, tdbType##CType24 CName24, tdbType##CType25 CName25, tdbType##CType26 CName26, tdbType##CType27 CName27, tdbType##CType28 CName28, tdbType##CType29 CName29, tdbType##CType30 CName30, tdbType##CType31 CName31, tdbType##CType32 CName32, tdbType##CType33 CName33, tdbType##CType34 CName34, tdbType##CType35 CName35, tdbType##CType36 CName36, tdbType##CType37 CName37, tdbType##CType38 CName38, tdbType##CType39 CName39, tdbType##CType40 CName40, tdbType##CType41 CName41, tdbType##CType42 CName42, tdbType##CType43 CName43, tdbType##CType44 CName44) { \
		Insert##CType1 (0, ndx, CName1); \
		Insert##CType2 (1, ndx, CName2); \
		Insert##CType3 (2, ndx, CName3); \
		Insert##CType4 (3, ndx, CName4); \
		Insert##CType5 (4, ndx, CName5); \
		Insert##CType6 (5, ndx, CName6); \
		Insert##CType7 (6, ndx, CName7); \
		Insert##CType8 (7, ndx, CName8); \
		Insert##CType9 (8, ndx, CName9); \
		Insert##CType10 (9, ndx, CName10); \
		Insert##CType11 (10, ndx, CName11); \
		Insert##CType12 (11, ndx, CName12); \
		Insert##CType13 (12, ndx, CName13); \
		Insert##CType14 (13, ndx, CName14); \
		Insert##CType15 (14, ndx, CName15); \
		Insert##CType16 (15, ndx, CName16); \
		Insert##CType17 (16, ndx, CName17); \
		Insert##CType18 (17, ndx, CName18); \
		Insert##CType19 (18, ndx, CName19); \
		Insert##CType20 (19, ndx, CName20); \
		Insert##CType21 (20, ndx, CName21); \
		Insert##CType22 (21, ndx, CName22); \
		Insert##CType23 (22, ndx, CName23); \
		Insert##CType24 (23, ndx, CName24); \
		Insert##CType25 (24, ndx, CName25); \
		Insert##CType26 (25, ndx, CName26); \
		Insert##CType27 (26, ndx, CName27); \
		Insert##CType28 (27, ndx, CName28); \
		Insert##CType29 (28, ndx, CName29); \
		Insert##CType30 (29, ndx, CName30); \
		Insert##CType31 (30, ndx, CName31); \
		Insert##CType32 (31, ndx, CName32); \
		Insert##CType33 (32, ndx, CName33); \
		Insert##CType34 (33, ndx, CName34); \
		Insert##CType35 (34, ndx, CName35); \
		Insert##CType36 (35, ndx, CName36); \
		Insert##CType37 (36, ndx, CName37); \
		Insert##CType38 (37, ndx, CName38); \
		Insert##CType39 (38, ndx, CName39); \
		Insert##CType40 (39, ndx, CName40); \
		Insert##CType41 (40, ndx, CName41); \
		Insert##CType42 (41, ndx, CName42); \
		Insert##CType43 (42, ndx, CName43); \
		Insert##CType44 (43, ndx, CName44); \
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
	ColumnProxy##CType3 CName3; \
	ColumnProxy##CType4 CName4; \
	ColumnProxy##CType5 CName5; \
	ColumnProxy##CType6 CName6; \
	ColumnProxy##CType7 CName7; \
	ColumnProxy##CType8 CName8; \
	ColumnProxy##CType9 CName9; \
	ColumnProxy##CType10 CName10; \
	ColumnProxy##CType11 CName11; \
	ColumnProxy##CType12 CName12; \
	ColumnProxy##CType13 CName13; \
	ColumnProxy##CType14 CName14; \
	ColumnProxy##CType15 CName15; \
	ColumnProxy##CType16 CName16; \
	ColumnProxy##CType17 CName17; \
	ColumnProxy##CType18 CName18; \
	ColumnProxy##CType19 CName19; \
	ColumnProxy##CType20 CName20; \
	ColumnProxy##CType21 CName21; \
	ColumnProxy##CType22 CName22; \
	ColumnProxy##CType23 CName23; \
	ColumnProxy##CType24 CName24; \
	ColumnProxy##CType25 CName25; \
	ColumnProxy##CType26 CName26; \
	ColumnProxy##CType27 CName27; \
	ColumnProxy##CType28 CName28; \
	ColumnProxy##CType29 CName29; \
	ColumnProxy##CType30 CName30; \
	ColumnProxy##CType31 CName31; \
	ColumnProxy##CType32 CName32; \
	ColumnProxy##CType33 CName33; \
	ColumnProxy##CType34 CName34; \
	ColumnProxy##CType35 CName35; \
	ColumnProxy##CType36 CName36; \
	ColumnProxy##CType37 CName37; \
	ColumnProxy##CType38 CName38; \
	ColumnProxy##CType39 CName39; \
	ColumnProxy##CType40 CName40; \
	ColumnProxy##CType41 CName41; \
	ColumnProxy##CType42 CName42; \
	ColumnProxy##CType43 CName43; \
	ColumnProxy##CType44 CName44; \
\
protected: \
	friend class Group; \
	TableName(Allocator& alloc, size_t ref, Array* parent, size_t pndx) : TopLevelTable(alloc, ref, parent, pndx) {}; \
\
private: \
	TableName(const TableName&) {} \
	TableName& operator=(const TableName&) {return *this;} \
};



#define TDB_TABLE_45(TableName, CType1, CName1, CType2, CName2, CType3, CName3, CType4, CName4, CType5, CName5, CType6, CName6, CType7, CName7, CType8, CName8, CType9, CName9, CType10, CName10, CType11, CName11, CType12, CName12, CType13, CName13, CType14, CName14, CType15, CName15, CType16, CName16, CType17, CName17, CType18, CName18, CType19, CName19, CType20, CName20, CType21, CName21, CType22, CName22, CType23, CName23, CType24, CName24, CType25, CName25, CType26, CName26, CType27, CName27, CType28, CName28, CType29, CName29, CType30, CName30, CType31, CName31, CType32, CName32, CType33, CName33, CType34, CName34, CType35, CName35, CType36, CName36, CType37, CName37, CType38, CName38, CType39, CName39, CType40, CName40, CType41, CName41, CType42, CName42, CType43, CName43, CType44, CName44, CType45, CName45) \
class TableName##Query { \
protected: \
	QueryAccessor##CType1 CName1; \
	QueryAccessor##CType2 CName2; \
	QueryAccessor##CType3 CName3; \
	QueryAccessor##CType4 CName4; \
	QueryAccessor##CType5 CName5; \
	QueryAccessor##CType6 CName6; \
	QueryAccessor##CType7 CName7; \
	QueryAccessor##CType8 CName8; \
	QueryAccessor##CType9 CName9; \
	QueryAccessor##CType10 CName10; \
	QueryAccessor##CType11 CName11; \
	QueryAccessor##CType12 CName12; \
	QueryAccessor##CType13 CName13; \
	QueryAccessor##CType14 CName14; \
	QueryAccessor##CType15 CName15; \
	QueryAccessor##CType16 CName16; \
	QueryAccessor##CType17 CName17; \
	QueryAccessor##CType18 CName18; \
	QueryAccessor##CType19 CName19; \
	QueryAccessor##CType20 CName20; \
	QueryAccessor##CType21 CName21; \
	QueryAccessor##CType22 CName22; \
	QueryAccessor##CType23 CName23; \
	QueryAccessor##CType24 CName24; \
	QueryAccessor##CType25 CName25; \
	QueryAccessor##CType26 CName26; \
	QueryAccessor##CType27 CName27; \
	QueryAccessor##CType28 CName28; \
	QueryAccessor##CType29 CName29; \
	QueryAccessor##CType30 CName30; \
	QueryAccessor##CType31 CName31; \
	QueryAccessor##CType32 CName32; \
	QueryAccessor##CType33 CName33; \
	QueryAccessor##CType34 CName34; \
	QueryAccessor##CType35 CName35; \
	QueryAccessor##CType36 CName36; \
	QueryAccessor##CType37 CName37; \
	QueryAccessor##CType38 CName38; \
	QueryAccessor##CType39 CName39; \
	QueryAccessor##CType40 CName40; \
	QueryAccessor##CType41 CName41; \
	QueryAccessor##CType42 CName42; \
	QueryAccessor##CType43 CName43; \
	QueryAccessor##CType44 CName44; \
	QueryAccessor##CType45 CName45; \
}; \
\
class TableName : public TopLevelTable { \
public: \
	TableName(Allocator& alloc=GetDefaultAllocator()) : TopLevelTable(alloc) { \
		RegisterColumn(Accessor##CType1::type, #CName1); \
		RegisterColumn(Accessor##CType2::type, #CName2); \
		RegisterColumn(Accessor##CType3::type, #CName3); \
		RegisterColumn(Accessor##CType4::type, #CName4); \
		RegisterColumn(Accessor##CType5::type, #CName5); \
		RegisterColumn(Accessor##CType6::type, #CName6); \
		RegisterColumn(Accessor##CType7::type, #CName7); \
		RegisterColumn(Accessor##CType8::type, #CName8); \
		RegisterColumn(Accessor##CType9::type, #CName9); \
		RegisterColumn(Accessor##CType10::type, #CName10); \
		RegisterColumn(Accessor##CType11::type, #CName11); \
		RegisterColumn(Accessor##CType12::type, #CName12); \
		RegisterColumn(Accessor##CType13::type, #CName13); \
		RegisterColumn(Accessor##CType14::type, #CName14); \
		RegisterColumn(Accessor##CType15::type, #CName15); \
		RegisterColumn(Accessor##CType16::type, #CName16); \
		RegisterColumn(Accessor##CType17::type, #CName17); \
		RegisterColumn(Accessor##CType18::type, #CName18); \
		RegisterColumn(Accessor##CType19::type, #CName19); \
		RegisterColumn(Accessor##CType20::type, #CName20); \
		RegisterColumn(Accessor##CType21::type, #CName21); \
		RegisterColumn(Accessor##CType22::type, #CName22); \
		RegisterColumn(Accessor##CType23::type, #CName23); \
		RegisterColumn(Accessor##CType24::type, #CName24); \
		RegisterColumn(Accessor##CType25::type, #CName25); \
		RegisterColumn(Accessor##CType26::type, #CName26); \
		RegisterColumn(Accessor##CType27::type, #CName27); \
		RegisterColumn(Accessor##CType28::type, #CName28); \
		RegisterColumn(Accessor##CType29::type, #CName29); \
		RegisterColumn(Accessor##CType30::type, #CName30); \
		RegisterColumn(Accessor##CType31::type, #CName31); \
		RegisterColumn(Accessor##CType32::type, #CName32); \
		RegisterColumn(Accessor##CType33::type, #CName33); \
		RegisterColumn(Accessor##CType34::type, #CName34); \
		RegisterColumn(Accessor##CType35::type, #CName35); \
		RegisterColumn(Accessor##CType36::type, #CName36); \
		RegisterColumn(Accessor##CType37::type, #CName37); \
		RegisterColumn(Accessor##CType38::type, #CName38); \
		RegisterColumn(Accessor##CType39::type, #CName39); \
		RegisterColumn(Accessor##CType40::type, #CName40); \
		RegisterColumn(Accessor##CType41::type, #CName41); \
		RegisterColumn(Accessor##CType42::type, #CName42); \
		RegisterColumn(Accessor##CType43::type, #CName43); \
		RegisterColumn(Accessor##CType44::type, #CName44); \
		RegisterColumn(Accessor##CType45::type, #CName45); \
\
		CName1.Create(this, 0); \
		CName2.Create(this, 1); \
		CName3.Create(this, 2); \
		CName4.Create(this, 3); \
		CName5.Create(this, 4); \
		CName6.Create(this, 5); \
		CName7.Create(this, 6); \
		CName8.Create(this, 7); \
		CName9.Create(this, 8); \
		CName10.Create(this, 9); \
		CName11.Create(this, 10); \
		CName12.Create(this, 11); \
		CName13.Create(this, 12); \
		CName14.Create(this, 13); \
		CName15.Create(this, 14); \
		CName16.Create(this, 15); \
		CName17.Create(this, 16); \
		CName18.Create(this, 17); \
		CName19.Create(this, 18); \
		CName20.Create(this, 19); \
		CName21.Create(this, 20); \
		CName22.Create(this, 21); \
		CName23.Create(this, 22); \
		CName24.Create(this, 23); \
		CName25.Create(this, 24); \
		CName26.Create(this, 25); \
		CName27.Create(this, 26); \
		CName28.Create(this, 27); \
		CName29.Create(this, 28); \
		CName30.Create(this, 29); \
		CName31.Create(this, 30); \
		CName32.Create(this, 31); \
		CName33.Create(this, 32); \
		CName34.Create(this, 33); \
		CName35.Create(this, 34); \
		CName36.Create(this, 35); \
		CName37.Create(this, 36); \
		CName38.Create(this, 37); \
		CName39.Create(this, 38); \
		CName40.Create(this, 39); \
		CName41.Create(this, 40); \
		CName42.Create(this, 41); \
		CName43.Create(this, 42); \
		CName44.Create(this, 43); \
		CName45.Create(this, 44); \
	}; \
\
	class TestQuery : public Query { \
	public: \
		TestQuery() : CName1(0), CName2(1), CName3(2), CName4(3), CName5(4), CName6(5), CName7(6), CName8(7), CName9(8), CName10(9), CName11(10), CName12(11), CName13(12), CName14(13), CName15(14), CName16(15), CName17(16), CName18(17), CName19(18), CName20(19), CName21(20), CName22(21), CName23(22), CName24(23), CName25(24), CName26(25), CName27(26), CName28(27), CName29(28), CName30(29), CName31(30), CName32(31), CName33(32), CName34(33), CName35(34), CName36(35), CName37(36), CName38(37), CName39(38), CName40(39), CName41(40), CName42(41), CName43(42), CName44(43), CName45(44) { \
			CName1.SetQuery(this); \
			CName2.SetQuery(this); \
			CName3.SetQuery(this); \
			CName4.SetQuery(this); \
			CName5.SetQuery(this); \
			CName6.SetQuery(this); \
			CName7.SetQuery(this); \
			CName8.SetQuery(this); \
			CName9.SetQuery(this); \
			CName10.SetQuery(this); \
			CName11.SetQuery(this); \
			CName12.SetQuery(this); \
			CName13.SetQuery(this); \
			CName14.SetQuery(this); \
			CName15.SetQuery(this); \
			CName16.SetQuery(this); \
			CName17.SetQuery(this); \
			CName18.SetQuery(this); \
			CName19.SetQuery(this); \
			CName20.SetQuery(this); \
			CName21.SetQuery(this); \
			CName22.SetQuery(this); \
			CName23.SetQuery(this); \
			CName24.SetQuery(this); \
			CName25.SetQuery(this); \
			CName26.SetQuery(this); \
			CName27.SetQuery(this); \
			CName28.SetQuery(this); \
			CName29.SetQuery(this); \
			CName30.SetQuery(this); \
			CName31.SetQuery(this); \
			CName32.SetQuery(this); \
			CName33.SetQuery(this); \
			CName34.SetQuery(this); \
			CName35.SetQuery(this); \
			CName36.SetQuery(this); \
			CName37.SetQuery(this); \
			CName38.SetQuery(this); \
			CName39.SetQuery(this); \
			CName40.SetQuery(this); \
			CName41.SetQuery(this); \
			CName42.SetQuery(this); \
			CName43.SetQuery(this); \
			CName44.SetQuery(this); \
			CName45.SetQuery(this); \
		} \
\
		TestQuery(const TestQuery& copy) : Query(copy), CName1(0), CName2(1), CName3(2), CName4(3), CName5(4), CName6(5), CName7(6), CName8(7), CName9(8), CName10(9), CName11(10), CName12(11), CName13(12), CName14(13), CName15(14), CName16(15), CName17(16), CName18(17), CName19(18), CName20(19), CName21(20), CName22(21), CName23(22), CName24(23), CName25(24), CName26(25), CName27(26), CName28(27), CName29(28), CName30(29), CName31(30), CName32(31), CName33(32), CName34(33), CName35(34), CName36(35), CName37(36), CName38(37), CName39(38), CName40(39), CName41(40), CName42(41), CName43(42), CName44(43), CName45(44) { \
			CName1.SetQuery(this); \
			CName2.SetQuery(this); \
			CName3.SetQuery(this); \
			CName4.SetQuery(this); \
			CName5.SetQuery(this); \
			CName6.SetQuery(this); \
			CName7.SetQuery(this); \
			CName8.SetQuery(this); \
			CName9.SetQuery(this); \
			CName10.SetQuery(this); \
			CName11.SetQuery(this); \
			CName12.SetQuery(this); \
			CName13.SetQuery(this); \
			CName14.SetQuery(this); \
			CName15.SetQuery(this); \
			CName16.SetQuery(this); \
			CName17.SetQuery(this); \
			CName18.SetQuery(this); \
			CName19.SetQuery(this); \
			CName20.SetQuery(this); \
			CName21.SetQuery(this); \
			CName22.SetQuery(this); \
			CName23.SetQuery(this); \
			CName24.SetQuery(this); \
			CName25.SetQuery(this); \
			CName26.SetQuery(this); \
			CName27.SetQuery(this); \
			CName28.SetQuery(this); \
			CName29.SetQuery(this); \
			CName30.SetQuery(this); \
			CName31.SetQuery(this); \
			CName32.SetQuery(this); \
			CName33.SetQuery(this); \
			CName34.SetQuery(this); \
			CName35.SetQuery(this); \
			CName36.SetQuery(this); \
			CName37.SetQuery(this); \
			CName38.SetQuery(this); \
			CName39.SetQuery(this); \
			CName40.SetQuery(this); \
			CName41.SetQuery(this); \
			CName42.SetQuery(this); \
			CName43.SetQuery(this); \
			CName44.SetQuery(this); \
			CName45.SetQuery(this); \
		} \
\
		class TestQueryQueryAccessorInt : private XQueryAccessorInt { \
		public: \
			TestQueryQueryAccessorInt(size_t column_id) : XQueryAccessorInt(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(int64_t value) {return (TestQuery &)XQueryAccessorInt::Equal(value);} \
			TestQuery& NotEqual(int64_t value) {return (TestQuery &)XQueryAccessorInt::NotEqual(value);} \
			TestQuery& Greater(int64_t value) {return (TestQuery &)XQueryAccessorInt::Greater(value);} \
			TestQuery& Less(int64_t value) {return (TestQuery &)XQueryAccessorInt::Less(value);} \
			TestQuery& Between(int64_t from, int64_t to) {return (TestQuery &)XQueryAccessorInt::Between(from, to);} \
		}; \
\
		template <class T> class TestQueryQueryAccessorEnum : public TestQueryQueryAccessorInt { \
		public: \
			TestQueryQueryAccessorEnum<T>(size_t column_id) : TestQueryQueryAccessorInt(column_id) {} \
		}; \
\
		class TestQueryQueryAccessorString : private XQueryAccessorString { \
		public: \
			TestQueryQueryAccessorString(size_t column_id) : XQueryAccessorString(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::Equal(value, CaseSensitive);} \
			TestQuery& NotEqual(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::NotEqual(value, CaseSensitive);} \
			TestQuery& BeginsWith(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::BeginsWith(value, CaseSensitive);} \
			TestQuery& EndsWith(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::EndsWith(value, CaseSensitive);} \
			TestQuery& Contains(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::Contains(value, CaseSensitive);} \
		}; \
\
		class TestQueryQueryAccessorBool : private XQueryAccessorBool { \
		public: \
			TestQueryQueryAccessorBool(size_t column_id) : XQueryAccessorBool(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(bool value) {return (TestQuery &)XQueryAccessorBool::Equal(value);} \
		}; \
\
		TestQueryQueryAccessor##CType1 CName1; \
		TestQueryQueryAccessor##CType2 CName2; \
		TestQueryQueryAccessor##CType3 CName3; \
		TestQueryQueryAccessor##CType4 CName4; \
		TestQueryQueryAccessor##CType5 CName5; \
		TestQueryQueryAccessor##CType6 CName6; \
		TestQueryQueryAccessor##CType7 CName7; \
		TestQueryQueryAccessor##CType8 CName8; \
		TestQueryQueryAccessor##CType9 CName9; \
		TestQueryQueryAccessor##CType10 CName10; \
		TestQueryQueryAccessor##CType11 CName11; \
		TestQueryQueryAccessor##CType12 CName12; \
		TestQueryQueryAccessor##CType13 CName13; \
		TestQueryQueryAccessor##CType14 CName14; \
		TestQueryQueryAccessor##CType15 CName15; \
		TestQueryQueryAccessor##CType16 CName16; \
		TestQueryQueryAccessor##CType17 CName17; \
		TestQueryQueryAccessor##CType18 CName18; \
		TestQueryQueryAccessor##CType19 CName19; \
		TestQueryQueryAccessor##CType20 CName20; \
		TestQueryQueryAccessor##CType21 CName21; \
		TestQueryQueryAccessor##CType22 CName22; \
		TestQueryQueryAccessor##CType23 CName23; \
		TestQueryQueryAccessor##CType24 CName24; \
		TestQueryQueryAccessor##CType25 CName25; \
		TestQueryQueryAccessor##CType26 CName26; \
		TestQueryQueryAccessor##CType27 CName27; \
		TestQueryQueryAccessor##CType28 CName28; \
		TestQueryQueryAccessor##CType29 CName29; \
		TestQueryQueryAccessor##CType30 CName30; \
		TestQueryQueryAccessor##CType31 CName31; \
		TestQueryQueryAccessor##CType32 CName32; \
		TestQueryQueryAccessor##CType33 CName33; \
		TestQueryQueryAccessor##CType34 CName34; \
		TestQueryQueryAccessor##CType35 CName35; \
		TestQueryQueryAccessor##CType36 CName36; \
		TestQueryQueryAccessor##CType37 CName37; \
		TestQueryQueryAccessor##CType38 CName38; \
		TestQueryQueryAccessor##CType39 CName39; \
		TestQueryQueryAccessor##CType40 CName40; \
		TestQueryQueryAccessor##CType41 CName41; \
		TestQueryQueryAccessor##CType42 CName42; \
		TestQueryQueryAccessor##CType43 CName43; \
		TestQueryQueryAccessor##CType44 CName44; \
		TestQueryQueryAccessor##CType45 CName45; \
\
		TestQuery& LeftParan(void) {Query::LeftParan(); return *this;}; \
		TestQuery& Or(void) {Query::Or(); return *this;}; \
		TestQuery& RightParan(void) {Query::RightParan(); return *this;}; \
		TestQuery& Subtable(size_t column) {Query::Subtable(column); return *this;}; \
		TestQuery& Parent() {Query::Parent(); return *this;}; \
	}; \
\
	TestQuery GetQuery() {return TestQuery();} \
\
	class Cursor : public CursorBase { \
	public: \
		Cursor(TableName& table, size_t ndx) : CursorBase(table, ndx) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
			CName17.Create(this, 16); \
			CName18.Create(this, 17); \
			CName19.Create(this, 18); \
			CName20.Create(this, 19); \
			CName21.Create(this, 20); \
			CName22.Create(this, 21); \
			CName23.Create(this, 22); \
			CName24.Create(this, 23); \
			CName25.Create(this, 24); \
			CName26.Create(this, 25); \
			CName27.Create(this, 26); \
			CName28.Create(this, 27); \
			CName29.Create(this, 28); \
			CName30.Create(this, 29); \
			CName31.Create(this, 30); \
			CName32.Create(this, 31); \
			CName33.Create(this, 32); \
			CName34.Create(this, 33); \
			CName35.Create(this, 34); \
			CName36.Create(this, 35); \
			CName37.Create(this, 36); \
			CName38.Create(this, 37); \
			CName39.Create(this, 38); \
			CName40.Create(this, 39); \
			CName41.Create(this, 40); \
			CName42.Create(this, 41); \
			CName43.Create(this, 42); \
			CName44.Create(this, 43); \
			CName45.Create(this, 44); \
		} \
		Cursor(const TableName& table, size_t ndx) : CursorBase(const_cast<TableName&>(table), ndx) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
			CName17.Create(this, 16); \
			CName18.Create(this, 17); \
			CName19.Create(this, 18); \
			CName20.Create(this, 19); \
			CName21.Create(this, 20); \
			CName22.Create(this, 21); \
			CName23.Create(this, 22); \
			CName24.Create(this, 23); \
			CName25.Create(this, 24); \
			CName26.Create(this, 25); \
			CName27.Create(this, 26); \
			CName28.Create(this, 27); \
			CName29.Create(this, 28); \
			CName30.Create(this, 29); \
			CName31.Create(this, 30); \
			CName32.Create(this, 31); \
			CName33.Create(this, 32); \
			CName34.Create(this, 33); \
			CName35.Create(this, 34); \
			CName36.Create(this, 35); \
			CName37.Create(this, 36); \
			CName38.Create(this, 37); \
			CName39.Create(this, 38); \
			CName40.Create(this, 39); \
			CName41.Create(this, 40); \
			CName42.Create(this, 41); \
			CName43.Create(this, 42); \
			CName44.Create(this, 43); \
			CName45.Create(this, 44); \
		} \
		Cursor(const Cursor& v) : CursorBase(v) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
			CName17.Create(this, 16); \
			CName18.Create(this, 17); \
			CName19.Create(this, 18); \
			CName20.Create(this, 19); \
			CName21.Create(this, 20); \
			CName22.Create(this, 21); \
			CName23.Create(this, 22); \
			CName24.Create(this, 23); \
			CName25.Create(this, 24); \
			CName26.Create(this, 25); \
			CName27.Create(this, 26); \
			CName28.Create(this, 27); \
			CName29.Create(this, 28); \
			CName30.Create(this, 29); \
			CName31.Create(this, 30); \
			CName32.Create(this, 31); \
			CName33.Create(this, 32); \
			CName34.Create(this, 33); \
			CName35.Create(this, 34); \
			CName36.Create(this, 35); \
			CName37.Create(this, 36); \
			CName38.Create(this, 37); \
			CName39.Create(this, 38); \
			CName40.Create(this, 39); \
			CName41.Create(this, 40); \
			CName42.Create(this, 41); \
			CName43.Create(this, 42); \
			CName44.Create(this, 43); \
			CName45.Create(this, 44); \
		} \
		Accessor##CType1 CName1; \
		Accessor##CType2 CName2; \
		Accessor##CType3 CName3; \
		Accessor##CType4 CName4; \
		Accessor##CType5 CName5; \
		Accessor##CType6 CName6; \
		Accessor##CType7 CName7; \
		Accessor##CType8 CName8; \
		Accessor##CType9 CName9; \
		Accessor##CType10 CName10; \
		Accessor##CType11 CName11; \
		Accessor##CType12 CName12; \
		Accessor##CType13 CName13; \
		Accessor##CType14 CName14; \
		Accessor##CType15 CName15; \
		Accessor##CType16 CName16; \
		Accessor##CType17 CName17; \
		Accessor##CType18 CName18; \
		Accessor##CType19 CName19; \
		Accessor##CType20 CName20; \
		Accessor##CType21 CName21; \
		Accessor##CType22 CName22; \
		Accessor##CType23 CName23; \
		Accessor##CType24 CName24; \
		Accessor##CType25 CName25; \
		Accessor##CType26 CName26; \
		Accessor##CType27 CName27; \
		Accessor##CType28 CName28; \
		Accessor##CType29 CName29; \
		Accessor##CType30 CName30; \
		Accessor##CType31 CName31; \
		Accessor##CType32 CName32; \
		Accessor##CType33 CName33; \
		Accessor##CType34 CName34; \
		Accessor##CType35 CName35; \
		Accessor##CType36 CName36; \
		Accessor##CType37 CName37; \
		Accessor##CType38 CName38; \
		Accessor##CType39 CName39; \
		Accessor##CType40 CName40; \
		Accessor##CType41 CName41; \
		Accessor##CType42 CName42; \
		Accessor##CType43 CName43; \
		Accessor##CType44 CName44; \
		Accessor##CType45 CName45; \
	}; \
\
	void Add(tdbType##CType1 CName1, tdbType##CType2 CName2, tdbType##CType3 CName3, tdbType##CType4 CName4, tdbType##CType5 CName5, tdbType##CType6 CName6, tdbType##CType7 CName7, tdbType##CType8 CName8, tdbType##CType9 CName9, tdbType##CType10 CName10, tdbType##CType11 CName11, tdbType##CType12 CName12, tdbType##CType13 CName13, tdbType##CType14 CName14, tdbType##CType15 CName15, tdbType##CType16 CName16, tdbType##CType17 CName17, tdbType##CType18 CName18, tdbType##CType19 CName19, tdbType##CType20 CName20, tdbType##CType21 CName21, tdbType##CType22 CName22, tdbType##CType23 CName23, tdbType##CType24 CName24, tdbType##CType25 CName25, tdbType##CType26 CName26, tdbType##CType27 CName27, tdbType##CType28 CName28, tdbType##CType29 CName29, tdbType##CType30 CName30, tdbType##CType31 CName31, tdbType##CType32 CName32, tdbType##CType33 CName33, tdbType##CType34 CName34, tdbType##CType35 CName35, tdbType##CType36 CName36, tdbType##CType37 CName37, tdbType##CType38 CName38, tdbType##CType39 CName39, tdbType##CType40 CName40, tdbType##CType41 CName41, tdbType##CType42 CName42, tdbType##CType43 CName43, tdbType##CType44 CName44, tdbType##CType45 CName45) { \
		const size_t ndx = GetSize(); \
		Insert##CType1 (0, ndx, CName1); \
		Insert##CType2 (1, ndx, CName2); \
		Insert##CType3 (2, ndx, CName3); \
		Insert##CType4 (3, ndx, CName4); \
		Insert##CType5 (4, ndx, CName5); \
		Insert##CType6 (5, ndx, CName6); \
		Insert##CType7 (6, ndx, CName7); \
		Insert##CType8 (7, ndx, CName8); \
		Insert##CType9 (8, ndx, CName9); \
		Insert##CType10 (9, ndx, CName10); \
		Insert##CType11 (10, ndx, CName11); \
		Insert##CType12 (11, ndx, CName12); \
		Insert##CType13 (12, ndx, CName13); \
		Insert##CType14 (13, ndx, CName14); \
		Insert##CType15 (14, ndx, CName15); \
		Insert##CType16 (15, ndx, CName16); \
		Insert##CType17 (16, ndx, CName17); \
		Insert##CType18 (17, ndx, CName18); \
		Insert##CType19 (18, ndx, CName19); \
		Insert##CType20 (19, ndx, CName20); \
		Insert##CType21 (20, ndx, CName21); \
		Insert##CType22 (21, ndx, CName22); \
		Insert##CType23 (22, ndx, CName23); \
		Insert##CType24 (23, ndx, CName24); \
		Insert##CType25 (24, ndx, CName25); \
		Insert##CType26 (25, ndx, CName26); \
		Insert##CType27 (26, ndx, CName27); \
		Insert##CType28 (27, ndx, CName28); \
		Insert##CType29 (28, ndx, CName29); \
		Insert##CType30 (29, ndx, CName30); \
		Insert##CType31 (30, ndx, CName31); \
		Insert##CType32 (31, ndx, CName32); \
		Insert##CType33 (32, ndx, CName33); \
		Insert##CType34 (33, ndx, CName34); \
		Insert##CType35 (34, ndx, CName35); \
		Insert##CType36 (35, ndx, CName36); \
		Insert##CType37 (36, ndx, CName37); \
		Insert##CType38 (37, ndx, CName38); \
		Insert##CType39 (38, ndx, CName39); \
		Insert##CType40 (39, ndx, CName40); \
		Insert##CType41 (40, ndx, CName41); \
		Insert##CType42 (41, ndx, CName42); \
		Insert##CType43 (42, ndx, CName43); \
		Insert##CType44 (43, ndx, CName44); \
		Insert##CType45 (44, ndx, CName45); \
		InsertDone(); \
	} \
\
	void Insert(size_t ndx, tdbType##CType1 CName1, tdbType##CType2 CName2, tdbType##CType3 CName3, tdbType##CType4 CName4, tdbType##CType5 CName5, tdbType##CType6 CName6, tdbType##CType7 CName7, tdbType##CType8 CName8, tdbType##CType9 CName9, tdbType##CType10 CName10, tdbType##CType11 CName11, tdbType##CType12 CName12, tdbType##CType13 CName13, tdbType##CType14 CName14, tdbType##CType15 CName15, tdbType##CType16 CName16, tdbType##CType17 CName17, tdbType##CType18 CName18, tdbType##CType19 CName19, tdbType##CType20 CName20, tdbType##CType21 CName21, tdbType##CType22 CName22, tdbType##CType23 CName23, tdbType##CType24 CName24, tdbType##CType25 CName25, tdbType##CType26 CName26, tdbType##CType27 CName27, tdbType##CType28 CName28, tdbType##CType29 CName29, tdbType##CType30 CName30, tdbType##CType31 CName31, tdbType##CType32 CName32, tdbType##CType33 CName33, tdbType##CType34 CName34, tdbType##CType35 CName35, tdbType##CType36 CName36, tdbType##CType37 CName37, tdbType##CType38 CName38, tdbType##CType39 CName39, tdbType##CType40 CName40, tdbType##CType41 CName41, tdbType##CType42 CName42, tdbType##CType43 CName43, tdbType##CType44 CName44, tdbType##CType45 CName45) { \
		Insert##CType1 (0, ndx, CName1); \
		Insert##CType2 (1, ndx, CName2); \
		Insert##CType3 (2, ndx, CName3); \
		Insert##CType4 (3, ndx, CName4); \
		Insert##CType5 (4, ndx, CName5); \
		Insert##CType6 (5, ndx, CName6); \
		Insert##CType7 (6, ndx, CName7); \
		Insert##CType8 (7, ndx, CName8); \
		Insert##CType9 (8, ndx, CName9); \
		Insert##CType10 (9, ndx, CName10); \
		Insert##CType11 (10, ndx, CName11); \
		Insert##CType12 (11, ndx, CName12); \
		Insert##CType13 (12, ndx, CName13); \
		Insert##CType14 (13, ndx, CName14); \
		Insert##CType15 (14, ndx, CName15); \
		Insert##CType16 (15, ndx, CName16); \
		Insert##CType17 (16, ndx, CName17); \
		Insert##CType18 (17, ndx, CName18); \
		Insert##CType19 (18, ndx, CName19); \
		Insert##CType20 (19, ndx, CName20); \
		Insert##CType21 (20, ndx, CName21); \
		Insert##CType22 (21, ndx, CName22); \
		Insert##CType23 (22, ndx, CName23); \
		Insert##CType24 (23, ndx, CName24); \
		Insert##CType25 (24, ndx, CName25); \
		Insert##CType26 (25, ndx, CName26); \
		Insert##CType27 (26, ndx, CName27); \
		Insert##CType28 (27, ndx, CName28); \
		Insert##CType29 (28, ndx, CName29); \
		Insert##CType30 (29, ndx, CName30); \
		Insert##CType31 (30, ndx, CName31); \
		Insert##CType32 (31, ndx, CName32); \
		Insert##CType33 (32, ndx, CName33); \
		Insert##CType34 (33, ndx, CName34); \
		Insert##CType35 (34, ndx, CName35); \
		Insert##CType36 (35, ndx, CName36); \
		Insert##CType37 (36, ndx, CName37); \
		Insert##CType38 (37, ndx, CName38); \
		Insert##CType39 (38, ndx, CName39); \
		Insert##CType40 (39, ndx, CName40); \
		Insert##CType41 (40, ndx, CName41); \
		Insert##CType42 (41, ndx, CName42); \
		Insert##CType43 (42, ndx, CName43); \
		Insert##CType44 (43, ndx, CName44); \
		Insert##CType45 (44, ndx, CName45); \
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
	ColumnProxy##CType3 CName3; \
	ColumnProxy##CType4 CName4; \
	ColumnProxy##CType5 CName5; \
	ColumnProxy##CType6 CName6; \
	ColumnProxy##CType7 CName7; \
	ColumnProxy##CType8 CName8; \
	ColumnProxy##CType9 CName9; \
	ColumnProxy##CType10 CName10; \
	ColumnProxy##CType11 CName11; \
	ColumnProxy##CType12 CName12; \
	ColumnProxy##CType13 CName13; \
	ColumnProxy##CType14 CName14; \
	ColumnProxy##CType15 CName15; \
	ColumnProxy##CType16 CName16; \
	ColumnProxy##CType17 CName17; \
	ColumnProxy##CType18 CName18; \
	ColumnProxy##CType19 CName19; \
	ColumnProxy##CType20 CName20; \
	ColumnProxy##CType21 CName21; \
	ColumnProxy##CType22 CName22; \
	ColumnProxy##CType23 CName23; \
	ColumnProxy##CType24 CName24; \
	ColumnProxy##CType25 CName25; \
	ColumnProxy##CType26 CName26; \
	ColumnProxy##CType27 CName27; \
	ColumnProxy##CType28 CName28; \
	ColumnProxy##CType29 CName29; \
	ColumnProxy##CType30 CName30; \
	ColumnProxy##CType31 CName31; \
	ColumnProxy##CType32 CName32; \
	ColumnProxy##CType33 CName33; \
	ColumnProxy##CType34 CName34; \
	ColumnProxy##CType35 CName35; \
	ColumnProxy##CType36 CName36; \
	ColumnProxy##CType37 CName37; \
	ColumnProxy##CType38 CName38; \
	ColumnProxy##CType39 CName39; \
	ColumnProxy##CType40 CName40; \
	ColumnProxy##CType41 CName41; \
	ColumnProxy##CType42 CName42; \
	ColumnProxy##CType43 CName43; \
	ColumnProxy##CType44 CName44; \
	ColumnProxy##CType45 CName45; \
\
protected: \
	friend class Group; \
	TableName(Allocator& alloc, size_t ref, Array* parent, size_t pndx) : TopLevelTable(alloc, ref, parent, pndx) {}; \
\
private: \
	TableName(const TableName&) {} \
	TableName& operator=(const TableName&) {return *this;} \
};



#define TDB_TABLE_46(TableName, CType1, CName1, CType2, CName2, CType3, CName3, CType4, CName4, CType5, CName5, CType6, CName6, CType7, CName7, CType8, CName8, CType9, CName9, CType10, CName10, CType11, CName11, CType12, CName12, CType13, CName13, CType14, CName14, CType15, CName15, CType16, CName16, CType17, CName17, CType18, CName18, CType19, CName19, CType20, CName20, CType21, CName21, CType22, CName22, CType23, CName23, CType24, CName24, CType25, CName25, CType26, CName26, CType27, CName27, CType28, CName28, CType29, CName29, CType30, CName30, CType31, CName31, CType32, CName32, CType33, CName33, CType34, CName34, CType35, CName35, CType36, CName36, CType37, CName37, CType38, CName38, CType39, CName39, CType40, CName40, CType41, CName41, CType42, CName42, CType43, CName43, CType44, CName44, CType45, CName45, CType46, CName46) \
class TableName##Query { \
protected: \
	QueryAccessor##CType1 CName1; \
	QueryAccessor##CType2 CName2; \
	QueryAccessor##CType3 CName3; \
	QueryAccessor##CType4 CName4; \
	QueryAccessor##CType5 CName5; \
	QueryAccessor##CType6 CName6; \
	QueryAccessor##CType7 CName7; \
	QueryAccessor##CType8 CName8; \
	QueryAccessor##CType9 CName9; \
	QueryAccessor##CType10 CName10; \
	QueryAccessor##CType11 CName11; \
	QueryAccessor##CType12 CName12; \
	QueryAccessor##CType13 CName13; \
	QueryAccessor##CType14 CName14; \
	QueryAccessor##CType15 CName15; \
	QueryAccessor##CType16 CName16; \
	QueryAccessor##CType17 CName17; \
	QueryAccessor##CType18 CName18; \
	QueryAccessor##CType19 CName19; \
	QueryAccessor##CType20 CName20; \
	QueryAccessor##CType21 CName21; \
	QueryAccessor##CType22 CName22; \
	QueryAccessor##CType23 CName23; \
	QueryAccessor##CType24 CName24; \
	QueryAccessor##CType25 CName25; \
	QueryAccessor##CType26 CName26; \
	QueryAccessor##CType27 CName27; \
	QueryAccessor##CType28 CName28; \
	QueryAccessor##CType29 CName29; \
	QueryAccessor##CType30 CName30; \
	QueryAccessor##CType31 CName31; \
	QueryAccessor##CType32 CName32; \
	QueryAccessor##CType33 CName33; \
	QueryAccessor##CType34 CName34; \
	QueryAccessor##CType35 CName35; \
	QueryAccessor##CType36 CName36; \
	QueryAccessor##CType37 CName37; \
	QueryAccessor##CType38 CName38; \
	QueryAccessor##CType39 CName39; \
	QueryAccessor##CType40 CName40; \
	QueryAccessor##CType41 CName41; \
	QueryAccessor##CType42 CName42; \
	QueryAccessor##CType43 CName43; \
	QueryAccessor##CType44 CName44; \
	QueryAccessor##CType45 CName45; \
	QueryAccessor##CType46 CName46; \
}; \
\
class TableName : public TopLevelTable { \
public: \
	TableName(Allocator& alloc=GetDefaultAllocator()) : TopLevelTable(alloc) { \
		RegisterColumn(Accessor##CType1::type, #CName1); \
		RegisterColumn(Accessor##CType2::type, #CName2); \
		RegisterColumn(Accessor##CType3::type, #CName3); \
		RegisterColumn(Accessor##CType4::type, #CName4); \
		RegisterColumn(Accessor##CType5::type, #CName5); \
		RegisterColumn(Accessor##CType6::type, #CName6); \
		RegisterColumn(Accessor##CType7::type, #CName7); \
		RegisterColumn(Accessor##CType8::type, #CName8); \
		RegisterColumn(Accessor##CType9::type, #CName9); \
		RegisterColumn(Accessor##CType10::type, #CName10); \
		RegisterColumn(Accessor##CType11::type, #CName11); \
		RegisterColumn(Accessor##CType12::type, #CName12); \
		RegisterColumn(Accessor##CType13::type, #CName13); \
		RegisterColumn(Accessor##CType14::type, #CName14); \
		RegisterColumn(Accessor##CType15::type, #CName15); \
		RegisterColumn(Accessor##CType16::type, #CName16); \
		RegisterColumn(Accessor##CType17::type, #CName17); \
		RegisterColumn(Accessor##CType18::type, #CName18); \
		RegisterColumn(Accessor##CType19::type, #CName19); \
		RegisterColumn(Accessor##CType20::type, #CName20); \
		RegisterColumn(Accessor##CType21::type, #CName21); \
		RegisterColumn(Accessor##CType22::type, #CName22); \
		RegisterColumn(Accessor##CType23::type, #CName23); \
		RegisterColumn(Accessor##CType24::type, #CName24); \
		RegisterColumn(Accessor##CType25::type, #CName25); \
		RegisterColumn(Accessor##CType26::type, #CName26); \
		RegisterColumn(Accessor##CType27::type, #CName27); \
		RegisterColumn(Accessor##CType28::type, #CName28); \
		RegisterColumn(Accessor##CType29::type, #CName29); \
		RegisterColumn(Accessor##CType30::type, #CName30); \
		RegisterColumn(Accessor##CType31::type, #CName31); \
		RegisterColumn(Accessor##CType32::type, #CName32); \
		RegisterColumn(Accessor##CType33::type, #CName33); \
		RegisterColumn(Accessor##CType34::type, #CName34); \
		RegisterColumn(Accessor##CType35::type, #CName35); \
		RegisterColumn(Accessor##CType36::type, #CName36); \
		RegisterColumn(Accessor##CType37::type, #CName37); \
		RegisterColumn(Accessor##CType38::type, #CName38); \
		RegisterColumn(Accessor##CType39::type, #CName39); \
		RegisterColumn(Accessor##CType40::type, #CName40); \
		RegisterColumn(Accessor##CType41::type, #CName41); \
		RegisterColumn(Accessor##CType42::type, #CName42); \
		RegisterColumn(Accessor##CType43::type, #CName43); \
		RegisterColumn(Accessor##CType44::type, #CName44); \
		RegisterColumn(Accessor##CType45::type, #CName45); \
		RegisterColumn(Accessor##CType46::type, #CName46); \
\
		CName1.Create(this, 0); \
		CName2.Create(this, 1); \
		CName3.Create(this, 2); \
		CName4.Create(this, 3); \
		CName5.Create(this, 4); \
		CName6.Create(this, 5); \
		CName7.Create(this, 6); \
		CName8.Create(this, 7); \
		CName9.Create(this, 8); \
		CName10.Create(this, 9); \
		CName11.Create(this, 10); \
		CName12.Create(this, 11); \
		CName13.Create(this, 12); \
		CName14.Create(this, 13); \
		CName15.Create(this, 14); \
		CName16.Create(this, 15); \
		CName17.Create(this, 16); \
		CName18.Create(this, 17); \
		CName19.Create(this, 18); \
		CName20.Create(this, 19); \
		CName21.Create(this, 20); \
		CName22.Create(this, 21); \
		CName23.Create(this, 22); \
		CName24.Create(this, 23); \
		CName25.Create(this, 24); \
		CName26.Create(this, 25); \
		CName27.Create(this, 26); \
		CName28.Create(this, 27); \
		CName29.Create(this, 28); \
		CName30.Create(this, 29); \
		CName31.Create(this, 30); \
		CName32.Create(this, 31); \
		CName33.Create(this, 32); \
		CName34.Create(this, 33); \
		CName35.Create(this, 34); \
		CName36.Create(this, 35); \
		CName37.Create(this, 36); \
		CName38.Create(this, 37); \
		CName39.Create(this, 38); \
		CName40.Create(this, 39); \
		CName41.Create(this, 40); \
		CName42.Create(this, 41); \
		CName43.Create(this, 42); \
		CName44.Create(this, 43); \
		CName45.Create(this, 44); \
		CName46.Create(this, 45); \
	}; \
\
	class TestQuery : public Query { \
	public: \
		TestQuery() : CName1(0), CName2(1), CName3(2), CName4(3), CName5(4), CName6(5), CName7(6), CName8(7), CName9(8), CName10(9), CName11(10), CName12(11), CName13(12), CName14(13), CName15(14), CName16(15), CName17(16), CName18(17), CName19(18), CName20(19), CName21(20), CName22(21), CName23(22), CName24(23), CName25(24), CName26(25), CName27(26), CName28(27), CName29(28), CName30(29), CName31(30), CName32(31), CName33(32), CName34(33), CName35(34), CName36(35), CName37(36), CName38(37), CName39(38), CName40(39), CName41(40), CName42(41), CName43(42), CName44(43), CName45(44), CName46(45) { \
			CName1.SetQuery(this); \
			CName2.SetQuery(this); \
			CName3.SetQuery(this); \
			CName4.SetQuery(this); \
			CName5.SetQuery(this); \
			CName6.SetQuery(this); \
			CName7.SetQuery(this); \
			CName8.SetQuery(this); \
			CName9.SetQuery(this); \
			CName10.SetQuery(this); \
			CName11.SetQuery(this); \
			CName12.SetQuery(this); \
			CName13.SetQuery(this); \
			CName14.SetQuery(this); \
			CName15.SetQuery(this); \
			CName16.SetQuery(this); \
			CName17.SetQuery(this); \
			CName18.SetQuery(this); \
			CName19.SetQuery(this); \
			CName20.SetQuery(this); \
			CName21.SetQuery(this); \
			CName22.SetQuery(this); \
			CName23.SetQuery(this); \
			CName24.SetQuery(this); \
			CName25.SetQuery(this); \
			CName26.SetQuery(this); \
			CName27.SetQuery(this); \
			CName28.SetQuery(this); \
			CName29.SetQuery(this); \
			CName30.SetQuery(this); \
			CName31.SetQuery(this); \
			CName32.SetQuery(this); \
			CName33.SetQuery(this); \
			CName34.SetQuery(this); \
			CName35.SetQuery(this); \
			CName36.SetQuery(this); \
			CName37.SetQuery(this); \
			CName38.SetQuery(this); \
			CName39.SetQuery(this); \
			CName40.SetQuery(this); \
			CName41.SetQuery(this); \
			CName42.SetQuery(this); \
			CName43.SetQuery(this); \
			CName44.SetQuery(this); \
			CName45.SetQuery(this); \
			CName46.SetQuery(this); \
		} \
\
		TestQuery(const TestQuery& copy) : Query(copy), CName1(0), CName2(1), CName3(2), CName4(3), CName5(4), CName6(5), CName7(6), CName8(7), CName9(8), CName10(9), CName11(10), CName12(11), CName13(12), CName14(13), CName15(14), CName16(15), CName17(16), CName18(17), CName19(18), CName20(19), CName21(20), CName22(21), CName23(22), CName24(23), CName25(24), CName26(25), CName27(26), CName28(27), CName29(28), CName30(29), CName31(30), CName32(31), CName33(32), CName34(33), CName35(34), CName36(35), CName37(36), CName38(37), CName39(38), CName40(39), CName41(40), CName42(41), CName43(42), CName44(43), CName45(44), CName46(45) { \
			CName1.SetQuery(this); \
			CName2.SetQuery(this); \
			CName3.SetQuery(this); \
			CName4.SetQuery(this); \
			CName5.SetQuery(this); \
			CName6.SetQuery(this); \
			CName7.SetQuery(this); \
			CName8.SetQuery(this); \
			CName9.SetQuery(this); \
			CName10.SetQuery(this); \
			CName11.SetQuery(this); \
			CName12.SetQuery(this); \
			CName13.SetQuery(this); \
			CName14.SetQuery(this); \
			CName15.SetQuery(this); \
			CName16.SetQuery(this); \
			CName17.SetQuery(this); \
			CName18.SetQuery(this); \
			CName19.SetQuery(this); \
			CName20.SetQuery(this); \
			CName21.SetQuery(this); \
			CName22.SetQuery(this); \
			CName23.SetQuery(this); \
			CName24.SetQuery(this); \
			CName25.SetQuery(this); \
			CName26.SetQuery(this); \
			CName27.SetQuery(this); \
			CName28.SetQuery(this); \
			CName29.SetQuery(this); \
			CName30.SetQuery(this); \
			CName31.SetQuery(this); \
			CName32.SetQuery(this); \
			CName33.SetQuery(this); \
			CName34.SetQuery(this); \
			CName35.SetQuery(this); \
			CName36.SetQuery(this); \
			CName37.SetQuery(this); \
			CName38.SetQuery(this); \
			CName39.SetQuery(this); \
			CName40.SetQuery(this); \
			CName41.SetQuery(this); \
			CName42.SetQuery(this); \
			CName43.SetQuery(this); \
			CName44.SetQuery(this); \
			CName45.SetQuery(this); \
			CName46.SetQuery(this); \
		} \
\
		class TestQueryQueryAccessorInt : private XQueryAccessorInt { \
		public: \
			TestQueryQueryAccessorInt(size_t column_id) : XQueryAccessorInt(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(int64_t value) {return (TestQuery &)XQueryAccessorInt::Equal(value);} \
			TestQuery& NotEqual(int64_t value) {return (TestQuery &)XQueryAccessorInt::NotEqual(value);} \
			TestQuery& Greater(int64_t value) {return (TestQuery &)XQueryAccessorInt::Greater(value);} \
			TestQuery& Less(int64_t value) {return (TestQuery &)XQueryAccessorInt::Less(value);} \
			TestQuery& Between(int64_t from, int64_t to) {return (TestQuery &)XQueryAccessorInt::Between(from, to);} \
		}; \
\
		template <class T> class TestQueryQueryAccessorEnum : public TestQueryQueryAccessorInt { \
		public: \
			TestQueryQueryAccessorEnum<T>(size_t column_id) : TestQueryQueryAccessorInt(column_id) {} \
		}; \
\
		class TestQueryQueryAccessorString : private XQueryAccessorString { \
		public: \
			TestQueryQueryAccessorString(size_t column_id) : XQueryAccessorString(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::Equal(value, CaseSensitive);} \
			TestQuery& NotEqual(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::NotEqual(value, CaseSensitive);} \
			TestQuery& BeginsWith(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::BeginsWith(value, CaseSensitive);} \
			TestQuery& EndsWith(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::EndsWith(value, CaseSensitive);} \
			TestQuery& Contains(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::Contains(value, CaseSensitive);} \
		}; \
\
		class TestQueryQueryAccessorBool : private XQueryAccessorBool { \
		public: \
			TestQueryQueryAccessorBool(size_t column_id) : XQueryAccessorBool(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(bool value) {return (TestQuery &)XQueryAccessorBool::Equal(value);} \
		}; \
\
		TestQueryQueryAccessor##CType1 CName1; \
		TestQueryQueryAccessor##CType2 CName2; \
		TestQueryQueryAccessor##CType3 CName3; \
		TestQueryQueryAccessor##CType4 CName4; \
		TestQueryQueryAccessor##CType5 CName5; \
		TestQueryQueryAccessor##CType6 CName6; \
		TestQueryQueryAccessor##CType7 CName7; \
		TestQueryQueryAccessor##CType8 CName8; \
		TestQueryQueryAccessor##CType9 CName9; \
		TestQueryQueryAccessor##CType10 CName10; \
		TestQueryQueryAccessor##CType11 CName11; \
		TestQueryQueryAccessor##CType12 CName12; \
		TestQueryQueryAccessor##CType13 CName13; \
		TestQueryQueryAccessor##CType14 CName14; \
		TestQueryQueryAccessor##CType15 CName15; \
		TestQueryQueryAccessor##CType16 CName16; \
		TestQueryQueryAccessor##CType17 CName17; \
		TestQueryQueryAccessor##CType18 CName18; \
		TestQueryQueryAccessor##CType19 CName19; \
		TestQueryQueryAccessor##CType20 CName20; \
		TestQueryQueryAccessor##CType21 CName21; \
		TestQueryQueryAccessor##CType22 CName22; \
		TestQueryQueryAccessor##CType23 CName23; \
		TestQueryQueryAccessor##CType24 CName24; \
		TestQueryQueryAccessor##CType25 CName25; \
		TestQueryQueryAccessor##CType26 CName26; \
		TestQueryQueryAccessor##CType27 CName27; \
		TestQueryQueryAccessor##CType28 CName28; \
		TestQueryQueryAccessor##CType29 CName29; \
		TestQueryQueryAccessor##CType30 CName30; \
		TestQueryQueryAccessor##CType31 CName31; \
		TestQueryQueryAccessor##CType32 CName32; \
		TestQueryQueryAccessor##CType33 CName33; \
		TestQueryQueryAccessor##CType34 CName34; \
		TestQueryQueryAccessor##CType35 CName35; \
		TestQueryQueryAccessor##CType36 CName36; \
		TestQueryQueryAccessor##CType37 CName37; \
		TestQueryQueryAccessor##CType38 CName38; \
		TestQueryQueryAccessor##CType39 CName39; \
		TestQueryQueryAccessor##CType40 CName40; \
		TestQueryQueryAccessor##CType41 CName41; \
		TestQueryQueryAccessor##CType42 CName42; \
		TestQueryQueryAccessor##CType43 CName43; \
		TestQueryQueryAccessor##CType44 CName44; \
		TestQueryQueryAccessor##CType45 CName45; \
		TestQueryQueryAccessor##CType46 CName46; \
\
		TestQuery& LeftParan(void) {Query::LeftParan(); return *this;}; \
		TestQuery& Or(void) {Query::Or(); return *this;}; \
		TestQuery& RightParan(void) {Query::RightParan(); return *this;}; \
		TestQuery& Subtable(size_t column) {Query::Subtable(column); return *this;}; \
		TestQuery& Parent() {Query::Parent(); return *this;}; \
	}; \
\
	TestQuery GetQuery() {return TestQuery();} \
\
	class Cursor : public CursorBase { \
	public: \
		Cursor(TableName& table, size_t ndx) : CursorBase(table, ndx) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
			CName17.Create(this, 16); \
			CName18.Create(this, 17); \
			CName19.Create(this, 18); \
			CName20.Create(this, 19); \
			CName21.Create(this, 20); \
			CName22.Create(this, 21); \
			CName23.Create(this, 22); \
			CName24.Create(this, 23); \
			CName25.Create(this, 24); \
			CName26.Create(this, 25); \
			CName27.Create(this, 26); \
			CName28.Create(this, 27); \
			CName29.Create(this, 28); \
			CName30.Create(this, 29); \
			CName31.Create(this, 30); \
			CName32.Create(this, 31); \
			CName33.Create(this, 32); \
			CName34.Create(this, 33); \
			CName35.Create(this, 34); \
			CName36.Create(this, 35); \
			CName37.Create(this, 36); \
			CName38.Create(this, 37); \
			CName39.Create(this, 38); \
			CName40.Create(this, 39); \
			CName41.Create(this, 40); \
			CName42.Create(this, 41); \
			CName43.Create(this, 42); \
			CName44.Create(this, 43); \
			CName45.Create(this, 44); \
			CName46.Create(this, 45); \
		} \
		Cursor(const TableName& table, size_t ndx) : CursorBase(const_cast<TableName&>(table), ndx) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
			CName17.Create(this, 16); \
			CName18.Create(this, 17); \
			CName19.Create(this, 18); \
			CName20.Create(this, 19); \
			CName21.Create(this, 20); \
			CName22.Create(this, 21); \
			CName23.Create(this, 22); \
			CName24.Create(this, 23); \
			CName25.Create(this, 24); \
			CName26.Create(this, 25); \
			CName27.Create(this, 26); \
			CName28.Create(this, 27); \
			CName29.Create(this, 28); \
			CName30.Create(this, 29); \
			CName31.Create(this, 30); \
			CName32.Create(this, 31); \
			CName33.Create(this, 32); \
			CName34.Create(this, 33); \
			CName35.Create(this, 34); \
			CName36.Create(this, 35); \
			CName37.Create(this, 36); \
			CName38.Create(this, 37); \
			CName39.Create(this, 38); \
			CName40.Create(this, 39); \
			CName41.Create(this, 40); \
			CName42.Create(this, 41); \
			CName43.Create(this, 42); \
			CName44.Create(this, 43); \
			CName45.Create(this, 44); \
			CName46.Create(this, 45); \
		} \
		Cursor(const Cursor& v) : CursorBase(v) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
			CName17.Create(this, 16); \
			CName18.Create(this, 17); \
			CName19.Create(this, 18); \
			CName20.Create(this, 19); \
			CName21.Create(this, 20); \
			CName22.Create(this, 21); \
			CName23.Create(this, 22); \
			CName24.Create(this, 23); \
			CName25.Create(this, 24); \
			CName26.Create(this, 25); \
			CName27.Create(this, 26); \
			CName28.Create(this, 27); \
			CName29.Create(this, 28); \
			CName30.Create(this, 29); \
			CName31.Create(this, 30); \
			CName32.Create(this, 31); \
			CName33.Create(this, 32); \
			CName34.Create(this, 33); \
			CName35.Create(this, 34); \
			CName36.Create(this, 35); \
			CName37.Create(this, 36); \
			CName38.Create(this, 37); \
			CName39.Create(this, 38); \
			CName40.Create(this, 39); \
			CName41.Create(this, 40); \
			CName42.Create(this, 41); \
			CName43.Create(this, 42); \
			CName44.Create(this, 43); \
			CName45.Create(this, 44); \
			CName46.Create(this, 45); \
		} \
		Accessor##CType1 CName1; \
		Accessor##CType2 CName2; \
		Accessor##CType3 CName3; \
		Accessor##CType4 CName4; \
		Accessor##CType5 CName5; \
		Accessor##CType6 CName6; \
		Accessor##CType7 CName7; \
		Accessor##CType8 CName8; \
		Accessor##CType9 CName9; \
		Accessor##CType10 CName10; \
		Accessor##CType11 CName11; \
		Accessor##CType12 CName12; \
		Accessor##CType13 CName13; \
		Accessor##CType14 CName14; \
		Accessor##CType15 CName15; \
		Accessor##CType16 CName16; \
		Accessor##CType17 CName17; \
		Accessor##CType18 CName18; \
		Accessor##CType19 CName19; \
		Accessor##CType20 CName20; \
		Accessor##CType21 CName21; \
		Accessor##CType22 CName22; \
		Accessor##CType23 CName23; \
		Accessor##CType24 CName24; \
		Accessor##CType25 CName25; \
		Accessor##CType26 CName26; \
		Accessor##CType27 CName27; \
		Accessor##CType28 CName28; \
		Accessor##CType29 CName29; \
		Accessor##CType30 CName30; \
		Accessor##CType31 CName31; \
		Accessor##CType32 CName32; \
		Accessor##CType33 CName33; \
		Accessor##CType34 CName34; \
		Accessor##CType35 CName35; \
		Accessor##CType36 CName36; \
		Accessor##CType37 CName37; \
		Accessor##CType38 CName38; \
		Accessor##CType39 CName39; \
		Accessor##CType40 CName40; \
		Accessor##CType41 CName41; \
		Accessor##CType42 CName42; \
		Accessor##CType43 CName43; \
		Accessor##CType44 CName44; \
		Accessor##CType45 CName45; \
		Accessor##CType46 CName46; \
	}; \
\
	void Add(tdbType##CType1 CName1, tdbType##CType2 CName2, tdbType##CType3 CName3, tdbType##CType4 CName4, tdbType##CType5 CName5, tdbType##CType6 CName6, tdbType##CType7 CName7, tdbType##CType8 CName8, tdbType##CType9 CName9, tdbType##CType10 CName10, tdbType##CType11 CName11, tdbType##CType12 CName12, tdbType##CType13 CName13, tdbType##CType14 CName14, tdbType##CType15 CName15, tdbType##CType16 CName16, tdbType##CType17 CName17, tdbType##CType18 CName18, tdbType##CType19 CName19, tdbType##CType20 CName20, tdbType##CType21 CName21, tdbType##CType22 CName22, tdbType##CType23 CName23, tdbType##CType24 CName24, tdbType##CType25 CName25, tdbType##CType26 CName26, tdbType##CType27 CName27, tdbType##CType28 CName28, tdbType##CType29 CName29, tdbType##CType30 CName30, tdbType##CType31 CName31, tdbType##CType32 CName32, tdbType##CType33 CName33, tdbType##CType34 CName34, tdbType##CType35 CName35, tdbType##CType36 CName36, tdbType##CType37 CName37, tdbType##CType38 CName38, tdbType##CType39 CName39, tdbType##CType40 CName40, tdbType##CType41 CName41, tdbType##CType42 CName42, tdbType##CType43 CName43, tdbType##CType44 CName44, tdbType##CType45 CName45, tdbType##CType46 CName46) { \
		const size_t ndx = GetSize(); \
		Insert##CType1 (0, ndx, CName1); \
		Insert##CType2 (1, ndx, CName2); \
		Insert##CType3 (2, ndx, CName3); \
		Insert##CType4 (3, ndx, CName4); \
		Insert##CType5 (4, ndx, CName5); \
		Insert##CType6 (5, ndx, CName6); \
		Insert##CType7 (6, ndx, CName7); \
		Insert##CType8 (7, ndx, CName8); \
		Insert##CType9 (8, ndx, CName9); \
		Insert##CType10 (9, ndx, CName10); \
		Insert##CType11 (10, ndx, CName11); \
		Insert##CType12 (11, ndx, CName12); \
		Insert##CType13 (12, ndx, CName13); \
		Insert##CType14 (13, ndx, CName14); \
		Insert##CType15 (14, ndx, CName15); \
		Insert##CType16 (15, ndx, CName16); \
		Insert##CType17 (16, ndx, CName17); \
		Insert##CType18 (17, ndx, CName18); \
		Insert##CType19 (18, ndx, CName19); \
		Insert##CType20 (19, ndx, CName20); \
		Insert##CType21 (20, ndx, CName21); \
		Insert##CType22 (21, ndx, CName22); \
		Insert##CType23 (22, ndx, CName23); \
		Insert##CType24 (23, ndx, CName24); \
		Insert##CType25 (24, ndx, CName25); \
		Insert##CType26 (25, ndx, CName26); \
		Insert##CType27 (26, ndx, CName27); \
		Insert##CType28 (27, ndx, CName28); \
		Insert##CType29 (28, ndx, CName29); \
		Insert##CType30 (29, ndx, CName30); \
		Insert##CType31 (30, ndx, CName31); \
		Insert##CType32 (31, ndx, CName32); \
		Insert##CType33 (32, ndx, CName33); \
		Insert##CType34 (33, ndx, CName34); \
		Insert##CType35 (34, ndx, CName35); \
		Insert##CType36 (35, ndx, CName36); \
		Insert##CType37 (36, ndx, CName37); \
		Insert##CType38 (37, ndx, CName38); \
		Insert##CType39 (38, ndx, CName39); \
		Insert##CType40 (39, ndx, CName40); \
		Insert##CType41 (40, ndx, CName41); \
		Insert##CType42 (41, ndx, CName42); \
		Insert##CType43 (42, ndx, CName43); \
		Insert##CType44 (43, ndx, CName44); \
		Insert##CType45 (44, ndx, CName45); \
		Insert##CType46 (45, ndx, CName46); \
		InsertDone(); \
	} \
\
	void Insert(size_t ndx, tdbType##CType1 CName1, tdbType##CType2 CName2, tdbType##CType3 CName3, tdbType##CType4 CName4, tdbType##CType5 CName5, tdbType##CType6 CName6, tdbType##CType7 CName7, tdbType##CType8 CName8, tdbType##CType9 CName9, tdbType##CType10 CName10, tdbType##CType11 CName11, tdbType##CType12 CName12, tdbType##CType13 CName13, tdbType##CType14 CName14, tdbType##CType15 CName15, tdbType##CType16 CName16, tdbType##CType17 CName17, tdbType##CType18 CName18, tdbType##CType19 CName19, tdbType##CType20 CName20, tdbType##CType21 CName21, tdbType##CType22 CName22, tdbType##CType23 CName23, tdbType##CType24 CName24, tdbType##CType25 CName25, tdbType##CType26 CName26, tdbType##CType27 CName27, tdbType##CType28 CName28, tdbType##CType29 CName29, tdbType##CType30 CName30, tdbType##CType31 CName31, tdbType##CType32 CName32, tdbType##CType33 CName33, tdbType##CType34 CName34, tdbType##CType35 CName35, tdbType##CType36 CName36, tdbType##CType37 CName37, tdbType##CType38 CName38, tdbType##CType39 CName39, tdbType##CType40 CName40, tdbType##CType41 CName41, tdbType##CType42 CName42, tdbType##CType43 CName43, tdbType##CType44 CName44, tdbType##CType45 CName45, tdbType##CType46 CName46) { \
		Insert##CType1 (0, ndx, CName1); \
		Insert##CType2 (1, ndx, CName2); \
		Insert##CType3 (2, ndx, CName3); \
		Insert##CType4 (3, ndx, CName4); \
		Insert##CType5 (4, ndx, CName5); \
		Insert##CType6 (5, ndx, CName6); \
		Insert##CType7 (6, ndx, CName7); \
		Insert##CType8 (7, ndx, CName8); \
		Insert##CType9 (8, ndx, CName9); \
		Insert##CType10 (9, ndx, CName10); \
		Insert##CType11 (10, ndx, CName11); \
		Insert##CType12 (11, ndx, CName12); \
		Insert##CType13 (12, ndx, CName13); \
		Insert##CType14 (13, ndx, CName14); \
		Insert##CType15 (14, ndx, CName15); \
		Insert##CType16 (15, ndx, CName16); \
		Insert##CType17 (16, ndx, CName17); \
		Insert##CType18 (17, ndx, CName18); \
		Insert##CType19 (18, ndx, CName19); \
		Insert##CType20 (19, ndx, CName20); \
		Insert##CType21 (20, ndx, CName21); \
		Insert##CType22 (21, ndx, CName22); \
		Insert##CType23 (22, ndx, CName23); \
		Insert##CType24 (23, ndx, CName24); \
		Insert##CType25 (24, ndx, CName25); \
		Insert##CType26 (25, ndx, CName26); \
		Insert##CType27 (26, ndx, CName27); \
		Insert##CType28 (27, ndx, CName28); \
		Insert##CType29 (28, ndx, CName29); \
		Insert##CType30 (29, ndx, CName30); \
		Insert##CType31 (30, ndx, CName31); \
		Insert##CType32 (31, ndx, CName32); \
		Insert##CType33 (32, ndx, CName33); \
		Insert##CType34 (33, ndx, CName34); \
		Insert##CType35 (34, ndx, CName35); \
		Insert##CType36 (35, ndx, CName36); \
		Insert##CType37 (36, ndx, CName37); \
		Insert##CType38 (37, ndx, CName38); \
		Insert##CType39 (38, ndx, CName39); \
		Insert##CType40 (39, ndx, CName40); \
		Insert##CType41 (40, ndx, CName41); \
		Insert##CType42 (41, ndx, CName42); \
		Insert##CType43 (42, ndx, CName43); \
		Insert##CType44 (43, ndx, CName44); \
		Insert##CType45 (44, ndx, CName45); \
		Insert##CType46 (45, ndx, CName46); \
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
	ColumnProxy##CType3 CName3; \
	ColumnProxy##CType4 CName4; \
	ColumnProxy##CType5 CName5; \
	ColumnProxy##CType6 CName6; \
	ColumnProxy##CType7 CName7; \
	ColumnProxy##CType8 CName8; \
	ColumnProxy##CType9 CName9; \
	ColumnProxy##CType10 CName10; \
	ColumnProxy##CType11 CName11; \
	ColumnProxy##CType12 CName12; \
	ColumnProxy##CType13 CName13; \
	ColumnProxy##CType14 CName14; \
	ColumnProxy##CType15 CName15; \
	ColumnProxy##CType16 CName16; \
	ColumnProxy##CType17 CName17; \
	ColumnProxy##CType18 CName18; \
	ColumnProxy##CType19 CName19; \
	ColumnProxy##CType20 CName20; \
	ColumnProxy##CType21 CName21; \
	ColumnProxy##CType22 CName22; \
	ColumnProxy##CType23 CName23; \
	ColumnProxy##CType24 CName24; \
	ColumnProxy##CType25 CName25; \
	ColumnProxy##CType26 CName26; \
	ColumnProxy##CType27 CName27; \
	ColumnProxy##CType28 CName28; \
	ColumnProxy##CType29 CName29; \
	ColumnProxy##CType30 CName30; \
	ColumnProxy##CType31 CName31; \
	ColumnProxy##CType32 CName32; \
	ColumnProxy##CType33 CName33; \
	ColumnProxy##CType34 CName34; \
	ColumnProxy##CType35 CName35; \
	ColumnProxy##CType36 CName36; \
	ColumnProxy##CType37 CName37; \
	ColumnProxy##CType38 CName38; \
	ColumnProxy##CType39 CName39; \
	ColumnProxy##CType40 CName40; \
	ColumnProxy##CType41 CName41; \
	ColumnProxy##CType42 CName42; \
	ColumnProxy##CType43 CName43; \
	ColumnProxy##CType44 CName44; \
	ColumnProxy##CType45 CName45; \
	ColumnProxy##CType46 CName46; \
\
protected: \
	friend class Group; \
	TableName(Allocator& alloc, size_t ref, Array* parent, size_t pndx) : TopLevelTable(alloc, ref, parent, pndx) {}; \
\
private: \
	TableName(const TableName&) {} \
	TableName& operator=(const TableName&) {return *this;} \
};



#define TDB_TABLE_47(TableName, CType1, CName1, CType2, CName2, CType3, CName3, CType4, CName4, CType5, CName5, CType6, CName6, CType7, CName7, CType8, CName8, CType9, CName9, CType10, CName10, CType11, CName11, CType12, CName12, CType13, CName13, CType14, CName14, CType15, CName15, CType16, CName16, CType17, CName17, CType18, CName18, CType19, CName19, CType20, CName20, CType21, CName21, CType22, CName22, CType23, CName23, CType24, CName24, CType25, CName25, CType26, CName26, CType27, CName27, CType28, CName28, CType29, CName29, CType30, CName30, CType31, CName31, CType32, CName32, CType33, CName33, CType34, CName34, CType35, CName35, CType36, CName36, CType37, CName37, CType38, CName38, CType39, CName39, CType40, CName40, CType41, CName41, CType42, CName42, CType43, CName43, CType44, CName44, CType45, CName45, CType46, CName46, CType47, CName47) \
class TableName##Query { \
protected: \
	QueryAccessor##CType1 CName1; \
	QueryAccessor##CType2 CName2; \
	QueryAccessor##CType3 CName3; \
	QueryAccessor##CType4 CName4; \
	QueryAccessor##CType5 CName5; \
	QueryAccessor##CType6 CName6; \
	QueryAccessor##CType7 CName7; \
	QueryAccessor##CType8 CName8; \
	QueryAccessor##CType9 CName9; \
	QueryAccessor##CType10 CName10; \
	QueryAccessor##CType11 CName11; \
	QueryAccessor##CType12 CName12; \
	QueryAccessor##CType13 CName13; \
	QueryAccessor##CType14 CName14; \
	QueryAccessor##CType15 CName15; \
	QueryAccessor##CType16 CName16; \
	QueryAccessor##CType17 CName17; \
	QueryAccessor##CType18 CName18; \
	QueryAccessor##CType19 CName19; \
	QueryAccessor##CType20 CName20; \
	QueryAccessor##CType21 CName21; \
	QueryAccessor##CType22 CName22; \
	QueryAccessor##CType23 CName23; \
	QueryAccessor##CType24 CName24; \
	QueryAccessor##CType25 CName25; \
	QueryAccessor##CType26 CName26; \
	QueryAccessor##CType27 CName27; \
	QueryAccessor##CType28 CName28; \
	QueryAccessor##CType29 CName29; \
	QueryAccessor##CType30 CName30; \
	QueryAccessor##CType31 CName31; \
	QueryAccessor##CType32 CName32; \
	QueryAccessor##CType33 CName33; \
	QueryAccessor##CType34 CName34; \
	QueryAccessor##CType35 CName35; \
	QueryAccessor##CType36 CName36; \
	QueryAccessor##CType37 CName37; \
	QueryAccessor##CType38 CName38; \
	QueryAccessor##CType39 CName39; \
	QueryAccessor##CType40 CName40; \
	QueryAccessor##CType41 CName41; \
	QueryAccessor##CType42 CName42; \
	QueryAccessor##CType43 CName43; \
	QueryAccessor##CType44 CName44; \
	QueryAccessor##CType45 CName45; \
	QueryAccessor##CType46 CName46; \
	QueryAccessor##CType47 CName47; \
}; \
\
class TableName : public TopLevelTable { \
public: \
	TableName(Allocator& alloc=GetDefaultAllocator()) : TopLevelTable(alloc) { \
		RegisterColumn(Accessor##CType1::type, #CName1); \
		RegisterColumn(Accessor##CType2::type, #CName2); \
		RegisterColumn(Accessor##CType3::type, #CName3); \
		RegisterColumn(Accessor##CType4::type, #CName4); \
		RegisterColumn(Accessor##CType5::type, #CName5); \
		RegisterColumn(Accessor##CType6::type, #CName6); \
		RegisterColumn(Accessor##CType7::type, #CName7); \
		RegisterColumn(Accessor##CType8::type, #CName8); \
		RegisterColumn(Accessor##CType9::type, #CName9); \
		RegisterColumn(Accessor##CType10::type, #CName10); \
		RegisterColumn(Accessor##CType11::type, #CName11); \
		RegisterColumn(Accessor##CType12::type, #CName12); \
		RegisterColumn(Accessor##CType13::type, #CName13); \
		RegisterColumn(Accessor##CType14::type, #CName14); \
		RegisterColumn(Accessor##CType15::type, #CName15); \
		RegisterColumn(Accessor##CType16::type, #CName16); \
		RegisterColumn(Accessor##CType17::type, #CName17); \
		RegisterColumn(Accessor##CType18::type, #CName18); \
		RegisterColumn(Accessor##CType19::type, #CName19); \
		RegisterColumn(Accessor##CType20::type, #CName20); \
		RegisterColumn(Accessor##CType21::type, #CName21); \
		RegisterColumn(Accessor##CType22::type, #CName22); \
		RegisterColumn(Accessor##CType23::type, #CName23); \
		RegisterColumn(Accessor##CType24::type, #CName24); \
		RegisterColumn(Accessor##CType25::type, #CName25); \
		RegisterColumn(Accessor##CType26::type, #CName26); \
		RegisterColumn(Accessor##CType27::type, #CName27); \
		RegisterColumn(Accessor##CType28::type, #CName28); \
		RegisterColumn(Accessor##CType29::type, #CName29); \
		RegisterColumn(Accessor##CType30::type, #CName30); \
		RegisterColumn(Accessor##CType31::type, #CName31); \
		RegisterColumn(Accessor##CType32::type, #CName32); \
		RegisterColumn(Accessor##CType33::type, #CName33); \
		RegisterColumn(Accessor##CType34::type, #CName34); \
		RegisterColumn(Accessor##CType35::type, #CName35); \
		RegisterColumn(Accessor##CType36::type, #CName36); \
		RegisterColumn(Accessor##CType37::type, #CName37); \
		RegisterColumn(Accessor##CType38::type, #CName38); \
		RegisterColumn(Accessor##CType39::type, #CName39); \
		RegisterColumn(Accessor##CType40::type, #CName40); \
		RegisterColumn(Accessor##CType41::type, #CName41); \
		RegisterColumn(Accessor##CType42::type, #CName42); \
		RegisterColumn(Accessor##CType43::type, #CName43); \
		RegisterColumn(Accessor##CType44::type, #CName44); \
		RegisterColumn(Accessor##CType45::type, #CName45); \
		RegisterColumn(Accessor##CType46::type, #CName46); \
		RegisterColumn(Accessor##CType47::type, #CName47); \
\
		CName1.Create(this, 0); \
		CName2.Create(this, 1); \
		CName3.Create(this, 2); \
		CName4.Create(this, 3); \
		CName5.Create(this, 4); \
		CName6.Create(this, 5); \
		CName7.Create(this, 6); \
		CName8.Create(this, 7); \
		CName9.Create(this, 8); \
		CName10.Create(this, 9); \
		CName11.Create(this, 10); \
		CName12.Create(this, 11); \
		CName13.Create(this, 12); \
		CName14.Create(this, 13); \
		CName15.Create(this, 14); \
		CName16.Create(this, 15); \
		CName17.Create(this, 16); \
		CName18.Create(this, 17); \
		CName19.Create(this, 18); \
		CName20.Create(this, 19); \
		CName21.Create(this, 20); \
		CName22.Create(this, 21); \
		CName23.Create(this, 22); \
		CName24.Create(this, 23); \
		CName25.Create(this, 24); \
		CName26.Create(this, 25); \
		CName27.Create(this, 26); \
		CName28.Create(this, 27); \
		CName29.Create(this, 28); \
		CName30.Create(this, 29); \
		CName31.Create(this, 30); \
		CName32.Create(this, 31); \
		CName33.Create(this, 32); \
		CName34.Create(this, 33); \
		CName35.Create(this, 34); \
		CName36.Create(this, 35); \
		CName37.Create(this, 36); \
		CName38.Create(this, 37); \
		CName39.Create(this, 38); \
		CName40.Create(this, 39); \
		CName41.Create(this, 40); \
		CName42.Create(this, 41); \
		CName43.Create(this, 42); \
		CName44.Create(this, 43); \
		CName45.Create(this, 44); \
		CName46.Create(this, 45); \
		CName47.Create(this, 46); \
	}; \
\
	class TestQuery : public Query { \
	public: \
		TestQuery() : CName1(0), CName2(1), CName3(2), CName4(3), CName5(4), CName6(5), CName7(6), CName8(7), CName9(8), CName10(9), CName11(10), CName12(11), CName13(12), CName14(13), CName15(14), CName16(15), CName17(16), CName18(17), CName19(18), CName20(19), CName21(20), CName22(21), CName23(22), CName24(23), CName25(24), CName26(25), CName27(26), CName28(27), CName29(28), CName30(29), CName31(30), CName32(31), CName33(32), CName34(33), CName35(34), CName36(35), CName37(36), CName38(37), CName39(38), CName40(39), CName41(40), CName42(41), CName43(42), CName44(43), CName45(44), CName46(45), CName47(46) { \
			CName1.SetQuery(this); \
			CName2.SetQuery(this); \
			CName3.SetQuery(this); \
			CName4.SetQuery(this); \
			CName5.SetQuery(this); \
			CName6.SetQuery(this); \
			CName7.SetQuery(this); \
			CName8.SetQuery(this); \
			CName9.SetQuery(this); \
			CName10.SetQuery(this); \
			CName11.SetQuery(this); \
			CName12.SetQuery(this); \
			CName13.SetQuery(this); \
			CName14.SetQuery(this); \
			CName15.SetQuery(this); \
			CName16.SetQuery(this); \
			CName17.SetQuery(this); \
			CName18.SetQuery(this); \
			CName19.SetQuery(this); \
			CName20.SetQuery(this); \
			CName21.SetQuery(this); \
			CName22.SetQuery(this); \
			CName23.SetQuery(this); \
			CName24.SetQuery(this); \
			CName25.SetQuery(this); \
			CName26.SetQuery(this); \
			CName27.SetQuery(this); \
			CName28.SetQuery(this); \
			CName29.SetQuery(this); \
			CName30.SetQuery(this); \
			CName31.SetQuery(this); \
			CName32.SetQuery(this); \
			CName33.SetQuery(this); \
			CName34.SetQuery(this); \
			CName35.SetQuery(this); \
			CName36.SetQuery(this); \
			CName37.SetQuery(this); \
			CName38.SetQuery(this); \
			CName39.SetQuery(this); \
			CName40.SetQuery(this); \
			CName41.SetQuery(this); \
			CName42.SetQuery(this); \
			CName43.SetQuery(this); \
			CName44.SetQuery(this); \
			CName45.SetQuery(this); \
			CName46.SetQuery(this); \
			CName47.SetQuery(this); \
		} \
\
		TestQuery(const TestQuery& copy) : Query(copy), CName1(0), CName2(1), CName3(2), CName4(3), CName5(4), CName6(5), CName7(6), CName8(7), CName9(8), CName10(9), CName11(10), CName12(11), CName13(12), CName14(13), CName15(14), CName16(15), CName17(16), CName18(17), CName19(18), CName20(19), CName21(20), CName22(21), CName23(22), CName24(23), CName25(24), CName26(25), CName27(26), CName28(27), CName29(28), CName30(29), CName31(30), CName32(31), CName33(32), CName34(33), CName35(34), CName36(35), CName37(36), CName38(37), CName39(38), CName40(39), CName41(40), CName42(41), CName43(42), CName44(43), CName45(44), CName46(45), CName47(46) { \
			CName1.SetQuery(this); \
			CName2.SetQuery(this); \
			CName3.SetQuery(this); \
			CName4.SetQuery(this); \
			CName5.SetQuery(this); \
			CName6.SetQuery(this); \
			CName7.SetQuery(this); \
			CName8.SetQuery(this); \
			CName9.SetQuery(this); \
			CName10.SetQuery(this); \
			CName11.SetQuery(this); \
			CName12.SetQuery(this); \
			CName13.SetQuery(this); \
			CName14.SetQuery(this); \
			CName15.SetQuery(this); \
			CName16.SetQuery(this); \
			CName17.SetQuery(this); \
			CName18.SetQuery(this); \
			CName19.SetQuery(this); \
			CName20.SetQuery(this); \
			CName21.SetQuery(this); \
			CName22.SetQuery(this); \
			CName23.SetQuery(this); \
			CName24.SetQuery(this); \
			CName25.SetQuery(this); \
			CName26.SetQuery(this); \
			CName27.SetQuery(this); \
			CName28.SetQuery(this); \
			CName29.SetQuery(this); \
			CName30.SetQuery(this); \
			CName31.SetQuery(this); \
			CName32.SetQuery(this); \
			CName33.SetQuery(this); \
			CName34.SetQuery(this); \
			CName35.SetQuery(this); \
			CName36.SetQuery(this); \
			CName37.SetQuery(this); \
			CName38.SetQuery(this); \
			CName39.SetQuery(this); \
			CName40.SetQuery(this); \
			CName41.SetQuery(this); \
			CName42.SetQuery(this); \
			CName43.SetQuery(this); \
			CName44.SetQuery(this); \
			CName45.SetQuery(this); \
			CName46.SetQuery(this); \
			CName47.SetQuery(this); \
		} \
\
		class TestQueryQueryAccessorInt : private XQueryAccessorInt { \
		public: \
			TestQueryQueryAccessorInt(size_t column_id) : XQueryAccessorInt(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(int64_t value) {return (TestQuery &)XQueryAccessorInt::Equal(value);} \
			TestQuery& NotEqual(int64_t value) {return (TestQuery &)XQueryAccessorInt::NotEqual(value);} \
			TestQuery& Greater(int64_t value) {return (TestQuery &)XQueryAccessorInt::Greater(value);} \
			TestQuery& Less(int64_t value) {return (TestQuery &)XQueryAccessorInt::Less(value);} \
			TestQuery& Between(int64_t from, int64_t to) {return (TestQuery &)XQueryAccessorInt::Between(from, to);} \
		}; \
\
		template <class T> class TestQueryQueryAccessorEnum : public TestQueryQueryAccessorInt { \
		public: \
			TestQueryQueryAccessorEnum<T>(size_t column_id) : TestQueryQueryAccessorInt(column_id) {} \
		}; \
\
		class TestQueryQueryAccessorString : private XQueryAccessorString { \
		public: \
			TestQueryQueryAccessorString(size_t column_id) : XQueryAccessorString(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::Equal(value, CaseSensitive);} \
			TestQuery& NotEqual(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::NotEqual(value, CaseSensitive);} \
			TestQuery& BeginsWith(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::BeginsWith(value, CaseSensitive);} \
			TestQuery& EndsWith(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::EndsWith(value, CaseSensitive);} \
			TestQuery& Contains(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::Contains(value, CaseSensitive);} \
		}; \
\
		class TestQueryQueryAccessorBool : private XQueryAccessorBool { \
		public: \
			TestQueryQueryAccessorBool(size_t column_id) : XQueryAccessorBool(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(bool value) {return (TestQuery &)XQueryAccessorBool::Equal(value);} \
		}; \
\
		TestQueryQueryAccessor##CType1 CName1; \
		TestQueryQueryAccessor##CType2 CName2; \
		TestQueryQueryAccessor##CType3 CName3; \
		TestQueryQueryAccessor##CType4 CName4; \
		TestQueryQueryAccessor##CType5 CName5; \
		TestQueryQueryAccessor##CType6 CName6; \
		TestQueryQueryAccessor##CType7 CName7; \
		TestQueryQueryAccessor##CType8 CName8; \
		TestQueryQueryAccessor##CType9 CName9; \
		TestQueryQueryAccessor##CType10 CName10; \
		TestQueryQueryAccessor##CType11 CName11; \
		TestQueryQueryAccessor##CType12 CName12; \
		TestQueryQueryAccessor##CType13 CName13; \
		TestQueryQueryAccessor##CType14 CName14; \
		TestQueryQueryAccessor##CType15 CName15; \
		TestQueryQueryAccessor##CType16 CName16; \
		TestQueryQueryAccessor##CType17 CName17; \
		TestQueryQueryAccessor##CType18 CName18; \
		TestQueryQueryAccessor##CType19 CName19; \
		TestQueryQueryAccessor##CType20 CName20; \
		TestQueryQueryAccessor##CType21 CName21; \
		TestQueryQueryAccessor##CType22 CName22; \
		TestQueryQueryAccessor##CType23 CName23; \
		TestQueryQueryAccessor##CType24 CName24; \
		TestQueryQueryAccessor##CType25 CName25; \
		TestQueryQueryAccessor##CType26 CName26; \
		TestQueryQueryAccessor##CType27 CName27; \
		TestQueryQueryAccessor##CType28 CName28; \
		TestQueryQueryAccessor##CType29 CName29; \
		TestQueryQueryAccessor##CType30 CName30; \
		TestQueryQueryAccessor##CType31 CName31; \
		TestQueryQueryAccessor##CType32 CName32; \
		TestQueryQueryAccessor##CType33 CName33; \
		TestQueryQueryAccessor##CType34 CName34; \
		TestQueryQueryAccessor##CType35 CName35; \
		TestQueryQueryAccessor##CType36 CName36; \
		TestQueryQueryAccessor##CType37 CName37; \
		TestQueryQueryAccessor##CType38 CName38; \
		TestQueryQueryAccessor##CType39 CName39; \
		TestQueryQueryAccessor##CType40 CName40; \
		TestQueryQueryAccessor##CType41 CName41; \
		TestQueryQueryAccessor##CType42 CName42; \
		TestQueryQueryAccessor##CType43 CName43; \
		TestQueryQueryAccessor##CType44 CName44; \
		TestQueryQueryAccessor##CType45 CName45; \
		TestQueryQueryAccessor##CType46 CName46; \
		TestQueryQueryAccessor##CType47 CName47; \
\
		TestQuery& LeftParan(void) {Query::LeftParan(); return *this;}; \
		TestQuery& Or(void) {Query::Or(); return *this;}; \
		TestQuery& RightParan(void) {Query::RightParan(); return *this;}; \
		TestQuery& Subtable(size_t column) {Query::Subtable(column); return *this;}; \
		TestQuery& Parent() {Query::Parent(); return *this;}; \
	}; \
\
	TestQuery GetQuery() {return TestQuery();} \
\
	class Cursor : public CursorBase { \
	public: \
		Cursor(TableName& table, size_t ndx) : CursorBase(table, ndx) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
			CName17.Create(this, 16); \
			CName18.Create(this, 17); \
			CName19.Create(this, 18); \
			CName20.Create(this, 19); \
			CName21.Create(this, 20); \
			CName22.Create(this, 21); \
			CName23.Create(this, 22); \
			CName24.Create(this, 23); \
			CName25.Create(this, 24); \
			CName26.Create(this, 25); \
			CName27.Create(this, 26); \
			CName28.Create(this, 27); \
			CName29.Create(this, 28); \
			CName30.Create(this, 29); \
			CName31.Create(this, 30); \
			CName32.Create(this, 31); \
			CName33.Create(this, 32); \
			CName34.Create(this, 33); \
			CName35.Create(this, 34); \
			CName36.Create(this, 35); \
			CName37.Create(this, 36); \
			CName38.Create(this, 37); \
			CName39.Create(this, 38); \
			CName40.Create(this, 39); \
			CName41.Create(this, 40); \
			CName42.Create(this, 41); \
			CName43.Create(this, 42); \
			CName44.Create(this, 43); \
			CName45.Create(this, 44); \
			CName46.Create(this, 45); \
			CName47.Create(this, 46); \
		} \
		Cursor(const TableName& table, size_t ndx) : CursorBase(const_cast<TableName&>(table), ndx) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
			CName17.Create(this, 16); \
			CName18.Create(this, 17); \
			CName19.Create(this, 18); \
			CName20.Create(this, 19); \
			CName21.Create(this, 20); \
			CName22.Create(this, 21); \
			CName23.Create(this, 22); \
			CName24.Create(this, 23); \
			CName25.Create(this, 24); \
			CName26.Create(this, 25); \
			CName27.Create(this, 26); \
			CName28.Create(this, 27); \
			CName29.Create(this, 28); \
			CName30.Create(this, 29); \
			CName31.Create(this, 30); \
			CName32.Create(this, 31); \
			CName33.Create(this, 32); \
			CName34.Create(this, 33); \
			CName35.Create(this, 34); \
			CName36.Create(this, 35); \
			CName37.Create(this, 36); \
			CName38.Create(this, 37); \
			CName39.Create(this, 38); \
			CName40.Create(this, 39); \
			CName41.Create(this, 40); \
			CName42.Create(this, 41); \
			CName43.Create(this, 42); \
			CName44.Create(this, 43); \
			CName45.Create(this, 44); \
			CName46.Create(this, 45); \
			CName47.Create(this, 46); \
		} \
		Cursor(const Cursor& v) : CursorBase(v) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
			CName17.Create(this, 16); \
			CName18.Create(this, 17); \
			CName19.Create(this, 18); \
			CName20.Create(this, 19); \
			CName21.Create(this, 20); \
			CName22.Create(this, 21); \
			CName23.Create(this, 22); \
			CName24.Create(this, 23); \
			CName25.Create(this, 24); \
			CName26.Create(this, 25); \
			CName27.Create(this, 26); \
			CName28.Create(this, 27); \
			CName29.Create(this, 28); \
			CName30.Create(this, 29); \
			CName31.Create(this, 30); \
			CName32.Create(this, 31); \
			CName33.Create(this, 32); \
			CName34.Create(this, 33); \
			CName35.Create(this, 34); \
			CName36.Create(this, 35); \
			CName37.Create(this, 36); \
			CName38.Create(this, 37); \
			CName39.Create(this, 38); \
			CName40.Create(this, 39); \
			CName41.Create(this, 40); \
			CName42.Create(this, 41); \
			CName43.Create(this, 42); \
			CName44.Create(this, 43); \
			CName45.Create(this, 44); \
			CName46.Create(this, 45); \
			CName47.Create(this, 46); \
		} \
		Accessor##CType1 CName1; \
		Accessor##CType2 CName2; \
		Accessor##CType3 CName3; \
		Accessor##CType4 CName4; \
		Accessor##CType5 CName5; \
		Accessor##CType6 CName6; \
		Accessor##CType7 CName7; \
		Accessor##CType8 CName8; \
		Accessor##CType9 CName9; \
		Accessor##CType10 CName10; \
		Accessor##CType11 CName11; \
		Accessor##CType12 CName12; \
		Accessor##CType13 CName13; \
		Accessor##CType14 CName14; \
		Accessor##CType15 CName15; \
		Accessor##CType16 CName16; \
		Accessor##CType17 CName17; \
		Accessor##CType18 CName18; \
		Accessor##CType19 CName19; \
		Accessor##CType20 CName20; \
		Accessor##CType21 CName21; \
		Accessor##CType22 CName22; \
		Accessor##CType23 CName23; \
		Accessor##CType24 CName24; \
		Accessor##CType25 CName25; \
		Accessor##CType26 CName26; \
		Accessor##CType27 CName27; \
		Accessor##CType28 CName28; \
		Accessor##CType29 CName29; \
		Accessor##CType30 CName30; \
		Accessor##CType31 CName31; \
		Accessor##CType32 CName32; \
		Accessor##CType33 CName33; \
		Accessor##CType34 CName34; \
		Accessor##CType35 CName35; \
		Accessor##CType36 CName36; \
		Accessor##CType37 CName37; \
		Accessor##CType38 CName38; \
		Accessor##CType39 CName39; \
		Accessor##CType40 CName40; \
		Accessor##CType41 CName41; \
		Accessor##CType42 CName42; \
		Accessor##CType43 CName43; \
		Accessor##CType44 CName44; \
		Accessor##CType45 CName45; \
		Accessor##CType46 CName46; \
		Accessor##CType47 CName47; \
	}; \
\
	void Add(tdbType##CType1 CName1, tdbType##CType2 CName2, tdbType##CType3 CName3, tdbType##CType4 CName4, tdbType##CType5 CName5, tdbType##CType6 CName6, tdbType##CType7 CName7, tdbType##CType8 CName8, tdbType##CType9 CName9, tdbType##CType10 CName10, tdbType##CType11 CName11, tdbType##CType12 CName12, tdbType##CType13 CName13, tdbType##CType14 CName14, tdbType##CType15 CName15, tdbType##CType16 CName16, tdbType##CType17 CName17, tdbType##CType18 CName18, tdbType##CType19 CName19, tdbType##CType20 CName20, tdbType##CType21 CName21, tdbType##CType22 CName22, tdbType##CType23 CName23, tdbType##CType24 CName24, tdbType##CType25 CName25, tdbType##CType26 CName26, tdbType##CType27 CName27, tdbType##CType28 CName28, tdbType##CType29 CName29, tdbType##CType30 CName30, tdbType##CType31 CName31, tdbType##CType32 CName32, tdbType##CType33 CName33, tdbType##CType34 CName34, tdbType##CType35 CName35, tdbType##CType36 CName36, tdbType##CType37 CName37, tdbType##CType38 CName38, tdbType##CType39 CName39, tdbType##CType40 CName40, tdbType##CType41 CName41, tdbType##CType42 CName42, tdbType##CType43 CName43, tdbType##CType44 CName44, tdbType##CType45 CName45, tdbType##CType46 CName46, tdbType##CType47 CName47) { \
		const size_t ndx = GetSize(); \
		Insert##CType1 (0, ndx, CName1); \
		Insert##CType2 (1, ndx, CName2); \
		Insert##CType3 (2, ndx, CName3); \
		Insert##CType4 (3, ndx, CName4); \
		Insert##CType5 (4, ndx, CName5); \
		Insert##CType6 (5, ndx, CName6); \
		Insert##CType7 (6, ndx, CName7); \
		Insert##CType8 (7, ndx, CName8); \
		Insert##CType9 (8, ndx, CName9); \
		Insert##CType10 (9, ndx, CName10); \
		Insert##CType11 (10, ndx, CName11); \
		Insert##CType12 (11, ndx, CName12); \
		Insert##CType13 (12, ndx, CName13); \
		Insert##CType14 (13, ndx, CName14); \
		Insert##CType15 (14, ndx, CName15); \
		Insert##CType16 (15, ndx, CName16); \
		Insert##CType17 (16, ndx, CName17); \
		Insert##CType18 (17, ndx, CName18); \
		Insert##CType19 (18, ndx, CName19); \
		Insert##CType20 (19, ndx, CName20); \
		Insert##CType21 (20, ndx, CName21); \
		Insert##CType22 (21, ndx, CName22); \
		Insert##CType23 (22, ndx, CName23); \
		Insert##CType24 (23, ndx, CName24); \
		Insert##CType25 (24, ndx, CName25); \
		Insert##CType26 (25, ndx, CName26); \
		Insert##CType27 (26, ndx, CName27); \
		Insert##CType28 (27, ndx, CName28); \
		Insert##CType29 (28, ndx, CName29); \
		Insert##CType30 (29, ndx, CName30); \
		Insert##CType31 (30, ndx, CName31); \
		Insert##CType32 (31, ndx, CName32); \
		Insert##CType33 (32, ndx, CName33); \
		Insert##CType34 (33, ndx, CName34); \
		Insert##CType35 (34, ndx, CName35); \
		Insert##CType36 (35, ndx, CName36); \
		Insert##CType37 (36, ndx, CName37); \
		Insert##CType38 (37, ndx, CName38); \
		Insert##CType39 (38, ndx, CName39); \
		Insert##CType40 (39, ndx, CName40); \
		Insert##CType41 (40, ndx, CName41); \
		Insert##CType42 (41, ndx, CName42); \
		Insert##CType43 (42, ndx, CName43); \
		Insert##CType44 (43, ndx, CName44); \
		Insert##CType45 (44, ndx, CName45); \
		Insert##CType46 (45, ndx, CName46); \
		Insert##CType47 (46, ndx, CName47); \
		InsertDone(); \
	} \
\
	void Insert(size_t ndx, tdbType##CType1 CName1, tdbType##CType2 CName2, tdbType##CType3 CName3, tdbType##CType4 CName4, tdbType##CType5 CName5, tdbType##CType6 CName6, tdbType##CType7 CName7, tdbType##CType8 CName8, tdbType##CType9 CName9, tdbType##CType10 CName10, tdbType##CType11 CName11, tdbType##CType12 CName12, tdbType##CType13 CName13, tdbType##CType14 CName14, tdbType##CType15 CName15, tdbType##CType16 CName16, tdbType##CType17 CName17, tdbType##CType18 CName18, tdbType##CType19 CName19, tdbType##CType20 CName20, tdbType##CType21 CName21, tdbType##CType22 CName22, tdbType##CType23 CName23, tdbType##CType24 CName24, tdbType##CType25 CName25, tdbType##CType26 CName26, tdbType##CType27 CName27, tdbType##CType28 CName28, tdbType##CType29 CName29, tdbType##CType30 CName30, tdbType##CType31 CName31, tdbType##CType32 CName32, tdbType##CType33 CName33, tdbType##CType34 CName34, tdbType##CType35 CName35, tdbType##CType36 CName36, tdbType##CType37 CName37, tdbType##CType38 CName38, tdbType##CType39 CName39, tdbType##CType40 CName40, tdbType##CType41 CName41, tdbType##CType42 CName42, tdbType##CType43 CName43, tdbType##CType44 CName44, tdbType##CType45 CName45, tdbType##CType46 CName46, tdbType##CType47 CName47) { \
		Insert##CType1 (0, ndx, CName1); \
		Insert##CType2 (1, ndx, CName2); \
		Insert##CType3 (2, ndx, CName3); \
		Insert##CType4 (3, ndx, CName4); \
		Insert##CType5 (4, ndx, CName5); \
		Insert##CType6 (5, ndx, CName6); \
		Insert##CType7 (6, ndx, CName7); \
		Insert##CType8 (7, ndx, CName8); \
		Insert##CType9 (8, ndx, CName9); \
		Insert##CType10 (9, ndx, CName10); \
		Insert##CType11 (10, ndx, CName11); \
		Insert##CType12 (11, ndx, CName12); \
		Insert##CType13 (12, ndx, CName13); \
		Insert##CType14 (13, ndx, CName14); \
		Insert##CType15 (14, ndx, CName15); \
		Insert##CType16 (15, ndx, CName16); \
		Insert##CType17 (16, ndx, CName17); \
		Insert##CType18 (17, ndx, CName18); \
		Insert##CType19 (18, ndx, CName19); \
		Insert##CType20 (19, ndx, CName20); \
		Insert##CType21 (20, ndx, CName21); \
		Insert##CType22 (21, ndx, CName22); \
		Insert##CType23 (22, ndx, CName23); \
		Insert##CType24 (23, ndx, CName24); \
		Insert##CType25 (24, ndx, CName25); \
		Insert##CType26 (25, ndx, CName26); \
		Insert##CType27 (26, ndx, CName27); \
		Insert##CType28 (27, ndx, CName28); \
		Insert##CType29 (28, ndx, CName29); \
		Insert##CType30 (29, ndx, CName30); \
		Insert##CType31 (30, ndx, CName31); \
		Insert##CType32 (31, ndx, CName32); \
		Insert##CType33 (32, ndx, CName33); \
		Insert##CType34 (33, ndx, CName34); \
		Insert##CType35 (34, ndx, CName35); \
		Insert##CType36 (35, ndx, CName36); \
		Insert##CType37 (36, ndx, CName37); \
		Insert##CType38 (37, ndx, CName38); \
		Insert##CType39 (38, ndx, CName39); \
		Insert##CType40 (39, ndx, CName40); \
		Insert##CType41 (40, ndx, CName41); \
		Insert##CType42 (41, ndx, CName42); \
		Insert##CType43 (42, ndx, CName43); \
		Insert##CType44 (43, ndx, CName44); \
		Insert##CType45 (44, ndx, CName45); \
		Insert##CType46 (45, ndx, CName46); \
		Insert##CType47 (46, ndx, CName47); \
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
	ColumnProxy##CType3 CName3; \
	ColumnProxy##CType4 CName4; \
	ColumnProxy##CType5 CName5; \
	ColumnProxy##CType6 CName6; \
	ColumnProxy##CType7 CName7; \
	ColumnProxy##CType8 CName8; \
	ColumnProxy##CType9 CName9; \
	ColumnProxy##CType10 CName10; \
	ColumnProxy##CType11 CName11; \
	ColumnProxy##CType12 CName12; \
	ColumnProxy##CType13 CName13; \
	ColumnProxy##CType14 CName14; \
	ColumnProxy##CType15 CName15; \
	ColumnProxy##CType16 CName16; \
	ColumnProxy##CType17 CName17; \
	ColumnProxy##CType18 CName18; \
	ColumnProxy##CType19 CName19; \
	ColumnProxy##CType20 CName20; \
	ColumnProxy##CType21 CName21; \
	ColumnProxy##CType22 CName22; \
	ColumnProxy##CType23 CName23; \
	ColumnProxy##CType24 CName24; \
	ColumnProxy##CType25 CName25; \
	ColumnProxy##CType26 CName26; \
	ColumnProxy##CType27 CName27; \
	ColumnProxy##CType28 CName28; \
	ColumnProxy##CType29 CName29; \
	ColumnProxy##CType30 CName30; \
	ColumnProxy##CType31 CName31; \
	ColumnProxy##CType32 CName32; \
	ColumnProxy##CType33 CName33; \
	ColumnProxy##CType34 CName34; \
	ColumnProxy##CType35 CName35; \
	ColumnProxy##CType36 CName36; \
	ColumnProxy##CType37 CName37; \
	ColumnProxy##CType38 CName38; \
	ColumnProxy##CType39 CName39; \
	ColumnProxy##CType40 CName40; \
	ColumnProxy##CType41 CName41; \
	ColumnProxy##CType42 CName42; \
	ColumnProxy##CType43 CName43; \
	ColumnProxy##CType44 CName44; \
	ColumnProxy##CType45 CName45; \
	ColumnProxy##CType46 CName46; \
	ColumnProxy##CType47 CName47; \
\
protected: \
	friend class Group; \
	TableName(Allocator& alloc, size_t ref, Array* parent, size_t pndx) : TopLevelTable(alloc, ref, parent, pndx) {}; \
\
private: \
	TableName(const TableName&) {} \
	TableName& operator=(const TableName&) {return *this;} \
};



#define TDB_TABLE_48(TableName, CType1, CName1, CType2, CName2, CType3, CName3, CType4, CName4, CType5, CName5, CType6, CName6, CType7, CName7, CType8, CName8, CType9, CName9, CType10, CName10, CType11, CName11, CType12, CName12, CType13, CName13, CType14, CName14, CType15, CName15, CType16, CName16, CType17, CName17, CType18, CName18, CType19, CName19, CType20, CName20, CType21, CName21, CType22, CName22, CType23, CName23, CType24, CName24, CType25, CName25, CType26, CName26, CType27, CName27, CType28, CName28, CType29, CName29, CType30, CName30, CType31, CName31, CType32, CName32, CType33, CName33, CType34, CName34, CType35, CName35, CType36, CName36, CType37, CName37, CType38, CName38, CType39, CName39, CType40, CName40, CType41, CName41, CType42, CName42, CType43, CName43, CType44, CName44, CType45, CName45, CType46, CName46, CType47, CName47, CType48, CName48) \
class TableName##Query { \
protected: \
	QueryAccessor##CType1 CName1; \
	QueryAccessor##CType2 CName2; \
	QueryAccessor##CType3 CName3; \
	QueryAccessor##CType4 CName4; \
	QueryAccessor##CType5 CName5; \
	QueryAccessor##CType6 CName6; \
	QueryAccessor##CType7 CName7; \
	QueryAccessor##CType8 CName8; \
	QueryAccessor##CType9 CName9; \
	QueryAccessor##CType10 CName10; \
	QueryAccessor##CType11 CName11; \
	QueryAccessor##CType12 CName12; \
	QueryAccessor##CType13 CName13; \
	QueryAccessor##CType14 CName14; \
	QueryAccessor##CType15 CName15; \
	QueryAccessor##CType16 CName16; \
	QueryAccessor##CType17 CName17; \
	QueryAccessor##CType18 CName18; \
	QueryAccessor##CType19 CName19; \
	QueryAccessor##CType20 CName20; \
	QueryAccessor##CType21 CName21; \
	QueryAccessor##CType22 CName22; \
	QueryAccessor##CType23 CName23; \
	QueryAccessor##CType24 CName24; \
	QueryAccessor##CType25 CName25; \
	QueryAccessor##CType26 CName26; \
	QueryAccessor##CType27 CName27; \
	QueryAccessor##CType28 CName28; \
	QueryAccessor##CType29 CName29; \
	QueryAccessor##CType30 CName30; \
	QueryAccessor##CType31 CName31; \
	QueryAccessor##CType32 CName32; \
	QueryAccessor##CType33 CName33; \
	QueryAccessor##CType34 CName34; \
	QueryAccessor##CType35 CName35; \
	QueryAccessor##CType36 CName36; \
	QueryAccessor##CType37 CName37; \
	QueryAccessor##CType38 CName38; \
	QueryAccessor##CType39 CName39; \
	QueryAccessor##CType40 CName40; \
	QueryAccessor##CType41 CName41; \
	QueryAccessor##CType42 CName42; \
	QueryAccessor##CType43 CName43; \
	QueryAccessor##CType44 CName44; \
	QueryAccessor##CType45 CName45; \
	QueryAccessor##CType46 CName46; \
	QueryAccessor##CType47 CName47; \
	QueryAccessor##CType48 CName48; \
}; \
\
class TableName : public TopLevelTable { \
public: \
	TableName(Allocator& alloc=GetDefaultAllocator()) : TopLevelTable(alloc) { \
		RegisterColumn(Accessor##CType1::type, #CName1); \
		RegisterColumn(Accessor##CType2::type, #CName2); \
		RegisterColumn(Accessor##CType3::type, #CName3); \
		RegisterColumn(Accessor##CType4::type, #CName4); \
		RegisterColumn(Accessor##CType5::type, #CName5); \
		RegisterColumn(Accessor##CType6::type, #CName6); \
		RegisterColumn(Accessor##CType7::type, #CName7); \
		RegisterColumn(Accessor##CType8::type, #CName8); \
		RegisterColumn(Accessor##CType9::type, #CName9); \
		RegisterColumn(Accessor##CType10::type, #CName10); \
		RegisterColumn(Accessor##CType11::type, #CName11); \
		RegisterColumn(Accessor##CType12::type, #CName12); \
		RegisterColumn(Accessor##CType13::type, #CName13); \
		RegisterColumn(Accessor##CType14::type, #CName14); \
		RegisterColumn(Accessor##CType15::type, #CName15); \
		RegisterColumn(Accessor##CType16::type, #CName16); \
		RegisterColumn(Accessor##CType17::type, #CName17); \
		RegisterColumn(Accessor##CType18::type, #CName18); \
		RegisterColumn(Accessor##CType19::type, #CName19); \
		RegisterColumn(Accessor##CType20::type, #CName20); \
		RegisterColumn(Accessor##CType21::type, #CName21); \
		RegisterColumn(Accessor##CType22::type, #CName22); \
		RegisterColumn(Accessor##CType23::type, #CName23); \
		RegisterColumn(Accessor##CType24::type, #CName24); \
		RegisterColumn(Accessor##CType25::type, #CName25); \
		RegisterColumn(Accessor##CType26::type, #CName26); \
		RegisterColumn(Accessor##CType27::type, #CName27); \
		RegisterColumn(Accessor##CType28::type, #CName28); \
		RegisterColumn(Accessor##CType29::type, #CName29); \
		RegisterColumn(Accessor##CType30::type, #CName30); \
		RegisterColumn(Accessor##CType31::type, #CName31); \
		RegisterColumn(Accessor##CType32::type, #CName32); \
		RegisterColumn(Accessor##CType33::type, #CName33); \
		RegisterColumn(Accessor##CType34::type, #CName34); \
		RegisterColumn(Accessor##CType35::type, #CName35); \
		RegisterColumn(Accessor##CType36::type, #CName36); \
		RegisterColumn(Accessor##CType37::type, #CName37); \
		RegisterColumn(Accessor##CType38::type, #CName38); \
		RegisterColumn(Accessor##CType39::type, #CName39); \
		RegisterColumn(Accessor##CType40::type, #CName40); \
		RegisterColumn(Accessor##CType41::type, #CName41); \
		RegisterColumn(Accessor##CType42::type, #CName42); \
		RegisterColumn(Accessor##CType43::type, #CName43); \
		RegisterColumn(Accessor##CType44::type, #CName44); \
		RegisterColumn(Accessor##CType45::type, #CName45); \
		RegisterColumn(Accessor##CType46::type, #CName46); \
		RegisterColumn(Accessor##CType47::type, #CName47); \
		RegisterColumn(Accessor##CType48::type, #CName48); \
\
		CName1.Create(this, 0); \
		CName2.Create(this, 1); \
		CName3.Create(this, 2); \
		CName4.Create(this, 3); \
		CName5.Create(this, 4); \
		CName6.Create(this, 5); \
		CName7.Create(this, 6); \
		CName8.Create(this, 7); \
		CName9.Create(this, 8); \
		CName10.Create(this, 9); \
		CName11.Create(this, 10); \
		CName12.Create(this, 11); \
		CName13.Create(this, 12); \
		CName14.Create(this, 13); \
		CName15.Create(this, 14); \
		CName16.Create(this, 15); \
		CName17.Create(this, 16); \
		CName18.Create(this, 17); \
		CName19.Create(this, 18); \
		CName20.Create(this, 19); \
		CName21.Create(this, 20); \
		CName22.Create(this, 21); \
		CName23.Create(this, 22); \
		CName24.Create(this, 23); \
		CName25.Create(this, 24); \
		CName26.Create(this, 25); \
		CName27.Create(this, 26); \
		CName28.Create(this, 27); \
		CName29.Create(this, 28); \
		CName30.Create(this, 29); \
		CName31.Create(this, 30); \
		CName32.Create(this, 31); \
		CName33.Create(this, 32); \
		CName34.Create(this, 33); \
		CName35.Create(this, 34); \
		CName36.Create(this, 35); \
		CName37.Create(this, 36); \
		CName38.Create(this, 37); \
		CName39.Create(this, 38); \
		CName40.Create(this, 39); \
		CName41.Create(this, 40); \
		CName42.Create(this, 41); \
		CName43.Create(this, 42); \
		CName44.Create(this, 43); \
		CName45.Create(this, 44); \
		CName46.Create(this, 45); \
		CName47.Create(this, 46); \
		CName48.Create(this, 47); \
	}; \
\
	class TestQuery : public Query { \
	public: \
		TestQuery() : CName1(0), CName2(1), CName3(2), CName4(3), CName5(4), CName6(5), CName7(6), CName8(7), CName9(8), CName10(9), CName11(10), CName12(11), CName13(12), CName14(13), CName15(14), CName16(15), CName17(16), CName18(17), CName19(18), CName20(19), CName21(20), CName22(21), CName23(22), CName24(23), CName25(24), CName26(25), CName27(26), CName28(27), CName29(28), CName30(29), CName31(30), CName32(31), CName33(32), CName34(33), CName35(34), CName36(35), CName37(36), CName38(37), CName39(38), CName40(39), CName41(40), CName42(41), CName43(42), CName44(43), CName45(44), CName46(45), CName47(46), CName48(47) { \
			CName1.SetQuery(this); \
			CName2.SetQuery(this); \
			CName3.SetQuery(this); \
			CName4.SetQuery(this); \
			CName5.SetQuery(this); \
			CName6.SetQuery(this); \
			CName7.SetQuery(this); \
			CName8.SetQuery(this); \
			CName9.SetQuery(this); \
			CName10.SetQuery(this); \
			CName11.SetQuery(this); \
			CName12.SetQuery(this); \
			CName13.SetQuery(this); \
			CName14.SetQuery(this); \
			CName15.SetQuery(this); \
			CName16.SetQuery(this); \
			CName17.SetQuery(this); \
			CName18.SetQuery(this); \
			CName19.SetQuery(this); \
			CName20.SetQuery(this); \
			CName21.SetQuery(this); \
			CName22.SetQuery(this); \
			CName23.SetQuery(this); \
			CName24.SetQuery(this); \
			CName25.SetQuery(this); \
			CName26.SetQuery(this); \
			CName27.SetQuery(this); \
			CName28.SetQuery(this); \
			CName29.SetQuery(this); \
			CName30.SetQuery(this); \
			CName31.SetQuery(this); \
			CName32.SetQuery(this); \
			CName33.SetQuery(this); \
			CName34.SetQuery(this); \
			CName35.SetQuery(this); \
			CName36.SetQuery(this); \
			CName37.SetQuery(this); \
			CName38.SetQuery(this); \
			CName39.SetQuery(this); \
			CName40.SetQuery(this); \
			CName41.SetQuery(this); \
			CName42.SetQuery(this); \
			CName43.SetQuery(this); \
			CName44.SetQuery(this); \
			CName45.SetQuery(this); \
			CName46.SetQuery(this); \
			CName47.SetQuery(this); \
			CName48.SetQuery(this); \
		} \
\
		TestQuery(const TestQuery& copy) : Query(copy), CName1(0), CName2(1), CName3(2), CName4(3), CName5(4), CName6(5), CName7(6), CName8(7), CName9(8), CName10(9), CName11(10), CName12(11), CName13(12), CName14(13), CName15(14), CName16(15), CName17(16), CName18(17), CName19(18), CName20(19), CName21(20), CName22(21), CName23(22), CName24(23), CName25(24), CName26(25), CName27(26), CName28(27), CName29(28), CName30(29), CName31(30), CName32(31), CName33(32), CName34(33), CName35(34), CName36(35), CName37(36), CName38(37), CName39(38), CName40(39), CName41(40), CName42(41), CName43(42), CName44(43), CName45(44), CName46(45), CName47(46), CName48(47) { \
			CName1.SetQuery(this); \
			CName2.SetQuery(this); \
			CName3.SetQuery(this); \
			CName4.SetQuery(this); \
			CName5.SetQuery(this); \
			CName6.SetQuery(this); \
			CName7.SetQuery(this); \
			CName8.SetQuery(this); \
			CName9.SetQuery(this); \
			CName10.SetQuery(this); \
			CName11.SetQuery(this); \
			CName12.SetQuery(this); \
			CName13.SetQuery(this); \
			CName14.SetQuery(this); \
			CName15.SetQuery(this); \
			CName16.SetQuery(this); \
			CName17.SetQuery(this); \
			CName18.SetQuery(this); \
			CName19.SetQuery(this); \
			CName20.SetQuery(this); \
			CName21.SetQuery(this); \
			CName22.SetQuery(this); \
			CName23.SetQuery(this); \
			CName24.SetQuery(this); \
			CName25.SetQuery(this); \
			CName26.SetQuery(this); \
			CName27.SetQuery(this); \
			CName28.SetQuery(this); \
			CName29.SetQuery(this); \
			CName30.SetQuery(this); \
			CName31.SetQuery(this); \
			CName32.SetQuery(this); \
			CName33.SetQuery(this); \
			CName34.SetQuery(this); \
			CName35.SetQuery(this); \
			CName36.SetQuery(this); \
			CName37.SetQuery(this); \
			CName38.SetQuery(this); \
			CName39.SetQuery(this); \
			CName40.SetQuery(this); \
			CName41.SetQuery(this); \
			CName42.SetQuery(this); \
			CName43.SetQuery(this); \
			CName44.SetQuery(this); \
			CName45.SetQuery(this); \
			CName46.SetQuery(this); \
			CName47.SetQuery(this); \
			CName48.SetQuery(this); \
		} \
\
		class TestQueryQueryAccessorInt : private XQueryAccessorInt { \
		public: \
			TestQueryQueryAccessorInt(size_t column_id) : XQueryAccessorInt(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(int64_t value) {return (TestQuery &)XQueryAccessorInt::Equal(value);} \
			TestQuery& NotEqual(int64_t value) {return (TestQuery &)XQueryAccessorInt::NotEqual(value);} \
			TestQuery& Greater(int64_t value) {return (TestQuery &)XQueryAccessorInt::Greater(value);} \
			TestQuery& Less(int64_t value) {return (TestQuery &)XQueryAccessorInt::Less(value);} \
			TestQuery& Between(int64_t from, int64_t to) {return (TestQuery &)XQueryAccessorInt::Between(from, to);} \
		}; \
\
		template <class T> class TestQueryQueryAccessorEnum : public TestQueryQueryAccessorInt { \
		public: \
			TestQueryQueryAccessorEnum<T>(size_t column_id) : TestQueryQueryAccessorInt(column_id) {} \
		}; \
\
		class TestQueryQueryAccessorString : private XQueryAccessorString { \
		public: \
			TestQueryQueryAccessorString(size_t column_id) : XQueryAccessorString(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::Equal(value, CaseSensitive);} \
			TestQuery& NotEqual(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::NotEqual(value, CaseSensitive);} \
			TestQuery& BeginsWith(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::BeginsWith(value, CaseSensitive);} \
			TestQuery& EndsWith(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::EndsWith(value, CaseSensitive);} \
			TestQuery& Contains(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::Contains(value, CaseSensitive);} \
		}; \
\
		class TestQueryQueryAccessorBool : private XQueryAccessorBool { \
		public: \
			TestQueryQueryAccessorBool(size_t column_id) : XQueryAccessorBool(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(bool value) {return (TestQuery &)XQueryAccessorBool::Equal(value);} \
		}; \
\
		TestQueryQueryAccessor##CType1 CName1; \
		TestQueryQueryAccessor##CType2 CName2; \
		TestQueryQueryAccessor##CType3 CName3; \
		TestQueryQueryAccessor##CType4 CName4; \
		TestQueryQueryAccessor##CType5 CName5; \
		TestQueryQueryAccessor##CType6 CName6; \
		TestQueryQueryAccessor##CType7 CName7; \
		TestQueryQueryAccessor##CType8 CName8; \
		TestQueryQueryAccessor##CType9 CName9; \
		TestQueryQueryAccessor##CType10 CName10; \
		TestQueryQueryAccessor##CType11 CName11; \
		TestQueryQueryAccessor##CType12 CName12; \
		TestQueryQueryAccessor##CType13 CName13; \
		TestQueryQueryAccessor##CType14 CName14; \
		TestQueryQueryAccessor##CType15 CName15; \
		TestQueryQueryAccessor##CType16 CName16; \
		TestQueryQueryAccessor##CType17 CName17; \
		TestQueryQueryAccessor##CType18 CName18; \
		TestQueryQueryAccessor##CType19 CName19; \
		TestQueryQueryAccessor##CType20 CName20; \
		TestQueryQueryAccessor##CType21 CName21; \
		TestQueryQueryAccessor##CType22 CName22; \
		TestQueryQueryAccessor##CType23 CName23; \
		TestQueryQueryAccessor##CType24 CName24; \
		TestQueryQueryAccessor##CType25 CName25; \
		TestQueryQueryAccessor##CType26 CName26; \
		TestQueryQueryAccessor##CType27 CName27; \
		TestQueryQueryAccessor##CType28 CName28; \
		TestQueryQueryAccessor##CType29 CName29; \
		TestQueryQueryAccessor##CType30 CName30; \
		TestQueryQueryAccessor##CType31 CName31; \
		TestQueryQueryAccessor##CType32 CName32; \
		TestQueryQueryAccessor##CType33 CName33; \
		TestQueryQueryAccessor##CType34 CName34; \
		TestQueryQueryAccessor##CType35 CName35; \
		TestQueryQueryAccessor##CType36 CName36; \
		TestQueryQueryAccessor##CType37 CName37; \
		TestQueryQueryAccessor##CType38 CName38; \
		TestQueryQueryAccessor##CType39 CName39; \
		TestQueryQueryAccessor##CType40 CName40; \
		TestQueryQueryAccessor##CType41 CName41; \
		TestQueryQueryAccessor##CType42 CName42; \
		TestQueryQueryAccessor##CType43 CName43; \
		TestQueryQueryAccessor##CType44 CName44; \
		TestQueryQueryAccessor##CType45 CName45; \
		TestQueryQueryAccessor##CType46 CName46; \
		TestQueryQueryAccessor##CType47 CName47; \
		TestQueryQueryAccessor##CType48 CName48; \
\
		TestQuery& LeftParan(void) {Query::LeftParan(); return *this;}; \
		TestQuery& Or(void) {Query::Or(); return *this;}; \
		TestQuery& RightParan(void) {Query::RightParan(); return *this;}; \
		TestQuery& Subtable(size_t column) {Query::Subtable(column); return *this;}; \
		TestQuery& Parent() {Query::Parent(); return *this;}; \
	}; \
\
	TestQuery GetQuery() {return TestQuery();} \
\
	class Cursor : public CursorBase { \
	public: \
		Cursor(TableName& table, size_t ndx) : CursorBase(table, ndx) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
			CName17.Create(this, 16); \
			CName18.Create(this, 17); \
			CName19.Create(this, 18); \
			CName20.Create(this, 19); \
			CName21.Create(this, 20); \
			CName22.Create(this, 21); \
			CName23.Create(this, 22); \
			CName24.Create(this, 23); \
			CName25.Create(this, 24); \
			CName26.Create(this, 25); \
			CName27.Create(this, 26); \
			CName28.Create(this, 27); \
			CName29.Create(this, 28); \
			CName30.Create(this, 29); \
			CName31.Create(this, 30); \
			CName32.Create(this, 31); \
			CName33.Create(this, 32); \
			CName34.Create(this, 33); \
			CName35.Create(this, 34); \
			CName36.Create(this, 35); \
			CName37.Create(this, 36); \
			CName38.Create(this, 37); \
			CName39.Create(this, 38); \
			CName40.Create(this, 39); \
			CName41.Create(this, 40); \
			CName42.Create(this, 41); \
			CName43.Create(this, 42); \
			CName44.Create(this, 43); \
			CName45.Create(this, 44); \
			CName46.Create(this, 45); \
			CName47.Create(this, 46); \
			CName48.Create(this, 47); \
		} \
		Cursor(const TableName& table, size_t ndx) : CursorBase(const_cast<TableName&>(table), ndx) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
			CName17.Create(this, 16); \
			CName18.Create(this, 17); \
			CName19.Create(this, 18); \
			CName20.Create(this, 19); \
			CName21.Create(this, 20); \
			CName22.Create(this, 21); \
			CName23.Create(this, 22); \
			CName24.Create(this, 23); \
			CName25.Create(this, 24); \
			CName26.Create(this, 25); \
			CName27.Create(this, 26); \
			CName28.Create(this, 27); \
			CName29.Create(this, 28); \
			CName30.Create(this, 29); \
			CName31.Create(this, 30); \
			CName32.Create(this, 31); \
			CName33.Create(this, 32); \
			CName34.Create(this, 33); \
			CName35.Create(this, 34); \
			CName36.Create(this, 35); \
			CName37.Create(this, 36); \
			CName38.Create(this, 37); \
			CName39.Create(this, 38); \
			CName40.Create(this, 39); \
			CName41.Create(this, 40); \
			CName42.Create(this, 41); \
			CName43.Create(this, 42); \
			CName44.Create(this, 43); \
			CName45.Create(this, 44); \
			CName46.Create(this, 45); \
			CName47.Create(this, 46); \
			CName48.Create(this, 47); \
		} \
		Cursor(const Cursor& v) : CursorBase(v) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
			CName17.Create(this, 16); \
			CName18.Create(this, 17); \
			CName19.Create(this, 18); \
			CName20.Create(this, 19); \
			CName21.Create(this, 20); \
			CName22.Create(this, 21); \
			CName23.Create(this, 22); \
			CName24.Create(this, 23); \
			CName25.Create(this, 24); \
			CName26.Create(this, 25); \
			CName27.Create(this, 26); \
			CName28.Create(this, 27); \
			CName29.Create(this, 28); \
			CName30.Create(this, 29); \
			CName31.Create(this, 30); \
			CName32.Create(this, 31); \
			CName33.Create(this, 32); \
			CName34.Create(this, 33); \
			CName35.Create(this, 34); \
			CName36.Create(this, 35); \
			CName37.Create(this, 36); \
			CName38.Create(this, 37); \
			CName39.Create(this, 38); \
			CName40.Create(this, 39); \
			CName41.Create(this, 40); \
			CName42.Create(this, 41); \
			CName43.Create(this, 42); \
			CName44.Create(this, 43); \
			CName45.Create(this, 44); \
			CName46.Create(this, 45); \
			CName47.Create(this, 46); \
			CName48.Create(this, 47); \
		} \
		Accessor##CType1 CName1; \
		Accessor##CType2 CName2; \
		Accessor##CType3 CName3; \
		Accessor##CType4 CName4; \
		Accessor##CType5 CName5; \
		Accessor##CType6 CName6; \
		Accessor##CType7 CName7; \
		Accessor##CType8 CName8; \
		Accessor##CType9 CName9; \
		Accessor##CType10 CName10; \
		Accessor##CType11 CName11; \
		Accessor##CType12 CName12; \
		Accessor##CType13 CName13; \
		Accessor##CType14 CName14; \
		Accessor##CType15 CName15; \
		Accessor##CType16 CName16; \
		Accessor##CType17 CName17; \
		Accessor##CType18 CName18; \
		Accessor##CType19 CName19; \
		Accessor##CType20 CName20; \
		Accessor##CType21 CName21; \
		Accessor##CType22 CName22; \
		Accessor##CType23 CName23; \
		Accessor##CType24 CName24; \
		Accessor##CType25 CName25; \
		Accessor##CType26 CName26; \
		Accessor##CType27 CName27; \
		Accessor##CType28 CName28; \
		Accessor##CType29 CName29; \
		Accessor##CType30 CName30; \
		Accessor##CType31 CName31; \
		Accessor##CType32 CName32; \
		Accessor##CType33 CName33; \
		Accessor##CType34 CName34; \
		Accessor##CType35 CName35; \
		Accessor##CType36 CName36; \
		Accessor##CType37 CName37; \
		Accessor##CType38 CName38; \
		Accessor##CType39 CName39; \
		Accessor##CType40 CName40; \
		Accessor##CType41 CName41; \
		Accessor##CType42 CName42; \
		Accessor##CType43 CName43; \
		Accessor##CType44 CName44; \
		Accessor##CType45 CName45; \
		Accessor##CType46 CName46; \
		Accessor##CType47 CName47; \
		Accessor##CType48 CName48; \
	}; \
\
	void Add(tdbType##CType1 CName1, tdbType##CType2 CName2, tdbType##CType3 CName3, tdbType##CType4 CName4, tdbType##CType5 CName5, tdbType##CType6 CName6, tdbType##CType7 CName7, tdbType##CType8 CName8, tdbType##CType9 CName9, tdbType##CType10 CName10, tdbType##CType11 CName11, tdbType##CType12 CName12, tdbType##CType13 CName13, tdbType##CType14 CName14, tdbType##CType15 CName15, tdbType##CType16 CName16, tdbType##CType17 CName17, tdbType##CType18 CName18, tdbType##CType19 CName19, tdbType##CType20 CName20, tdbType##CType21 CName21, tdbType##CType22 CName22, tdbType##CType23 CName23, tdbType##CType24 CName24, tdbType##CType25 CName25, tdbType##CType26 CName26, tdbType##CType27 CName27, tdbType##CType28 CName28, tdbType##CType29 CName29, tdbType##CType30 CName30, tdbType##CType31 CName31, tdbType##CType32 CName32, tdbType##CType33 CName33, tdbType##CType34 CName34, tdbType##CType35 CName35, tdbType##CType36 CName36, tdbType##CType37 CName37, tdbType##CType38 CName38, tdbType##CType39 CName39, tdbType##CType40 CName40, tdbType##CType41 CName41, tdbType##CType42 CName42, tdbType##CType43 CName43, tdbType##CType44 CName44, tdbType##CType45 CName45, tdbType##CType46 CName46, tdbType##CType47 CName47, tdbType##CType48 CName48) { \
		const size_t ndx = GetSize(); \
		Insert##CType1 (0, ndx, CName1); \
		Insert##CType2 (1, ndx, CName2); \
		Insert##CType3 (2, ndx, CName3); \
		Insert##CType4 (3, ndx, CName4); \
		Insert##CType5 (4, ndx, CName5); \
		Insert##CType6 (5, ndx, CName6); \
		Insert##CType7 (6, ndx, CName7); \
		Insert##CType8 (7, ndx, CName8); \
		Insert##CType9 (8, ndx, CName9); \
		Insert##CType10 (9, ndx, CName10); \
		Insert##CType11 (10, ndx, CName11); \
		Insert##CType12 (11, ndx, CName12); \
		Insert##CType13 (12, ndx, CName13); \
		Insert##CType14 (13, ndx, CName14); \
		Insert##CType15 (14, ndx, CName15); \
		Insert##CType16 (15, ndx, CName16); \
		Insert##CType17 (16, ndx, CName17); \
		Insert##CType18 (17, ndx, CName18); \
		Insert##CType19 (18, ndx, CName19); \
		Insert##CType20 (19, ndx, CName20); \
		Insert##CType21 (20, ndx, CName21); \
		Insert##CType22 (21, ndx, CName22); \
		Insert##CType23 (22, ndx, CName23); \
		Insert##CType24 (23, ndx, CName24); \
		Insert##CType25 (24, ndx, CName25); \
		Insert##CType26 (25, ndx, CName26); \
		Insert##CType27 (26, ndx, CName27); \
		Insert##CType28 (27, ndx, CName28); \
		Insert##CType29 (28, ndx, CName29); \
		Insert##CType30 (29, ndx, CName30); \
		Insert##CType31 (30, ndx, CName31); \
		Insert##CType32 (31, ndx, CName32); \
		Insert##CType33 (32, ndx, CName33); \
		Insert##CType34 (33, ndx, CName34); \
		Insert##CType35 (34, ndx, CName35); \
		Insert##CType36 (35, ndx, CName36); \
		Insert##CType37 (36, ndx, CName37); \
		Insert##CType38 (37, ndx, CName38); \
		Insert##CType39 (38, ndx, CName39); \
		Insert##CType40 (39, ndx, CName40); \
		Insert##CType41 (40, ndx, CName41); \
		Insert##CType42 (41, ndx, CName42); \
		Insert##CType43 (42, ndx, CName43); \
		Insert##CType44 (43, ndx, CName44); \
		Insert##CType45 (44, ndx, CName45); \
		Insert##CType46 (45, ndx, CName46); \
		Insert##CType47 (46, ndx, CName47); \
		Insert##CType48 (47, ndx, CName48); \
		InsertDone(); \
	} \
\
	void Insert(size_t ndx, tdbType##CType1 CName1, tdbType##CType2 CName2, tdbType##CType3 CName3, tdbType##CType4 CName4, tdbType##CType5 CName5, tdbType##CType6 CName6, tdbType##CType7 CName7, tdbType##CType8 CName8, tdbType##CType9 CName9, tdbType##CType10 CName10, tdbType##CType11 CName11, tdbType##CType12 CName12, tdbType##CType13 CName13, tdbType##CType14 CName14, tdbType##CType15 CName15, tdbType##CType16 CName16, tdbType##CType17 CName17, tdbType##CType18 CName18, tdbType##CType19 CName19, tdbType##CType20 CName20, tdbType##CType21 CName21, tdbType##CType22 CName22, tdbType##CType23 CName23, tdbType##CType24 CName24, tdbType##CType25 CName25, tdbType##CType26 CName26, tdbType##CType27 CName27, tdbType##CType28 CName28, tdbType##CType29 CName29, tdbType##CType30 CName30, tdbType##CType31 CName31, tdbType##CType32 CName32, tdbType##CType33 CName33, tdbType##CType34 CName34, tdbType##CType35 CName35, tdbType##CType36 CName36, tdbType##CType37 CName37, tdbType##CType38 CName38, tdbType##CType39 CName39, tdbType##CType40 CName40, tdbType##CType41 CName41, tdbType##CType42 CName42, tdbType##CType43 CName43, tdbType##CType44 CName44, tdbType##CType45 CName45, tdbType##CType46 CName46, tdbType##CType47 CName47, tdbType##CType48 CName48) { \
		Insert##CType1 (0, ndx, CName1); \
		Insert##CType2 (1, ndx, CName2); \
		Insert##CType3 (2, ndx, CName3); \
		Insert##CType4 (3, ndx, CName4); \
		Insert##CType5 (4, ndx, CName5); \
		Insert##CType6 (5, ndx, CName6); \
		Insert##CType7 (6, ndx, CName7); \
		Insert##CType8 (7, ndx, CName8); \
		Insert##CType9 (8, ndx, CName9); \
		Insert##CType10 (9, ndx, CName10); \
		Insert##CType11 (10, ndx, CName11); \
		Insert##CType12 (11, ndx, CName12); \
		Insert##CType13 (12, ndx, CName13); \
		Insert##CType14 (13, ndx, CName14); \
		Insert##CType15 (14, ndx, CName15); \
		Insert##CType16 (15, ndx, CName16); \
		Insert##CType17 (16, ndx, CName17); \
		Insert##CType18 (17, ndx, CName18); \
		Insert##CType19 (18, ndx, CName19); \
		Insert##CType20 (19, ndx, CName20); \
		Insert##CType21 (20, ndx, CName21); \
		Insert##CType22 (21, ndx, CName22); \
		Insert##CType23 (22, ndx, CName23); \
		Insert##CType24 (23, ndx, CName24); \
		Insert##CType25 (24, ndx, CName25); \
		Insert##CType26 (25, ndx, CName26); \
		Insert##CType27 (26, ndx, CName27); \
		Insert##CType28 (27, ndx, CName28); \
		Insert##CType29 (28, ndx, CName29); \
		Insert##CType30 (29, ndx, CName30); \
		Insert##CType31 (30, ndx, CName31); \
		Insert##CType32 (31, ndx, CName32); \
		Insert##CType33 (32, ndx, CName33); \
		Insert##CType34 (33, ndx, CName34); \
		Insert##CType35 (34, ndx, CName35); \
		Insert##CType36 (35, ndx, CName36); \
		Insert##CType37 (36, ndx, CName37); \
		Insert##CType38 (37, ndx, CName38); \
		Insert##CType39 (38, ndx, CName39); \
		Insert##CType40 (39, ndx, CName40); \
		Insert##CType41 (40, ndx, CName41); \
		Insert##CType42 (41, ndx, CName42); \
		Insert##CType43 (42, ndx, CName43); \
		Insert##CType44 (43, ndx, CName44); \
		Insert##CType45 (44, ndx, CName45); \
		Insert##CType46 (45, ndx, CName46); \
		Insert##CType47 (46, ndx, CName47); \
		Insert##CType48 (47, ndx, CName48); \
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
	ColumnProxy##CType3 CName3; \
	ColumnProxy##CType4 CName4; \
	ColumnProxy##CType5 CName5; \
	ColumnProxy##CType6 CName6; \
	ColumnProxy##CType7 CName7; \
	ColumnProxy##CType8 CName8; \
	ColumnProxy##CType9 CName9; \
	ColumnProxy##CType10 CName10; \
	ColumnProxy##CType11 CName11; \
	ColumnProxy##CType12 CName12; \
	ColumnProxy##CType13 CName13; \
	ColumnProxy##CType14 CName14; \
	ColumnProxy##CType15 CName15; \
	ColumnProxy##CType16 CName16; \
	ColumnProxy##CType17 CName17; \
	ColumnProxy##CType18 CName18; \
	ColumnProxy##CType19 CName19; \
	ColumnProxy##CType20 CName20; \
	ColumnProxy##CType21 CName21; \
	ColumnProxy##CType22 CName22; \
	ColumnProxy##CType23 CName23; \
	ColumnProxy##CType24 CName24; \
	ColumnProxy##CType25 CName25; \
	ColumnProxy##CType26 CName26; \
	ColumnProxy##CType27 CName27; \
	ColumnProxy##CType28 CName28; \
	ColumnProxy##CType29 CName29; \
	ColumnProxy##CType30 CName30; \
	ColumnProxy##CType31 CName31; \
	ColumnProxy##CType32 CName32; \
	ColumnProxy##CType33 CName33; \
	ColumnProxy##CType34 CName34; \
	ColumnProxy##CType35 CName35; \
	ColumnProxy##CType36 CName36; \
	ColumnProxy##CType37 CName37; \
	ColumnProxy##CType38 CName38; \
	ColumnProxy##CType39 CName39; \
	ColumnProxy##CType40 CName40; \
	ColumnProxy##CType41 CName41; \
	ColumnProxy##CType42 CName42; \
	ColumnProxy##CType43 CName43; \
	ColumnProxy##CType44 CName44; \
	ColumnProxy##CType45 CName45; \
	ColumnProxy##CType46 CName46; \
	ColumnProxy##CType47 CName47; \
	ColumnProxy##CType48 CName48; \
\
protected: \
	friend class Group; \
	TableName(Allocator& alloc, size_t ref, Array* parent, size_t pndx) : TopLevelTable(alloc, ref, parent, pndx) {}; \
\
private: \
	TableName(const TableName&) {} \
	TableName& operator=(const TableName&) {return *this;} \
};



#define TDB_TABLE_49(TableName, CType1, CName1, CType2, CName2, CType3, CName3, CType4, CName4, CType5, CName5, CType6, CName6, CType7, CName7, CType8, CName8, CType9, CName9, CType10, CName10, CType11, CName11, CType12, CName12, CType13, CName13, CType14, CName14, CType15, CName15, CType16, CName16, CType17, CName17, CType18, CName18, CType19, CName19, CType20, CName20, CType21, CName21, CType22, CName22, CType23, CName23, CType24, CName24, CType25, CName25, CType26, CName26, CType27, CName27, CType28, CName28, CType29, CName29, CType30, CName30, CType31, CName31, CType32, CName32, CType33, CName33, CType34, CName34, CType35, CName35, CType36, CName36, CType37, CName37, CType38, CName38, CType39, CName39, CType40, CName40, CType41, CName41, CType42, CName42, CType43, CName43, CType44, CName44, CType45, CName45, CType46, CName46, CType47, CName47, CType48, CName48, CType49, CName49) \
class TableName##Query { \
protected: \
	QueryAccessor##CType1 CName1; \
	QueryAccessor##CType2 CName2; \
	QueryAccessor##CType3 CName3; \
	QueryAccessor##CType4 CName4; \
	QueryAccessor##CType5 CName5; \
	QueryAccessor##CType6 CName6; \
	QueryAccessor##CType7 CName7; \
	QueryAccessor##CType8 CName8; \
	QueryAccessor##CType9 CName9; \
	QueryAccessor##CType10 CName10; \
	QueryAccessor##CType11 CName11; \
	QueryAccessor##CType12 CName12; \
	QueryAccessor##CType13 CName13; \
	QueryAccessor##CType14 CName14; \
	QueryAccessor##CType15 CName15; \
	QueryAccessor##CType16 CName16; \
	QueryAccessor##CType17 CName17; \
	QueryAccessor##CType18 CName18; \
	QueryAccessor##CType19 CName19; \
	QueryAccessor##CType20 CName20; \
	QueryAccessor##CType21 CName21; \
	QueryAccessor##CType22 CName22; \
	QueryAccessor##CType23 CName23; \
	QueryAccessor##CType24 CName24; \
	QueryAccessor##CType25 CName25; \
	QueryAccessor##CType26 CName26; \
	QueryAccessor##CType27 CName27; \
	QueryAccessor##CType28 CName28; \
	QueryAccessor##CType29 CName29; \
	QueryAccessor##CType30 CName30; \
	QueryAccessor##CType31 CName31; \
	QueryAccessor##CType32 CName32; \
	QueryAccessor##CType33 CName33; \
	QueryAccessor##CType34 CName34; \
	QueryAccessor##CType35 CName35; \
	QueryAccessor##CType36 CName36; \
	QueryAccessor##CType37 CName37; \
	QueryAccessor##CType38 CName38; \
	QueryAccessor##CType39 CName39; \
	QueryAccessor##CType40 CName40; \
	QueryAccessor##CType41 CName41; \
	QueryAccessor##CType42 CName42; \
	QueryAccessor##CType43 CName43; \
	QueryAccessor##CType44 CName44; \
	QueryAccessor##CType45 CName45; \
	QueryAccessor##CType46 CName46; \
	QueryAccessor##CType47 CName47; \
	QueryAccessor##CType48 CName48; \
	QueryAccessor##CType49 CName49; \
}; \
\
class TableName : public TopLevelTable { \
public: \
	TableName(Allocator& alloc=GetDefaultAllocator()) : TopLevelTable(alloc) { \
		RegisterColumn(Accessor##CType1::type, #CName1); \
		RegisterColumn(Accessor##CType2::type, #CName2); \
		RegisterColumn(Accessor##CType3::type, #CName3); \
		RegisterColumn(Accessor##CType4::type, #CName4); \
		RegisterColumn(Accessor##CType5::type, #CName5); \
		RegisterColumn(Accessor##CType6::type, #CName6); \
		RegisterColumn(Accessor##CType7::type, #CName7); \
		RegisterColumn(Accessor##CType8::type, #CName8); \
		RegisterColumn(Accessor##CType9::type, #CName9); \
		RegisterColumn(Accessor##CType10::type, #CName10); \
		RegisterColumn(Accessor##CType11::type, #CName11); \
		RegisterColumn(Accessor##CType12::type, #CName12); \
		RegisterColumn(Accessor##CType13::type, #CName13); \
		RegisterColumn(Accessor##CType14::type, #CName14); \
		RegisterColumn(Accessor##CType15::type, #CName15); \
		RegisterColumn(Accessor##CType16::type, #CName16); \
		RegisterColumn(Accessor##CType17::type, #CName17); \
		RegisterColumn(Accessor##CType18::type, #CName18); \
		RegisterColumn(Accessor##CType19::type, #CName19); \
		RegisterColumn(Accessor##CType20::type, #CName20); \
		RegisterColumn(Accessor##CType21::type, #CName21); \
		RegisterColumn(Accessor##CType22::type, #CName22); \
		RegisterColumn(Accessor##CType23::type, #CName23); \
		RegisterColumn(Accessor##CType24::type, #CName24); \
		RegisterColumn(Accessor##CType25::type, #CName25); \
		RegisterColumn(Accessor##CType26::type, #CName26); \
		RegisterColumn(Accessor##CType27::type, #CName27); \
		RegisterColumn(Accessor##CType28::type, #CName28); \
		RegisterColumn(Accessor##CType29::type, #CName29); \
		RegisterColumn(Accessor##CType30::type, #CName30); \
		RegisterColumn(Accessor##CType31::type, #CName31); \
		RegisterColumn(Accessor##CType32::type, #CName32); \
		RegisterColumn(Accessor##CType33::type, #CName33); \
		RegisterColumn(Accessor##CType34::type, #CName34); \
		RegisterColumn(Accessor##CType35::type, #CName35); \
		RegisterColumn(Accessor##CType36::type, #CName36); \
		RegisterColumn(Accessor##CType37::type, #CName37); \
		RegisterColumn(Accessor##CType38::type, #CName38); \
		RegisterColumn(Accessor##CType39::type, #CName39); \
		RegisterColumn(Accessor##CType40::type, #CName40); \
		RegisterColumn(Accessor##CType41::type, #CName41); \
		RegisterColumn(Accessor##CType42::type, #CName42); \
		RegisterColumn(Accessor##CType43::type, #CName43); \
		RegisterColumn(Accessor##CType44::type, #CName44); \
		RegisterColumn(Accessor##CType45::type, #CName45); \
		RegisterColumn(Accessor##CType46::type, #CName46); \
		RegisterColumn(Accessor##CType47::type, #CName47); \
		RegisterColumn(Accessor##CType48::type, #CName48); \
		RegisterColumn(Accessor##CType49::type, #CName49); \
\
		CName1.Create(this, 0); \
		CName2.Create(this, 1); \
		CName3.Create(this, 2); \
		CName4.Create(this, 3); \
		CName5.Create(this, 4); \
		CName6.Create(this, 5); \
		CName7.Create(this, 6); \
		CName8.Create(this, 7); \
		CName9.Create(this, 8); \
		CName10.Create(this, 9); \
		CName11.Create(this, 10); \
		CName12.Create(this, 11); \
		CName13.Create(this, 12); \
		CName14.Create(this, 13); \
		CName15.Create(this, 14); \
		CName16.Create(this, 15); \
		CName17.Create(this, 16); \
		CName18.Create(this, 17); \
		CName19.Create(this, 18); \
		CName20.Create(this, 19); \
		CName21.Create(this, 20); \
		CName22.Create(this, 21); \
		CName23.Create(this, 22); \
		CName24.Create(this, 23); \
		CName25.Create(this, 24); \
		CName26.Create(this, 25); \
		CName27.Create(this, 26); \
		CName28.Create(this, 27); \
		CName29.Create(this, 28); \
		CName30.Create(this, 29); \
		CName31.Create(this, 30); \
		CName32.Create(this, 31); \
		CName33.Create(this, 32); \
		CName34.Create(this, 33); \
		CName35.Create(this, 34); \
		CName36.Create(this, 35); \
		CName37.Create(this, 36); \
		CName38.Create(this, 37); \
		CName39.Create(this, 38); \
		CName40.Create(this, 39); \
		CName41.Create(this, 40); \
		CName42.Create(this, 41); \
		CName43.Create(this, 42); \
		CName44.Create(this, 43); \
		CName45.Create(this, 44); \
		CName46.Create(this, 45); \
		CName47.Create(this, 46); \
		CName48.Create(this, 47); \
		CName49.Create(this, 48); \
	}; \
\
	class TestQuery : public Query { \
	public: \
		TestQuery() : CName1(0), CName2(1), CName3(2), CName4(3), CName5(4), CName6(5), CName7(6), CName8(7), CName9(8), CName10(9), CName11(10), CName12(11), CName13(12), CName14(13), CName15(14), CName16(15), CName17(16), CName18(17), CName19(18), CName20(19), CName21(20), CName22(21), CName23(22), CName24(23), CName25(24), CName26(25), CName27(26), CName28(27), CName29(28), CName30(29), CName31(30), CName32(31), CName33(32), CName34(33), CName35(34), CName36(35), CName37(36), CName38(37), CName39(38), CName40(39), CName41(40), CName42(41), CName43(42), CName44(43), CName45(44), CName46(45), CName47(46), CName48(47), CName49(48) { \
			CName1.SetQuery(this); \
			CName2.SetQuery(this); \
			CName3.SetQuery(this); \
			CName4.SetQuery(this); \
			CName5.SetQuery(this); \
			CName6.SetQuery(this); \
			CName7.SetQuery(this); \
			CName8.SetQuery(this); \
			CName9.SetQuery(this); \
			CName10.SetQuery(this); \
			CName11.SetQuery(this); \
			CName12.SetQuery(this); \
			CName13.SetQuery(this); \
			CName14.SetQuery(this); \
			CName15.SetQuery(this); \
			CName16.SetQuery(this); \
			CName17.SetQuery(this); \
			CName18.SetQuery(this); \
			CName19.SetQuery(this); \
			CName20.SetQuery(this); \
			CName21.SetQuery(this); \
			CName22.SetQuery(this); \
			CName23.SetQuery(this); \
			CName24.SetQuery(this); \
			CName25.SetQuery(this); \
			CName26.SetQuery(this); \
			CName27.SetQuery(this); \
			CName28.SetQuery(this); \
			CName29.SetQuery(this); \
			CName30.SetQuery(this); \
			CName31.SetQuery(this); \
			CName32.SetQuery(this); \
			CName33.SetQuery(this); \
			CName34.SetQuery(this); \
			CName35.SetQuery(this); \
			CName36.SetQuery(this); \
			CName37.SetQuery(this); \
			CName38.SetQuery(this); \
			CName39.SetQuery(this); \
			CName40.SetQuery(this); \
			CName41.SetQuery(this); \
			CName42.SetQuery(this); \
			CName43.SetQuery(this); \
			CName44.SetQuery(this); \
			CName45.SetQuery(this); \
			CName46.SetQuery(this); \
			CName47.SetQuery(this); \
			CName48.SetQuery(this); \
			CName49.SetQuery(this); \
		} \
\
		TestQuery(const TestQuery& copy) : Query(copy), CName1(0), CName2(1), CName3(2), CName4(3), CName5(4), CName6(5), CName7(6), CName8(7), CName9(8), CName10(9), CName11(10), CName12(11), CName13(12), CName14(13), CName15(14), CName16(15), CName17(16), CName18(17), CName19(18), CName20(19), CName21(20), CName22(21), CName23(22), CName24(23), CName25(24), CName26(25), CName27(26), CName28(27), CName29(28), CName30(29), CName31(30), CName32(31), CName33(32), CName34(33), CName35(34), CName36(35), CName37(36), CName38(37), CName39(38), CName40(39), CName41(40), CName42(41), CName43(42), CName44(43), CName45(44), CName46(45), CName47(46), CName48(47), CName49(48) { \
			CName1.SetQuery(this); \
			CName2.SetQuery(this); \
			CName3.SetQuery(this); \
			CName4.SetQuery(this); \
			CName5.SetQuery(this); \
			CName6.SetQuery(this); \
			CName7.SetQuery(this); \
			CName8.SetQuery(this); \
			CName9.SetQuery(this); \
			CName10.SetQuery(this); \
			CName11.SetQuery(this); \
			CName12.SetQuery(this); \
			CName13.SetQuery(this); \
			CName14.SetQuery(this); \
			CName15.SetQuery(this); \
			CName16.SetQuery(this); \
			CName17.SetQuery(this); \
			CName18.SetQuery(this); \
			CName19.SetQuery(this); \
			CName20.SetQuery(this); \
			CName21.SetQuery(this); \
			CName22.SetQuery(this); \
			CName23.SetQuery(this); \
			CName24.SetQuery(this); \
			CName25.SetQuery(this); \
			CName26.SetQuery(this); \
			CName27.SetQuery(this); \
			CName28.SetQuery(this); \
			CName29.SetQuery(this); \
			CName30.SetQuery(this); \
			CName31.SetQuery(this); \
			CName32.SetQuery(this); \
			CName33.SetQuery(this); \
			CName34.SetQuery(this); \
			CName35.SetQuery(this); \
			CName36.SetQuery(this); \
			CName37.SetQuery(this); \
			CName38.SetQuery(this); \
			CName39.SetQuery(this); \
			CName40.SetQuery(this); \
			CName41.SetQuery(this); \
			CName42.SetQuery(this); \
			CName43.SetQuery(this); \
			CName44.SetQuery(this); \
			CName45.SetQuery(this); \
			CName46.SetQuery(this); \
			CName47.SetQuery(this); \
			CName48.SetQuery(this); \
			CName49.SetQuery(this); \
		} \
\
		class TestQueryQueryAccessorInt : private XQueryAccessorInt { \
		public: \
			TestQueryQueryAccessorInt(size_t column_id) : XQueryAccessorInt(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(int64_t value) {return (TestQuery &)XQueryAccessorInt::Equal(value);} \
			TestQuery& NotEqual(int64_t value) {return (TestQuery &)XQueryAccessorInt::NotEqual(value);} \
			TestQuery& Greater(int64_t value) {return (TestQuery &)XQueryAccessorInt::Greater(value);} \
			TestQuery& Less(int64_t value) {return (TestQuery &)XQueryAccessorInt::Less(value);} \
			TestQuery& Between(int64_t from, int64_t to) {return (TestQuery &)XQueryAccessorInt::Between(from, to);} \
		}; \
\
		template <class T> class TestQueryQueryAccessorEnum : public TestQueryQueryAccessorInt { \
		public: \
			TestQueryQueryAccessorEnum<T>(size_t column_id) : TestQueryQueryAccessorInt(column_id) {} \
		}; \
\
		class TestQueryQueryAccessorString : private XQueryAccessorString { \
		public: \
			TestQueryQueryAccessorString(size_t column_id) : XQueryAccessorString(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::Equal(value, CaseSensitive);} \
			TestQuery& NotEqual(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::NotEqual(value, CaseSensitive);} \
			TestQuery& BeginsWith(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::BeginsWith(value, CaseSensitive);} \
			TestQuery& EndsWith(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::EndsWith(value, CaseSensitive);} \
			TestQuery& Contains(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::Contains(value, CaseSensitive);} \
		}; \
\
		class TestQueryQueryAccessorBool : private XQueryAccessorBool { \
		public: \
			TestQueryQueryAccessorBool(size_t column_id) : XQueryAccessorBool(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(bool value) {return (TestQuery &)XQueryAccessorBool::Equal(value);} \
		}; \
\
		TestQueryQueryAccessor##CType1 CName1; \
		TestQueryQueryAccessor##CType2 CName2; \
		TestQueryQueryAccessor##CType3 CName3; \
		TestQueryQueryAccessor##CType4 CName4; \
		TestQueryQueryAccessor##CType5 CName5; \
		TestQueryQueryAccessor##CType6 CName6; \
		TestQueryQueryAccessor##CType7 CName7; \
		TestQueryQueryAccessor##CType8 CName8; \
		TestQueryQueryAccessor##CType9 CName9; \
		TestQueryQueryAccessor##CType10 CName10; \
		TestQueryQueryAccessor##CType11 CName11; \
		TestQueryQueryAccessor##CType12 CName12; \
		TestQueryQueryAccessor##CType13 CName13; \
		TestQueryQueryAccessor##CType14 CName14; \
		TestQueryQueryAccessor##CType15 CName15; \
		TestQueryQueryAccessor##CType16 CName16; \
		TestQueryQueryAccessor##CType17 CName17; \
		TestQueryQueryAccessor##CType18 CName18; \
		TestQueryQueryAccessor##CType19 CName19; \
		TestQueryQueryAccessor##CType20 CName20; \
		TestQueryQueryAccessor##CType21 CName21; \
		TestQueryQueryAccessor##CType22 CName22; \
		TestQueryQueryAccessor##CType23 CName23; \
		TestQueryQueryAccessor##CType24 CName24; \
		TestQueryQueryAccessor##CType25 CName25; \
		TestQueryQueryAccessor##CType26 CName26; \
		TestQueryQueryAccessor##CType27 CName27; \
		TestQueryQueryAccessor##CType28 CName28; \
		TestQueryQueryAccessor##CType29 CName29; \
		TestQueryQueryAccessor##CType30 CName30; \
		TestQueryQueryAccessor##CType31 CName31; \
		TestQueryQueryAccessor##CType32 CName32; \
		TestQueryQueryAccessor##CType33 CName33; \
		TestQueryQueryAccessor##CType34 CName34; \
		TestQueryQueryAccessor##CType35 CName35; \
		TestQueryQueryAccessor##CType36 CName36; \
		TestQueryQueryAccessor##CType37 CName37; \
		TestQueryQueryAccessor##CType38 CName38; \
		TestQueryQueryAccessor##CType39 CName39; \
		TestQueryQueryAccessor##CType40 CName40; \
		TestQueryQueryAccessor##CType41 CName41; \
		TestQueryQueryAccessor##CType42 CName42; \
		TestQueryQueryAccessor##CType43 CName43; \
		TestQueryQueryAccessor##CType44 CName44; \
		TestQueryQueryAccessor##CType45 CName45; \
		TestQueryQueryAccessor##CType46 CName46; \
		TestQueryQueryAccessor##CType47 CName47; \
		TestQueryQueryAccessor##CType48 CName48; \
		TestQueryQueryAccessor##CType49 CName49; \
\
		TestQuery& LeftParan(void) {Query::LeftParan(); return *this;}; \
		TestQuery& Or(void) {Query::Or(); return *this;}; \
		TestQuery& RightParan(void) {Query::RightParan(); return *this;}; \
		TestQuery& Subtable(size_t column) {Query::Subtable(column); return *this;}; \
		TestQuery& Parent() {Query::Parent(); return *this;}; \
	}; \
\
	TestQuery GetQuery() {return TestQuery();} \
\
	class Cursor : public CursorBase { \
	public: \
		Cursor(TableName& table, size_t ndx) : CursorBase(table, ndx) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
			CName17.Create(this, 16); \
			CName18.Create(this, 17); \
			CName19.Create(this, 18); \
			CName20.Create(this, 19); \
			CName21.Create(this, 20); \
			CName22.Create(this, 21); \
			CName23.Create(this, 22); \
			CName24.Create(this, 23); \
			CName25.Create(this, 24); \
			CName26.Create(this, 25); \
			CName27.Create(this, 26); \
			CName28.Create(this, 27); \
			CName29.Create(this, 28); \
			CName30.Create(this, 29); \
			CName31.Create(this, 30); \
			CName32.Create(this, 31); \
			CName33.Create(this, 32); \
			CName34.Create(this, 33); \
			CName35.Create(this, 34); \
			CName36.Create(this, 35); \
			CName37.Create(this, 36); \
			CName38.Create(this, 37); \
			CName39.Create(this, 38); \
			CName40.Create(this, 39); \
			CName41.Create(this, 40); \
			CName42.Create(this, 41); \
			CName43.Create(this, 42); \
			CName44.Create(this, 43); \
			CName45.Create(this, 44); \
			CName46.Create(this, 45); \
			CName47.Create(this, 46); \
			CName48.Create(this, 47); \
			CName49.Create(this, 48); \
		} \
		Cursor(const TableName& table, size_t ndx) : CursorBase(const_cast<TableName&>(table), ndx) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
			CName17.Create(this, 16); \
			CName18.Create(this, 17); \
			CName19.Create(this, 18); \
			CName20.Create(this, 19); \
			CName21.Create(this, 20); \
			CName22.Create(this, 21); \
			CName23.Create(this, 22); \
			CName24.Create(this, 23); \
			CName25.Create(this, 24); \
			CName26.Create(this, 25); \
			CName27.Create(this, 26); \
			CName28.Create(this, 27); \
			CName29.Create(this, 28); \
			CName30.Create(this, 29); \
			CName31.Create(this, 30); \
			CName32.Create(this, 31); \
			CName33.Create(this, 32); \
			CName34.Create(this, 33); \
			CName35.Create(this, 34); \
			CName36.Create(this, 35); \
			CName37.Create(this, 36); \
			CName38.Create(this, 37); \
			CName39.Create(this, 38); \
			CName40.Create(this, 39); \
			CName41.Create(this, 40); \
			CName42.Create(this, 41); \
			CName43.Create(this, 42); \
			CName44.Create(this, 43); \
			CName45.Create(this, 44); \
			CName46.Create(this, 45); \
			CName47.Create(this, 46); \
			CName48.Create(this, 47); \
			CName49.Create(this, 48); \
		} \
		Cursor(const Cursor& v) : CursorBase(v) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
			CName17.Create(this, 16); \
			CName18.Create(this, 17); \
			CName19.Create(this, 18); \
			CName20.Create(this, 19); \
			CName21.Create(this, 20); \
			CName22.Create(this, 21); \
			CName23.Create(this, 22); \
			CName24.Create(this, 23); \
			CName25.Create(this, 24); \
			CName26.Create(this, 25); \
			CName27.Create(this, 26); \
			CName28.Create(this, 27); \
			CName29.Create(this, 28); \
			CName30.Create(this, 29); \
			CName31.Create(this, 30); \
			CName32.Create(this, 31); \
			CName33.Create(this, 32); \
			CName34.Create(this, 33); \
			CName35.Create(this, 34); \
			CName36.Create(this, 35); \
			CName37.Create(this, 36); \
			CName38.Create(this, 37); \
			CName39.Create(this, 38); \
			CName40.Create(this, 39); \
			CName41.Create(this, 40); \
			CName42.Create(this, 41); \
			CName43.Create(this, 42); \
			CName44.Create(this, 43); \
			CName45.Create(this, 44); \
			CName46.Create(this, 45); \
			CName47.Create(this, 46); \
			CName48.Create(this, 47); \
			CName49.Create(this, 48); \
		} \
		Accessor##CType1 CName1; \
		Accessor##CType2 CName2; \
		Accessor##CType3 CName3; \
		Accessor##CType4 CName4; \
		Accessor##CType5 CName5; \
		Accessor##CType6 CName6; \
		Accessor##CType7 CName7; \
		Accessor##CType8 CName8; \
		Accessor##CType9 CName9; \
		Accessor##CType10 CName10; \
		Accessor##CType11 CName11; \
		Accessor##CType12 CName12; \
		Accessor##CType13 CName13; \
		Accessor##CType14 CName14; \
		Accessor##CType15 CName15; \
		Accessor##CType16 CName16; \
		Accessor##CType17 CName17; \
		Accessor##CType18 CName18; \
		Accessor##CType19 CName19; \
		Accessor##CType20 CName20; \
		Accessor##CType21 CName21; \
		Accessor##CType22 CName22; \
		Accessor##CType23 CName23; \
		Accessor##CType24 CName24; \
		Accessor##CType25 CName25; \
		Accessor##CType26 CName26; \
		Accessor##CType27 CName27; \
		Accessor##CType28 CName28; \
		Accessor##CType29 CName29; \
		Accessor##CType30 CName30; \
		Accessor##CType31 CName31; \
		Accessor##CType32 CName32; \
		Accessor##CType33 CName33; \
		Accessor##CType34 CName34; \
		Accessor##CType35 CName35; \
		Accessor##CType36 CName36; \
		Accessor##CType37 CName37; \
		Accessor##CType38 CName38; \
		Accessor##CType39 CName39; \
		Accessor##CType40 CName40; \
		Accessor##CType41 CName41; \
		Accessor##CType42 CName42; \
		Accessor##CType43 CName43; \
		Accessor##CType44 CName44; \
		Accessor##CType45 CName45; \
		Accessor##CType46 CName46; \
		Accessor##CType47 CName47; \
		Accessor##CType48 CName48; \
		Accessor##CType49 CName49; \
	}; \
\
	void Add(tdbType##CType1 CName1, tdbType##CType2 CName2, tdbType##CType3 CName3, tdbType##CType4 CName4, tdbType##CType5 CName5, tdbType##CType6 CName6, tdbType##CType7 CName7, tdbType##CType8 CName8, tdbType##CType9 CName9, tdbType##CType10 CName10, tdbType##CType11 CName11, tdbType##CType12 CName12, tdbType##CType13 CName13, tdbType##CType14 CName14, tdbType##CType15 CName15, tdbType##CType16 CName16, tdbType##CType17 CName17, tdbType##CType18 CName18, tdbType##CType19 CName19, tdbType##CType20 CName20, tdbType##CType21 CName21, tdbType##CType22 CName22, tdbType##CType23 CName23, tdbType##CType24 CName24, tdbType##CType25 CName25, tdbType##CType26 CName26, tdbType##CType27 CName27, tdbType##CType28 CName28, tdbType##CType29 CName29, tdbType##CType30 CName30, tdbType##CType31 CName31, tdbType##CType32 CName32, tdbType##CType33 CName33, tdbType##CType34 CName34, tdbType##CType35 CName35, tdbType##CType36 CName36, tdbType##CType37 CName37, tdbType##CType38 CName38, tdbType##CType39 CName39, tdbType##CType40 CName40, tdbType##CType41 CName41, tdbType##CType42 CName42, tdbType##CType43 CName43, tdbType##CType44 CName44, tdbType##CType45 CName45, tdbType##CType46 CName46, tdbType##CType47 CName47, tdbType##CType48 CName48, tdbType##CType49 CName49) { \
		const size_t ndx = GetSize(); \
		Insert##CType1 (0, ndx, CName1); \
		Insert##CType2 (1, ndx, CName2); \
		Insert##CType3 (2, ndx, CName3); \
		Insert##CType4 (3, ndx, CName4); \
		Insert##CType5 (4, ndx, CName5); \
		Insert##CType6 (5, ndx, CName6); \
		Insert##CType7 (6, ndx, CName7); \
		Insert##CType8 (7, ndx, CName8); \
		Insert##CType9 (8, ndx, CName9); \
		Insert##CType10 (9, ndx, CName10); \
		Insert##CType11 (10, ndx, CName11); \
		Insert##CType12 (11, ndx, CName12); \
		Insert##CType13 (12, ndx, CName13); \
		Insert##CType14 (13, ndx, CName14); \
		Insert##CType15 (14, ndx, CName15); \
		Insert##CType16 (15, ndx, CName16); \
		Insert##CType17 (16, ndx, CName17); \
		Insert##CType18 (17, ndx, CName18); \
		Insert##CType19 (18, ndx, CName19); \
		Insert##CType20 (19, ndx, CName20); \
		Insert##CType21 (20, ndx, CName21); \
		Insert##CType22 (21, ndx, CName22); \
		Insert##CType23 (22, ndx, CName23); \
		Insert##CType24 (23, ndx, CName24); \
		Insert##CType25 (24, ndx, CName25); \
		Insert##CType26 (25, ndx, CName26); \
		Insert##CType27 (26, ndx, CName27); \
		Insert##CType28 (27, ndx, CName28); \
		Insert##CType29 (28, ndx, CName29); \
		Insert##CType30 (29, ndx, CName30); \
		Insert##CType31 (30, ndx, CName31); \
		Insert##CType32 (31, ndx, CName32); \
		Insert##CType33 (32, ndx, CName33); \
		Insert##CType34 (33, ndx, CName34); \
		Insert##CType35 (34, ndx, CName35); \
		Insert##CType36 (35, ndx, CName36); \
		Insert##CType37 (36, ndx, CName37); \
		Insert##CType38 (37, ndx, CName38); \
		Insert##CType39 (38, ndx, CName39); \
		Insert##CType40 (39, ndx, CName40); \
		Insert##CType41 (40, ndx, CName41); \
		Insert##CType42 (41, ndx, CName42); \
		Insert##CType43 (42, ndx, CName43); \
		Insert##CType44 (43, ndx, CName44); \
		Insert##CType45 (44, ndx, CName45); \
		Insert##CType46 (45, ndx, CName46); \
		Insert##CType47 (46, ndx, CName47); \
		Insert##CType48 (47, ndx, CName48); \
		Insert##CType49 (48, ndx, CName49); \
		InsertDone(); \
	} \
\
	void Insert(size_t ndx, tdbType##CType1 CName1, tdbType##CType2 CName2, tdbType##CType3 CName3, tdbType##CType4 CName4, tdbType##CType5 CName5, tdbType##CType6 CName6, tdbType##CType7 CName7, tdbType##CType8 CName8, tdbType##CType9 CName9, tdbType##CType10 CName10, tdbType##CType11 CName11, tdbType##CType12 CName12, tdbType##CType13 CName13, tdbType##CType14 CName14, tdbType##CType15 CName15, tdbType##CType16 CName16, tdbType##CType17 CName17, tdbType##CType18 CName18, tdbType##CType19 CName19, tdbType##CType20 CName20, tdbType##CType21 CName21, tdbType##CType22 CName22, tdbType##CType23 CName23, tdbType##CType24 CName24, tdbType##CType25 CName25, tdbType##CType26 CName26, tdbType##CType27 CName27, tdbType##CType28 CName28, tdbType##CType29 CName29, tdbType##CType30 CName30, tdbType##CType31 CName31, tdbType##CType32 CName32, tdbType##CType33 CName33, tdbType##CType34 CName34, tdbType##CType35 CName35, tdbType##CType36 CName36, tdbType##CType37 CName37, tdbType##CType38 CName38, tdbType##CType39 CName39, tdbType##CType40 CName40, tdbType##CType41 CName41, tdbType##CType42 CName42, tdbType##CType43 CName43, tdbType##CType44 CName44, tdbType##CType45 CName45, tdbType##CType46 CName46, tdbType##CType47 CName47, tdbType##CType48 CName48, tdbType##CType49 CName49) { \
		Insert##CType1 (0, ndx, CName1); \
		Insert##CType2 (1, ndx, CName2); \
		Insert##CType3 (2, ndx, CName3); \
		Insert##CType4 (3, ndx, CName4); \
		Insert##CType5 (4, ndx, CName5); \
		Insert##CType6 (5, ndx, CName6); \
		Insert##CType7 (6, ndx, CName7); \
		Insert##CType8 (7, ndx, CName8); \
		Insert##CType9 (8, ndx, CName9); \
		Insert##CType10 (9, ndx, CName10); \
		Insert##CType11 (10, ndx, CName11); \
		Insert##CType12 (11, ndx, CName12); \
		Insert##CType13 (12, ndx, CName13); \
		Insert##CType14 (13, ndx, CName14); \
		Insert##CType15 (14, ndx, CName15); \
		Insert##CType16 (15, ndx, CName16); \
		Insert##CType17 (16, ndx, CName17); \
		Insert##CType18 (17, ndx, CName18); \
		Insert##CType19 (18, ndx, CName19); \
		Insert##CType20 (19, ndx, CName20); \
		Insert##CType21 (20, ndx, CName21); \
		Insert##CType22 (21, ndx, CName22); \
		Insert##CType23 (22, ndx, CName23); \
		Insert##CType24 (23, ndx, CName24); \
		Insert##CType25 (24, ndx, CName25); \
		Insert##CType26 (25, ndx, CName26); \
		Insert##CType27 (26, ndx, CName27); \
		Insert##CType28 (27, ndx, CName28); \
		Insert##CType29 (28, ndx, CName29); \
		Insert##CType30 (29, ndx, CName30); \
		Insert##CType31 (30, ndx, CName31); \
		Insert##CType32 (31, ndx, CName32); \
		Insert##CType33 (32, ndx, CName33); \
		Insert##CType34 (33, ndx, CName34); \
		Insert##CType35 (34, ndx, CName35); \
		Insert##CType36 (35, ndx, CName36); \
		Insert##CType37 (36, ndx, CName37); \
		Insert##CType38 (37, ndx, CName38); \
		Insert##CType39 (38, ndx, CName39); \
		Insert##CType40 (39, ndx, CName40); \
		Insert##CType41 (40, ndx, CName41); \
		Insert##CType42 (41, ndx, CName42); \
		Insert##CType43 (42, ndx, CName43); \
		Insert##CType44 (43, ndx, CName44); \
		Insert##CType45 (44, ndx, CName45); \
		Insert##CType46 (45, ndx, CName46); \
		Insert##CType47 (46, ndx, CName47); \
		Insert##CType48 (47, ndx, CName48); \
		Insert##CType49 (48, ndx, CName49); \
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
	ColumnProxy##CType3 CName3; \
	ColumnProxy##CType4 CName4; \
	ColumnProxy##CType5 CName5; \
	ColumnProxy##CType6 CName6; \
	ColumnProxy##CType7 CName7; \
	ColumnProxy##CType8 CName8; \
	ColumnProxy##CType9 CName9; \
	ColumnProxy##CType10 CName10; \
	ColumnProxy##CType11 CName11; \
	ColumnProxy##CType12 CName12; \
	ColumnProxy##CType13 CName13; \
	ColumnProxy##CType14 CName14; \
	ColumnProxy##CType15 CName15; \
	ColumnProxy##CType16 CName16; \
	ColumnProxy##CType17 CName17; \
	ColumnProxy##CType18 CName18; \
	ColumnProxy##CType19 CName19; \
	ColumnProxy##CType20 CName20; \
	ColumnProxy##CType21 CName21; \
	ColumnProxy##CType22 CName22; \
	ColumnProxy##CType23 CName23; \
	ColumnProxy##CType24 CName24; \
	ColumnProxy##CType25 CName25; \
	ColumnProxy##CType26 CName26; \
	ColumnProxy##CType27 CName27; \
	ColumnProxy##CType28 CName28; \
	ColumnProxy##CType29 CName29; \
	ColumnProxy##CType30 CName30; \
	ColumnProxy##CType31 CName31; \
	ColumnProxy##CType32 CName32; \
	ColumnProxy##CType33 CName33; \
	ColumnProxy##CType34 CName34; \
	ColumnProxy##CType35 CName35; \
	ColumnProxy##CType36 CName36; \
	ColumnProxy##CType37 CName37; \
	ColumnProxy##CType38 CName38; \
	ColumnProxy##CType39 CName39; \
	ColumnProxy##CType40 CName40; \
	ColumnProxy##CType41 CName41; \
	ColumnProxy##CType42 CName42; \
	ColumnProxy##CType43 CName43; \
	ColumnProxy##CType44 CName44; \
	ColumnProxy##CType45 CName45; \
	ColumnProxy##CType46 CName46; \
	ColumnProxy##CType47 CName47; \
	ColumnProxy##CType48 CName48; \
	ColumnProxy##CType49 CName49; \
\
protected: \
	friend class Group; \
	TableName(Allocator& alloc, size_t ref, Array* parent, size_t pndx) : TopLevelTable(alloc, ref, parent, pndx) {}; \
\
private: \
	TableName(const TableName&) {} \
	TableName& operator=(const TableName&) {return *this;} \
};



#define TDB_TABLE_50(TableName, CType1, CName1, CType2, CName2, CType3, CName3, CType4, CName4, CType5, CName5, CType6, CName6, CType7, CName7, CType8, CName8, CType9, CName9, CType10, CName10, CType11, CName11, CType12, CName12, CType13, CName13, CType14, CName14, CType15, CName15, CType16, CName16, CType17, CName17, CType18, CName18, CType19, CName19, CType20, CName20, CType21, CName21, CType22, CName22, CType23, CName23, CType24, CName24, CType25, CName25, CType26, CName26, CType27, CName27, CType28, CName28, CType29, CName29, CType30, CName30, CType31, CName31, CType32, CName32, CType33, CName33, CType34, CName34, CType35, CName35, CType36, CName36, CType37, CName37, CType38, CName38, CType39, CName39, CType40, CName40, CType41, CName41, CType42, CName42, CType43, CName43, CType44, CName44, CType45, CName45, CType46, CName46, CType47, CName47, CType48, CName48, CType49, CName49, CType50, CName50) \
class TableName##Query { \
protected: \
	QueryAccessor##CType1 CName1; \
	QueryAccessor##CType2 CName2; \
	QueryAccessor##CType3 CName3; \
	QueryAccessor##CType4 CName4; \
	QueryAccessor##CType5 CName5; \
	QueryAccessor##CType6 CName6; \
	QueryAccessor##CType7 CName7; \
	QueryAccessor##CType8 CName8; \
	QueryAccessor##CType9 CName9; \
	QueryAccessor##CType10 CName10; \
	QueryAccessor##CType11 CName11; \
	QueryAccessor##CType12 CName12; \
	QueryAccessor##CType13 CName13; \
	QueryAccessor##CType14 CName14; \
	QueryAccessor##CType15 CName15; \
	QueryAccessor##CType16 CName16; \
	QueryAccessor##CType17 CName17; \
	QueryAccessor##CType18 CName18; \
	QueryAccessor##CType19 CName19; \
	QueryAccessor##CType20 CName20; \
	QueryAccessor##CType21 CName21; \
	QueryAccessor##CType22 CName22; \
	QueryAccessor##CType23 CName23; \
	QueryAccessor##CType24 CName24; \
	QueryAccessor##CType25 CName25; \
	QueryAccessor##CType26 CName26; \
	QueryAccessor##CType27 CName27; \
	QueryAccessor##CType28 CName28; \
	QueryAccessor##CType29 CName29; \
	QueryAccessor##CType30 CName30; \
	QueryAccessor##CType31 CName31; \
	QueryAccessor##CType32 CName32; \
	QueryAccessor##CType33 CName33; \
	QueryAccessor##CType34 CName34; \
	QueryAccessor##CType35 CName35; \
	QueryAccessor##CType36 CName36; \
	QueryAccessor##CType37 CName37; \
	QueryAccessor##CType38 CName38; \
	QueryAccessor##CType39 CName39; \
	QueryAccessor##CType40 CName40; \
	QueryAccessor##CType41 CName41; \
	QueryAccessor##CType42 CName42; \
	QueryAccessor##CType43 CName43; \
	QueryAccessor##CType44 CName44; \
	QueryAccessor##CType45 CName45; \
	QueryAccessor##CType46 CName46; \
	QueryAccessor##CType47 CName47; \
	QueryAccessor##CType48 CName48; \
	QueryAccessor##CType49 CName49; \
	QueryAccessor##CType50 CName50; \
}; \
\
class TableName : public TopLevelTable { \
public: \
	TableName(Allocator& alloc=GetDefaultAllocator()) : TopLevelTable(alloc) { \
		RegisterColumn(Accessor##CType1::type, #CName1); \
		RegisterColumn(Accessor##CType2::type, #CName2); \
		RegisterColumn(Accessor##CType3::type, #CName3); \
		RegisterColumn(Accessor##CType4::type, #CName4); \
		RegisterColumn(Accessor##CType5::type, #CName5); \
		RegisterColumn(Accessor##CType6::type, #CName6); \
		RegisterColumn(Accessor##CType7::type, #CName7); \
		RegisterColumn(Accessor##CType8::type, #CName8); \
		RegisterColumn(Accessor##CType9::type, #CName9); \
		RegisterColumn(Accessor##CType10::type, #CName10); \
		RegisterColumn(Accessor##CType11::type, #CName11); \
		RegisterColumn(Accessor##CType12::type, #CName12); \
		RegisterColumn(Accessor##CType13::type, #CName13); \
		RegisterColumn(Accessor##CType14::type, #CName14); \
		RegisterColumn(Accessor##CType15::type, #CName15); \
		RegisterColumn(Accessor##CType16::type, #CName16); \
		RegisterColumn(Accessor##CType17::type, #CName17); \
		RegisterColumn(Accessor##CType18::type, #CName18); \
		RegisterColumn(Accessor##CType19::type, #CName19); \
		RegisterColumn(Accessor##CType20::type, #CName20); \
		RegisterColumn(Accessor##CType21::type, #CName21); \
		RegisterColumn(Accessor##CType22::type, #CName22); \
		RegisterColumn(Accessor##CType23::type, #CName23); \
		RegisterColumn(Accessor##CType24::type, #CName24); \
		RegisterColumn(Accessor##CType25::type, #CName25); \
		RegisterColumn(Accessor##CType26::type, #CName26); \
		RegisterColumn(Accessor##CType27::type, #CName27); \
		RegisterColumn(Accessor##CType28::type, #CName28); \
		RegisterColumn(Accessor##CType29::type, #CName29); \
		RegisterColumn(Accessor##CType30::type, #CName30); \
		RegisterColumn(Accessor##CType31::type, #CName31); \
		RegisterColumn(Accessor##CType32::type, #CName32); \
		RegisterColumn(Accessor##CType33::type, #CName33); \
		RegisterColumn(Accessor##CType34::type, #CName34); \
		RegisterColumn(Accessor##CType35::type, #CName35); \
		RegisterColumn(Accessor##CType36::type, #CName36); \
		RegisterColumn(Accessor##CType37::type, #CName37); \
		RegisterColumn(Accessor##CType38::type, #CName38); \
		RegisterColumn(Accessor##CType39::type, #CName39); \
		RegisterColumn(Accessor##CType40::type, #CName40); \
		RegisterColumn(Accessor##CType41::type, #CName41); \
		RegisterColumn(Accessor##CType42::type, #CName42); \
		RegisterColumn(Accessor##CType43::type, #CName43); \
		RegisterColumn(Accessor##CType44::type, #CName44); \
		RegisterColumn(Accessor##CType45::type, #CName45); \
		RegisterColumn(Accessor##CType46::type, #CName46); \
		RegisterColumn(Accessor##CType47::type, #CName47); \
		RegisterColumn(Accessor##CType48::type, #CName48); \
		RegisterColumn(Accessor##CType49::type, #CName49); \
		RegisterColumn(Accessor##CType50::type, #CName50); \
\
		CName1.Create(this, 0); \
		CName2.Create(this, 1); \
		CName3.Create(this, 2); \
		CName4.Create(this, 3); \
		CName5.Create(this, 4); \
		CName6.Create(this, 5); \
		CName7.Create(this, 6); \
		CName8.Create(this, 7); \
		CName9.Create(this, 8); \
		CName10.Create(this, 9); \
		CName11.Create(this, 10); \
		CName12.Create(this, 11); \
		CName13.Create(this, 12); \
		CName14.Create(this, 13); \
		CName15.Create(this, 14); \
		CName16.Create(this, 15); \
		CName17.Create(this, 16); \
		CName18.Create(this, 17); \
		CName19.Create(this, 18); \
		CName20.Create(this, 19); \
		CName21.Create(this, 20); \
		CName22.Create(this, 21); \
		CName23.Create(this, 22); \
		CName24.Create(this, 23); \
		CName25.Create(this, 24); \
		CName26.Create(this, 25); \
		CName27.Create(this, 26); \
		CName28.Create(this, 27); \
		CName29.Create(this, 28); \
		CName30.Create(this, 29); \
		CName31.Create(this, 30); \
		CName32.Create(this, 31); \
		CName33.Create(this, 32); \
		CName34.Create(this, 33); \
		CName35.Create(this, 34); \
		CName36.Create(this, 35); \
		CName37.Create(this, 36); \
		CName38.Create(this, 37); \
		CName39.Create(this, 38); \
		CName40.Create(this, 39); \
		CName41.Create(this, 40); \
		CName42.Create(this, 41); \
		CName43.Create(this, 42); \
		CName44.Create(this, 43); \
		CName45.Create(this, 44); \
		CName46.Create(this, 45); \
		CName47.Create(this, 46); \
		CName48.Create(this, 47); \
		CName49.Create(this, 48); \
		CName50.Create(this, 49); \
	}; \
\
	class TestQuery : public Query { \
	public: \
		TestQuery() : CName1(0), CName2(1), CName3(2), CName4(3), CName5(4), CName6(5), CName7(6), CName8(7), CName9(8), CName10(9), CName11(10), CName12(11), CName13(12), CName14(13), CName15(14), CName16(15), CName17(16), CName18(17), CName19(18), CName20(19), CName21(20), CName22(21), CName23(22), CName24(23), CName25(24), CName26(25), CName27(26), CName28(27), CName29(28), CName30(29), CName31(30), CName32(31), CName33(32), CName34(33), CName35(34), CName36(35), CName37(36), CName38(37), CName39(38), CName40(39), CName41(40), CName42(41), CName43(42), CName44(43), CName45(44), CName46(45), CName47(46), CName48(47), CName49(48), CName50(49) { \
			CName1.SetQuery(this); \
			CName2.SetQuery(this); \
			CName3.SetQuery(this); \
			CName4.SetQuery(this); \
			CName5.SetQuery(this); \
			CName6.SetQuery(this); \
			CName7.SetQuery(this); \
			CName8.SetQuery(this); \
			CName9.SetQuery(this); \
			CName10.SetQuery(this); \
			CName11.SetQuery(this); \
			CName12.SetQuery(this); \
			CName13.SetQuery(this); \
			CName14.SetQuery(this); \
			CName15.SetQuery(this); \
			CName16.SetQuery(this); \
			CName17.SetQuery(this); \
			CName18.SetQuery(this); \
			CName19.SetQuery(this); \
			CName20.SetQuery(this); \
			CName21.SetQuery(this); \
			CName22.SetQuery(this); \
			CName23.SetQuery(this); \
			CName24.SetQuery(this); \
			CName25.SetQuery(this); \
			CName26.SetQuery(this); \
			CName27.SetQuery(this); \
			CName28.SetQuery(this); \
			CName29.SetQuery(this); \
			CName30.SetQuery(this); \
			CName31.SetQuery(this); \
			CName32.SetQuery(this); \
			CName33.SetQuery(this); \
			CName34.SetQuery(this); \
			CName35.SetQuery(this); \
			CName36.SetQuery(this); \
			CName37.SetQuery(this); \
			CName38.SetQuery(this); \
			CName39.SetQuery(this); \
			CName40.SetQuery(this); \
			CName41.SetQuery(this); \
			CName42.SetQuery(this); \
			CName43.SetQuery(this); \
			CName44.SetQuery(this); \
			CName45.SetQuery(this); \
			CName46.SetQuery(this); \
			CName47.SetQuery(this); \
			CName48.SetQuery(this); \
			CName49.SetQuery(this); \
			CName50.SetQuery(this); \
		} \
\
		TestQuery(const TestQuery& copy) : Query(copy), CName1(0), CName2(1), CName3(2), CName4(3), CName5(4), CName6(5), CName7(6), CName8(7), CName9(8), CName10(9), CName11(10), CName12(11), CName13(12), CName14(13), CName15(14), CName16(15), CName17(16), CName18(17), CName19(18), CName20(19), CName21(20), CName22(21), CName23(22), CName24(23), CName25(24), CName26(25), CName27(26), CName28(27), CName29(28), CName30(29), CName31(30), CName32(31), CName33(32), CName34(33), CName35(34), CName36(35), CName37(36), CName38(37), CName39(38), CName40(39), CName41(40), CName42(41), CName43(42), CName44(43), CName45(44), CName46(45), CName47(46), CName48(47), CName49(48), CName50(49) { \
			CName1.SetQuery(this); \
			CName2.SetQuery(this); \
			CName3.SetQuery(this); \
			CName4.SetQuery(this); \
			CName5.SetQuery(this); \
			CName6.SetQuery(this); \
			CName7.SetQuery(this); \
			CName8.SetQuery(this); \
			CName9.SetQuery(this); \
			CName10.SetQuery(this); \
			CName11.SetQuery(this); \
			CName12.SetQuery(this); \
			CName13.SetQuery(this); \
			CName14.SetQuery(this); \
			CName15.SetQuery(this); \
			CName16.SetQuery(this); \
			CName17.SetQuery(this); \
			CName18.SetQuery(this); \
			CName19.SetQuery(this); \
			CName20.SetQuery(this); \
			CName21.SetQuery(this); \
			CName22.SetQuery(this); \
			CName23.SetQuery(this); \
			CName24.SetQuery(this); \
			CName25.SetQuery(this); \
			CName26.SetQuery(this); \
			CName27.SetQuery(this); \
			CName28.SetQuery(this); \
			CName29.SetQuery(this); \
			CName30.SetQuery(this); \
			CName31.SetQuery(this); \
			CName32.SetQuery(this); \
			CName33.SetQuery(this); \
			CName34.SetQuery(this); \
			CName35.SetQuery(this); \
			CName36.SetQuery(this); \
			CName37.SetQuery(this); \
			CName38.SetQuery(this); \
			CName39.SetQuery(this); \
			CName40.SetQuery(this); \
			CName41.SetQuery(this); \
			CName42.SetQuery(this); \
			CName43.SetQuery(this); \
			CName44.SetQuery(this); \
			CName45.SetQuery(this); \
			CName46.SetQuery(this); \
			CName47.SetQuery(this); \
			CName48.SetQuery(this); \
			CName49.SetQuery(this); \
			CName50.SetQuery(this); \
		} \
\
		class TestQueryQueryAccessorInt : private XQueryAccessorInt { \
		public: \
			TestQueryQueryAccessorInt(size_t column_id) : XQueryAccessorInt(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(int64_t value) {return (TestQuery &)XQueryAccessorInt::Equal(value);} \
			TestQuery& NotEqual(int64_t value) {return (TestQuery &)XQueryAccessorInt::NotEqual(value);} \
			TestQuery& Greater(int64_t value) {return (TestQuery &)XQueryAccessorInt::Greater(value);} \
			TestQuery& Less(int64_t value) {return (TestQuery &)XQueryAccessorInt::Less(value);} \
			TestQuery& Between(int64_t from, int64_t to) {return (TestQuery &)XQueryAccessorInt::Between(from, to);} \
		}; \
\
		template <class T> class TestQueryQueryAccessorEnum : public TestQueryQueryAccessorInt { \
		public: \
			TestQueryQueryAccessorEnum<T>(size_t column_id) : TestQueryQueryAccessorInt(column_id) {} \
		}; \
\
		class TestQueryQueryAccessorString : private XQueryAccessorString { \
		public: \
			TestQueryQueryAccessorString(size_t column_id) : XQueryAccessorString(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::Equal(value, CaseSensitive);} \
			TestQuery& NotEqual(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::NotEqual(value, CaseSensitive);} \
			TestQuery& BeginsWith(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::BeginsWith(value, CaseSensitive);} \
			TestQuery& EndsWith(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::EndsWith(value, CaseSensitive);} \
			TestQuery& Contains(const char *value, bool CaseSensitive = true) {return (TestQuery &)XQueryAccessorString::Contains(value, CaseSensitive);} \
		}; \
\
		class TestQueryQueryAccessorBool : private XQueryAccessorBool { \
		public: \
			TestQueryQueryAccessorBool(size_t column_id) : XQueryAccessorBool(column_id) {} \
			void SetQuery(Query* query) {m_query = query;} \
\
			TestQuery& Equal(bool value) {return (TestQuery &)XQueryAccessorBool::Equal(value);} \
		}; \
\
		TestQueryQueryAccessor##CType1 CName1; \
		TestQueryQueryAccessor##CType2 CName2; \
		TestQueryQueryAccessor##CType3 CName3; \
		TestQueryQueryAccessor##CType4 CName4; \
		TestQueryQueryAccessor##CType5 CName5; \
		TestQueryQueryAccessor##CType6 CName6; \
		TestQueryQueryAccessor##CType7 CName7; \
		TestQueryQueryAccessor##CType8 CName8; \
		TestQueryQueryAccessor##CType9 CName9; \
		TestQueryQueryAccessor##CType10 CName10; \
		TestQueryQueryAccessor##CType11 CName11; \
		TestQueryQueryAccessor##CType12 CName12; \
		TestQueryQueryAccessor##CType13 CName13; \
		TestQueryQueryAccessor##CType14 CName14; \
		TestQueryQueryAccessor##CType15 CName15; \
		TestQueryQueryAccessor##CType16 CName16; \
		TestQueryQueryAccessor##CType17 CName17; \
		TestQueryQueryAccessor##CType18 CName18; \
		TestQueryQueryAccessor##CType19 CName19; \
		TestQueryQueryAccessor##CType20 CName20; \
		TestQueryQueryAccessor##CType21 CName21; \
		TestQueryQueryAccessor##CType22 CName22; \
		TestQueryQueryAccessor##CType23 CName23; \
		TestQueryQueryAccessor##CType24 CName24; \
		TestQueryQueryAccessor##CType25 CName25; \
		TestQueryQueryAccessor##CType26 CName26; \
		TestQueryQueryAccessor##CType27 CName27; \
		TestQueryQueryAccessor##CType28 CName28; \
		TestQueryQueryAccessor##CType29 CName29; \
		TestQueryQueryAccessor##CType30 CName30; \
		TestQueryQueryAccessor##CType31 CName31; \
		TestQueryQueryAccessor##CType32 CName32; \
		TestQueryQueryAccessor##CType33 CName33; \
		TestQueryQueryAccessor##CType34 CName34; \
		TestQueryQueryAccessor##CType35 CName35; \
		TestQueryQueryAccessor##CType36 CName36; \
		TestQueryQueryAccessor##CType37 CName37; \
		TestQueryQueryAccessor##CType38 CName38; \
		TestQueryQueryAccessor##CType39 CName39; \
		TestQueryQueryAccessor##CType40 CName40; \
		TestQueryQueryAccessor##CType41 CName41; \
		TestQueryQueryAccessor##CType42 CName42; \
		TestQueryQueryAccessor##CType43 CName43; \
		TestQueryQueryAccessor##CType44 CName44; \
		TestQueryQueryAccessor##CType45 CName45; \
		TestQueryQueryAccessor##CType46 CName46; \
		TestQueryQueryAccessor##CType47 CName47; \
		TestQueryQueryAccessor##CType48 CName48; \
		TestQueryQueryAccessor##CType49 CName49; \
		TestQueryQueryAccessor##CType50 CName50; \
\
		TestQuery& LeftParan(void) {Query::LeftParan(); return *this;}; \
		TestQuery& Or(void) {Query::Or(); return *this;}; \
		TestQuery& RightParan(void) {Query::RightParan(); return *this;}; \
		TestQuery& Subtable(size_t column) {Query::Subtable(column); return *this;}; \
		TestQuery& Parent() {Query::Parent(); return *this;}; \
	}; \
\
	TestQuery GetQuery() {return TestQuery();} \
\
	class Cursor : public CursorBase { \
	public: \
		Cursor(TableName& table, size_t ndx) : CursorBase(table, ndx) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
			CName17.Create(this, 16); \
			CName18.Create(this, 17); \
			CName19.Create(this, 18); \
			CName20.Create(this, 19); \
			CName21.Create(this, 20); \
			CName22.Create(this, 21); \
			CName23.Create(this, 22); \
			CName24.Create(this, 23); \
			CName25.Create(this, 24); \
			CName26.Create(this, 25); \
			CName27.Create(this, 26); \
			CName28.Create(this, 27); \
			CName29.Create(this, 28); \
			CName30.Create(this, 29); \
			CName31.Create(this, 30); \
			CName32.Create(this, 31); \
			CName33.Create(this, 32); \
			CName34.Create(this, 33); \
			CName35.Create(this, 34); \
			CName36.Create(this, 35); \
			CName37.Create(this, 36); \
			CName38.Create(this, 37); \
			CName39.Create(this, 38); \
			CName40.Create(this, 39); \
			CName41.Create(this, 40); \
			CName42.Create(this, 41); \
			CName43.Create(this, 42); \
			CName44.Create(this, 43); \
			CName45.Create(this, 44); \
			CName46.Create(this, 45); \
			CName47.Create(this, 46); \
			CName48.Create(this, 47); \
			CName49.Create(this, 48); \
			CName50.Create(this, 49); \
		} \
		Cursor(const TableName& table, size_t ndx) : CursorBase(const_cast<TableName&>(table), ndx) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
			CName17.Create(this, 16); \
			CName18.Create(this, 17); \
			CName19.Create(this, 18); \
			CName20.Create(this, 19); \
			CName21.Create(this, 20); \
			CName22.Create(this, 21); \
			CName23.Create(this, 22); \
			CName24.Create(this, 23); \
			CName25.Create(this, 24); \
			CName26.Create(this, 25); \
			CName27.Create(this, 26); \
			CName28.Create(this, 27); \
			CName29.Create(this, 28); \
			CName30.Create(this, 29); \
			CName31.Create(this, 30); \
			CName32.Create(this, 31); \
			CName33.Create(this, 32); \
			CName34.Create(this, 33); \
			CName35.Create(this, 34); \
			CName36.Create(this, 35); \
			CName37.Create(this, 36); \
			CName38.Create(this, 37); \
			CName39.Create(this, 38); \
			CName40.Create(this, 39); \
			CName41.Create(this, 40); \
			CName42.Create(this, 41); \
			CName43.Create(this, 42); \
			CName44.Create(this, 43); \
			CName45.Create(this, 44); \
			CName46.Create(this, 45); \
			CName47.Create(this, 46); \
			CName48.Create(this, 47); \
			CName49.Create(this, 48); \
			CName50.Create(this, 49); \
		} \
		Cursor(const Cursor& v) : CursorBase(v) { \
			CName1.Create(this, 0); \
			CName2.Create(this, 1); \
			CName3.Create(this, 2); \
			CName4.Create(this, 3); \
			CName5.Create(this, 4); \
			CName6.Create(this, 5); \
			CName7.Create(this, 6); \
			CName8.Create(this, 7); \
			CName9.Create(this, 8); \
			CName10.Create(this, 9); \
			CName11.Create(this, 10); \
			CName12.Create(this, 11); \
			CName13.Create(this, 12); \
			CName14.Create(this, 13); \
			CName15.Create(this, 14); \
			CName16.Create(this, 15); \
			CName17.Create(this, 16); \
			CName18.Create(this, 17); \
			CName19.Create(this, 18); \
			CName20.Create(this, 19); \
			CName21.Create(this, 20); \
			CName22.Create(this, 21); \
			CName23.Create(this, 22); \
			CName24.Create(this, 23); \
			CName25.Create(this, 24); \
			CName26.Create(this, 25); \
			CName27.Create(this, 26); \
			CName28.Create(this, 27); \
			CName29.Create(this, 28); \
			CName30.Create(this, 29); \
			CName31.Create(this, 30); \
			CName32.Create(this, 31); \
			CName33.Create(this, 32); \
			CName34.Create(this, 33); \
			CName35.Create(this, 34); \
			CName36.Create(this, 35); \
			CName37.Create(this, 36); \
			CName38.Create(this, 37); \
			CName39.Create(this, 38); \
			CName40.Create(this, 39); \
			CName41.Create(this, 40); \
			CName42.Create(this, 41); \
			CName43.Create(this, 42); \
			CName44.Create(this, 43); \
			CName45.Create(this, 44); \
			CName46.Create(this, 45); \
			CName47.Create(this, 46); \
			CName48.Create(this, 47); \
			CName49.Create(this, 48); \
			CName50.Create(this, 49); \
		} \
		Accessor##CType1 CName1; \
		Accessor##CType2 CName2; \
		Accessor##CType3 CName3; \
		Accessor##CType4 CName4; \
		Accessor##CType5 CName5; \
		Accessor##CType6 CName6; \
		Accessor##CType7 CName7; \
		Accessor##CType8 CName8; \
		Accessor##CType9 CName9; \
		Accessor##CType10 CName10; \
		Accessor##CType11 CName11; \
		Accessor##CType12 CName12; \
		Accessor##CType13 CName13; \
		Accessor##CType14 CName14; \
		Accessor##CType15 CName15; \
		Accessor##CType16 CName16; \
		Accessor##CType17 CName17; \
		Accessor##CType18 CName18; \
		Accessor##CType19 CName19; \
		Accessor##CType20 CName20; \
		Accessor##CType21 CName21; \
		Accessor##CType22 CName22; \
		Accessor##CType23 CName23; \
		Accessor##CType24 CName24; \
		Accessor##CType25 CName25; \
		Accessor##CType26 CName26; \
		Accessor##CType27 CName27; \
		Accessor##CType28 CName28; \
		Accessor##CType29 CName29; \
		Accessor##CType30 CName30; \
		Accessor##CType31 CName31; \
		Accessor##CType32 CName32; \
		Accessor##CType33 CName33; \
		Accessor##CType34 CName34; \
		Accessor##CType35 CName35; \
		Accessor##CType36 CName36; \
		Accessor##CType37 CName37; \
		Accessor##CType38 CName38; \
		Accessor##CType39 CName39; \
		Accessor##CType40 CName40; \
		Accessor##CType41 CName41; \
		Accessor##CType42 CName42; \
		Accessor##CType43 CName43; \
		Accessor##CType44 CName44; \
		Accessor##CType45 CName45; \
		Accessor##CType46 CName46; \
		Accessor##CType47 CName47; \
		Accessor##CType48 CName48; \
		Accessor##CType49 CName49; \
		Accessor##CType50 CName50; \
	}; \
\
	void Add(tdbType##CType1 CName1, tdbType##CType2 CName2, tdbType##CType3 CName3, tdbType##CType4 CName4, tdbType##CType5 CName5, tdbType##CType6 CName6, tdbType##CType7 CName7, tdbType##CType8 CName8, tdbType##CType9 CName9, tdbType##CType10 CName10, tdbType##CType11 CName11, tdbType##CType12 CName12, tdbType##CType13 CName13, tdbType##CType14 CName14, tdbType##CType15 CName15, tdbType##CType16 CName16, tdbType##CType17 CName17, tdbType##CType18 CName18, tdbType##CType19 CName19, tdbType##CType20 CName20, tdbType##CType21 CName21, tdbType##CType22 CName22, tdbType##CType23 CName23, tdbType##CType24 CName24, tdbType##CType25 CName25, tdbType##CType26 CName26, tdbType##CType27 CName27, tdbType##CType28 CName28, tdbType##CType29 CName29, tdbType##CType30 CName30, tdbType##CType31 CName31, tdbType##CType32 CName32, tdbType##CType33 CName33, tdbType##CType34 CName34, tdbType##CType35 CName35, tdbType##CType36 CName36, tdbType##CType37 CName37, tdbType##CType38 CName38, tdbType##CType39 CName39, tdbType##CType40 CName40, tdbType##CType41 CName41, tdbType##CType42 CName42, tdbType##CType43 CName43, tdbType##CType44 CName44, tdbType##CType45 CName45, tdbType##CType46 CName46, tdbType##CType47 CName47, tdbType##CType48 CName48, tdbType##CType49 CName49, tdbType##CType50 CName50) { \
		const size_t ndx = GetSize(); \
		Insert##CType1 (0, ndx, CName1); \
		Insert##CType2 (1, ndx, CName2); \
		Insert##CType3 (2, ndx, CName3); \
		Insert##CType4 (3, ndx, CName4); \
		Insert##CType5 (4, ndx, CName5); \
		Insert##CType6 (5, ndx, CName6); \
		Insert##CType7 (6, ndx, CName7); \
		Insert##CType8 (7, ndx, CName8); \
		Insert##CType9 (8, ndx, CName9); \
		Insert##CType10 (9, ndx, CName10); \
		Insert##CType11 (10, ndx, CName11); \
		Insert##CType12 (11, ndx, CName12); \
		Insert##CType13 (12, ndx, CName13); \
		Insert##CType14 (13, ndx, CName14); \
		Insert##CType15 (14, ndx, CName15); \
		Insert##CType16 (15, ndx, CName16); \
		Insert##CType17 (16, ndx, CName17); \
		Insert##CType18 (17, ndx, CName18); \
		Insert##CType19 (18, ndx, CName19); \
		Insert##CType20 (19, ndx, CName20); \
		Insert##CType21 (20, ndx, CName21); \
		Insert##CType22 (21, ndx, CName22); \
		Insert##CType23 (22, ndx, CName23); \
		Insert##CType24 (23, ndx, CName24); \
		Insert##CType25 (24, ndx, CName25); \
		Insert##CType26 (25, ndx, CName26); \
		Insert##CType27 (26, ndx, CName27); \
		Insert##CType28 (27, ndx, CName28); \
		Insert##CType29 (28, ndx, CName29); \
		Insert##CType30 (29, ndx, CName30); \
		Insert##CType31 (30, ndx, CName31); \
		Insert##CType32 (31, ndx, CName32); \
		Insert##CType33 (32, ndx, CName33); \
		Insert##CType34 (33, ndx, CName34); \
		Insert##CType35 (34, ndx, CName35); \
		Insert##CType36 (35, ndx, CName36); \
		Insert##CType37 (36, ndx, CName37); \
		Insert##CType38 (37, ndx, CName38); \
		Insert##CType39 (38, ndx, CName39); \
		Insert##CType40 (39, ndx, CName40); \
		Insert##CType41 (40, ndx, CName41); \
		Insert##CType42 (41, ndx, CName42); \
		Insert##CType43 (42, ndx, CName43); \
		Insert##CType44 (43, ndx, CName44); \
		Insert##CType45 (44, ndx, CName45); \
		Insert##CType46 (45, ndx, CName46); \
		Insert##CType47 (46, ndx, CName47); \
		Insert##CType48 (47, ndx, CName48); \
		Insert##CType49 (48, ndx, CName49); \
		Insert##CType50 (49, ndx, CName50); \
		InsertDone(); \
	} \
\
	void Insert(size_t ndx, tdbType##CType1 CName1, tdbType##CType2 CName2, tdbType##CType3 CName3, tdbType##CType4 CName4, tdbType##CType5 CName5, tdbType##CType6 CName6, tdbType##CType7 CName7, tdbType##CType8 CName8, tdbType##CType9 CName9, tdbType##CType10 CName10, tdbType##CType11 CName11, tdbType##CType12 CName12, tdbType##CType13 CName13, tdbType##CType14 CName14, tdbType##CType15 CName15, tdbType##CType16 CName16, tdbType##CType17 CName17, tdbType##CType18 CName18, tdbType##CType19 CName19, tdbType##CType20 CName20, tdbType##CType21 CName21, tdbType##CType22 CName22, tdbType##CType23 CName23, tdbType##CType24 CName24, tdbType##CType25 CName25, tdbType##CType26 CName26, tdbType##CType27 CName27, tdbType##CType28 CName28, tdbType##CType29 CName29, tdbType##CType30 CName30, tdbType##CType31 CName31, tdbType##CType32 CName32, tdbType##CType33 CName33, tdbType##CType34 CName34, tdbType##CType35 CName35, tdbType##CType36 CName36, tdbType##CType37 CName37, tdbType##CType38 CName38, tdbType##CType39 CName39, tdbType##CType40 CName40, tdbType##CType41 CName41, tdbType##CType42 CName42, tdbType##CType43 CName43, tdbType##CType44 CName44, tdbType##CType45 CName45, tdbType##CType46 CName46, tdbType##CType47 CName47, tdbType##CType48 CName48, tdbType##CType49 CName49, tdbType##CType50 CName50) { \
		Insert##CType1 (0, ndx, CName1); \
		Insert##CType2 (1, ndx, CName2); \
		Insert##CType3 (2, ndx, CName3); \
		Insert##CType4 (3, ndx, CName4); \
		Insert##CType5 (4, ndx, CName5); \
		Insert##CType6 (5, ndx, CName6); \
		Insert##CType7 (6, ndx, CName7); \
		Insert##CType8 (7, ndx, CName8); \
		Insert##CType9 (8, ndx, CName9); \
		Insert##CType10 (9, ndx, CName10); \
		Insert##CType11 (10, ndx, CName11); \
		Insert##CType12 (11, ndx, CName12); \
		Insert##CType13 (12, ndx, CName13); \
		Insert##CType14 (13, ndx, CName14); \
		Insert##CType15 (14, ndx, CName15); \
		Insert##CType16 (15, ndx, CName16); \
		Insert##CType17 (16, ndx, CName17); \
		Insert##CType18 (17, ndx, CName18); \
		Insert##CType19 (18, ndx, CName19); \
		Insert##CType20 (19, ndx, CName20); \
		Insert##CType21 (20, ndx, CName21); \
		Insert##CType22 (21, ndx, CName22); \
		Insert##CType23 (22, ndx, CName23); \
		Insert##CType24 (23, ndx, CName24); \
		Insert##CType25 (24, ndx, CName25); \
		Insert##CType26 (25, ndx, CName26); \
		Insert##CType27 (26, ndx, CName27); \
		Insert##CType28 (27, ndx, CName28); \
		Insert##CType29 (28, ndx, CName29); \
		Insert##CType30 (29, ndx, CName30); \
		Insert##CType31 (30, ndx, CName31); \
		Insert##CType32 (31, ndx, CName32); \
		Insert##CType33 (32, ndx, CName33); \
		Insert##CType34 (33, ndx, CName34); \
		Insert##CType35 (34, ndx, CName35); \
		Insert##CType36 (35, ndx, CName36); \
		Insert##CType37 (36, ndx, CName37); \
		Insert##CType38 (37, ndx, CName38); \
		Insert##CType39 (38, ndx, CName39); \
		Insert##CType40 (39, ndx, CName40); \
		Insert##CType41 (40, ndx, CName41); \
		Insert##CType42 (41, ndx, CName42); \
		Insert##CType43 (42, ndx, CName43); \
		Insert##CType44 (43, ndx, CName44); \
		Insert##CType45 (44, ndx, CName45); \
		Insert##CType46 (45, ndx, CName46); \
		Insert##CType47 (46, ndx, CName47); \
		Insert##CType48 (47, ndx, CName48); \
		Insert##CType49 (48, ndx, CName49); \
		Insert##CType50 (49, ndx, CName50); \
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
	ColumnProxy##CType3 CName3; \
	ColumnProxy##CType4 CName4; \
	ColumnProxy##CType5 CName5; \
	ColumnProxy##CType6 CName6; \
	ColumnProxy##CType7 CName7; \
	ColumnProxy##CType8 CName8; \
	ColumnProxy##CType9 CName9; \
	ColumnProxy##CType10 CName10; \
	ColumnProxy##CType11 CName11; \
	ColumnProxy##CType12 CName12; \
	ColumnProxy##CType13 CName13; \
	ColumnProxy##CType14 CName14; \
	ColumnProxy##CType15 CName15; \
	ColumnProxy##CType16 CName16; \
	ColumnProxy##CType17 CName17; \
	ColumnProxy##CType18 CName18; \
	ColumnProxy##CType19 CName19; \
	ColumnProxy##CType20 CName20; \
	ColumnProxy##CType21 CName21; \
	ColumnProxy##CType22 CName22; \
	ColumnProxy##CType23 CName23; \
	ColumnProxy##CType24 CName24; \
	ColumnProxy##CType25 CName25; \
	ColumnProxy##CType26 CName26; \
	ColumnProxy##CType27 CName27; \
	ColumnProxy##CType28 CName28; \
	ColumnProxy##CType29 CName29; \
	ColumnProxy##CType30 CName30; \
	ColumnProxy##CType31 CName31; \
	ColumnProxy##CType32 CName32; \
	ColumnProxy##CType33 CName33; \
	ColumnProxy##CType34 CName34; \
	ColumnProxy##CType35 CName35; \
	ColumnProxy##CType36 CName36; \
	ColumnProxy##CType37 CName37; \
	ColumnProxy##CType38 CName38; \
	ColumnProxy##CType39 CName39; \
	ColumnProxy##CType40 CName40; \
	ColumnProxy##CType41 CName41; \
	ColumnProxy##CType42 CName42; \
	ColumnProxy##CType43 CName43; \
	ColumnProxy##CType44 CName44; \
	ColumnProxy##CType45 CName45; \
	ColumnProxy##CType46 CName46; \
	ColumnProxy##CType47 CName47; \
	ColumnProxy##CType48 CName48; \
	ColumnProxy##CType49 CName49; \
	ColumnProxy##CType50 CName50; \
\
protected: \
	friend class Group; \
	TableName(Allocator& alloc, size_t ref, Array* parent, size_t pndx) : TopLevelTable(alloc, ref, parent, pndx) {}; \
\
private: \
	TableName(const TableName&) {} \
	TableName& operator=(const TableName&) {return *this;} \
};

#endif //__TIGHTDB_H__
