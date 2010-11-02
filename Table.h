#ifndef __TDB_TABLE__
#define __TDB_TABLE__

#include "Column.h"

class Accessor;

class Table {
public:
	Table(const char* name);
	~Table();

	size_t AddRow();
	void Clear();
	void DeleteRow(size_t ndx);
	int Get(size_t column_id, size_t ndx) const;
	void Set(size_t column_id, size_t ndx, int value);

	void RegisterColumn(const char* name);

private:
	const char* m_name;
	size_t m_size;
	Column m_columnNames;
	Column m_columns;
};

class CursorBase {
public:
	CursorBase(Table& table, size_t ndx) : m_table(table), m_index(ndx) {};

protected:
	Table& m_table;
	size_t m_index;
	friend class Accessor;
};

class Accessor {
public:
	Accessor() {};
	void Create(CursorBase* cursor, size_t column_ndx) {m_cursor = cursor; m_column = column_ndx;}

protected:
	int Get() const {return m_cursor->m_table.Get(m_column, m_cursor->m_index);}
	void Set(int value) {m_cursor->m_table.Set(m_column, m_cursor->m_index, value);}

	CursorBase* m_cursor;
	size_t m_column;
};

class AccessorInt : public Accessor {
public:
	operator int() const {return Get();}
	void operator=(int value) {Set(value);}
};

class AccessorBool : public Accessor {
public:
	operator bool() const {return (Get() != 0);}
	void operator=(bool value) {Set(value ? 1 : 0);}
};

template<class T> class AccessorEnum : public Accessor {
public:
	operator T() const {return (T)Get();}
	void operator=(T value) {Set((int)value);}
};

#define TDB_TABLE_4(TableName, CType1, CName1, CType2, CName2, CType3, CName3, CType4, CName4) \
class TableName : public Table { \
public: \
	TableName() : Table(#TableName) { \
		RegisterColumn( #CName1 ); \
		RegisterColumn( #CName2 ); \
		RegisterColumn( #CName3 ); \
		RegisterColumn( #CName4 ); \
	}; \
\
	class Cursor : public CursorBase { \
	public: \
		Cursor(TableName& table, size_t ndx) : CursorBase(table, ndx) { \
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
	Cursor Add() {return Cursor(*this, AddRow());} \
	Cursor Get(size_t ndx) {return Cursor(*this, ndx);} \
	Cursor operator[](size_t ndx) {return Cursor(*this, ndx);} \
};

class MyTable : public Table {
public:
	MyTable() : Table("MyTable") {
		RegisterColumn("first");
		RegisterColumn("second");
		RegisterColumn("third");
		RegisterColumn("fourth");
	};

	class Cursor : public CursorBase {
	public:
		Cursor(MyTable& table, size_t ndx) : CursorBase(table, ndx) {
			first.Create(this, 0);
			second.Create(this, 1);
			third.Create(this, 2);
			fourth.Create(this, 3);
		};

		// Accessors
		AccessorInt first;
		AccessorInt second;
		AccessorInt third;
		AccessorBool fourth;
	};

	Cursor Add(int first, int second, int third, bool fourth) {
		Cursor r = Add();
		r.first = first;
		r.second = second;
		r.third = third;
		r.fourth = fourth;
		return r;
	}

	Cursor Add() {return Cursor(*this, AddRow());}
	Cursor Get(size_t ndx) {return Cursor(*this, ndx);}
	Cursor operator[](size_t ndx) {return Cursor(*this, ndx);}
};

#endif //__TDB_TABLE__
