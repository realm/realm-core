#ifndef __TDB_TABLE__
#define __TDB_TABLE__

#include "Column.h"

class Accessor;

class Table {
public:
	Table(const char* name);
	~Table();

	bool IsEmpty() const {return m_size == 0;}
	size_t GetSize() const {return m_size;}

	size_t AddRow();
	void Clear();
	void DeleteRow(size_t ndx);
	void PopBack() {if (!IsEmpty()) DeleteRow(m_size-1);}

	int Get(size_t column_id, size_t ndx) const;
	void Set(size_t column_id, size_t ndx, int value);

	void RegisterColumn(const char* name);
	Column* GetColumn(size_t ndx);

protected:
	const char* m_name;
	size_t m_size;
	Column m_columnNames;
	Column m_columns;
};

class CursorBase {
public:
	CursorBase(Table& table, size_t ndx) : m_table(table), m_index(ndx) {};
	CursorBase(const CursorBase& v) : m_table(v.m_table), m_index(v.m_index) {};

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
	void operator+=(int value) {Set(Get()+value);}
};

class AccessorBool : public Accessor {
public:
	operator bool() const {return (Get() != 0);}
	void operator=(bool value) {Set(value ? 1 : 0);}
	void Flip() {Set(Get() != 0 ? 0 : 1);}
};

template<class T> class AccessorEnum : public Accessor {
public:
	operator T() const {return (T)Get();}
	void operator=(T value) {Set((int)value);}
};

class ColumnProxy {
public:
	ColumnProxy() {}
	void Create(Column* column) {m_column = column;}
protected:
	Column* m_column;
};

class ColumnProxyInt : public ColumnProxy {
public:
	size_t Find(int value) const {return m_column->Find(value);}
};

class ColumnProxyBool : public ColumnProxy {
public:
	size_t Find(bool value) const {return m_column->Find(value ? 1 : 0);}
};

template<class T> class ColumnProxyEnum : public ColumnProxy {
public:
	size_t Find(T value) const {return m_column->Find((int)value);}
};

template<class T> class TypeEnum {
public:
	TypeEnum(T v) : m_value(v) {};
	operator T() const {return m_value;}
private:
	const T m_value;
};
#define TypeInt int
#define TypeBool bool

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
