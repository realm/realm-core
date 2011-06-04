#ifndef __TDB_TABLE__
#define __TDB_TABLE__

#include "Column.h"
#include "string.h"

class Accessor;

enum ColumnType {
	COLUMN_TYPE_INT,
	COLUMN_TYPE_BOOL,
	COLUMN_TYPE_STRING
};

class Table {
public:
	Table(const char* name);
	Table(const Table& t);
	~Table();

	Table& operator=(const Table& t);

	bool IsEmpty() const {return m_size == 0;}
	size_t GetSize() const {return m_size;}

	size_t AddRow();
	void Clear();
	void DeleteRow(size_t ndx);
	void PopBack() {if (!IsEmpty()) DeleteRow(m_size-1);}

	// Adaptive ints
	int Get(size_t column_id, size_t ndx) const;
	void Set(size_t column_id, size_t ndx, int value);

	// Strings
	const char* GetString(size_t column_id, size_t ndx) const;
	void SetString(size_t column_id, size_t ndx, const char* value);

	void RegisterColumn(ColumnType type, const char* name);

	Column& GetColumn(size_t ndx);
	const Column& GetColumn(size_t ndx) const;
	AdaptiveStringColumn& GetColumnString(size_t ndx);
	const AdaptiveStringColumn& GetColumnString(size_t ndx) const;

	// Indexing
	bool HasIndex(size_t column_id) const;
	void SetIndex(size_t column_ud);

protected:
	ColumnBase& GetColumnBase(size_t ndx);
	const ColumnBase& GetColumnBase(size_t ndx) const;

	const char* m_name;
	size_t m_size;
	
	// On-disk format
	Column m_spec;
	Column m_columns;
	Column m_columnNames;

	// Cached columns
	Column m_cols;
};

class CursorBase {
public:
	CursorBase(Table& table, size_t ndx) : m_table(table), m_index(ndx) {};
	CursorBase(const CursorBase& v) : m_table(v.m_table), m_index(v.m_index) {};
	CursorBase& operator=(const CursorBase& v) {m_table = v.m_table; m_index = v.m_index;}

protected:
	Table& m_table;
	size_t m_index;
	friend class Accessor;
};

class Accessor {
public:
	Accessor() {};
	void Create(CursorBase* cursor, size_t column_ndx) {m_cursor = cursor; m_column = column_ndx;}
	static const ColumnType type;

protected:
	int Get() const {return m_cursor->m_table.Get(m_column, m_cursor->m_index);}
	void Set(int value) {m_cursor->m_table.Set(m_column, m_cursor->m_index, value);}

	const char* GetString() const {return m_cursor->m_table.GetString(m_column, m_cursor->m_index);}
	void SetString(const char* value) {m_cursor->m_table.SetString(m_column, m_cursor->m_index, value);}

	CursorBase* m_cursor;
	size_t m_column;
};

class AccessorInt : public Accessor {
public:
	operator int() const {return Get();}
	void operator=(int value) {Set(value);}
	void operator=(unsigned int value) {Set(value);}
	void operator+=(int value) {Set(Get()+value);}
	void operator+=(unsigned int value) {Set(Get()+value);}
};

class AccessorBool : public Accessor {
public:
	operator bool() const {return (Get() != 0);}
	void operator=(bool value) {Set(value ? 1 : 0);}
	void Flip() {Set(Get() != 0 ? 0 : 1);}
	static const ColumnType type;
};

template<class T> class AccessorEnum : public Accessor {
public:
	operator T() const {return (T)Get();}
	void operator=(T value) {Set((int)value);}
};

class AccessorString : public Accessor {
public:
	operator const char*() const {return GetString();}
	void operator=(const char* value) {SetString(value);}
	bool operator==(const char* value) {return (strcmp(GetString(), value) == 0);}
	static const ColumnType type;
};


class ColumnProxy {
public:
	ColumnProxy() {}
	void Create(Table* table, size_t column) {
		m_table = table;
		m_column = column;
	}
protected:
	Table* m_table;
	size_t m_column;
};

class ColumnProxyInt : public ColumnProxy {
public:
	size_t Find(int value) const {return m_table->GetColumn(m_column).Find(value);}
	int operator+=(int value) {m_table->GetColumn(m_column).Increment64(value); return 0;}
};

class ColumnProxyBool : public ColumnProxy {
public:
	size_t Find(bool value) const {return m_table->GetColumn(m_column).Find(value ? 1 : 0);}
};

template<class T> class ColumnProxyEnum : public ColumnProxy {
public:
	size_t Find(T value) const {return m_table->GetColumn(m_column).Find((int)value);}
};

class ColumnProxyString : public ColumnProxy {
public:
	size_t Find(const char* value) const {return m_table->GetColumnString(m_column).Find(value);}
	void Stats() const {m_table->GetColumnString(m_column).Stats();}
};

template<class T> class TypeEnum {
public:
	TypeEnum(T v) : m_value(v) {};
	operator T() const {return m_value;}
	TypeEnum<T>& operator=(const TypeEnum<T>& v) {m_value = v.m_value;}
private:
	const T m_value;
};
#define TypeInt int
#define TypeBool bool
#define TypeString const char*

// Make all enum types return int type
template<typename T> struct COLUMN_TYPE_Enum {
public:
	COLUMN_TYPE_Enum() {};
	operator ColumnType() const {return COLUMN_TYPE_INT;}
};

class QueryItem {
public:
	QueryItem operator&&(const QueryItem&) {return QueryItem();}
	QueryItem operator||(const QueryItem&) {return QueryItem();}
};

class QueryAccessorBool {
public:
	QueryItem operator==(int) {return QueryItem();}
	QueryItem operator!=(int) {return QueryItem();}
};

class QueryAccessorInt {
public:
	QueryItem operator==(int) {return QueryItem();}
	QueryItem operator!=(int) {return QueryItem();}
	QueryItem operator<(int) {return QueryItem();}
	QueryItem operator>(int) {return QueryItem();}
	QueryItem operator<=(int) {return QueryItem();}
	QueryItem operator>=(int) {return QueryItem();}
	QueryItem Between(int, int) {return QueryItem();}
};

class QueryAccessorString {
public:
	QueryItem operator==(const char*) {return QueryItem();}
	QueryItem operator!=(const char*) {return QueryItem();}
	QueryItem Contains(const char*) {return QueryItem();}
	QueryItem StartsWith(const char*) {return QueryItem();}
	QueryItem EndsWith(const char*) {return QueryItem();}
	QueryItem MatchRegEx(const char*) {return QueryItem();}
};

template<class T> class QueryAccessorEnum {
public:
	QueryItem operator==(T) {return QueryItem();}
	QueryItem operator!=(T) {return QueryItem();}
	QueryItem operator<(T) {return QueryItem();}
	QueryItem operator>(T) {return QueryItem();}
	QueryItem operator<=(T) {return QueryItem();}
	QueryItem operator>=(T) {return QueryItem();}
	QueryItem between(T, T) {return QueryItem();}
};

#endif //__TDB_TABLE__
