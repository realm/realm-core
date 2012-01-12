#ifndef __TDB_TABLE__
#define __TDB_TABLE__

#include <cstring> // strcmp()
#include <time.h>
#include "Column.h"
#include "ColumnString.h"
#include "ColumnStringEnum.h"
#include "ColumnBinary.h"
#include "alloc.h"
#include "ColumnType.h"

class Accessor;
class TableView;
class Group;
class ColumnTable;

class Spec {
public:
	Spec(Allocator& alloc, size_t ref, Array* parent, size_t pndx);
	Spec(const Spec& s);

	void AddColumn(ColumnType type, const char* name);
	Spec AddColumnTable(const char* name);

	Spec GetSpec(size_t column_id);
	const Spec GetSpec(size_t column_id) const;

	size_t GetColumnCount() const;
	ColumnType GetColumnType(size_t ndx) const;
	const char* GetColumnName(size_t ndx) const;
	size_t GetColumnIndex(const char* name) const;

	size_t GetRef() const {return m_specSet.GetRef();}

	// Serialization
	template<class S> size_t Write(S& out, size_t& pos) const;

private:
	void Create(size_t ref, Array* parent, size_t pndx);

	Array m_specSet;
	Array m_spec;
	ArrayString m_names;
	Array m_subSpecs;
};

class Table {
public:
	Table(Allocator& alloc=GetDefaultAllocator());
	Table(const Table& t);
	~Table();

	// Column meta info
	size_t GetColumnCount() const;
	const char* GetColumnName(size_t ndx) const;
	size_t GetColumnIndex(const char* name) const;
	ColumnType GetColumnType(size_t ndx) const;
	Spec GetSpec();
	const Spec GetSpec() const;

	bool IsEmpty() const {return m_size == 0;}
	size_t GetSize() const {return m_size;}

	size_t AddRow();
	void Clear();
	void DeleteRow(size_t ndx);
	void PopBack() {if (!IsEmpty()) DeleteRow(m_size-1);}

	// Adaptive ints
	int64_t Get(size_t column_id, size_t ndx) const;
	void Set(size_t column_id, size_t ndx, int64_t value);
	bool GetBool(size_t column_id, size_t ndx) const;
	void SetBool(size_t column_id, size_t ndx, bool value);
	time_t GetDate(size_t column_id, size_t ndx) const;
	void SetDate(size_t column_id, size_t ndx, time_t value);

	// NOTE: Low-level insert functions. Always insert in all columns at once
	// and call InsertDone after to avoid table getting un-balanced.
	void InsertInt(size_t column_id, size_t ndx, int64_t value);
	void InsertBool(size_t column_id, size_t ndx, bool value) {InsertInt(column_id, ndx, value ? 1 :0);}
	void InsertDate(size_t column_id, size_t ndx, time_t value) {InsertInt(column_id, ndx, (int64_t)value);}
	template<class T> void InsertEnum(size_t column_id, size_t ndx, T value) {
		InsertInt(column_id, ndx, (int)value);
	}
	void InsertString(size_t column_id, size_t ndx, const char* value);
	void InsertBinary(size_t column_id, size_t ndx, const void* value, size_t len);
	void InsertDone();

	// Strings
	const char* GetString(size_t column_id, size_t ndx) const;
	void SetString(size_t column_id, size_t ndx, const char* value);
	
	// Binary
	BinaryData GetBinary(size_t column_id, size_t ndx) const;
	void SetBinary(size_t column_id, size_t ndx, const void* value, size_t len);

	// Sub-tables
	Table  GetTable(size_t column_id, size_t ndx);
	Table* GetTablePtr(size_t column_id, size_t ndx);
	size_t GetTableSize(size_t column_id, size_t ndx) const;
	void   InsertTable(size_t column_id, size_t ndx);
	void   ClearTable(size_t column_id, size_t ndx);

	size_t RegisterColumn(ColumnType type, const char* name);

	Column& GetColumn(size_t ndx);
	const Column& GetColumn(size_t ndx) const;
	AdaptiveStringColumn& GetColumnString(size_t ndx);
	const AdaptiveStringColumn& GetColumnString(size_t ndx) const;
	ColumnBinary& GetColumnBinary(size_t ndx);
	const ColumnBinary& GetColumnBinary(size_t ndx) const;
	ColumnStringEnum& GetColumnStringEnum(size_t ndx);
	const ColumnStringEnum& GetColumnStringEnum(size_t ndx) const;
	ColumnTable& GetColumnTable(size_t ndx);
	const ColumnTable& GetColumnTable(size_t ndx) const;

	// Searching
	size_t Find(size_t column_id, int64_t value) const;
	size_t FindBool(size_t column_id, bool value) const;
	size_t FindString(size_t column_id, const char* value) const;
	size_t FindDate(size_t column_id, time_t value) const;
	void FindAll(TableView& tv, size_t column_id, int64_t value);
	void FindAllBool(TableView& tv, size_t column_id, bool value);
	void FindAllString(TableView& tv, size_t column_id, const char *value);
	void FindAllHamming(TableView& tv, size_t column_id, uint64_t value, size_t max);

	// Indexing
	bool HasIndex(size_t column_id) const;
	void SetIndex(size_t column_id);

	// Optimizing
	void Optimize();

	// Debug
#ifdef _DEBUG
	bool Compare(const Table& c) const;
	void Verify() const;
	void ToDot(const char* filename) const;
	void Print() const;
	MemStats Stats() const;
#endif //_DEBUG

	// todo, note, these three functions have been protected
	ColumnBase& GetColumnBase(size_t ndx);
	const ColumnBase& GetColumnBase(size_t ndx) const;
	ColumnType GetRealColumnType(size_t ndx) const;

protected:
	friend class Group;
	friend class ColumnTable;

	Table(Allocator& alloc, bool dontInit); // Construct un-initialized
	Table(Allocator& alloc, size_t ref_specSet, size_t ref_columns, Array* parent_columns, size_t pndx_columns); // Construct from ref

	void Create(size_t ref_specSet, size_t ref_columns, Array* parent_columns, size_t pndx_columns);
	void CreateColumns();
	void CacheColumns();
	void ClearCachedColumns();

	// Serialization
	template<class S> size_t Write(S& out, size_t& pos) const;
	static Table LoadFromFile(const char* path);

	// Specification
	size_t GetColumnRefPos(size_t column_ndx) const;
	void UpdateColumnRefs(size_t column_ndx, int diff);

	void InstantiateBeforeChange();

	// Member variables
	size_t m_size;
	
	// On-disk format
	Array m_specSet;
	Array m_spec;
	ArrayString m_columnNames;
	Array m_subSpecs;
	Array m_columns;

	// Cached columns
	Array m_cols;

private:
	Table& operator=(const Table& t); // non assignable
};

class TopLevelTable : public Table {
public:
	TopLevelTable(Allocator& alloc=GetDefaultAllocator());
	~TopLevelTable();

	void UpdateFromSpec(size_t ref_specSet);

	// Debug
#ifdef _DEBUG
	MemStats Stats() const;
#endif //_DEBUG

protected:
	friend class Group;

	// Construct from ref
	TopLevelTable(Allocator& alloc, size_t ref_top, Array* parent, size_t pndx);

	void SetParent(Array* parent, size_t pndx);
	size_t GetRef() const;

	void Invalidate() {m_top.Invalidate();}

	// Serialization
	template<class S> size_t Write(S& out, size_t& pos) const;

	// On-disk format
	Array m_top;

private:
	TopLevelTable(const TopLevelTable&) {}            // not copyable
	TopLevelTable& operator=(const TopLevelTable&) {return *this;} // non assignable
};

class TableView {
public:
	TableView(Table& source);
	TableView(const TableView& v);
	~TableView();

	Table& GetParent() {return m_table;}
	Array& GetRefColumn() {return m_refs;}
	size_t GetRef(size_t ndx) const {return m_refs.Get(ndx);}

	bool IsEmpty() const {return m_refs.IsEmpty();}
	size_t GetSize() const {return m_refs.Size();}

	// Getting values
	int64_t Get(size_t column_id, size_t ndx) const;
	bool GetBool(size_t column_id, size_t ndx) const;
	time_t GetDate(size_t column_id, size_t ndx) const;
	const char* GetString(size_t column_id, size_t ndx) const;

	// Setting values
	void Set(size_t column_id, size_t ndx, int64_t value);
	void SetBool(size_t column_id, size_t ndx, bool value);
	void SetDate(size_t column_id, size_t ndx, time_t value);
	void SetString(size_t column_id, size_t ndx, const char* value);

	// Finding
	size_t Find(size_t column_id, int64_t value) const;
	void FindAll(TableView& tv, size_t column_id, int64_t value);
	size_t FindString(size_t column_id, const char* value) const;
	void FindAllString(TableView& tv, size_t column_id, const char *value);

	// Aggregate functions
	int64_t Sum(size_t column_id) const;
	int64_t Max(size_t column_id) const;
	int64_t Min(size_t column_id) const;

	Table *GetTable(void); // todo, temporary for tests

private:
	// Don't allow copying
	TableView& operator=(const TableView&) {return *this;}

	Table& m_table;
	Array m_refs;
};


class CursorBase {
public:
	CursorBase(Table& table, size_t ndx) : m_table(table), m_index(ndx) {};
	CursorBase(const CursorBase& v) : m_table(v.m_table), m_index(v.m_index) {};

protected:
	Table& m_table;
	size_t m_index;
	friend class Accessor;

private:
	CursorBase& operator=(const CursorBase&) {return *this;}  // non assignable
};

class Accessor {
public:
	Accessor() {};
	void Create(CursorBase* cursor, size_t column_ndx) {m_cursor = cursor; m_column = column_ndx;}
	static const ColumnType type;

protected:
	int64_t Get() const {return m_cursor->m_table.Get(m_column, m_cursor->m_index);}
	void Set(int64_t value) {m_cursor->m_table.Set(m_column, m_cursor->m_index, value);}
	bool GetBool() const {return m_cursor->m_table.GetBool(m_column, m_cursor->m_index);}
	void SetBool(bool value) {m_cursor->m_table.SetBool(m_column, m_cursor->m_index, value);}
	time_t GetDate() const {return m_cursor->m_table.GetDate(m_column, m_cursor->m_index);}
	void SetDate(time_t value) {m_cursor->m_table.SetDate(m_column, m_cursor->m_index, value);}

	const char* GetString() const {return m_cursor->m_table.GetString(m_column, m_cursor->m_index);}
	void SetString(const char* value) {m_cursor->m_table.SetString(m_column, m_cursor->m_index, value);}

	CursorBase* m_cursor;
	size_t m_column;
};

class AccessorInt : public Accessor {
public:
	operator int64_t() const {return Get();}
	void operator=(int64_t value) {Set(value);}
	void operator+=(int64_t value) {Set(Get()+value);}
};

class AccessorBool : public Accessor {
public:
	operator bool() const {return GetBool();}
	void operator=(bool value) {SetBool(value);}
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

class AccessorDate : public Accessor {
public:
	operator time_t() const {return GetDate();}
	void operator=(time_t value) {SetDate(value);}
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
	size_t Find(int64_t value) const {return m_table->Find(m_column, value);}
	size_t FindPos(int64_t value) const {return m_table->GetColumn(m_column).FindPos(value);}
// todo, fixme: array that m_data points at becomes invalid during function exit in debug mode in VC. Added this workaround, please verify 
// or fix properly
//	TableView FindAll(int value) {TableView *tv = new TableView(*m_table); m_table->FindAll(*tv, m_column, value); return *tv;}
	TableView FindAll(int value) {TableView tv(*m_table); m_table->FindAll(tv, m_column, value); return tv;}
	
	TableView FindAllHamming(uint64_t value, size_t max) {TableView tv(*m_table); m_table->FindAllHamming(tv, m_column, value, max); return tv;}
	int operator+=(int value) {m_table->GetColumn(m_column).Increment64(value); return 0;}
};

class ColumnProxyBool : public ColumnProxy {
public:
	size_t Find(bool value) const {return m_table->FindBool(m_column, value);}
};

class ColumnProxyDate : public ColumnProxy {
public:
	size_t Find(time_t value) const {return m_table->FindDate(m_column, value);}
};

template<class T> class ColumnProxyEnum : public ColumnProxy {
public:
	size_t Find(T value) const {return m_table->Find(m_column, (int64_t)value);}
};

class ColumnProxyString : public ColumnProxy {
public:
	size_t Find(const char* value) const {return m_table->FindString(m_column, value);}
	TableView FindAll(const char *value) {TableView tv(*m_table); m_table->FindAllString(tv, m_column, value); return tv;}
	//void Stats() const {m_table->GetColumnString(m_column).Stats();}

};

template<class T> class TypeEnum {
public:
	TypeEnum(T v) : m_value(v) {};
	operator T() const {return m_value;}
	TypeEnum<T>& operator=(const TypeEnum<T>& v) {m_value = v.m_value;}
private:
	const T m_value;
};
#define TypeInt int64_t
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

// Templates

#include "ColumnTable.h"

template<class S>
size_t Spec::Write(S& out, size_t& pos) const {
	Array specSet(COLUMN_HASREFS);

	// Spec
    const size_t specPos = pos;
    pos += m_spec.Write(out);
	specSet.Add(specPos);

    // Names
    const size_t namesPos = pos;
    pos += m_names.Write(out);
	specSet.Add(namesPos);

	// Sub-Specs
	if (m_specSet.Size() == 3) {
		Allocator& alloc = m_specSet.GetAllocator();
		Array subSpecs(COLUMN_HASREFS);

		for (size_t i = 0; i < m_subSpecs.Size(); ++i) {
			const size_t ref = m_subSpecs.Get(i);
			const Spec spec(alloc, ref, NULL, 0);
			const size_t subpos = spec.Write(out, pos);
			subSpecs.Add(subpos);
		}

		const size_t subspecsPos = pos;
		pos += subSpecs.Write(out);
		specSet.Add(subspecsPos);

		// Clean-up
		subSpecs.SetType(COLUMN_NORMAL); // avoid recursive del
		subSpecs.Destroy();
	}

	// SpecSet
	const size_t specSetPos = pos;
	pos += specSet.Write(out);

	// Clean-up
	specSet.SetType(COLUMN_NORMAL); // avoid recursive del
	specSet.Destroy();

	return specSetPos;
}

template<class S>
size_t Table::Write(S& out, size_t& pos) const {
    // Write Columns
    Array columns(COLUMN_HASREFS);
    const size_t column_count = GetColumnCount();
	for (size_t i = 0; i < column_count; ++i) {
		const ColumnType type = GetRealColumnType(i);
		switch (type) {
			case COLUMN_TYPE_INT:
			case COLUMN_TYPE_BOOL:
			case COLUMN_TYPE_DATE:
            {
                const Column& column = GetColumn(i);
                const size_t cpos = column.Write(out, pos);
                columns.Add(cpos);
            }
				break;
			case COLUMN_TYPE_STRING:
            {
                const AdaptiveStringColumn& column = GetColumnString(i);
                const size_t cpos = column.Write(out, pos);
                columns.Add(cpos);
            }
				break;
			case COLUMN_TYPE_STRING_ENUM:
            {
                const ColumnStringEnum& column = GetColumnStringEnum(i);
                size_t ref_keys;
				size_t ref_values;
				column.Write(out, pos, ref_keys, ref_values);
                columns.Add(ref_keys);
				columns.Add(ref_values);
            }
				break;
			case COLUMN_TYPE_TABLE:
            {
				const ColumnTable& column = GetColumnTable(i);
                const size_t cpos = column.Write(out, pos);
                columns.Add(cpos);
			}
				break;
			case COLUMN_TYPE_BINARY:
            {
                const ColumnBinary& column = GetColumnBinary(i);
                const size_t cpos = column.Write(out, pos);
                columns.Add(cpos);
            }
				break;
			default: assert(false);
		}
	}
    const size_t columnsPos = pos;
    pos += columns.Write(out);

	// Clean-up
	columns.SetType(COLUMN_NORMAL); // avoid recursive del
	columns.Destroy();

    return columnsPos;
}

template<class S>
size_t TopLevelTable::Write(S& out, size_t& pos) const {
	// Write entire spec tree
	const Spec spec = GetSpec();
	const size_t specSetPos = spec.Write(out, pos);

	// Write columns
	const size_t columnsPos = Table::Write(out, pos);

	// Top-level Table array
    Array top(COLUMN_HASREFS);
	top.Add(specSetPos);
    top.Add(columnsPos);
    const size_t topPos = pos; // sized for top ref
    pos += top.Write(out);

    // Clean-up
	top.SetType(COLUMN_NORMAL); // avoid recursive del
	top.Destroy();

    return topPos;
}


#endif //__TDB_TABLE__
