#ifndef __TIGHTDB_H__
#define __TIGHTDB_H__

#include "Table.h"

#define TDB_TABLE_2(TableName, CType1, CName1, CType2, CName2) \
class TableName : public Table { \
public: \
	TableName() : Table(#TableName) { \
		RegisterColumn(Accessor##CType1::type, #CName1); \
		RegisterColumn(Accessor##CType2::type, #CName2); \
		\
		CName1.Create(this, 0); \
		CName2.Create(this, 1); \
	}; \
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
	Cursor Add(Type##CType1 CName1, Type##CType2 CName2) { \
		Cursor r = Add(); \
		r.CName1 = CName1; \
		r.CName2 = CName2; \
		return r; \
	} \
\
	Cursor Add() {return Cursor(*this, AddRow());} \
	Cursor Get(size_t ndx) {return Cursor(*this, ndx);} \
	Cursor operator[](size_t ndx) {return Cursor(*this, ndx);} \
	const Cursor operator[](size_t ndx) const {return Cursor(*this, ndx);} \
	Cursor operator[](int ndx) {return Cursor(*this, (ndx < 0) ? GetSize() - ndx : ndx);} \
	Cursor Back() {return Cursor(*this, m_size-1);} \
\
	ColumnProxy##CType1 CName1; \
	ColumnProxy##CType2 CName2; \
};

#define TDB_TABLE_4(TableName, CType1, CName1, CType2, CName2, CType3, CName3, CType4, CName4) \
class TableName : public Table { \
public: \
	TableName() : Table(#TableName) { \
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
\
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
	Cursor Add(Type##CType1 v1, Type##CType2 v2,Type##CType3 v3, Type##CType4 v4) { \
		Cursor r = Add(); \
		r.CName1 = v1; \
		r.CName2 = v2; \
		r.CName3 = v3; \
		r.CName4 = v4; \
		return r; \
	} \
\
	Cursor Add() {return Cursor(*this, AddRow());} \
	Cursor Get(size_t ndx) {return Cursor(*this, ndx);} \
	Cursor operator[](size_t ndx) {return Cursor(*this, ndx);} \
\
	ColumnProxy##CType1 CName1; \
	ColumnProxy##CType2 CName2; \
	ColumnProxy##CType3 CName3; \
	ColumnProxy##CType4 CName4; \
};

#endif //__TIGHTDB_H__