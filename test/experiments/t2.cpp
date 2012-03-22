#include <iostream>
using namespace std;

#include "TableRef.hpp"



/*

tab[6] = tab[4] // Copy row by value
sort(tab.begin(), tab.end()); // Inefficient way of sorting rows ...

Column proxy - wait!

Cursors - probably not!


Query API.

Iterators:
MyTable t;
for (RowIter<MyTable> i = t.begin(); i!=t.end(); ++i) {
  cerr << i->foo << endl;
}
MyTableRef r;
for (RowIter<MyTable> i = r->begin(); i!=r->end(); ++i) {
  cerr << i->foo << endl;
}
for (RowIter<MyTable> i = t[3].subtab.begin(); i!=t[3].subtab.end(); ++i) { // ERROR! (because subtable is not kept alive)
  cerr << i->foo << endl;
}

Compile time unit testing.


 */



class Table;
typedef BasicTableRef<Table> TableRef;
typedef BasicTableRef<Table const> ConstTableRef;



class Table
{
public:
	typedef int Cursor;
	typedef int const ConstCursor;

	std::size_t GetSize() const { return 7; }

	int Get(std::size_t col, std::size_t row) const { return col+row; }
	void Set(std::size_t col, std::size_t row, int v) const { cerr << "Set("<<col<<", "<<row<<", "<<v<<")" << endl; }

	TableRef GetRef() { return TableRef(this); }
	ConstTableRef GetRef() const { return ConstTableRef(this); }

	TableRef GetTable(std::size_t col, std::size_t row) { return TableRef(create_subtable(col, row)); }
	ConstTableRef GetTable(std::size_t col, std::size_t row) const { return ConstTableRef(create_subtable(col, row)); }

protected:
	Table(Table *parent): m_ref_count(0), m_parent(parent) {}

	class NoRefDestroyTag {};
	Table(NoRefDestroyTag): m_ref_count(1) {} // Reference count will never reach zero

	~Table() { cerr << "~Table" << endl; }

	Table *create_subtable(std::size_t col, std::size_t row) const {
		Get(col, row);
		return new Table(const_cast<Table *>(this));
	}

	template<class T> static void set_ref(BasicTableRef<T> &r, T *t) {
		r.reset(t);
	}

	template<class T> static BasicTableIter<T> make_iter(T *t, std::size_t i) { return BasicTableIter<T>(t,i); }

private:
	template<class> friend class BasicTableRef;
	template<class, class, int, class> friend class SubtableFieldAccessorBase;

	Table(Table const &); // Disable
	Table &operator=(Table const &); // Disable

	mutable std::size_t m_ref_count;
	TableRef m_parent;
};



template<class Tab, class Row> class FieldAccessorBase {
protected:
	FieldAccessorBase(Row *row): m_row(row) {}

	Tab *tab_ptr() const { return m_row->tab_ptr(); }
	std::size_t row_idx() const { return m_row->row_idx(); }

private:
	Row *const m_row;

	FieldAccessorBase(FieldAccessorBase const &); // Disable
	FieldAccessorBase &operator=(FieldAccessorBase const &); // Disable
};

// 'Tab' has constness included when access is const.
// 'Sub' never has constness included.
template<class Tab, class Row, int col, class Sub> class SubtableFieldAccessorBase: public FieldAccessorBase<Tab, Row> {
public:
	BasicTableSubscr<Sub> operator[](std::size_t i) { return BasicTableSubscr<Sub>(subtab_ptr(), i); }
	BasicTableSubscr<Sub const> operator[](std::size_t i) const { return BasicTableSubscr<Sub const>(subtab_ptr(), i); }

	BasicTableRef<Sub> GetRef() { ensure_subtab(); return m_subtable; }
	BasicTableRef<Sub const> GetRef() const { ensure_subtab(); return m_subtable; }

protected:
	SubtableFieldAccessorBase(Row *row_ref): FieldAccessorBase<Tab, Row>(row_ref) {}

private:
	mutable BasicTableRef<Sub> m_subtable;
	Sub *subtab_ptr() const { ensure_subtab(); return m_subtable.m_table; }
	void ensure_subtab() const {
		if (!m_subtable) {
			Table::set_ref(m_subtable, static_cast<Sub *>(this->tab_ptr()->create_subtable(col, this->row_idx())));
		}
	}
};



template<class Tab, class Row, int col, class FieldType> class FieldAccessor: FieldAccessorBase<Tab, Row> {};

template<class Tab, class Row, int col> class FieldAccessor<Tab, Row, col, int>: FieldAccessorBase<Tab, Row> {
public:
	operator int() const { return this->tab_ptr()->Get(col, this->row_idx()); }
	FieldAccessor &operator=(int v) { this->tab_ptr()->Set(col, this->row_idx(), v); return *this; }

private:
 	friend class BasicTableSubscrFields<Tab, BasicTableSubscr<Tab> >;
	FieldAccessor(Row *row): FieldAccessorBase<Tab, Row>(row) {}
};





class MySubTable;
typedef BasicTableIter<MySubTable> MySubTableIter;
typedef BasicTableIter<MySubTable const> MySubTableConstIter;
typedef BasicTableRef<MySubTable> MySubTableRef;
typedef BasicTableRef<MySubTable const> MySubTableConstRef;

class MySubTable: public Table {
public:
	MySubTable(): Table(NoRefDestroyTag()) {}
	MySubTableRef GetRef() { MySubTableRef r; set_ref(r, this); return r; }
	MySubTableConstRef GetRef() const { MySubTableConstRef r; set_ref(r, this); return r; }
	MySubTableIter begin() { return make_iter(this, 0); }
	MySubTableIter end() { return make_iter(this, GetSize()); }
	MySubTableConstIter begin() const { return make_iter(this, 0); }
	MySubTableConstIter end() const { return make_iter(this, GetSize()); }
};

template<class Row> class BasicTableSubscrFields<MySubTable, Row> {
private:
	friend class BasicTableSubscr<MySubTable>;
	BasicTableSubscrFields(Row *r): foo(r), bar(r) {}

public:
	FieldAccessor<MySubTable, Row, 0, int> foo;
	FieldAccessor<MySubTable, Row, 1, int> bar;
};

template<class Row> class BasicTableSubscrFields<MySubTable const, Row> {
private:
	friend class BasicTableSubscr<MySubTable const>;
	BasicTableSubscrFields(Row *r): foo(r), bar(r) {}

public:
	FieldAccessor<MySubTable const, Row, 0, int> const foo;
	FieldAccessor<MySubTable const, Row, 1, int> const bar;
};

template<class Tab, class Row, int col> class FieldAccessor<Tab, Row, col, MySubTable>: public SubtableFieldAccessorBase<Tab, Row, col, MySubTable> {
private:
	friend class BasicTableSubscrFields<Tab, BasicTableSubscr<Tab> >;
	FieldAccessor(Row *row): SubtableFieldAccessorBase<Tab, Row, col, MySubTable>(row) {}
};




class MyTable;
typedef BasicTableIter<MyTable> MyTableIter;
typedef BasicTableIter<MyTable const> MyTableConstIter;
typedef BasicTableRef<MyTable> MyTableRef;
typedef BasicTableRef<MyTable const> MyTableConstRef;

class MyTable: public Table {
public:
	MyTable(): Table(NoRefDestroyTag()) {}
	MyTableRef GetRef() { MyTableRef r; set_ref(r, this); return r; }
	MyTableConstRef GetRef() const { MyTableConstRef r; set_ref(r, this); return r; }
	MyTableIter begin() { return make_iter(this, 0); }
	MyTableIter end() { return make_iter(this, GetSize()); }
	MyTableConstIter begin() const { return make_iter(this, 0); }
	MyTableConstIter end() const { return make_iter(this, GetSize()); }
};

template<class Row> class BasicTableSubscrFields<MyTable, Row> {
private:
	friend class BasicTableSubscr<MyTable>;
	BasicTableSubscrFields(Row *r): count(r), tab(r) {}

public:
	FieldAccessor<MyTable, Row, 0, int> count;
	FieldAccessor<MyTable, Row, 1, MySubTable> tab;
};

template<class Row> class BasicTableSubscrFields<MyTable const, Row> {
private:
	friend class BasicTableSubscr<MyTable const>;
	BasicTableSubscrFields(Row *r): count(r), tab(r) {}

public:
	FieldAccessor<MyTable const, Row, 0, int> const count;
	FieldAccessor<MyTable const, Row, 1, MySubTable> const tab;
};




/*
HOW TO SUPPORT STRICTLY TYPED API THROUGH REF?

MyTableRef r = top[6].sub;
int i = r[7].idx;
int i = top[6].sub[7].idx;

class TopLevelTable: virtual Table {};

class MyTableBase: virtual Table {}

class MyTable: MyTableBase, TopLevelTable {};

*/


int main()
{
	//	MySubTable b;
	MyTable a;
	TableRef s = a.GetTable(0,0);
	MyTableRef r = a.GetRef();
	TableRef r2 = r;
	//	int i = r[7].count;
	int v = r[7].tab[8].foo;
	cerr << v << endl;
	r[7].tab[8].foo = 9;
	cerr << r[7].tab[8].foo << endl;
	for (BasicTableIter<MyTable> i=r->begin(); i!=r->end(); ++i) {
		cerr << (*i).count << endl;
		MySubTableRef s = i->tab.GetRef();
		for (BasicTableIter<MySubTable> j=s->begin(); j!=s->end(); ++j) {
			cerr << j->foo << endl;
			cerr << j->bar << endl;
		}
	}
	return 0;
}
