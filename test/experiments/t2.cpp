#include <iostream>
using namespace std;

#include "TableRef.hpp"



/*



Table::GetSpec()
const
Modify array parent...???



Fast track to fixing dynamically typed subtables:
-------------------------------------------------

Introduce class TableRef:
  Table *TableRef::m_table;
  Table *TableRef::operator->();

Table:
  private mutable std::size_t Table::m_ref_count;
  private TableRef const Table::m_parent;
  private Table::Table(Table *parent): m_parent(parent) {}
  Table::~Table() - delete self from map in parent column
  TableRef Table::GetTable() -> defer to ColumnTable

ColumnTable
  TableRef ColumnTable::GetTable()
    consult map of existing tables - what is the key? Must be row number, but then the keys need to be updated on row insert/remove
  Map is two arrays, one for keys, one for Table pointers.
  On row insert or delete, update relevant row number keys of map. Also the corresponding parent indices of the subtables must be updated.
  If the top level array gets reallocated, then all the subtables in the map, must be updated accordingly.

Array:
  bool Array::m_is_subtable_root - which means that m_parent is the top level-array of the b-tree of the parent column, and m_parentNdx is the index if this subtable withing that column



Any modifying operation on a table, must be able to forward the array renewal to the parent table.

Table::modify()
  Column::modify()

InstantiateBeforeChange ????

Modifying ops:
        non-const GetColumn() and friends due to InstantiateBeforeChange()
	size_t AddRow();
	void Clear();
	void DeleteRow(size_t ndx);
	void Set(size_t column_id, size_t ndx, int64_t value);
	void SetBool(size_t column_id, size_t ndx, bool value);
	void SetDate(size_t column_id, size_t ndx, time_t value);
	void InsertInt(size_t column_id, size_t ndx, int64_t value);
	void InsertString(size_t column_id, size_t ndx, const char* value);
	void InsertBinary(size_t column_id, size_t ndx, const void* value, size_t len);
	void SetString(size_t column_id, size_t ndx, const char* value);
	void SetBinary(size_t column_id, size_t ndx, const void* value, size_t len);
	void InsertTable(size_t column_id, size_t ndx);
	void ClearTable(size_t column_id, size_t ndx);
	void InsertMixed(size_t column_id, size_t ndx, Mixed value);
	void SetMixed(size_t column_id, size_t ndx, Mixed value);
	void SetIndex(size_t column_id);
	void Optimize();


Attached array - what difference does it make?








Introduce table_new.hpp

Make it such that there is no typewise distinction between a top level table and a subtable for statically typed tables.

For dynamically typed tables, TopLevelTable will still exist and will expose api to manage dynamic type.

For a mixed column, subtables will always be dynamically typed. How to manage the type of it? How to do it in the current implementation?

Construct typed table



Goals:
  Feel like STL



Think of TableRef as an alias.
tabel.Columns().foo


Design criteria:
	No overhead from statically typed layer.
  Feel like STL.
  Work syntactically as regular array over a struct.

  Strict propagation of constness from top table to subtables.
  Clear distiction between value and reference semantics.
  Seamless mixing of statically and dynamically typed APIs.
  Retain all elements of the current API whenver possible.

Implementation:
  Clone table_new.hpp, tightdb_new.hpp.
  Make Table be able to act like a TopLevelTable.
	Introduce table constructor into Spec and ColumnTable.
	Replace meta classes with new ones.



Query API.

Instantiate correct subtable type, even from untyped Table::GetTable().
  Add a table constructor function pointer to Spec for subtable
  Copy it into ColumnTable

Setup spec from static type info.

TOP LEVEL vs. SUB LEVEL.
  Make Table be able to act like a TopLevelTable
        Keep the special public TopLevelTable API in TopLevelTable

AddRow.

Mixed.

Namespace 'tightdb'

at(std::size_t) to match operator[]


tab[6] = tab[4] // Copy row by value
sort(tab.begin(), tab.end()); // Inefficient way of sorting rows ...

Column proxy - wait!

Cursors - probably not!



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

doc:
- basics
  - add row
  - row subscr
  - assign row to row
  - sort()?
- iters
- refs
  - some ops copied from Table
- subtables
- constness


Dangers:
  Table &subtab = *table.GetTable(3,4)); // Error - smart subtable reference is destroyed, so 'subtable' may become a dangeling reference.

Questions:
  TableView v = query->FindAll(*table.GetTable(3,4)); // Error or not - should TableView keep a counted reference?

 */



class Table;
typedef BasicTableRef<Table> TableRef;
typedef BasicTableRef<Table const> TableConstRef;



class Table
{
public:
  typedef int Cursor;
  typedef int const ConstCursor;

  std::size_t GetSize() const { return 7; }

  int Get(std::size_t col, std::size_t row) const { return col+row; }
  void Set(std::size_t col, std::size_t row, int v) const { cerr << "Set("<<col<<", "<<row<<", "<<v<<")" << endl; }

  TableRef GetRef() { return TableRef(this); }
  TableConstRef GetRef() const { return TableConstRef(this); }

  TableRef GetTable(std::size_t col, std::size_t row) { return TableRef(get_subtable(col, row)); }
  TableConstRef GetTable(std::size_t col, std::size_t row) const { return TableConstRef(get_subtable(col, row)); }

protected:
  Table(Table *parent): m_ref_count(0), m_parent(parent) {}

  class TopLevelTag {};
  Table(TopLevelTag): m_ref_count(1) {} // Reference count will never reach zero

  ~Table() { cerr << "~Table" << endl; }

  Table *get_subtable(std::size_t col, std::size_t row) const {
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
  TableSubscr<Sub> operator[](std::size_t i) { return TableSubscr<Sub>(subtab_ptr(), i); }
  TableSubscr<Sub const> operator[](std::size_t i) const { return TableSubscr<Sub const>(subtab_ptr(), i); }

  BasicTableRef<Sub> GetRef() { ensure_subtab(); return m_subtable; }
  BasicTableRef<Sub const> GetRef() const { ensure_subtab(); return m_subtable; }

  // SubtableFieldAccessorBase &operator=(FieldAccessor const &a) { return *this = int(a); }

protected:
  SubtableFieldAccessorBase(Row *row_ref): FieldAccessorBase<Tab, Row>(row_ref) {}

private:
  mutable BasicTableRef<Sub> m_subtable;
  Sub *subtab_ptr() const { ensure_subtab(); return m_subtable.m_table; }
  void ensure_subtab() const {
    if (!m_subtable) {
      Table::set_ref(m_subtable, static_cast<Sub *>(this->tab_ptr()->get_subtable(col, this->row_idx())));
    }
  }
};



template<class Tab, class Row, int col, class FieldType> class FieldAccessor: FieldAccessorBase<Tab, Row> {};

template<class Tab, class Row, int col> class FieldAccessor<Tab, Row, col, int>: FieldAccessorBase<Tab, Row> {
public:
  operator int() const { return this->tab_ptr()->Get(col, this->row_idx()); }
  FieldAccessor &operator=(int v) { this->tab_ptr()->Set(col, this->row_idx(), v); return *this; }
  FieldAccessor &operator=(FieldAccessor const &a) { return *this = int(a); }

private:
  friend class TableSubscrFields<Tab, TableSubscr<Tab> >;
  FieldAccessor(Row *row): FieldAccessorBase<Tab, Row>(row) {}
};





class MySubTable;
typedef BasicTableIter<MySubTable> MySubTableIter;
typedef BasicTableIter<MySubTable const> MySubTableConstIter;
typedef BasicTableRef<MySubTable> MySubTableRef;
typedef BasicTableRef<MySubTable const> MySubTableConstRef;

class MySubTable: public Table {
public:
  MySubTable(): Table(TopLevelTag()) {}
  MySubTableRef GetRef() { MySubTableRef r; set_ref(r, this); return r; }
  MySubTableConstRef GetRef() const { MySubTableConstRef r; set_ref(r, this); return r; }
  MySubTableIter begin() { return make_iter(this, 0); }
  MySubTableIter end() { return make_iter(this, GetSize()); }
  MySubTableConstIter begin() const { return make_iter(this, 0); }
  MySubTableConstIter end() const { return make_iter(this, GetSize()); }
};

template<class Row> class TableSubscrFields<MySubTable, Row> {
private:
  friend class TableSubscr<MySubTable>;
  TableSubscrFields(Row *r): foo(r), bar(r) {}

public:
  FieldAccessor<MySubTable, Row, 0, int> foo;
  FieldAccessor<MySubTable, Row, 1, int> bar;
};

template<class Row> class TableSubscrFields<MySubTable const, Row> {
private:
  friend class TableSubscr<MySubTable const>;
  TableSubscrFields(Row *r): foo(r), bar(r) {}

public:
  FieldAccessor<MySubTable const, Row, 0, int> const foo;
  FieldAccessor<MySubTable const, Row, 1, int> const bar;
};

template<class Tab, class Row, int col> class FieldAccessor<Tab, Row, col, MySubTable>: public SubtableFieldAccessorBase<Tab, Row, col, MySubTable> {
private:
  friend class TableSubscrFields<Tab, TableSubscr<Tab> >;
  FieldAccessor(Row *row): SubtableFieldAccessorBase<Tab, Row, col, MySubTable>(row) {}
};





class MyTable;
typedef BasicTableIter<MyTable> MyTableIter;
typedef BasicTableIter<MyTable const> MyTableConstIter;
typedef BasicTableRef<MyTable> MyTableRef;
typedef BasicTableRef<MyTable const> MyTableConstRef;

class MyTable: public Table {
public:
  MyTable(): Table(TopLevelTag()) {}
  /*
    TableSubscr<MyTable> operator[](std::size_t i) { return }
    TableSubscr<MyTable const> operator[](std::size_t i) const { return }
  */
  MyTableIter begin() { return make_iter(this, 0); }
  MyTableIter end() { return make_iter(this, GetSize()); }
  MyTableConstIter begin() const { return make_iter(this, 0); }
  MyTableConstIter end() const { return make_iter(this, GetSize()); }
  MyTableRef GetRef() { MyTableRef r; set_ref(r, this); return r; }
  MyTableConstRef GetRef() const { MyTableConstRef r; set_ref(r, this); return r; }
};

template<class Row> class TableSubscrFields<MyTable, Row> {
private:
  friend class TableSubscr<MyTable>;
  TableSubscrFields(Row *r): count(r), tab(r) {}

public:
  FieldAccessor<MyTable, Row, 0, int> count;
  FieldAccessor<MyTable, Row, 1, MySubTable> tab;
};

template<class Row> class TableSubscrFields<MyTable const, Row> {
private:
  friend class TableSubscr<MyTable const>;
  TableSubscrFields(Row *r): count(r), tab(r) {}

public:
  FieldAccessor<MyTable const, Row, 0, int> const count;
  FieldAccessor<MyTable const, Row, 1, MySubTable> const tab;
};

template<class Tab, class Row, int col> class FieldAccessor<Tab, Row, col, MyTable>: public SubtableFieldAccessorBase<Tab, Row, col, MyTable> {
private:
  friend class TableSubscrFields<Tab, TableSubscr<Tab> >;
  FieldAccessor(Row *row): SubtableFieldAccessorBase<Tab, Row, col, MyTable>(row) {}
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
  // MySubTable b;
  MyTable a;
  TableConstRef s = a.GetTable(0,0);
  MyTableRef r = a.GetRef();
  TableConstRef r2 = r;
  //    int i = r[7].count;
  int v = r[7].tab[8].foo;
  cerr << v << endl;
  // r[7].tab[8].foo = 9;
  // r[5] = r[7];
  // r[5] == r[7];
  cerr << r[7].tab[8].foo << endl;
  for (MyTableConstIter i=r->begin(); i!=r->end(); ++i) {
    cerr << (*i).count << endl;
    MySubTableConstRef s = i->tab.GetRef();
    for (MySubTableConstIter j=s->begin(); j!=s->end(); ++j) {
      cerr << j->foo << endl;
      cerr << j->bar << endl;
    }
  }
  return 0;
}
