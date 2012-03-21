#include <algorithm>


template<class> class BasicTableRef;
template<class, class> class FieldAccessorBase;


template<class, class> class BasicTableSubscrFields {};

template<class T> class BasicTableSubscr: public BasicTableSubscrFields<T, BasicTableSubscr<T> > {
private:
	friend class BasicTableRef<T>;
	friend class FieldAccessorBase<T, BasicTableSubscr<T> >;
	template<class, class, int, class> friend class SubtableFieldAccessorBase;

	T *const m_table;
	size_t const m_row_index;

	BasicTableSubscr(T *t, size_t i): BasicTableSubscrFields<T, BasicTableSubscr<T> >(this), m_table(t), m_row_index(i) {}

	BasicTableSubscr(BasicTableSubscr const &s): BasicTableSubscrFields<T, BasicTableSubscr<T> >(this), m_table(s.m_table), m_row_index(s.m_row_index) {} // Hide
	BasicTableSubscr &operator=(BasicTableSubscr const &); // Disable

	T *tab_ptr() const { return m_table; }
	size_t row_idx() const { return m_row_index; }
};


template<class T> class BasicTableRef {
public:
	BasicTableSubscr<T> operator[](size_t i) const { return BasicTableSubscr<T>(m_table, i); }

	/**
	 * Construct a null reference.
	 */
	BasicTableRef(): m_table(0) {}

	/**
	 * Copy a reference.
	 */
	BasicTableRef(BasicTableRef const &r) { bind(r.m_table); }

	/**
	 * Copy a reference from a pointer compatible table type.
	 */
	template<class U> BasicTableRef(BasicTableRef<U> const &r) { bind(r.m_table); }

	~BasicTableRef() { unbind(); }

	/**
	 * Copy a reference.
	 */
	BasicTableRef &operator=(BasicTableRef const &r) { reset(r.m_table); return *this; }

	/**
	 * Copy a reference from a pointer compatible table type.
	 */
	template<class U> BasicTableRef &operator=(BasicTableRef<U> const &r);

	/**
	 * Allow comparison between related reference types.
	 */
	template<class U> bool operator==(BasicTableRef<U> const &) const;

	/**
	 * Allow comparison between related reference types.
	 */
	template<class U> bool operator!=(BasicTableRef<U> const &) const;

	/**
	 * Dereference this table reference.
	 */
	T &operator*() const { return *m_table; }

	/**
	 * Dereference this table reference for method invocation.
	 */
	T *operator->() const { return m_table; }

	/**
	 * Efficient swapping that avoids binding and unbinding.
	 */
	void swap(BasicTableRef &r) { using std::swap; swap(m_table, r.m_table); }

private:
	typedef T *BasicTableRef::*unspecified_bool_type;

public:
	/**
	 * Test if this is a proper reference (ie. not a null reference.)
	 *
	 * \return True if, and only if this is a proper reference.
	 */
	operator unspecified_bool_type() const;

private:
	friend class Table;
	friend class BasicTableSubscr<T>;
	template<class> friend class BasicTableRef;
	template<class, class, int, class> friend class SubtableFieldAccessorBase;

	T *m_table;

	BasicTableRef(T *t) { bind(t); }

	void reset(T * = 0);
	void bind(T *);
	void unbind();
};


/**
 * Efficient swapping that avoids access to the referenced object,
 * in particular, its reference count.
 */
template<class T> inline void swap(BasicTableRef<T> &r, BasicTableRef<T> &s) {
	r.swap(s);
}





// Implementation:

template<class T> template<class U>
inline BasicTableRef<T> &BasicTableRef<T>::operator=(BasicTableRef<U> const &r) {
	reset(r.m_table);
	return *this;
}

template<class T> template<class U>
inline bool BasicTableRef<T>::operator==(BasicTableRef<U> const &r) const {
	return m_table == r.m_table;
}

template<class T> template<class U>
inline bool BasicTableRef<T>::operator!=(BasicTableRef<U> const &r) const {
	return m_table != r.m_table;
}

template<class T>
inline BasicTableRef<T>::operator unspecified_bool_type() const {
	return m_table ? &BasicTableRef::m_table : 0;
}

template<class T> inline void BasicTableRef<T>::reset(T *t) {
	if(t == m_table) return;
	unbind();
	bind(t);
}

template<class T> inline void BasicTableRef<T>::bind(T *t) {
	if (t) ++t->m_ref_count;
	m_table = t;
}

template<class T> inline void BasicTableRef<T>::unbind() {
	if (m_table && --m_table->m_ref_count == 0) delete m_table;
}
