template<class T> class BasicTableRef {
public:
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

	~BasicTableRef() throw () { unbind(m_table); }

	/**
	 * Copy a reference.
	 */
	BasicTableRef &operator=(BasicTableRef const &r) { reset(r.m_table); return *this; }

	/**
	 * Copy a reference from a pointer compatible table type.
	 */
	template<class U> BasicTableRef &operator=(BasicTableRef<U> const &r);

	/**
	 * Efficient swapping that avoids binding and unbinding.
	 */
	void swap(BasicTableRef &r) throw () { using std::swap; swap(m_table, r.m_table); }

	/**
	 * Allow comparison between related reference types.
	 */
	template<class U> bool operator==(BasicTableRef<U> const &) const throw ();

	/**
	 * Allow comparison between related reference types.
	 */
	template<class U> bool operator!=(BasicTableRef<U> const &) const throw ();

private:
	typedef T *BasicTableRef::*unspecified_bool_type;

public:
	/**
	 * Test if this is a proper reference (ie. not a null reference.)
	 *
	 * \return True if, and only if this is a proper reference.
	 */
	operator unspecified_bool_type() const throw ();

private:
	friend class Table;
	template<class> friend class BasicTableRef;

	T *m_table;

	BasicTableRef(T *t) { bind(t); }

	void reset(T * = 0) throw ();
	void bind(T *) throw ();
	void unbind() throw ();
};

class Table;
typedef BasicTableRef<Table> TableRef;


/**
 * Efficient swapping that avoids access to the referenced object,
 * in particular, its reference count.
 */
template<class T> inline void swap(BasicTableRef<T> &r, BasicTableRef<T> &s) throw () {
	r.swap(s);
}





// Implementation:

template<class T>

template<class U>

BasicTableRef &

BasicTableRef<T>

::operator=(BasicTableRef<U> const &r) {
	reset(r.m_table);
	return *this;
}

template<class T> template<class U>
inline bool BasicTableRef<T>::operator==(BasicTableRef<U> const &r) const throw () {
	return m_table == r.m_table;
}

template<class T> template<class U>
inline bool BasicTableRef<T>::operator!=(BasicTableRef<U> const &r) const throw () {
	return m_table != r.m_table;
}

template<class T> inline BasicTableRef<T>::operator unspecified_bool_type() const throw () {
	return m_table ? &BasicTableRef::m_table : 0;
}

template<class T> inline void BasicTableRef<T>::reset(T *t) throw ()
{
	if(t == m_table) return;
	unbind();
	bind(t);
}

template<class T> inline void BasicTableRef<T>::bind(T *t) throw () {
	if (t) ++t->m_ref_count;
	m_table = t;
}

template<class T> inline void BasicTableRef<T>::unbind() throw () {
	if (m_table && --m_table->m_ref_count == 0) delete m_table;
}
