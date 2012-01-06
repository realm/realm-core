#include <string>
#include "table.h"


struct EQUAL { 
	bool operator()(const char *v1, const char *v2) { return strcmp(v1, v2) == 0; }
	template<class T> bool operator()(T& v1, T& v2) {return v1 == v2;}  
};

struct NOTEQUAL { 
	bool operator()(const char *v1, const char *v2) { return strcmp(v1, v2) != 0; }
	template<class T> bool operator()(T& v1, T& v2) { return v1 != v2; }
};

struct GREATER { 
	template<class T> bool operator()(const T& v1, const T& v2) {return v1 > v2;} 
};

struct LESS { 
	template<class T> bool operator()(const T& v1, const T& v2) {return v1 < v2;} 
};

struct LESSEQUAL { 
	template<class T> bool operator()(const T& v1, const T& v2) {return v1 <= v2;} 
};

struct GREATEREQUAL { 
	template<class T> bool operator()(const T& v1, const T& v2) {return v1 >= v2;} 
};


class ParentNode { 
public:
	virtual size_t Find(size_t start, size_t end, const Table& table) = 0;
};


template <class T> class Node : public ParentNode {
public:
	Node<T>(ParentNode *p, T v, size_t column) : m_child(p), m_value(v), m_column(column) {}
protected:
	size_t m_column;
	T m_value;
	ParentNode *m_child;
};


template <class T, class C, class F> class NODE : public ParentNode {
public:
	size_t m_column;
	T m_value;
	ParentNode *m_child;

	size_t Find(size_t start, size_t end, const Table& table) {
//		C& column = static_cast<C&>(table.GetColumnBase(m_column));
		const C& column = (C&)(table.GetColumnBase(m_column));
		F function;
		for(size_t s = start; s < end; s++) {
			T t = column.Get(s);
			if(function(t, m_value)) {
				if(m_child == 0)
					return s;
				else {
					size_t a = m_child->Find(s, end, table);
					if(s == a)
						return s;
					else
						s = a - 1;
				}
			}
		}
		return end;
	}

	NODE<T, C, F>(ParentNode *p, T v, size_t column) : m_child(p), m_value(v), m_column(column) {}
};
