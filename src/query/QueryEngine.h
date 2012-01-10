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
	ParentNode *m_child;
};


template <class T> class Node : public ParentNode {
public:
	Node<T>(ParentNode *p, T v, size_t column) : m_child(p), m_value(v), m_column(column) {}
	ParentNode *m_child;
protected:
	size_t m_column;
	T m_value;
};


template <class T, class C, class F> class NODE : public ParentNode {
public:
	size_t m_column;
	T m_value;
	ParentNode *m_child;

	size_t Find(size_t start, size_t end, const Table& table) {
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


class OR_NODE : public ParentNode {
public:
	ParentNode *m_cond1;
	ParentNode *m_cond2;
	ParentNode *m_child;

	size_t Find(size_t start, size_t end, const Table& table) {
		size_t f1, f2;

		for(size_t s = start; s < end; s++) {
			// Todo, redundant searches can occur
			f1 = m_cond2->Find(s, end, table);
			f2 = m_cond1->Find(s, f1, table);
			s = f1 < f2 ? f1 : f2;

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

	OR_NODE(ParentNode *p1, ParentNode *p2, ParentNode *c) : m_cond1(p1), m_cond2(p2), m_child(c) {};
};
