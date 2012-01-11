#include <string>
#include "table.h"


struct EQUAL { 
	bool operator()(const char *v1, const char *v2) const { return strcmp(v1, v2) == 0; }
	template<class T> bool operator()(const T& v1, const T& v2) const {return v1 == v2;}
};

struct NOTEQUAL { 
	bool operator()(const char *v1, const char *v2) const { return strcmp(v1, v2) != 0; }
	template<class T> bool operator()(const T& v1, const T& v2) const { return v1 != v2; }
};

struct GREATER { 
	template<class T> bool operator()(const T& v1, const T& v2) const {return v1 > v2;}
};

struct LESS { 
	template<class T> bool operator()(const T& v1, const T& v2) const {return v1 < v2;}
};

struct LESSEQUAL { 
	template<class T> bool operator()(const T& v1, const T& v2) const {return v1 <= v2;}
};

struct GREATEREQUAL { 
	template<class T> bool operator()(const T& v1, const T& v2) const {return v1 >= v2;}
};


class ParentNode { 
public:
	virtual ~ParentNode() {}
	virtual size_t Find(size_t start, size_t end, const Table& table) = 0;
};


template <class T, class C, class F> class NODE : public ParentNode {
private:
	size_t m_column;
	ParentNode* m_child;
protected:
	T m_value;
public:
	~NODE() {if(m_child != NULL) delete m_child; }
	NODE<T, C, F>(ParentNode *p, T v, size_t column) : m_child(p), m_value(v), m_column(column) {}

	size_t Find(size_t start, size_t end, const Table& table) {
		const C& column = (C&)(table.GetColumnBase(m_column));
		const F function = {};
		for (size_t s = start; s < end; ++s) {
			const T t = column.Get(s);
			if (function(t, m_value)) {
				if (m_child == 0)
					return s;
				else {
					const size_t a = m_child->Find(s, end, table);
					if (s == a)
						return s;
					else
						s = a - 1;
				}
			}
		}
		return end;
	}
};

template <class F> class STRINGNODE : public NODE<const char *, AdaptiveStringColumn, F> {
public:
	STRINGNODE<F>(ParentNode *p, const char *v, size_t column) : NODE(p, v, column) {}
	~STRINGNODE<F>() {free((void *)m_value);}
};


class OR_NODE : public ParentNode {
private:
	ParentNode *m_cond1;
	ParentNode *m_cond2;
	ParentNode *m_child;
public:
	~OR_NODE() {
		delete m_cond1;
		delete m_cond2;
		if(m_child != NULL)
			delete m_child;
	}
	OR_NODE(ParentNode* p1, ParentNode* p2, ParentNode* c) : m_cond1(p1), m_cond2(p2), m_child(c) {};

	size_t Find(size_t start, size_t end, const Table& table) {
		for (size_t s = start; s < end; ++s) {
			// Todo, redundant searches can occur
			const size_t f1 = m_cond2->Find(s, end, table);
			const size_t f2 = m_cond1->Find(s, f1, table);
			s = f1 < f2 ? f1 : f2;

			if (m_child == 0)
				return s;
			else {
				const size_t a = m_child->Find(s, end, table);
				if (s == a)
					return s;
				else
					s = a - 1;
			}
		}
		return end;
	}
};
