#include <string>
#include "table.h"

// does v2 contain v1?
struct CONTAINS { 
	bool operator()(const char *v1, const char *v2) const { return(strstr(v1, v2) != 0); }
};

// is v2 a prefix of v1?
struct BEGINSWITH { 
	bool operator()(const char *v1, const char *v2) const { return(strstr(v1, v2) == v1); }
};

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
public:
	NODE(ParentNode *p, T v, size_t column) : m_child(p), m_value(v), m_column(column) {}
	~NODE() {delete m_child; }

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

protected:
	ParentNode* m_child;
	T m_value;
	size_t m_column;
};


template <class T, class C> class NODE <T, C, EQUAL>: public ParentNode {
public:
	NODE(ParentNode *p, T v, size_t column) : m_child(p), m_value(v), m_column(column) {}
	~NODE() {delete m_child; }

	size_t Find(size_t start, size_t end, const Table& table) {
		const C& column = (C&)(table.GetColumnBase(m_column));
		for (size_t s = start; s < end; ++s) {
			s = column.Find(m_value, s, end);
			if(s == -1) 
				s = end;

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

protected:
	ParentNode* m_child;
	T m_value;
	size_t m_column;
};



template <class F> class STRINGNODE : public ParentNode {
public:
	STRINGNODE(ParentNode *p, const char* v, size_t column) : m_child(p), m_value(v), m_column(column) {}
	~STRINGNODE() {delete m_child; free((void*)m_value);}

	size_t Find(size_t start, size_t end, const Table& table) {
		int column_type = table.GetRealColumnType(m_column);

		const F function = {};
		for (size_t s = start; s < end; ++s) {
			const char* t;

			// todo, can be optimized by placing outside loop
			if (column_type == COLUMN_TYPE_STRING)
				t = table.GetColumnString(m_column).Get(s);
			else
				t = table.GetColumnStringEnum(m_column).Get(s);

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

protected:
	ParentNode* m_child;
	const char* m_value;
	size_t m_column;
};


class OR_NODE : public ParentNode {
public:
	OR_NODE(ParentNode* p1, ParentNode* p2, ParentNode* c) : m_cond1(p1), m_cond2(p2), m_child(c) {};
	~OR_NODE() {
		delete m_cond1;
		delete m_cond2;
		delete m_child;
	}

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

protected:
	ParentNode* m_cond1;
	ParentNode* m_cond2;
	ParentNode* m_child;
};
