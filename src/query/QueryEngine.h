#include <string>
#include "table.h"

class ParentNode { 
public:
	virtual size_t Find(size_t start, size_t end, Table &table);
};

template <class T> class Node : public ParentNode {
public:
	size_t _column;
	T _value;
	ParentNode *_child;
	virtual size_t Find(size_t start, size_t end, Table &table);
	Node<T>(ParentNode *p, T v, size_t column) : _child(p), _value(v), _column(column) {}
};

template <class T> class NodeEqual : public Node<T> {
public:
	size_t Find(size_t start, size_t end, Table &table);
	NodeEqual<T>(ParentNode *p, T v, size_t column) : Node(p, v, column) {}
};

template <class T> class NodeNotEqual : public Node<T> {
public:
	size_t Find(size_t start, size_t end, Table &table);
	NodeNotEqual<T>(ParentNode *p, T v, size_t column) : Node(p, v, column) {}
};


template <class T> class NodeGreater : public Node<T> {
public:
	size_t Find(size_t start, size_t end, Table &table);
	NodeGreater<T>(ParentNode *p, T v, size_t column) : Node(p, v, column) {}
};

template <class T> class NodeGreaterEq : public Node<T> {
public:
	size_t Find(size_t start, size_t end, Table &table);
	NodeGreaterEq<T>(ParentNode *p, T v, size_t column) : Node(p, v, column) {}
};

template <class T> class NodeLessEq : public Node<T> {
public:
	size_t Find(size_t start, size_t end, Table &table);
	NodeLessEq<T>(ParentNode *p, T v, size_t column) : Node(p, v, column) {}
};

template <class T> class NodeLess : public Node<T> {
public:
	size_t Find(size_t start, size_t end, Table &table);
	NodeLess<T>(ParentNode *p, T v, size_t column) : Node(p, v, column) {}
};

template <class T> size_t Node<T>::Find(size_t start, size_t end, Table &table) {
	(void)start;
	(void)end;
	(void)table;
	assert(false);
	return 0;
}

