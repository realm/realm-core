
#ifndef Testing_Query_h
#define Testing_Query_h

#include <string>
#include "QueryEngine.h"

class XQueryAccessorInt;
class XQueryAccessorString;

class Query {
friend class XQueryAccessorInt;
friend class XQueryAccessorString;

public:
	Query &Equal(size_t column_id, int64_t value) {
		ParentNode *p = new NodeEqual<int64_t>(m_parent_node, value, column_id);
		m_parent_node = p;
		return *this;
	};
	Query &NotEqual(size_t column_id, int64_t value) {
		ParentNode *p = new NodeNotEqual<int64_t>(m_parent_node, value, column_id);
		m_parent_node = p;
		return *this;
	};
	Query &Greater(size_t column_id, int64_t value) {
		ParentNode *p = new NodeGreater<int64_t>(m_parent_node, value, column_id);
		m_parent_node = p;
		return *this;
	};
	Query &Equal(size_t column_id, std::string value) {
		ParentNode *p = new NodeEqual<std::string>(m_parent_node, value, column_id);
		m_parent_node = p;
		return *this;
	};
	Query &NotEqual(size_t column_id, std::string value) {
		ParentNode *p = new NodeNotEqual<std::string>(m_parent_node, value, column_id);
		m_parent_node = p;
		return *this;
	};

	Query() : m_parent_node(0) {}

	TableView FindAll(Table &table) {
		TableView tv(table);
		size_t r = (size_t)-1;
		for(;;) {
			r = m_parent_node->Find(r + 1, table.GetSize(), table);
			if(r == table.GetSize())
				break;
			tv.GetRefColumn().Add(r);
		}
		return tv;
	}

	size_t Find(Table &table, size_t start, size_t end = -1) {
		TableView tv(table);
		if(end == -1)
			end = table.GetSize();
		size_t r = m_parent_node->Find(start, end, table);
		if(r == table.GetSize())
			return (size_t)-1;
		else
			return r;
	}

	ParentNode *m_parent_node;
};

class XQueryAccessorInt {
public:
	XQueryAccessorInt(size_t column_id) : m_column_id(column_id) {}
	Query &Equal(int64_t value) {return m_query->Equal(m_column_id, value);}
	Query &NotEqual(int64_t value) {return m_query->NotEqual(m_column_id, value);}
	Query &Greater(int64_t value) {return m_query->Greater(m_column_id, value);}
protected:
	Query* m_query;
	size_t m_column_id;
}; 
 
class XQueryAccessorString {
public:
	XQueryAccessorString(size_t column_id) : m_column_id(column_id) {}
	Query &Equal(std::string value) {return m_query->Equal(m_column_id, value);}
	Query &NotEqual(std::string value) {return m_query->NotEqual(m_column_id, value);}
protected:
	Query* m_query;
	size_t m_column_id;
};

#endif
