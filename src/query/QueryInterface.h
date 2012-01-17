
#ifndef Testing_Query_h
#define Testing_Query_h

#include <string>
#include <vector>
#include "QueryEngine.h"

#include <stdio.h>

/*
third.Equal(10).LeftParan().first.Equal(3).second.Greater(2).Or().first.Greater(5).RightParan() 

todo, comments about how it works and rename variable names

*/


class Query {
public:
	Query() { 
		update.push_back(0);
		update_override.push_back(0);
		first.push_back(0);
	}
	Query(const Query& copy) {
		update = copy.update;
		update_override = copy.update_override;
		first = copy.first;
		copy.first[0] = 0;
	}

	~Query() {
		delete first[0];
	}

	void UpdatePointers(ParentNode *p, ParentNode **newnode) {
		if(first[first.size()-1] == 0)
			first[first.size()-1] = p;

		if(update[update.size()-1] != 0)
			*update[update.size()-1] = p;
		
		update[update.size()-1] = newnode;
	}

	Query& Equal(size_t column_id, int64_t value) {
		ParentNode *p = new NODE<int64_t, Column, EQUAL>(0, value, column_id);
		UpdatePointers(p, &p->m_child);
		return *this;
	};
	Query& NotEqual(size_t column_id, int64_t value) {
		ParentNode *p = new NODE<int64_t, Column, NOTEQUAL>(0, value, column_id);
		UpdatePointers(p, &p->m_child);
		return *this;
	};
	Query& Greater(size_t column_id, int64_t value) {
		ParentNode *p = new NODE<int64_t, Column, GREATER>(0, value, column_id);
		UpdatePointers(p, &p->m_child);
		return *this;
	};
	Query& GreaterEqual(size_t column_id, int64_t value) {
		ParentNode *p = new NODE<int64_t, Column, GREATEREQUAL>(0, value, column_id);
		UpdatePointers(p, &p->m_child);
		return *this;
	};
	Query& LessEqual(size_t column_id, int64_t value) {
		ParentNode *p = new NODE<int64_t, Column, LESSEQUAL>(0, value, column_id);
		UpdatePointers(p, &p->m_child);
		return *this;
	};
	Query& Less(size_t column_id, int64_t value) {
		ParentNode *p = new NODE<int64_t, Column, LESS>(0, value, column_id);
		UpdatePointers(p, &p->m_child);
		return *this;
	};
	Query& Equal(size_t column_id, const char *value, bool CaseSensitive) {
		char *copy = (char *)malloc(strlen(value) + 1);
		memcpy(copy, value, strlen(value) + 1);
		ParentNode *p = new STRINGNODE<EQUAL>(0, (const char *)copy, column_id);
		UpdatePointers(p, &p->m_child);
		return *this;
	};
	Query& BeginsWith(size_t column_id, const char *value, bool CaseSensitive) {
		char *copy = (char *)malloc(strlen(value) + 1);
		memcpy(copy, value, strlen(value) + 1);
		ParentNode *p = new STRINGNODE<BEGINSWITH>(0, (const char *)copy, column_id);
		UpdatePointers(p, &p->m_child);
		return *this;
	};
	Query& Contains(size_t column_id, const char *value, bool CaseSensitive) {
		char *copy = (char *)malloc(strlen(value) + 1);
		memcpy(copy, value, strlen(value) + 1);
		ParentNode *p = new STRINGNODE<CONTAINS>(0, (const char *)copy, column_id);
		UpdatePointers(p, &p->m_child);
		return *this;
	};
	Query& NotEqual(size_t column_id, const char * value, bool CaseSensitive) {
		char *copy = (char *)malloc(strlen(value) + 1);
		memcpy(copy, value, strlen(value) + 1);
		ParentNode *p = new STRINGNODE<NOTEQUAL>(0, (const char *)copy, column_id);
		UpdatePointers(p, &p->m_child);
		return *this;
	};
	Query& Between(size_t column_id, int64_t from, int64_t to) {
		ParentNode *p = new NODE<int64_t, Column, GREATEREQUAL>(0, from, column_id);
		p = new NODE<int64_t, Column, LESSEQUAL>(p, to, column_id);
		UpdatePointers(p, &p->m_child);
		return *this;
	};
	Query& Equal(size_t column_id, bool value) {
		ParentNode *p = new NODE<bool, Column, EQUAL>(0, value, column_id);
		UpdatePointers(p, &p->m_child);
		return *this;
	};


	void m_LeftParan(void) {
		update.push_back(0);
		update_override.push_back(0);
		first.push_back(0);
	};

	void m_Or(void) {
		ParentNode *o = new OR_NODE(first[first.size()-1], 0, 0);
		first[first.size()-1] = o;
		update[update.size()-1] = &((OR_NODE*)o)->m_cond2;
		update_override[update_override.size()-1] = &((OR_NODE*)o)->m_cond2;
	};

	void m_RightParan(void) {
		if(update_override[update_override.size()-2] != 0)
			*update_override[update_override.size()-2] = first[first.size()-1]; 
		else if (update[update.size()-2] != 0)
			 *update[update.size()-2] = first[first.size()-1];
			
		if(first[first.size()-2] == 0)
			first[first.size()-2] = first[first.size()-1];

		first.pop_back();
		update.pop_back();
		update_override.pop_back();
	};

	TableView FindAll(Table& table, size_t start = 0, size_t end = -1) {
		TableView tv(table);
		FindAll(table, tv, start, end);
		return tv;
	}

	void FindAll(Table& table, TableView& tv, size_t start = 0, size_t end = -1) {
		size_t r = start - 1;
		if(end == -1)
			end = table.GetSize();
		for(;;) {
			if(first[0] != 0) 
				r = first[0]->Find(r + 1, table.GetSize(), table);
			else
				r++; // user built an empty query; return everything
			if(r == table.GetSize())
				break;
			tv.GetRefColumn().Add(r);
		}
	}

	size_t Find(Table& table, size_t start, size_t end = -1) {
		size_t r;
		TableView tv(table);
		if(end == -1)
			end = table.GetSize();
		if(first[0] != 0)
			r = first[0]->Find(start, end, table);
		else
			r = 0; // user built an empty query; return any first
		if(r == table.GetSize())
			return (size_t)-1;
		else
			return r;
	}


	mutable std::vector<ParentNode *>first;

protected:
	friend class XQueryAccessorInt;
	friend class XQueryAccessorString;

	std::vector<ParentNode **>update;
	std::vector<ParentNode **>update_override;
};

class XQueryAccessorInt {
public:
	XQueryAccessorInt(size_t column_id) : m_column_id(column_id) {}
	Query& Equal(int64_t value) {return m_query->Equal(m_column_id, value);}
	Query& NotEqual(int64_t value) {return m_query->NotEqual(m_column_id, value);}
	Query& Greater(int64_t value) {return m_query->Greater(m_column_id, value);}
	Query& GreaterEqual(int64_t value) {return m_query->GreaterEqual(m_column_id, value);}
	Query& Less(int64_t value) {return m_query->Less(m_column_id, value);}
	Query& LessEqual(int64_t value) {return m_query->LessEqual(m_column_id, value);}
	Query& Between(int64_t from, int64_t to) {return m_query->Between(m_column_id, from, to);}
protected:
	Query* m_query;
	size_t m_column_id;
}; 
 
class XQueryAccessorString {
public:
	XQueryAccessorString(size_t column_id) : m_column_id(column_id) {}
	Query& Equal(const char *value, bool CaseSensitive) {return m_query->Equal(m_column_id, value, CaseSensitive);}
	Query& BeginsWith(const char *value, bool CaseSensitive) {return m_query->Equal(m_column_id, value, CaseSensitive);}
	Query& Contains(const char *value, bool CaseSensitive) {return m_query->Contains(m_column_id, value, CaseSensitive);}
	Query& NotEqual(const char *value, bool CaseSensitive) {return m_query->NotEqual(m_column_id, value, CaseSensitive);}
protected:
	Query* m_query;
	size_t m_column_id;
};

class XQueryAccessorBool {
public:
	XQueryAccessorBool(size_t column_id) : m_column_id(column_id) {}
	Query& Equal(bool value) {return m_query->Equal(m_column_id, value);}
protected:
	Query* m_query;
	size_t m_column_id;
};

#endif
