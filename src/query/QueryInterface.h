
#ifndef Testing_Query_h
#define Testing_Query_h

#include <string>
#include <vector>
#include "query/QueryEngine.h"
#include <stdio.h>

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
		error_code = copy.error_code;
		copy.first[0] = 0;
	}

	~Query() {
		delete first[0];
	}

	Query& Equal(size_t column_id, int64_t value) {
		ParentNode* const p = new NODE<int64_t, Column, EQUAL>(value, column_id);
		UpdatePointers(p, &p->m_child);
		return *this;
	};
	Query& NotEqual(size_t column_id, int64_t value) {
		ParentNode* const p = new NODE<int64_t, Column, NOTEQUAL>(value, column_id);
		UpdatePointers(p, &p->m_child);
		return *this;
	};
	Query& Greater(size_t column_id, int64_t value) {
		ParentNode* const p = new NODE<int64_t, Column, GREATER>(value, column_id);
		UpdatePointers(p, &p->m_child);
		return *this;
	};
	Query& GreaterEqual(size_t column_id, int64_t value) {
		ParentNode* const p = new NODE<int64_t, Column, GREATEREQUAL>(value, column_id);
		UpdatePointers(p, &p->m_child);
		return *this;
	};
	Query& LessEqual(size_t column_id, int64_t value) {
		ParentNode* const p = new NODE<int64_t, Column, LESSEQUAL>(value, column_id);
		UpdatePointers(p, &p->m_child);
		return *this;
	};
	Query& Less(size_t column_id, int64_t value) {
		ParentNode* const p = new NODE<int64_t, Column, LESS>(value, column_id);
		UpdatePointers(p, &p->m_child);
		return *this;
	};

	Query& Between(size_t column_id, int64_t from, int64_t to) {
		ParentNode *p = new NODE<int64_t, Column, GREATEREQUAL>(from, column_id);
		ParentNode* const p2 = p;
		p = new NODE<int64_t, Column, LESSEQUAL>(to, column_id);
		p->m_child = p2;
		UpdatePointers(p, &p->m_child);
		return *this;
	};
	Query& Equal(size_t column_id, bool value) {
		ParentNode* const p = new NODE<bool, Column, EQUAL>(value, column_id);
		UpdatePointers(p, &p->m_child);
		return *this;
	};


	// STRINGS
	Query& Equal(size_t column_id, const char* value, bool caseSensitive=true) {
		ParentNode* p;
		if(caseSensitive)
			p = new STRINGNODE<EQUAL>(value, column_id);
		else
			p = new STRINGNODE<EQUAL_INS>(value, column_id);
		UpdatePointers(p, &p->m_child);
		return *this;
	};
	Query& BeginsWith(size_t column_id, const char* value, bool caseSensitive=true) {
		ParentNode* p;
		if(caseSensitive)
			p = new STRINGNODE<BEGINSWITH>(value, column_id);
		else
			p = new STRINGNODE<BEGINSWITH_INS>(value, column_id);
		UpdatePointers(p, &p->m_child);
		return *this;
	};
	Query& EndsWith(size_t column_id, const char* value, bool caseSensitive=true) {
		ParentNode* p; 
		if(caseSensitive)
			p = new STRINGNODE<ENDSWITH>(value, column_id);
		else
			p = new STRINGNODE<ENDSWITH_INS>(value, column_id);
		UpdatePointers(p, &p->m_child);
		return *this;
	};
	Query& Contains(size_t column_id, const char* value, bool caseSensitive=true) {
		ParentNode* p; 
		if(caseSensitive)
			p = new STRINGNODE<CONTAINS>(value, column_id);
		else
			p = new STRINGNODE<CONTAINS_INS>(value, column_id);
		UpdatePointers(p, &p->m_child);
		return *this;
	};
	Query& NotEqual(size_t column_id, const char* value, bool caseSensitive=true) {
		ParentNode* p;
		if(caseSensitive)
			p = new STRINGNODE<NOTEQUAL>(value, column_id);
		else
			p = new STRINGNODE<NOTEQUAL_INS>(value, column_id);
		UpdatePointers(p, &p->m_child);
		return *this;
	};



	void LeftParan(void) {
		update.push_back(0);
		update_override.push_back(0);
		first.push_back(0);
	};

	void Or(void) {
		ParentNode* const o = new OR_NODE(first[first.size()-1]);
		first[first.size()-1] = o;
		update[update.size()-1] = &((OR_NODE*)o)->m_cond2;
		update_override[update_override.size()-1] = &((OR_NODE*)o)->m_child;
	};

	void RightParan(void) {
		if(first.size() < 2) {
			error_code = "Unbalanced blockBegin/blockEnd";
			return;
		}

		if (update[update.size()-2] != 0)
			*update[update.size()-2] = first[first.size()-1];
		
		if(first[first.size()-2] == 0)
			first[first.size()-2] = first[first.size()-1];

		if(update_override[update_override.size()-1] != 0)
			update[update.size() - 2] = update_override[update_override.size()-1];
		else if(update[update.size()-1] != 0)
			update[update.size() - 2] = update[update.size()-1];

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

	std::string Verify(void) {
		if(first.size() == 0)
			return "";

		if(error_code != "") // errors detected by QueryInterface
			return error_code;

		if(first[0] == 0)
			return "Syntax error";

		return first[0]->Verify(); // errors detected by QueryEngine
	}

protected:
	friend class XQueryAccessorInt;
	friend class XQueryAccessorString;

	void UpdatePointers(ParentNode *p, ParentNode **newnode) {
		if(first[first.size()-1] == 0)
			first[first.size()-1] = p;

		if(update[update.size()-1] != 0)
			*update[update.size()-1] = p;

		update[update.size()-1] = newnode;
	}

	mutable std::vector<ParentNode *>first;
	std::vector<ParentNode **>update;
	std::vector<ParentNode **>update_override;

	private:
		std::string error_code;
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
	Query& EndsWith(const char *value, bool CaseSensitive) {return m_query->EndsWith(m_column_id, value, CaseSensitive);}
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
