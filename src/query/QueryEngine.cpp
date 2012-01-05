#include "QueryInterface.h"
#include "table.h"

// todo, put everything into 1 function

template <> size_t NodeEqual<std::string>::Find(size_t start, size_t end, Table &table) {
	AdaptiveStringColumn &c = table.GetColumnString(_column);
	for(size_t s = start; s < end; s++) {
		const char *t = c.Get(s);
		if(strcmp(t, _value.c_str()) == 0) {
			if(_child == 0)
				return s;
			else {
				size_t a = _child->Find(s, end, table);
				if(s == a)
					return s;
				else
					s = a - 1;
			}
		}
	}
	return end;
}

template <> size_t NodeNotEqual<std::string>::Find(size_t start, size_t end, Table &table) {
	AdaptiveStringColumn &c = table.GetColumnString(_column);
	for(size_t s = start; s < end; s++) {
		const char *t = c.Get(s);
		if(strcmp(t, _value.c_str()) != 0) {
			if(_child == 0)
				return s;
			else {
				size_t a = _child->Find(s, end, table);
				if(s == a)
					return s;
				else
					s = a - 1;
			}
		}
	}
	return end;
}

template <> size_t NodeEqual<int64_t>::Find(size_t start, size_t end, Table &table) {
	Column &c = table.GetColumn(_column);
	for(size_t s = start; s < end; s++) {
		if(c.Get(s) == _value) {
			if(_child == 0)
				return s;
			else {
				size_t a = _child->Find(s, end, table);
				if(s == a)
					return s;
				else
					s = a - 1;
			}
		}
	}
	return end;
}

template <> size_t NodeNotEqual<int64_t>::Find(size_t start, size_t end, Table &table) {
	Column &c = table.GetColumn(_column);
	for(size_t s = start; s < end; s++) {
		if(c.Get(s) != _value) {
			if(_child == 0)
				return s;
			else {
				size_t a = _child->Find(s, end, table);
				if(s == a)
					return s;
				else
					s = a - 1;
			}
		}
	}
	return end;
}

template <> size_t NodeLess<int64_t>::Find(size_t start, size_t end, Table &table) {
	Column &c = table.GetColumn(_column);
	for(size_t s = start; s < end; s++) {
		if(c.Get(s) < _value) {
			if(_child == 0)
				return s;
			else {
				size_t a = _child->Find(s, end, table);
				if(s == a)
					return s;
				else
					s = a - 1;
			}
		}
	}
	return end;
}

template <> size_t NodeGreater<int64_t>::Find(size_t start, size_t end, Table &table) {
	Column &c = table.GetColumn(_column);
	for(size_t s = start; s < end; s++) {
		if(c.Get(s) > _value) {
			if(_child == 0)
				return s;
			else {
				size_t a = _child->Find(s, end, table);
				if(s == a)
					return s;
				else
					s = a - 1;
			}
		}
	}
	return end;
}

template <> size_t NodeLessEq<int64_t>::Find(size_t start, size_t end, Table &table) {
	Column &c = table.GetColumn(_column);
	for(size_t s = start; s < end; s++) {
		if(c.Get(s) <= _value) {
			if(_child == 0)
				return s;
			else {
				size_t a = _child->Find(s, end, table);
				if(s == a)
					return s;
				else
					s = a - 1;
			}
		}
	}
	return end;
}

template <> size_t NodeGreaterEq<int64_t>::Find(size_t start, size_t end, Table &table) {
	Column &c = table.GetColumn(_column);
	for(size_t s = start; s < end; s++) {
		if(c.Get(s) >= _value) {
			if(_child == 0)
				return s;
			else {
				size_t a = _child->Find(s, end, table);
				if(s == a)
					return s;
				else
					s = a - 1;
			}
		}
	}
	return end;
}


size_t ParentNode::Find(size_t start, size_t end, Table &table) {
	assert(false);
	return 0;
}

