#include "stdint.h"

class Column {
public:
	Column();
	~Column();

	size_t Size() const {return m_len;};
	bool IsEmpty() const {return m_len == 0;};

	int Get(size_t ndx) const {return (int)Get64(ndx);}
	bool Set(size_t ndx, int value) {return Set64(ndx, value);}
	bool Insert(size_t ndx, int value) {return Insert64(ndx, value);}
	bool Add(int value) {return Add64(value);}
	
	int64_t Get64(size_t ndx) const;
	bool Set64(size_t ndx, int64_t value);
	bool Insert64(size_t ndx, int64_t value);
	bool Add64(int64_t value);
	
	void Clear();
	void Delete(size_t ndx);
	//void Resize(size_t len);
	bool Reserve(size_t len, size_t width=8);
	size_t Find(int64_t value, size_t start=0, size_t end=-1) const;

private:
	typedef int64_t(Column::*Getter)(size_t) const;
    typedef void(Column::*Setter)(size_t, int64_t);

	int64_t Get_0b(size_t ndx) const;
	int64_t Get_1b(size_t ndx) const;
	int64_t Get_2b(size_t ndx) const;
	int64_t Get_4b(size_t ndx) const;
	int64_t Get_8b(size_t ndx) const;
	int64_t Get_16b(size_t ndx) const;
	int64_t Get_32b(size_t ndx) const;
	int64_t Get_64b(size_t ndx) const;

	void Set_0b(size_t ndx, int64_t value);
	void Set_1b(size_t ndx, int64_t value);
	void Set_2b(size_t ndx, int64_t value);
	void Set_4b(size_t ndx, int64_t value);
	void Set_8b(size_t ndx, int64_t value);
	void Set_16b(size_t ndx, int64_t value);
	void Set_32b(size_t ndx, int64_t value);
	void Set_64b(size_t ndx, int64_t value);

	bool Alloc(size_t count, size_t width);
	void SetWidth(size_t width);

	// Member variables
	unsigned char* m_data;
	size_t m_len;
	size_t m_capacity;
	size_t m_width;
	Getter m_getter;
	Setter m_setter;
};