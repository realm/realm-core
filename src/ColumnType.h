#ifndef __TDB_COLUMNTYPE__
#define __TDB_COLUMNTYPE__

enum ColumnType {
	COLUMN_TYPE_INT,
	COLUMN_TYPE_BOOL,
	COLUMN_TYPE_STRING,
	COLUMN_TYPE_DATE,
	COLUMN_TYPE_BINARY
};

struct BinaryData {
	const void* pointer;
	size_t len;
};

#endif //__TDB_COLUMNTYPE__