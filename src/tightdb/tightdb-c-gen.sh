TIGHTDB_H="$1"

python tightdb-c-gen.py 8 > tightdb-c.h
exit 1

if python tightdb-c-gen.py 8 >/tmp/tightdb-c.h; then
	mv /tmp/tightdb-c.h "$TIGHTDB_H"
else
	if [ -e tightdb-c.h ]; then
		echo "WARNING: Failed to update '$TIGHTDB_H'"
	else
		exit 1
	fi
fi
read nothing
