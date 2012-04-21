TIGHTDB_H="$1"

if python tightdb-gen.py 8 >/tmp/tightdb.hpp; then
	mv /tmp/tightdb.hpp "$TIGHTDB_H"
else
	if [ -e "$TIGHTDB_H" ]; then
		echo "WARNING: Failed to update '$TIGHTDB_H'"
	else
		exit 1
	fi
fi
