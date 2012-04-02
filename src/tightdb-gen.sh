TIGHTDB_H="$1"

echo -n Generating header src/tightdb.h
if python src/tightdb-gen.py 8 >/tmp/tightdb.h 2>/tmp/tightdb.log; then
	echo
	mv /tmp/tightdb.h "$TIGHTDB_H"
	rm /tmp/tightdb.log
else
	echo ... Failed!
	if ! [ -e src/tightdb.h ]; then
		cat /tmp/tightdb.log
		rm /tmp/tightdb.log
		exit 1
	fi
fi
