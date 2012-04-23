echo "-------------------------------------------------------------------------------"
echo ""
echo ".:: Unit tests ::."
./tightdb-tests
echo ""
echo ".:: TightDB ::."
test-tightdb/test-tightdb
echo ""
echo ".:: SQLite 3 ::."
test-stl/test-sqlite3
echo ""
echo ".:: STL Vector ::."
test-stl/test-stl
