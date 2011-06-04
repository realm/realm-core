echo "-------------------------------------------------------------------------------"
echo ""
echo ".:: Unit tests ::."
./tightdb-tests
echo ""
echo ".:: TightDB ::."
test-tightdb/test-tightdb
echo ""
echo ".:: STL Vector ::."
test-stl_vector/test-stl_vector
