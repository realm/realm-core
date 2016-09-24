make test-debug-norun static
make -C test/test-realm # Runs with DEBUG disabled (because it would take forever)

#valgrind --leak-check=full --xml=yes --xml-file=valgrind.xml test/realm-tests-dbg --no-error-exit-staus
echo valgrind > valgrind.csv
(echo 0; cat valgrind.xml | perl -ne 'if (m/lost in loss record \d+ of (\d+)/){ print "$1\n"; }') | tail -1 >> valgrind.csv

valgrind --leak-check=full --xml=yes --xml-file=valgrind_test.xml test/test-realm/test-realm
echo valgrind_test > valgrind_test.csv
(echo 0; cat valgrind_test.xml | perl -ne 'if (m/lost in loss record \d+ of (\d+)/){ print "$1\n"; }') | tail -1 >> valgrind_test.csv
