# `newdate` Benchmark

This benchmark compares old date time to the new timestamp column type. The
branch for this change was originally called `newdate`, hence the name of the
benchmark.

The benchmark should ideally be split into two, when the old date time column
type actually disappears from `master`. When this happens, you can implement
cross-commit benchmarking by modifying the `REF` variable in the
`CMakeLists.txt` for the old date time benchmark to some commit that still
contains the old column type.
