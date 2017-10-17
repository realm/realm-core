It's useful to use the latest version of clang to test core with dynamic analyzers.

To do this:

1. Build the image
```
docker build -f clang.Dockerfile -t realm-core-clang:snapshot .
```
2. Run the image
```
docker run -ti -v $(pwd):/tmp -w /tmp realm-core-clang:snapshot bash
```

Now you can build core and run the tests as usual. An example for the address sanitizer:
```
mkdir build
cd build
cmake -G Ninja -D REALM_ASAN=ON ..
ninja
cd test
./realm-test
```