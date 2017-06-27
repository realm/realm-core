cmake_minimum_required (VERSION 2.6)

if(CMAKE_COMPILER_IS_GNUCC OR CMAKE_COMPILER_IS_CLANG)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -ggdb -std=c++11 -Wall")
endif()

add_executable(sha512sum sha224.cpp sha256.cpp sha384.cpp sha512.cpp sum.cpp)
add_executable(sha384sum sha224.cpp sha256.cpp sha384.cpp sha512.cpp sum.cpp)
add_executable(sha256sum sha224.cpp sha256.cpp sha384.cpp sha512.cpp sum.cpp)
add_executable(sha224sum sha224.cpp sha256.cpp sha384.cpp sha512.cpp sum.cpp)
