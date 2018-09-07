cmake_minimum_required(VERSION 3.0.0)

project(abitest)

add_library(abitestlib STATIC IMPORTED)
set_target_properties(abitestlib PROPERTIES
	IMPORTED_LOCATION ${CMAKE_CURRENT_LIST_DIR}/abitest.a
)

add_executable(abitest abimain.cpp)
target_link_libraries(abitest abitestlib)
