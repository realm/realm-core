cmake_minimum_required(VERSION 3.15)

project(s2geometry)

include_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}/../
    ${CMAKE_CURRENT_SOURCE_DIR}
)

set(S2_SOURCES
    s1angle.cc
    s2.cc
    s2cellid.cc
    s2latlng.cc
    s1interval.cc
    s2cap.cc
    s2cell.cc
    s2cellunion.cc
    s2edgeindex.cc
    s2edgeutil.cc
    s2latlngrect.cc
    s2loop.cc
    s2pointregion.cc
    s2polygon.cc
    s2polygonbuilder.cc
    s2polyline.cc
    s2r2rect.cc
    s2region.cc
    s2regioncoverer.cc
    s2regionintersection.cc
    s2regionunion.cc
)

set(S2_BASE_SOURCES
    base/int128.cc
    base/logging.cc
    base/stringprintf.cc
    base/strtoint.cc
)

set(S2_STRINGS_SOURCES
    strings/split.cc
    strings/stringprintf.cc
    strings/strutil.cc
)

set(S2_UTIL_CODING_SOURCES
    util/coding/coder.cc
    util/coding/varint.cc
)

set(S2_UTIL_MATH_SOURCES
    util/math/mathutil.cc
)

add_library(s2geometry STATIC
    ${S2_SOURCES}
    ${S2_BASE_SOURCES}
    ${S2_STRINGS_SOURCES}
    ${S2_UTIL_CODING_SOURCES}
    ${S2_UTIL_MATH_SOURCES}
)

add_library(Realm::external::s2geometry ALIAS s2geometry)

target_compile_options(s2geometry PUBLIC
    -Wno-macro-redefined
    -Wno-deprecated-declarations
    -Wno-unused-local-typedef
    -Wno-unknown-pragmas
    -Wno-shorten-64-to-32
    -Wno-unused-parameter
    -Wno-undefined-var-template
    -Wno-missing-prototypes
    -Wno-unused-const-variable
    -Wno-unused-function
)

