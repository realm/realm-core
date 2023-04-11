cmake_minimum_required(VERSION 3.15)

project(s2geometry)

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

set(S2_UTIL_MATH_SOURCES
    util/math/mathutil.cc
    util/math/mathlimits.cc
)

add_library(s2geometry OBJECT
    ${S2_SOURCES}
    ${S2_UTIL_MATH_SOURCES}
)

target_include_directories(s2geometry PRIVATE "." "..")

# suppress s2 specific warnings (lib is very noisy)
target_compile_options(s2geometry PRIVATE
    $<$<CXX_COMPILER_ID:MSVC>: /wd4068 /wd4244 /wd4267 /wd4305>
)
if (NOT MSVC)
    set(CMAKE_REQUIRED_QUIET ON)
    add_target_option_if_supported(s2geometry PRIVATE
        -Wno-deprecated-declarations
        -Wno-ignored-qualifiers
        -Wno-macro-redefined
        -Wno-missing-prototypes
        -Wno-shorten-64-to-32
        -Wno-undefined-var-template
        -Wno-unknown-pragmas
        -Wno-unused-const-variable
        -Wno-unused-function
        -Wno-unused-local-typedefs
        -Wno-unused-parameter
    )
endif()
