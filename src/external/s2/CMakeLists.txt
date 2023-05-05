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

    base/basictypes.h
    base/casts.h
    base/commandlineflags.h
    base/definer.h
    base/integral_types.h
    base/logging.h
    base/macros.h
    base/stl_decl.h
    base/stl_decl_msvc.h
    base/stl_decl_osx.h
    base/template_util.h
    hash.h
    r1interval.h
    s1angle.h
    s1interval.h
    s2.h
    s2cap.h
    s2cell.h
    s2cellid.h
    s2cellunion.h
    s2edgeindex.h
    s2edgeutil.h
    s2latlng.h
    s2latlngrect.h
    s2loop.h
    s2pointregion.h
    s2polygon.h
    s2polygonbuilder.h
    s2polyline.h
    s2r2rect.h
    s2region.h
    s2regioncoverer.h
    s2regionintersection.h
    s2regionunion.h
    util/math/mathlimits.h
    util/math/mathutil.h
    util/math/matrix3x3-inl.h
    util/math/matrix3x3.h
    util/math/vector2-inl.h
    util/math/vector2.h
    util/math/vector3-inl.h
    util/math/vector3.h
    util/math/vector4-inl.h
    util/math/vector4.h
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
set_target_properties(s2geometry PROPERTIES CXX_VISIBILITY_PRESET hidden)
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
