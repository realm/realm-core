set(BID_SOURCES
LIBRARY/src/bid128.c
LIBRARY/src/bid128_compare.c
LIBRARY/src/bid128_mul.c
LIBRARY/src/bid128_div.c
LIBRARY/src/bid128_add.c
LIBRARY/src/bid128_fma.c
LIBRARY/src/bid128_string.c
LIBRARY/src/bid128_2_str_tables.c
LIBRARY/src/bid32_to_bid128.c
LIBRARY/src/bid64_to_bid128.c
LIBRARY/src/bid128_to_int64.c
LIBRARY/src/bid128_quantize.c
LIBRARY/src/bid_convert_data.c
LIBRARY/src/bid_decimal_data.c
LIBRARY/src/bid_decimal_globals.c
LIBRARY/src/bid_from_int.c
LIBRARY/src/bid_round.c
)

add_library(Bid OBJECT ${BID_SOURCES})
if(MSVC)
    get_target_property(comp_opts Bid COMPILE_OPTIONS)
    string(REGEX REPLACE "/W[1234]" "/W1" comp_opts "${comp_opts}")
    set_target_properties(Bid PROPERTIES COMPILE_OPTIONS "${comp_opts}")
    target_compile_options(Bid PUBLIC /sdl-)
endif()

# Note: checking for -Wfoo since unknown -Wno-foo flags are ignored.
check_cxx_compiler_flag(-Wunused-but-set-variable HAVE-Wunused-but-set-variable)
if(HAVE-Wunused-but-set-variable)
    target_compile_options(Bid PRIVATE -Wno-unused-but-set-variable)
endif()

set_target_properties(Bid PROPERTIES CXX_VISIBILITY_PRESET hidden)
