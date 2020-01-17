set(BID_SOURCES
LIBRARY/src/bid128.c
LIBRARY/src/bid128_compare.c
LIBRARY/src/bid128_div.c
LIBRARY/src/bid128_add.c
LIBRARY/src/bid128_fma.c
LIBRARY/src/bid64_to_bid128.c
LIBRARY/src/bid_convert_data.c
LIBRARY/src/bid_decimal_data.c
LIBRARY/src/bid_decimal_globals.c
LIBRARY/src/bid_from_int.c
LIBRARY/src/bid_round.c
)

add_library(Bid OBJECT ${BID_SOURCES})
