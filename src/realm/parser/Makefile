all: query_flex.cpp query_bison.cpp

query_flex.cpp: query_flex.ll
	flex --noline --header-file=query_flex.hpp -o $@ $<

query_bison.cpp: query_bison.yy
	bison --defines=query_bison.hpp -o $@ $<