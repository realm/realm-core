check_PROGRAMS = fuzz-transact-log fuzz-group generate-fuzzy-input

fuzz_transact_log_SOURCES = fuzz_transact_log.cpp
fuzz_transact_log_LIBS = ../util/test-util.a

fuzz_group_SOURCES = afl.cpp ../fuzz_group.cpp
fuzz_group_LIBS = ../util/test-util.a

generate_fuzzy_input_SOURCES = generate.cpp
generate_fuzzy_input_LIBS = ../util/test-util.a

include ../../src/generic.mk
