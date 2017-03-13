noinst_PROGRAMS = fuzz-transact-log fuzz-group generate-fuzzy-input
noinst_LIBS = ../../src/realm/librealm.a

fuzz_transact_log_SOURCES = fuzz_transact_log.cpp
fuzz_transact_log_LIBS = ../../src/realm/librealm.a

fuzz_group_SOURCES = afl.cpp ../fuzz_group.cpp ../util/test_path.cpp
fuzz_group_LIBS = ../../src/realm/librealm.a

generate_fuzzy_input_SOURCES = generate.cpp
generate_fuzzy_input_LIBS = ../../src/realm/librealm.a

include ../../src/generic.mk
