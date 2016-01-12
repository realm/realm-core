#ifndef REALM_FUZZ_GROUP_HPP
#define REALM_FUZZ_GROUP_HPP

#include <string>
#include <realm/group.hpp>
#include <realm/util/optional.hpp>

int run_fuzzy(int argc, const char* argv[]);
void parse_and_apply_instructions(std::string& in, realm::Group& g, realm::util::Optional<std::ostream&> log);

#endif
