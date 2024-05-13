#include <realm/impl/transact_log.hpp>
#include <realm/sync/instructions.hpp>

using namespace realm;
using namespace realm::_impl;

namespace realm::sync {

const InternString InternString::npos = InternString{uint32_t(-1)};

} // namespace realm::sync
