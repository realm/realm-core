#include <realm/impl/transact_log.hpp>
#include <realm/sync/instructions.hpp>

#include <algorithm>
#include <ostream>
#include <set>

using namespace realm;
using namespace realm::_impl;

namespace realm {
namespace sync {

const InternString InternString::npos = InternString{uint32_t(-1)};


} // namespace sync
} // namespace realm
