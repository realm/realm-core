#ifndef REALM_SYNC_CLOCK_HPP
#define REALM_SYNC_CLOCK_HPP

#include <chrono>

namespace realm {
namespace sync {

class Clock {
public:
    using clock = std::chrono::system_clock;
    using time_point = clock::time_point;
    using duration = clock::duration;

    virtual ~Clock() {}

    /// Implementation must be thread-safe.
    virtual time_point now() const noexcept = 0;
};

} // namespace sync
} // namespace realm

#endif // REALM_SYNC_CLOCK_HPP
