#include <atomic>
#include <chrono>

#ifdef _WIN32
#define NOMINMAX
#include <windows.h>
#else
#include <unistd.h>
#endif

#include <realm/util/random.hpp>


namespace {

std::atomic<unsigned int> g_counter{0};

} // unnamed namespace


namespace realm {
namespace _impl {

void get_extra_seed_entropy(unsigned int& extra_entropy_1, unsigned int& extra_entropy_2,
                            unsigned int& extra_entropy_3)
{
    using clock = std::chrono::high_resolution_clock;
    using uint = unsigned int;
    extra_entropy_1 = uint(clock::now().time_since_epoch().count());

#ifdef _WIN32
    extra_entropy_2 = uint(GetCurrentProcessId());
#else
    extra_entropy_2 = uint(getpid());
#endif

    extra_entropy_3 = ++g_counter;
}

} // namespace _impl
} // namespace realm
