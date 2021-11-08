#ifndef REALM_TEST_UTIL_MOCK_METRICS_HPP
#define REALM_TEST_UTIL_MOCK_METRICS_HPP

#include <string>
#include <utility>
#include <vector>

#include <realm/util/logger.hpp>
#include <realm/util/thread.hpp>
#include <realm/sync/noinst/server/metrics.hpp>

namespace realm {
namespace test_util {

class MockMetrics : public sync::Metrics {
public:
    explicit MockMetrics(util::Logger* = nullptr);
    explicit MockMetrics(const sync::MetricsExclusion& exclusions, util::Logger* = nullptr);
    ~MockMetrics() {}

    void increment(const char*, int) override;
    void decrement(const char*, int) override;
    void gauge(const char*, double) override;
    void gauge_relative(const char*, double) override;
    void timing(const char*, double) override;
    void histogram(const char*, double) override;

    double sum_equal(const char* key) const;
    double sum_contains(const char* key) const;
    double last_equal(const char* key) const;
    double last_contains(const char* key) const;
    std::size_t count_equal(const char* key) const;
    std::size_t count_contains(const char* key) const;
    std::size_t count_beginswith(const char* key) const;

    std::size_t size() const;

private:
    util::Logger* const m_logger;
    mutable util::Mutex m_mutex;
    std::vector<std::pair<std::string, double>> m_state; // Protected my `m_mutex`
};


// Implementation

inline MockMetrics::MockMetrics(util::Logger* logger)
    : sync::Metrics()
    , m_logger{logger}
{
}

inline MockMetrics::MockMetrics(const sync::MetricsExclusion& exclusions, util::Logger* logger)
    : sync::Metrics{exclusions}
    , m_logger{logger}
{
}

} // namespace test_util
} // namespace realm

#endif // REALM_TEST_UTIL_MOCK_METRICS_HPP
