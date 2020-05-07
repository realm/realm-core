#include "mock_metrics.hpp"

#include <algorithm>
#include <string.h>

namespace realm {
namespace test_util {

void MockMetrics::increment(const char* key, int value)
{
    if (m_logger)
        m_logger->info("Metrics: Increment(%1, %2)", key, value);
    util::LockGuard lock{m_mutex};
    const char* metric = key;
    m_state.push_back({metric, value});
}


void MockMetrics::decrement(const char* key, int value)
{
    if (m_logger)
        m_logger->info("Metrics: Decrement(%1, %2)", key, value);
    util::LockGuard lock{m_mutex};
    const char* metric = key;
    m_state.push_back({metric, -value});
}


void MockMetrics::gauge(const char* key, double value)
{
    if (m_logger)
        m_logger->info("Metrics: Gauge(%1, %2)", key, value);
    util::LockGuard lock{m_mutex};
    const char* metric = key;
    m_state.push_back({metric, value});
}


void MockMetrics::gauge_relative(const char* key, double value)
{
    if (m_logger)
        m_logger->info("Metrics: GaugeRelative(%1, %2)", key, value);
    util::LockGuard lock{m_mutex};
    const char* metric = key;
    double last_existing_gague_value = 0;
    auto iter = std::find_if(m_state.rbegin(), m_state.rend(), [key](const std::pair<std::string, double>& pair) {
        return (pair.first == key);
    });
    if (iter != m_state.rend()) {
        last_existing_gague_value = iter->second;
    }

    m_state.push_back({metric, value + last_existing_gague_value});
}


void MockMetrics::timing(const char* key, double value)
{
    if (m_logger)
        m_logger->info("Metrics: Timing(%1, %2)", key, value);
    util::LockGuard lock{m_mutex};
    const char* metric = key;
    m_state.push_back({metric, value});
}


void MockMetrics::histogram(const char* key, double value)
{
    if (m_logger)
        m_logger->info("Metrics: Histogram(%1, %2)", key, value);
    util::LockGuard lock{m_mutex};
    const char* metric = key;
    m_state.push_back({metric, value});
}


double MockMetrics::sum_equal(const char* key) const
{
    util::LockGuard lock{m_mutex};
    double sum = 0;
    std::for_each(m_state.begin(), m_state.end(), [key, &sum](const auto& item) {
        if (item.first == key)
            sum += item.second;
    });
    return sum;
}


double MockMetrics::sum_contains(const char* key) const
{
    util::LockGuard lock{m_mutex};
    double sum = 0;
    std::for_each(m_state.begin(), m_state.end(), [key, &sum](const auto& item) {
        if (item.first.find(key) != std::string::npos)
            sum += item.second;
    });
    return sum;
}


double MockMetrics::last_equal(const char* key) const
{
    util::LockGuard lock{m_mutex};
    double value = 0;
    auto iter = std::find_if(m_state.rbegin(), m_state.rend(), [key](const std::pair<std::string, double>& pair) {
        return (pair.first == key);
    });
    if (iter != m_state.rend()) {
        value = iter->second;
    }
    return value;
}


double MockMetrics::last_contains(const char* key) const
{
    util::LockGuard lock{m_mutex};
    double value = 0;
    auto iter = std::find_if(m_state.rbegin(), m_state.rend(), [key](const auto& pair) {
        return (pair.first.find(key) != std::string::npos);
    });
    if (iter != m_state.rend()) {
        value = iter->second;
    }
    return value;
}


std::size_t MockMetrics::count_equal(const char* key) const
{
    util::LockGuard lock{m_mutex};
    std::size_t count = 0;
    std::for_each(m_state.begin(), m_state.end(), [key, &count](const auto& item) {
        if (item.first == key)
            ++count;
    });
    return count;
}


std::size_t MockMetrics::count_contains(const char* key) const
{
    util::LockGuard lock{m_mutex};
    std::size_t count = 0;
    std::for_each(m_state.begin(), m_state.end(), [key, &count](const auto& item) {
        if (item.first.find(key) != std::string::npos)
            ++count;
    });
    return count;
}


std::size_t MockMetrics::count_beginswith(const char* key) const
{
    util::LockGuard lock{m_mutex};
    std::size_t count = 0;
    std::size_t key_length = strlen(key);
    std::for_each(m_state.begin(), m_state.end(), [key, &count, key_length](const auto& item) {
        if (std::strncmp(key, item.first.c_str(), key_length) == 0)
            ++count;
    });
    return count;
}


std::size_t MockMetrics::size() const
{
    return m_state.size();
}

} // namespace test_util
} // namespace realm
