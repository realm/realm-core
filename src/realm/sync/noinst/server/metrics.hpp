#ifndef REALM_SYNC_METRICS_HPP
#define REALM_SYNC_METRICS_HPP

#include <memory>
#include <string>
#include <vector>

namespace realm {
namespace sync {


namespace MetricsOptions {
typedef uint64_t OptionType;

// Any changes in these values must be mirrored in src/node/sync-server/src/index.ts:RealmMetricsExclusions
constexpr OptionType Core_Query = 0b0000'0001;
constexpr OptionType Core_Transaction_Read = 0b0000'0010;
constexpr OptionType Core_Transaction_Write = 0b0000'0100;

// Combinations for convenience
constexpr OptionType Core_Transaction = Core_Transaction_Read | Core_Transaction_Write;
constexpr OptionType Core_All = Core_Query | Core_Transaction;

} // end namespace MetricsOptions

struct MetricsExclusion {
    MetricsExclusion(MetricsOptions::OptionType mask);
    MetricsExclusion();
    // return true only if all the `options` specified are present in the exclusion
    bool will_exclude(MetricsOptions::OptionType options);

private:
    MetricsOptions::OptionType m_mask;
};


/// All member function implementations must be thread-safe.
///
/// FIXME: Consider adding support for specification of sample rate. The Dogless
/// API already supports this.
class Metrics {
public:
    /// Increment the counter identified by the specified metrics key.
    ///
    /// FIXME: Change to take arguments of type std::string_view
    virtual void increment(const char* key, int value = 1) = 0;

    /// Decrement the counter identified by the specified metrics key.
    virtual void decrement(const char* key, int value = 1) = 0;

    /// Set the value of the gauge identified by the specified metrics key.
    virtual void gauge(const char* key, double value) = 0;

    /// Adjust the gauge identified by the specified metrics key by adding the
    /// specified value to its current value.
    virtual void gauge_relative(const char* key, double value) = 0;

    /// Submit a timing, in milliseconds, for the specified metrics key.
    virtual void timing(const char* key, double value) = 0;

    /// Submit a value to the histogram identified by the specified metrics key.
    virtual void histogram(const char* key, double value) = 0;

    /// Checks if the key will be filtered due to the exclusion list specified in the constructor.
    bool will_exclude(MetricsOptions::OptionType options);

    Metrics(const MetricsExclusion& exclusions);
    Metrics();
    virtual ~Metrics() {}

    static std::string percent_encode(const std::string& string);

protected:
    MetricsExclusion m_exclusions;
};

class NullMetrics : public Metrics {
public:
    void increment(const char*, int) override;
    void decrement(const char*, int) override;
    void gauge(const char*, double) override;
    void gauge_relative(const char*, double) override;
    void timing(const char*, double) override;
    void histogram(const char*, double) override;
};


std::unique_ptr<Metrics> make_buffered_statsd_metrics(const std::string& endpoint, const std::string& prefix,
                                                      const MetricsExclusion& exclusions);


inline bool MetricsExclusion::will_exclude(sync::MetricsOptions::OptionType options)
{
    return (m_mask & options) == options;
}

inline bool Metrics::will_exclude(sync::MetricsOptions::OptionType options)
{
    return m_exclusions.will_exclude(options);
}

} // namespace sync
} // namespace realm

#endif // REALM_SYNC_METRICS_HPP
