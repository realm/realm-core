#if REALM_HAVE_DOGLESS
#include <dogless.hpp>
#endif

#include <algorithm>
#include <iomanip>
#include <locale>
#include <sstream>

#include <realm/sync/noinst/server/metrics.hpp>

using namespace realm;

sync::MetricsExclusion::MetricsExclusion(sync::MetricsOptions::OptionType mask)
    : m_mask(mask)
{
}

sync::MetricsExclusion::MetricsExclusion() {}

sync::Metrics::Metrics(const MetricsExclusion& exclusions)
    : m_exclusions(exclusions)
{
}


sync::Metrics::Metrics()
    : m_exclusions(0)
{
}

std::string sync::Metrics::percent_encode(const std::string& string)
{
    // This implementation of percent encoding deviates from a standard
    // implementation by encoding all characters that are not alphanumerical.
    std::locale loc = std::locale::classic();
    std::ostringstream out;
    out.imbue(loc); // Throws
    out.fill('0');
    out << std::hex;
    out << std::uppercase;

    const std::ctype<char>& ctype = std::use_facet<std::ctype<char>>(loc);
    for (char ch : string) {
        bool pass_through = ctype.is(std::ctype<char>::alnum, ch);
        if (pass_through) {
            out << ch; // Throws
        }
        else {
            int ch_2 = std::string::traits_type::to_int_type(ch);
            out << '%' << std::setw(2) << ch_2; // Throws
        }
    }

    return out.str(); // Throws
}


#if REALM_HAVE_DOGLESS

namespace {

class DoglessMetrics : public sync::Metrics {
public:
    DoglessMetrics(const std::string& prefix, const sync::MetricsExclusion& exclusions)
        : Metrics(exclusions)
        , m_dogless{prefix} // Throws
    {
        m_dogless.loop_interval(1);
    }

    void increment(const char* key, int value) override final
    {
        const char* metric = key;
        m_dogless.increment(metric, value); // Throws
    }

    void decrement(const char* key, int value) override final
    {
        const char* metric = key;
        m_dogless.decrement(metric, value); // Throws
    }

    void gauge(const char* key, double value) override final
    {
        const char* metric = key;
        m_dogless.gauge(metric, value); // Throws
    }

    void gauge_relative(const char* key, double value) override final
    {
        const char* metric = key;
        m_dogless.gauge_relative(metric, value); // Throws
    }

    void timing(const char* key, double value) override final
    {
        const char* metric = key;
        m_dogless.timing(metric, value); // Throws
    }

    void histogram(const char* key, double value) override final
    {
        const char* metric = key;
        m_dogless.histogram(metric, value); // Throws
    }

    void add_endpoint(const std::string& endpoint)
    {
        m_dogless.add_endpoint(endpoint);
    }

private:
    dogless::BufferedStatsd m_dogless;
};

} // unnamed namespace

#endif // REALM_HAVE_DOGLESS


void sync::NullMetrics::increment(const char*, int)
{
    // No-op
}


void sync::NullMetrics::decrement(const char*, int)
{
    // No-op
}


void sync::NullMetrics::gauge(const char*, double)
{
    // No-op
}


void sync::NullMetrics::gauge_relative(const char*, double)
{
    // No-op
}


void sync::NullMetrics::timing(const char*, double)
{
    // No-op
}


void sync::NullMetrics::histogram(const char*, double)
{
    // No-op
}


std::unique_ptr<sync::Metrics> sync::make_buffered_statsd_metrics(const std::string& endpoint,
                                                                  const std::string& prefix,
                                                                  const MetricsExclusion& exclusions)
{
#if REALM_HAVE_DOGLESS
    std::unique_ptr<DoglessMetrics> metrics = std::make_unique<DoglessMetrics>(prefix, exclusions); // Throws
    metrics->add_endpoint(endpoint);                                                                // Throws
    return metrics;
#else
    static_cast<void>(endpoint);
    static_cast<void>(prefix);
    static_cast<void>(exclusions);
    return nullptr;
#endif
}
