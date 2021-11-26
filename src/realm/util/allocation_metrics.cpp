#include <realm/util/allocation_metrics.hpp>
#include <realm/util/assert.hpp>
#include <realm/string_data.hpp>
#include <cstdlib>

using namespace realm;
using namespace realm::util;

static const AllocationMetricName* g_last = nullptr;
static const AllocationMetricName g_unknown_name("unknown");
static size_t g_num_metric_names = 0;
static bool g_metric_names_locked = false;

#if !REALM_MOBILE
// thread_local not supported on iOS, and we likely don't care so much about
// heap metrics on mobile devices anyway.
thread_local const AllocationMetricName* g_current_name = &g_unknown_name;
thread_local AllocationMetricsContext* g_current_context = nullptr;
#endif

AllocationMetricNameScope::AllocationMetricNameScope(const AllocationMetricName& name) noexcept
    : m_name(name)
{
#if REALM_MOBILE
    m_previous = nullptr;
#else
    m_previous = g_current_name;
    g_current_name = &m_name;
#endif
}

AllocationMetricNameScope::~AllocationMetricNameScope()
{
#if !REALM_MOBILE
    REALM_ASSERT(g_current_name == &m_name);
    g_current_name = m_previous;
#else
    static_cast<void>(m_name); // Avoid warning about unused member
#endif
}

#if !REALM_MOBILE

AllocationMetricsContextScope::AllocationMetricsContextScope(AllocationMetricsContext& context) noexcept
    : m_context(context)
    , m_previous(AllocationMetricsContext::get_current())
{
    g_current_context = &m_context;
#if REALM_DEBUG
    ++m_context.m_refcount;
#endif // REALM_DEBUG
}

AllocationMetricsContextScope::~AllocationMetricsContextScope()
{
    REALM_ASSERT(&m_context == g_current_context);
#if REALM_DEBUG
    --m_context.m_refcount;
#endif // REALM_DEBUG
    g_current_context = &m_previous;
}

#endif // REALM_MOBILE

AllocationMetricName::AllocationMetricName(const char* name) noexcept
    : m_name(name)
    , m_index(g_num_metric_names++)
{
    // Creating a new AllocationMetricName after at least one instance
    // of AllocationMetricsContext has been created would mean all those
    // instances would need to have their m_metrics array resized.
    // We don't want to do all that bookkeeping, so just forbid it.
    REALM_ASSERT_RELEASE(!g_metric_names_locked);

    m_next = g_last;
    g_last = this;
}

AllocationMetricsContext::AllocationMetricsContext()
    : m_refcount(0)
{
    g_metric_names_locked = true;
    // FIXME: With C++17, use aligned new to get the proper alignment
    // for MeteredAllocator (should be 64).
    m_metrics.reset(new MeteredAllocator[g_num_metric_names]);
}

AllocationMetricsContext::~AllocationMetricsContext()
{
#if !REALM_MOBILE
    REALM_ASSERT(&get_current() != this);
#endif

#if REALM_DEBUG
    if (this != &get_unknown()) {
        REALM_ASSERT(m_refcount == 0); // references exist!
    }
#endif // REALM_DEBUG
}

#if !REALM_MOBILE
AllocationMetricsContext& AllocationMetricsContext::get_current() noexcept
{
    if (g_current_context == nullptr) {
        g_current_context = &get_unknown();
    }
    return *g_current_context;
}
#endif // !REALM_MOBILE

AllocationMetricsContext& AllocationMetricsContext::get_unknown()
{
    static AllocationMetricsContext& unknown = *new AllocationMetricsContext;
    return unknown;
}

MeteredAllocator& AllocationMetricsContext::get_metric(const AllocationMetricName& name) noexcept
{
    REALM_ASSERT(name.index() <= g_num_metric_names);
    return m_metrics.get()[name.index()];
}

MeteredAllocator::MeteredAllocator() noexcept
    : m_allocated_bytes(0)
    , m_deallocated_bytes(0)
{
    static_cast<void>(dummy);
    static_cast<void>(dummy2);
}

MeteredAllocator& MeteredAllocator::get_default() noexcept
{
#if REALM_MOBILE
    return unknown();
#else
    AllocationMetricsContext& tenant = AllocationMetricsContext::get_current();
    return tenant.get_metric(*g_current_name);
#endif
}

const AllocationMetricName* AllocationMetricName::get_top() noexcept
{
    return g_last;
}

const AllocationMetricName* AllocationMetricName::find(const char* name) noexcept
{
    StringData n(name);
    const AllocationMetricName* p = g_last;
    while (p) {
        if (n == p->m_name) {
            return p;
        }
        p = p->m_next;
    }
    return nullptr;
}

MeteredAllocator& MeteredAllocator::unknown() noexcept
{
#if REALM_MOBILE
    return AllocationMetricsContext::get_unknown().get_metric(g_unknown_name);
#else
    return AllocationMetricsContext::get_current().get_metric(g_unknown_name);
#endif
}
