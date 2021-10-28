#include "test.hpp"

#include <realm/util/allocation_metrics.hpp>
#include <realm/util/metered/vector.hpp>
#include <thread>

using namespace realm;
using namespace realm::util;

AllocationMetricName test_component("test");
AllocationMetricName unique_ptr_component("unique_ptr");

NONCONCURRENT_TEST(AllocationMetric_Basic)
{
    AllocationMetricsContext tenant;
    AllocationMetricsContextScope tenant_scope{tenant};
    {
        AllocationMetricNameScope scope(test_component);
        metered::vector<char> vec;
        vec.reserve(1000);
    }
    {
        AllocationMetricNameScope scope(unique_ptr_component);
        auto ptr = util::make_unique<metered::vector<int>>(MeteredAllocator::get_default());
        ptr->resize(1000);
    }
    {
        // This should fall in the "Unknown" scope
        metered::vector<char> vec;
        vec.reserve(1000);
    }

    const MeteredAllocator& component = tenant.get_metric(test_component);
    CHECK(component.get_total_allocated_bytes() >= 1000);

    const MeteredAllocator& uptr = tenant.get_metric(unique_ptr_component);
    CHECK_GREATER_EQUAL(uptr.get_total_allocated_bytes(), 4000);

    const MeteredAllocator& unknown = tenant.get_metric(*AllocationMetricName::find("unknown"));
    CHECK(unknown.get_total_allocated_bytes() >= 1000);
}

NONCONCURRENT_TEST(AllocationMetric_Tenants)
{
    std::vector<std::unique_ptr<AllocationMetricsContext>> tenants;
    tenants.resize(10);
    std::generate(tenants.begin(), tenants.end(), []() {
        return std::make_unique<AllocationMetricsContext>();
    });

    std::vector<std::thread> threads;
    threads.reserve(10);
    for (size_t i = 0; i < 10; ++i) {
        threads.emplace_back([i = i, &tenants] {
            AllocationMetricsContextScope tenant_scope{*tenants[i]};
            AllocationMetricNameScope scope{test_component};
            util::metered::vector<char> memory;
            memory.resize(1024);
        });
    }
    for (size_t i = 0; i < 10; ++i) {
        threads[i].join();
    }

    for (size_t i = 0; i < 10; ++i) {
        MeteredAllocator& metric = tenants[i]->get_metric(test_component);
        CHECK_GREATER_EQUAL(metric.get_total_allocated_bytes(), 1024);
        CHECK_GREATER_EQUAL(metric.get_total_deallocated_bytes(), 1024);
    }
}
