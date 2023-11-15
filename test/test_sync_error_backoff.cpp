#include "realm/sync/noinst/client_impl_base.hpp"
#include "realm/util/time.hpp"

#include "test.hpp"
#include "util/test_path.hpp"

using namespace realm;
using namespace realm::sync;
using namespace realm::util;

TEST(Sync_ErrorBackoffCalculation)
{
    std::seed_seq seed_seq{test_util::random_int<int64_t>()};
    std::default_random_engine rand_eng(seed_seq);

    ResumptionDelayInfo delay_info{std::chrono::milliseconds{4}, std::chrono::milliseconds{1}, 2, 0};
    sync::ErrorBackoffState<int, decltype(rand_eng)> backoff(delay_info, rand_eng);
    backoff.update(1, std::nullopt);

    // Setup and check the first backoff. We should get the default delay interval and the triggering
    // error should be set.
    CHECK_EQUAL(backoff.delay_interval(), delay_info.resumption_delay_interval);
    CHECK_EQUAL(backoff.triggering_error, 1);

    // It should double from 1ms to 2ms on the next call to delay_interval()
    auto next_val = delay_info.resumption_delay_interval * delay_info.resumption_delay_backoff_multiplier;
    CHECK_EQUAL(backoff.delay_interval(), next_val);

    // It should double again from 2ms to 4ms on the next call to delay_interval()
    next_val *= delay_info.resumption_delay_backoff_multiplier;
    CHECK_EQUAL(backoff.delay_interval(), next_val);

    // But now we've git the maximum delay interval, so it should stay at 4ms
    CHECK_EQUAL(backoff.delay_interval(), next_val);

    // Changing the error code should reset us back to 1ms again
    backoff.update(2, std::nullopt);
    CHECK_EQUAL(backoff.delay_interval(), delay_info.resumption_delay_interval);
    CHECK_EQUAL(backoff.triggering_error, 2);

    // Then restart the incrementing sequence
    next_val = delay_info.resumption_delay_interval * delay_info.resumption_delay_backoff_multiplier;
    CHECK_EQUAL(backoff.delay_interval(), next_val);

    ResumptionDelayInfo new_delay_info{std::chrono::milliseconds{6}, std::chrono::milliseconds{3}, 2, 0};

    // Updating the delay info but not the error code to a different error should be a noop
    backoff.update(2, new_delay_info);
    CHECK_EQUAL(backoff.triggering_error, 2);
    CHECK(backoff.cur_delay_interval);
    CHECK_EQUAL(*backoff.cur_delay_interval, next_val);

    // But updating the error code and a new eror code should change all the math.
    backoff.update(3, new_delay_info);
    CHECK_EQUAL(backoff.delay_interval(), new_delay_info.resumption_delay_interval);
    CHECK_EQUAL(backoff.triggering_error, 3);

    // Check that the backoff uses the new delay info
    next_val = new_delay_info.resumption_delay_interval * new_delay_info.resumption_delay_backoff_multiplier;
    CHECK_EQUAL(backoff.delay_interval(), next_val);
    CHECK_EQUAL(backoff.delay_interval(), next_val);

    // Reset should go back to the original backoff info.
    backoff.reset();
    CHECK_EQUAL(backoff.triggering_error, std::optional<int>{});
    CHECK_EQUAL(backoff.delay_interval(), delay_info.resumption_delay_interval);

    // Update the delay info with jitter enabled.
    new_delay_info.delay_jitter_divisor = 4;
    backoff.update(4, new_delay_info);

    auto upper_bound = new_delay_info.resumption_delay_interval;
    auto lower_bound = upper_bound - (upper_bound / new_delay_info.delay_jitter_divisor);

    auto jitter_val = backoff.delay_interval();
    CHECK_GREATER_EQUAL(jitter_val, lower_bound);
    CHECK_LESS_EQUAL(jitter_val, upper_bound);

    upper_bound *= new_delay_info.resumption_delay_backoff_multiplier;
    lower_bound = upper_bound - (upper_bound / new_delay_info.delay_jitter_divisor);

    jitter_val = backoff.delay_interval();
    CHECK_GREATER_EQUAL(jitter_val, lower_bound);
    CHECK_LESS_EQUAL(jitter_val, upper_bound);

    jitter_val = backoff.delay_interval();
    CHECK_GREATER_EQUAL(jitter_val, lower_bound);
    CHECK_LESS_EQUAL(jitter_val, upper_bound);
}
