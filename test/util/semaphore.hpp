#ifndef REALM_TEST_UTIL_SEMAPHORE_HPP
#define REALM_TEST_UTIL_SEMAPHORE_HPP

#include <realm/util/thread.hpp>

namespace realm {
namespace test_util {

class BowlOfStonesSemaphore {
public:
    BowlOfStonesSemaphore(int initial_number_of_stones = 0):
        m_num_stones(initial_number_of_stones)
    {
    }
    void get_stone()
    {
        util::LockGuard lock(m_mutex);
        while (m_num_stones == 0)
            m_cond_var.wait(lock);
        --m_num_stones;
    }
    void add_stone()
    {
        util::LockGuard lock(m_mutex);
        ++m_num_stones;
        m_cond_var.notify();
    }
private:
    util::Mutex m_mutex;
    int m_num_stones;
    util::CondVar m_cond_var;
};

} // namespace test_util
} // namespace realm

#endif // REALM_TEST_UTIL_SEMAPHORE_HPP
