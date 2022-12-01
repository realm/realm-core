#include <realm/util/default_websocket.hpp>

#include <realm/util/network.hpp>
#include <realm/util/network_ssl.hpp>
#include <realm/util/random.hpp>
#include <realm/util/scope_exit.hpp>
#include <realm/util/websocket.hpp>

namespace realm::util::websocket {

namespace {

class DefaultServiceClientImpl : public DefaultServiceClient {
public:
    enum State { NotStarted = 0, Running = 1, Stopping = 3, Stopped = 2 };

    class Timer : public EventLoopClient::Timer {
    public:
        Timer() = default;

        Timer(util::network::Service& service, std::chrono::milliseconds delay,
              util::UniqueFunction<void(std::error_code)>&& handler)
            : m_timer(std::make_unique<util::network::DeadlineTimer>(service))
        {
            m_timer->async_wait(delay, std::move(handler)); // Throws
        }

        virtual ~Timer()
        {
            cancel();
        }

        static std::unique_ptr<EventLoopClient::Timer>
        async_wait(util::network::Service& service, std::chrono::milliseconds delay,
                   util::UniqueFunction<void(std::error_code)>&& handler) // Throws
        {
            return std::make_unique<DefaultServiceClientImpl::Timer>(service, delay, std::move(handler));
        }


        void cancel() override
        {
            if (auto timer_ptr = std::move(m_timer); timer_ptr != nullptr) {
                timer_ptr->cancel();
            }
            // DeadlineTimer instance is destroyed when this function exits
        }

    private:
        std::unique_ptr<util::network::DeadlineTimer> m_timer;
    };

    /// \brief The trigger object used to activate the trigger callback handler.
    ///
    /// This object provides a trigger mechanism to run a callback handler one or more times. The trigger
    /// will remain registered and can be called multiple times until the Trigger object is destroyed.
    class Trigger : public EventLoopClient::Trigger {
    public:
        Trigger() = default;

        Trigger(util::network::Service& service, util::UniqueFunction<void()>&& handler)
            : m_trigger(service, std::move(handler))
        {
        }

        virtual ~Trigger() = default;

        void trigger() override
        {
            m_trigger.trigger();
        }

    private:
        util::network::Trigger m_trigger;
    };

    DefaultServiceClientImpl(const std::shared_ptr<util::Logger>& logger)
        : m_logger_ptr(logger)
        , m_logger(*m_logger_ptr)
        , m_service{}
        , m_state(State::NotStarted)
    {
        // Lazy start the service until start() is called or the first operation is performed
    }

    virtual ~DefaultServiceClientImpl() REQUIRES(!m_mutex)
    {
        stop();
        // Join the thread before destruction so stop() can be called within the event loop thread
        if (m_thread != nullptr && m_thread->joinable()) {
            m_thread->join();
        }
    }

    void post(util::UniqueFunction<void()>&& handler) override REQUIRES(!m_mutex)
    {
        if (!is_stopped()) {
            m_service.post(std::move(handler));
        }
    }

    std::unique_ptr<EventLoopClient::Trigger> create_trigger(util::UniqueFunction<void()>&& handler) override
        REQUIRES(!m_mutex)
    {
        if (!is_stopped()) {
            return std::make_unique<DefaultServiceClientImpl::Trigger>(m_service, std::move(handler));
        }
        return nullptr;
    }

    bool is_started() override REQUIRES(!m_mutex)
    {
        util::CheckedLockGuard lock(m_mutex);
        return m_state != State::NotStarted;
    }

    bool is_stopped() override REQUIRES(!m_mutex)
    {
        util::CheckedLockGuard lock(m_mutex);
        return m_state == State::Stopped || m_state == State::Stopping;
    }

    void register_event_loop_observer(EventLoopObserver* observer) override REQUIRES(!m_mutex)
    {
        util::CheckedLockGuard lock(m_mutex);
        if (m_state == State::NotStarted) {
            m_observer = observer;
        }
    }

    void start() override REQUIRES(!m_mutex)
    {
        // start() should not be called twice
        REALM_ASSERT(ensure_service_is_running());
    }

    void stop() override REQUIRES(!m_mutex);

    util::network::Service& get_service() override
    {
        return m_service;
    }

private:
    std::unique_ptr<EventLoopClient::Timer>
    do_create_timer(std::chrono::milliseconds delay, util::UniqueFunction<void(std::error_code)>&& handler) override
        REQUIRES(!m_mutex)
    {
        if (!is_stopped()) {
            return Timer::async_wait(m_service, delay, std::move(handler));
        }
        return nullptr;
    }

    // If the service thread is not running, make sure it has been started. There
    // must be something pending on the event loop at all times, otherwise, the
    // service.run() thread will exit prematurely.
    bool ensure_service_is_running() REQUIRES(!m_mutex);

    void update_state(State new_state) REQUIRES(m_mutex);

    //@{
    // Thread Helper Functions
    void thread_update_state(State new_state) REQUIRES(!m_mutex)
    {
        util::CheckedLockGuard lock(m_mutex);
        update_state(new_state);
    }
    //@}

    util::CheckedMutex m_mutex;
    std::shared_ptr<util::Logger> m_logger_ptr;
    util::Logger& m_logger;
    // The original util::network::Service object that used to live in client_impl
    util::network::Service m_service;
    // The event loop thread that calls Service->run()
    std::unique_ptr<std::thread> m_thread;
    // The event loop can only be started once, it cannot be restarted later
    State m_state GUARDED_BY(m_mutex);
    // Optional observer for passing along event loop thread state
    EventLoopObserver* m_observer = nullptr;
};

void DefaultServiceClientImpl::stop()
{
    // Need to release the lock for the join
    util::CheckedUniqueLock lock(m_mutex);
    if (m_state == State::NotStarted || m_state == State::Running) {
        update_state(State::Stopping);
        m_service.stop();
        // In case stop() was called from the event loop thread, wait to join the thread
        // until the event loop is destructed
    }
}

void DefaultServiceClientImpl::update_state(State new_state)
{
    switch (m_state) {
        case State::NotStarted: // NotStarted -> any state other than NotStarted
            if (new_state != State::NotStarted)
                m_state = new_state;
            break;
        case State::Running: // Running -> Stopping or Stopped
            if (new_state == State::Stopping || new_state == State::Stopped)
                m_state = new_state;
            break;
        case State::Stopping: // Stopping -> Stopped
            if (new_state == State::Stopped)
                m_state = new_state;
            break;
        case State::Stopped: // Stopped -> not allowed
            break;
    }
}

// If the service thread is not running, make sure it has been started
bool DefaultServiceClientImpl::ensure_service_is_running()
{
    util::CheckedLockGuard lock(m_mutex);
    if (m_state == State::NotStarted) {
        if (m_thread == nullptr) {
            m_thread = std::make_unique<std::thread>([this]() mutable {
                if (m_observer) {
                    m_observer->did_create_thread();
                }
                auto will_destroy_thread = util::make_scope_exit([this]() noexcept {
                    if (m_observer) {
                        m_observer->will_destroy_thread();
                    }
                    thread_update_state(State::Stopped);
                });
                // If we're already stopped, exit thread
                if (!is_stopped()) {
                    thread_update_state(State::Running);
                    try {
                        m_service.run(); // Throws
                        thread_update_state(State::Stopping);
                    }
                    catch (const std::exception& e) {
                        thread_update_state(State::Stopping);
                        if (m_observer) {
                            m_observer->handle_error(e);
                        }
                    }
                }
                //}
            });
        }
        // Return true to indicate the thread is in the process of being started
        // The state will be updated once the thread is up and running
        return true;
    }
    // Return true if the event loop is running
    return m_state == State::Running;
}

} // namespace

std::shared_ptr<EventLoopClient> DefaultWebSocketFactory::create_event_loop()
{
    m_logger_ptr->trace("DefaultWebSocketFactory: creating event loop instance");
    // If there is an existing event loop, it will be stopped/joined in the destructor
    m_event_loop = std::make_shared<DefaultServiceClientImpl>(m_logger_ptr);
    return m_event_loop;
}


} // namespace realm::util::websocket
