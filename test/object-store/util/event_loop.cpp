/*************************************************************************
 *
 * Copyright 2016 Realm Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **************************************************************************/

#include "util/event_loop.hpp"

#include <realm/object-store/util/scheduler.hpp>
#include <realm/object-store/util/event_loop_dispatcher.hpp>
#include <realm/util/scope_exit.hpp>
#include <realm/util/features.h>

#include <mutex>
#include <stdexcept>
#include <vector>

#if TEST_SCHEDULER_UV
#include <uv.h>
#elif REALM_PLATFORM_APPLE
#include <realm/util/cf_ptr.hpp>
#include <CoreFoundation/CoreFoundation.h>
#elif REALM_ANDROID
// TODO: implement event loop for android: see Scheduler::make_alooper()
#elif defined(__EMSCRIPTEN__)
// TODO: implement event loop for Emscripten
#else
#error "No EventLoop implementation selected, tests will fail"
#endif

using namespace realm::util;

namespace {
template <typename Desired, typename Actual>
void static_assert_EventLoopDispatcher_guide(const EventLoopDispatcher<Actual>&)
{
    static_assert(std::is_same_v<Actual, Desired>);
}

[[maybe_unused]] void check_EventLoopDispatcher_guides()
{
    // This doesn't actually run, the only "test" is that it compiles.
    static_assert_EventLoopDispatcher_guide<void()>(EventLoopDispatcher([] {}));
    static_assert_EventLoopDispatcher_guide<void()>(EventLoopDispatcher(+[] {}));
    static_assert_EventLoopDispatcher_guide<void()>(EventLoopDispatcher([]() mutable {}));
    static_assert_EventLoopDispatcher_guide<void()>(EventLoopDispatcher(+[]() mutable {}));
    static_assert_EventLoopDispatcher_guide<void()>(EventLoopDispatcher([]() noexcept {}));
    static_assert_EventLoopDispatcher_guide<void()>(EventLoopDispatcher(+[]() noexcept {}));
    static_assert_EventLoopDispatcher_guide<void()>(EventLoopDispatcher([]() mutable noexcept {}));
    static_assert_EventLoopDispatcher_guide<void()>(EventLoopDispatcher(+[]() mutable noexcept {}));

    static_assert_EventLoopDispatcher_guide<void(int)>(EventLoopDispatcher([](int) {}));
    static_assert_EventLoopDispatcher_guide<void(int)>(EventLoopDispatcher(+[](int) {}));
    static_assert_EventLoopDispatcher_guide<void(int, const double&)>(EventLoopDispatcher([](int, const double&) {}));
    static_assert_EventLoopDispatcher_guide<void(int, const double&)>(
        EventLoopDispatcher(+[](int, const double&) {}));

    struct Funcy {
        void operator()(int) const& noexcept {}
    };
    static_assert_EventLoopDispatcher_guide<void(int)>(EventLoopDispatcher(Funcy()));

    // Passing a scheduler as second argument
    auto scheduler = Scheduler::make_dummy();
    static_assert_EventLoopDispatcher_guide<void()>(EventLoopDispatcher([] {}, scheduler));
}
} // namespace

struct EventLoop::Impl {
    // Returns the main event loop.
    static std::unique_ptr<Impl> main();

    // Run the event loop until the given return predicate returns true
    void run_until(util::FunctionRef<bool()> predicate);

    // Schedule execution of the given function on the event loop.
    void perform(util::UniqueFunction<void()>);

    // Run the event loop until all currently pending work has been run.
    void run_pending();

    ~Impl();

private:
#if TEST_SCHEDULER_UV
    Impl(uv_loop_t* loop);

    std::vector<util::UniqueFunction<void()>> m_pending_work;
    std::mutex m_mutex;
    uv_loop_t* m_loop;
    uv_async_t m_perform_work;
#elif REALM_PLATFORM_APPLE
    Impl(util::CFPtr<CFRunLoopRef> loop)
        : m_loop(std::move(loop))
    {
    }

    util::CFPtr<CFRunLoopRef> m_loop;
#endif
};

EventLoop& EventLoop::main()
{
    static EventLoop main(Impl::main());
    return main;
}

EventLoop::EventLoop(std::unique_ptr<Impl> impl)
    : m_impl(std::move(impl))
{
}

EventLoop::~EventLoop() = default;

void EventLoop::run_until(util::FunctionRef<bool()> predicate)
{
    return m_impl->run_until(predicate);
}

void EventLoop::perform(util::UniqueFunction<void()> function)
{
    return m_impl->perform(std::move(function));
}

void EventLoop::run_pending()
{
    return m_impl->run_pending();
}

#if TEST_SCHEDULER_UV

bool EventLoop::has_implementation()
{
    return true;
}

std::unique_ptr<EventLoop::Impl> EventLoop::Impl::main()
{
    return std::unique_ptr<Impl>(new Impl(uv_default_loop()));
}

EventLoop::Impl::Impl(uv_loop_t* loop)
    : m_loop(loop)
{
    m_perform_work.data = this;
    uv_async_init(uv_default_loop(), &m_perform_work, [](uv_async_t* handle) {
        std::vector<util::UniqueFunction<void()>> pending_work;
        {
            Impl& self = *static_cast<Impl*>(handle->data);
            std::lock_guard<std::mutex> lock(self.m_mutex);
            std::swap(pending_work, self.m_pending_work);
        }

        for (auto& f : pending_work)
            f();
    });
}

EventLoop::Impl::~Impl()
{
    uv_close((uv_handle_t*)&m_perform_work, [](uv_handle_t*) {});
    uv_loop_close(m_loop);
}

struct IdleHandler {
    uv_idle_t* idle = new uv_idle_t;

    IdleHandler(uv_loop_t* loop)
    {
        uv_idle_init(loop, idle);
    }
    ~IdleHandler()
    {
        uv_close(reinterpret_cast<uv_handle_t*>(idle), [](uv_handle_t* handle) {
            delete reinterpret_cast<uv_idle_t*>(handle);
        });
    }
};

void EventLoop::Impl::run_until(util::FunctionRef<bool()> predicate)
{
    if (predicate())
        return;

    IdleHandler observer(m_loop);
    observer.idle->data = &predicate;

    uv_idle_start(observer.idle, [](uv_idle_t* handle) {
        auto& predicate = *static_cast<util::FunctionRef<bool()>*>(handle->data);
        if (predicate()) {
            uv_stop(handle->loop);
        }
    });

    auto cleanup = make_scope_exit([&]() noexcept {
        uv_idle_stop(observer.idle);
    });
    uv_run(m_loop, UV_RUN_DEFAULT);
}

void EventLoop::Impl::perform(util::UniqueFunction<void()> f)
{
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_pending_work.push_back(std::move(f));
    }
    uv_async_send(&m_perform_work);
}

void EventLoop::Impl::run_pending()
{
    uv_run(m_loop, UV_RUN_NOWAIT);
}

#elif REALM_PLATFORM_APPLE

bool EventLoop::has_implementation()
{
    return true;
}

std::unique_ptr<EventLoop::Impl> EventLoop::Impl::main()
{
    return std::unique_ptr<Impl>(new Impl(retainCF(CFRunLoopGetMain())));
}

EventLoop::Impl::~Impl() = default;

void EventLoop::Impl::run_until(util::FunctionRef<bool()> predicate)
{
    REALM_ASSERT(m_loop.get() == CFRunLoopGetCurrent());

    auto callback = [](CFRunLoopObserverRef, CFRunLoopActivity, void* info) {
        if ((*static_cast<util::FunctionRef<bool()>*>(info))()) {
            CFRunLoopStop(CFRunLoopGetCurrent());
        }
    };
    CFRunLoopObserverContext ctx{};
    ctx.info = &predicate;
    auto observer =
        adoptCF(CFRunLoopObserverCreate(kCFAllocatorDefault, kCFRunLoopAllActivities, true, 0, callback, &ctx));
    auto timer = adoptCF(CFRunLoopTimerCreateWithHandler(
        kCFAllocatorDefault, CFAbsoluteTimeGetCurrent(), 0.0005, 0, 0,
        ^(CFRunLoopTimerRef){
            // Do nothing. The timer firing is sufficient to cause our runloop observer to run.
        }));
    CFRunLoopAddObserver(CFRunLoopGetCurrent(), observer.get(), kCFRunLoopCommonModes);
    CFRunLoopAddTimer(CFRunLoopGetCurrent(), timer.get(), kCFRunLoopCommonModes);
    auto cleanup = make_scope_exit([&]() noexcept {
        CFRunLoopRemoveTimer(CFRunLoopGetCurrent(), timer.get(), kCFRunLoopCommonModes);
        CFRunLoopRemoveObserver(CFRunLoopGetCurrent(), observer.get(), kCFRunLoopCommonModes);
    });
    CFRunLoopRun();
}

void EventLoop::Impl::perform(util::UniqueFunction<void()> func)
{
    __block auto f = std::move(func);
    CFRunLoopPerformBlock(m_loop.get(), kCFRunLoopDefaultMode, ^{
        f();
    });
    CFRunLoopWakeUp(m_loop.get());
}

void EventLoop::Impl::run_pending()
{
    while (CFRunLoopRunInMode(kCFRunLoopDefaultMode, 0, true) == kCFRunLoopRunHandledSource)
        ;
}

#else

bool EventLoop::has_implementation()
{
    return false;
}
std::unique_ptr<EventLoop::Impl> EventLoop::Impl::main()
{
    return nullptr;
}
EventLoop::Impl::~Impl() = default;
void EventLoop::Impl::run_until(util::FunctionRef<bool()>)
{
    printf("WARNING: there is no event loop implementation and nothing is happening.\n");
}
void EventLoop::Impl::perform(util::UniqueFunction<void()>)
{
    printf("WARNING: there is no event loop implementation and nothing is happening.\n");
}
void EventLoop::Impl::run_pending()
{
    printf("WARNING: there is no event loop implementation and nothing is happening.\n");
}

#endif
