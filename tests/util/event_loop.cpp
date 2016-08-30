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

#include <util/event_loop.hpp>

#include <realm/util/features.h>

#if REALM_PLATFORM_NODE
#include <uv.h>

namespace realm {
namespace util {
bool has_event_loop_implementation() { return true; }

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

void run_event_loop_until(std::function<bool()> predicate)
{
    if (predicate())
        return;

    auto loop = uv_default_loop();

    IdleHandler observer(loop);
    observer.idle->data = &predicate;

    uv_idle_start(observer.idle, [](uv_idle_t* handle) {
        auto& predicate = *static_cast<std::function<bool()>*>(handle->data);
        if (predicate()) {
            uv_stop(handle->loop);
        }
    });

    uv_run(loop, UV_RUN_DEFAULT);
    uv_idle_stop(observer.idle);
}
} // namespace util
} // namespace realm

#elif REALM_PLATFORM_APPLE
#include <CoreFoundation/CoreFoundation.h>

namespace realm {
namespace util {
bool has_event_loop_implementation() { return true; }

void run_event_loop_until(std::function<bool()> predicate)
{
    if (predicate())
        return;

    auto callback = [](CFRunLoopObserverRef, CFRunLoopActivity, void* info) {
        if ((*static_cast<std::function<bool()>*>(info))()) {
            CFRunLoopStop(CFRunLoopGetCurrent());
        }
    };
    CFRunLoopObserverContext ctx{};
    ctx.info = &predicate;
    auto observer = CFRunLoopObserverCreate(kCFAllocatorDefault, kCFRunLoopAllActivities,
                                            true, 0, callback, &ctx);
    CFRunLoopAddObserver(CFRunLoopGetCurrent(), observer, kCFRunLoopCommonModes);
    CFRunLoopRun();
    CFRunLoopRemoveObserver(CFRunLoopGetCurrent(), observer, kCFRunLoopCommonModes);
}
} // namespace util
} // namespace realm

#else

namespace realm {
namespace util {
bool has_event_loop_implementation() { return false; }
void run_event_loop_until(std::function<bool()>) { }
} // namespace util
} // namespace realm
#endif
