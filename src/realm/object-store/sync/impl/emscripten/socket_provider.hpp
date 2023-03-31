////////////////////////////////////////////////////////////////////////////
//
// Copyright 2023 Realm Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
////////////////////////////////////////////////////////////////////////////

#pragma once

#include <realm/sync/socket_provider.hpp>
#include <realm/object-store/util/emscripten/scheduler.hpp>

namespace realm::_impl {

class EmscriptenSocketProvider final : public sync::SyncSocketProvider {
public:
    EmscriptenSocketProvider();
    ~EmscriptenSocketProvider() final;

    SyncTimer create_timer(std::chrono::milliseconds delay, FunctionHandler&& handler) final;

    void post(FunctionHandler&& handler) final
    {
        m_scheduler.invoke([handler = std::move(handler)]() {
            handler(Status::OK());
        });
    }

    std::unique_ptr<sync::WebSocketInterface> connect(std::unique_ptr<sync::WebSocketObserver> observer,
                                                      sync::WebSocketEndpoint&& endpoint) final;

private:
    util::EmscriptenScheduler m_scheduler;
};

} // namespace realm::_impl
