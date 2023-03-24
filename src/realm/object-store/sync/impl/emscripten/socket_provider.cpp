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

#include "socket_provider.hpp"
#include <emscripten/websocket.h>

using namespace realm;
using namespace realm::_impl;
using namespace realm::sync;

namespace {
struct EmscriptenTimer final : SyncSocketProvider::Timer {
    using doubleMiliseconds = std::chrono::duration<double, std::chrono::milliseconds::period>;

public:
    EmscriptenTimer(std::chrono::milliseconds delay, SyncSocketProvider::FunctionHandler&& handler,
                    util::EmscriptenScheduler& scheduler)
        : m_handler(std::move(handler))
        , m_timeout(emscripten_set_timeout(timeout_callback, doubleMiliseconds(delay).count(), this))
        , m_scheduler(scheduler)
    {
    }
    virtual ~EmscriptenTimer() final
    {
        cancel();
    }
    virtual void cancel() final
    {
        if (m_timeout && m_handler) {
            emscripten_clear_timeout(*m_timeout);
            m_scheduler.invoke([handler = std::move(m_handler)]() {
                handler(Status(ErrorCodes::OperationAborted, "Timer canceled"));
            });
        }
    }

private:
    SyncSocketProvider::FunctionHandler m_handler;
    std::optional<int> m_timeout;
    util::EmscriptenScheduler& m_scheduler;

    static void timeout_callback(void* user_data)
    {
        auto timer = reinterpret_cast<EmscriptenTimer*>(user_data);
        timer->m_handler(Status::OK());
    }
};

struct EmscriptenWebSocket final : public WebSocketInterface {
public:
    EmscriptenWebSocket(EMSCRIPTEN_WEBSOCKET_T socket, std::unique_ptr<WebSocketObserver> observer)
        : m_socket(socket)
        , m_sentinel(std::make_shared<LivenessSentinel>())
        , m_observer(std::move(observer))
    {
        emscripten_websocket_set_onopen_callback(m_socket, m_observer.get(), open_callback);
        emscripten_websocket_set_onmessage_callback(m_socket, m_observer.get(), message_callback);
        emscripten_websocket_set_onerror_callback(m_socket, m_observer.get(), error_callback);
        emscripten_websocket_set_onclose_callback(m_socket, m_observer.get(), close_callback);
    }

    virtual ~EmscriptenWebSocket() final
    {
        emscripten_websocket_close(m_socket, 0, nullptr);
        emscripten_websocket_delete(m_socket);
    }

    virtual void async_write_binary(util::Span<const char> data, SyncSocketProvider::FunctionHandler&& handler) final
    {
        emscripten_websocket_send_binary(m_socket, const_cast<char*>(data.data()), data.size());

        unsigned long long buffered_amount;
        emscripten_websocket_get_buffered_amount(m_socket, &buffered_amount);
        auto weak_handler = [handler = std::move(handler), sentinel = std::weak_ptr(m_sentinel)](Status status) {
            if (sentinel.lock()) {
                // only call the real handler if the socket object is still alive
                handler(status);
            }
        };
        emscripten_set_timeout(sending_poll_check, 0, new PollCheckState{m_socket, std::move(weak_handler)});
    }

private:
    struct PollCheckState {
        EMSCRIPTEN_WEBSOCKET_T socket;
        SyncSocketProvider::FunctionHandler handler;
        double next_delay = 1;
    };

    struct LivenessSentinel {
    };

    static void sending_poll_check(void* user_data)
    {
        std::unique_ptr<PollCheckState> state(reinterpret_cast<PollCheckState*>(user_data));
        unsigned long long buffered_amount;
        emscripten_websocket_get_buffered_amount(state->socket, &buffered_amount);

        constexpr unsigned long long blocking_send_threshold = 65536;
        if (buffered_amount < blocking_send_threshold) {
            state->handler(Status::OK());
            return;
        }

        unsigned short ready_state;
        emscripten_websocket_get_ready_state(state->socket, &ready_state);
        if (buffered_amount == 0) {
            state->handler(Status::OK());
        }
        else if (ready_state != 1 /* OPEN */) {
            // TODO: should we communicate this down the stack?
        }
        else {
            auto delay = state->next_delay;
            state->next_delay = std::min(delay * 1.5, 1000.0);
            emscripten_set_timeout(sending_poll_check, delay, state.release());
        }
    }

    static EM_BOOL open_callback(int, const EmscriptenWebSocketOpenEvent* event, void* user_data)
    {
        auto observer = reinterpret_cast<WebSocketObserver*>(user_data);
        int length;
        emscripten_websocket_get_protocol_length(event->socket, &length);
        std::string protocol;
        protocol.resize(length - 1);
        emscripten_websocket_get_protocol(event->socket, protocol.data(), length);
        observer->websocket_connected_handler(protocol);
        return EM_TRUE;
    }

    static EM_BOOL message_callback(int, const EmscriptenWebSocketMessageEvent* event, void* user_data)
    {
        auto observer = reinterpret_cast<WebSocketObserver*>(user_data);
        REALM_ASSERT(!event->isText);
        observer->websocket_binary_message_received(
            util::Span(reinterpret_cast<char*>(event->data), event->numBytes));
        return EM_TRUE;
    }

    static EM_BOOL error_callback(int, const EmscriptenWebSocketErrorEvent*, void* user_data)
    {
        auto observer = reinterpret_cast<WebSocketObserver*>(user_data);
        observer->websocket_error_handler();
        return EM_TRUE;
    }

    static EM_BOOL close_callback(int, const EmscriptenWebSocketCloseEvent* event, void* user_data)
    {
        auto observer = reinterpret_cast<WebSocketObserver*>(user_data);
        REALM_ASSERT(event->code >= 1000 && event->code < 5000);
        auto status = event->code == 1000 ? Status::OK() : Status(ErrorCodes::Error(event->code), event->reason);
        observer->websocket_closed_handler(event->wasClean, std::move(status));
        return EM_TRUE;
    }

    EMSCRIPTEN_WEBSOCKET_T m_socket;
    std::shared_ptr<LivenessSentinel> m_sentinel;
    std::unique_ptr<WebSocketObserver> m_observer;
};
} // namespace

EmscriptenSocketProvider::EmscriptenSocketProvider()
    : SyncSocketProvider()
    , EmscriptenScheduler()
{
}

EmscriptenSocketProvider::~EmscriptenSocketProvider() = default;

SyncSocketProvider::SyncTimer EmscriptenSocketProvider::create_timer(std::chrono::milliseconds delay,
                                                                     FunctionHandler&& handler)
{
    util::EmscriptenScheduler& scheduler = *this;
    return std::make_unique<EmscriptenTimer>(delay, std::move(handler), scheduler);
}

std::unique_ptr<WebSocketInterface> EmscriptenSocketProvider::connect(std::unique_ptr<WebSocketObserver> observer,
                                                                      WebSocketEndpoint&& endpoint)
{
    // Convert the list of protocols to a string
    std::ostringstream protocol_list;
    protocol_list.exceptions(std::ios_base::failbit | std::ios_base::badbit);
    protocol_list.imbue(std::locale::classic());
    if (endpoint.protocols.size() > 1)
        std::copy(endpoint.protocols.begin(), endpoint.protocols.end() - 1,
                  std::ostream_iterator<std::string>(protocol_list, ","));
    protocol_list << endpoint.protocols.back();
    std::string protocols = protocol_list.str();
    for (size_t i = 0; i < protocols.size(); i++) {
        if (protocols[i] == '/')
            protocols[i] = '#';
    }

    std::string url =
        util::format("%1://%2:%3%4", endpoint.is_ssl ? "wss" : "ws", endpoint.address, endpoint.port, endpoint.path);

    EmscriptenWebSocketCreateAttributes attr;
    emscripten_websocket_init_create_attributes(&attr);
    attr.protocols = protocols.c_str();
    attr.url = url.c_str();
    int result = emscripten_websocket_new(&attr);
    REALM_ASSERT(result > 0);
    return std::make_unique<EmscriptenWebSocket>(result, std::move(observer));
}
