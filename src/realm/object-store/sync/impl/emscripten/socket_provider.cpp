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

#include <realm/object-store/sync/impl/emscripten/socket_provider.hpp>
#include <emscripten/websocket.h>

using namespace realm;
using namespace realm::sync;

namespace realm::_impl {
#define check_result(expr)                                                                                           \
    do {                                                                                                             \
        [[maybe_unused]] auto result = expr;                                                                         \
        REALM_ASSERT_3(result, ==, EMSCRIPTEN_RESULT_SUCCESS);                                                       \
    } while (0)

struct EmscriptenTimer final : SyncSocketProvider::Timer {
    using DoubleMiliseconds = std::chrono::duration<double, std::chrono::milliseconds::period>;

public:
    EmscriptenTimer(std::chrono::milliseconds delay, SyncSocketProvider::FunctionHandler&& handler,
                    util::EmscriptenScheduler& scheduler)
        : m_handler(std::move(handler))
        , m_timeout(emscripten_set_timeout(timeout_callback, DoubleMiliseconds(delay).count(), this))
        , m_scheduler(scheduler)
    {
    }
    ~EmscriptenTimer() final
    {
        cancel();
    }
    void cancel() final
    {
        if (m_timeout) {
            emscripten_clear_timeout(*m_timeout);
            m_timeout.reset();
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
        timer->m_timeout.reset();
        std::exchange(timer->m_handler, {})(Status::OK());
    }
};

struct EmscriptenWebSocket final : public WebSocketInterface {
public:
    EmscriptenWebSocket(EMSCRIPTEN_WEBSOCKET_T socket, std::unique_ptr<WebSocketObserver> observer)
        : m_socket(socket)
        , m_sentinel(std::make_shared<LivenessSentinel>())
        , m_observer(std::move(observer))
    {
        check_result(emscripten_websocket_set_onopen_callback(m_socket, m_observer.get(), open_callback));
        check_result(emscripten_websocket_set_onmessage_callback(m_socket, m_observer.get(), message_callback));
        check_result(emscripten_websocket_set_onerror_callback(m_socket, m_observer.get(), error_callback));
        check_result(emscripten_websocket_set_onclose_callback(m_socket, m_observer.get(), close_callback));
    }

    ~EmscriptenWebSocket() final
    {
        m_sentinel.reset();
        check_result(emscripten_websocket_close(m_socket, 0, nullptr));
        check_result(emscripten_websocket_delete(m_socket));
    }

    // Adapted from
    // https://github.com/dotnet/runtime/blob/60b480424d51f42dfd66e09b010297dc041602f2/src/mono/wasm/runtime/web-socket.ts#L187:
    // The WebSocket.send method doesn't provide a done callback, so we need to guess when the operation is done
    // by observing the outgoing buffer on the websocket.
    void async_write_binary(util::Span<const char> data, SyncSocketProvider::FunctionHandler&& handler) final
    {
        check_result(emscripten_websocket_send_binary(m_socket, const_cast<char*>(data.data()), data.size()));

        // If the buffered amount is small enough we can just run the handler right away.
        size_t buffered_amount;
        check_result(emscripten_websocket_get_buffered_amount(m_socket, &buffered_amount));
        constexpr size_t blocking_send_threshold = 65536;
        if (buffered_amount < blocking_send_threshold) {
            emscripten_set_timeout(
                [](void* user_data) {
                    std::unique_ptr<SyncSocketProvider::FunctionHandler> handler(
                        reinterpret_cast<SyncSocketProvider::FunctionHandler*>(user_data));
                    (*handler)(Status::OK());
                },
                0, new SyncSocketProvider::FunctionHandler(std::move(handler)));
        }
        else {
            // Otherwise we start polling the buffer in a recursive timeout (see sending_poll_check below).
            emscripten_set_timeout(
                sending_poll_check, 0,
                new PollCheckState{
                    m_socket, [handler = std::move(handler), sentinel = std::weak_ptr(m_sentinel)](Status status) {
                        if (sentinel.lock()) {
                            // only call the real handler if the socket object is still alive
                            handler(status);
                        }
                    }});
        }
    }

private:
    struct PollCheckState {
        EMSCRIPTEN_WEBSOCKET_T socket;
        SyncSocketProvider::FunctionHandler handler;
        double next_delay = 1;
    };

    struct LivenessSentinel {};

    static void sending_poll_check(void* user_data)
    {
        std::unique_ptr<PollCheckState> state(reinterpret_cast<PollCheckState*>(user_data));

        size_t buffered_amount;
        check_result(emscripten_websocket_get_buffered_amount(state->socket, &buffered_amount));
        if (buffered_amount == 0) {
            state->handler(Status::OK());
            return;
        }

        unsigned short ready_state;
        check_result(emscripten_websocket_get_ready_state(state->socket, &ready_state));
        if (ready_state != 1 /* OPEN */) {
            // TODO: what's the right error code for websocket was closed while sending?
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
        check_result(emscripten_websocket_get_protocol_length(event->socket, &length));
        std::string protocol;
        protocol.resize(length - 1);
        check_result(emscripten_websocket_get_protocol(event->socket, protocol.data(), length));
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

EmscriptenSocketProvider::EmscriptenSocketProvider() = default;

EmscriptenSocketProvider::~EmscriptenSocketProvider() = default;

SyncSocketProvider::SyncTimer EmscriptenSocketProvider::create_timer(std::chrono::milliseconds delay,
                                                                     FunctionHandler&& handler)
{
    return std::make_unique<EmscriptenTimer>(delay, std::move(handler), m_scheduler);
}

std::unique_ptr<WebSocketInterface> EmscriptenSocketProvider::connect(std::unique_ptr<WebSocketObserver> observer,
                                                                      WebSocketEndpoint&& endpoint)
{
    std::string protocols;
    for (size_t i = 0; i < endpoint.protocols.size(); i++) {
        if (i > 0)
            protocols += ',';
        protocols += endpoint.protocols[i];
    }

    // The `/` delimiter character is not allowed in the protocol list. Replace it with #.
    // TODO: Remove once RCORE-1427 is resolved.
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
    attr.createOnMainThread = EM_FALSE;
    int result = emscripten_websocket_new(&attr);
    REALM_ASSERT(result > 0);
    return std::make_unique<EmscriptenWebSocket>(result, std::move(observer));
}
} // namespace realm::_impl
