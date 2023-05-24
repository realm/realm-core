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

#include <realm/object-store/sync/impl/emscripten/network_transport.hpp>
#include <realm/util/scope_exit.hpp>
#include <emscripten/fetch.h>

using namespace realm;
using namespace realm::app;

namespace realm::_impl {
struct FetchState {
    std::string request_body;
    util::UniqueFunction<void(const Response&)> completion_block;
};

static std::string_view trim_whitespace(std::string_view str)
{
    auto begin = str.begin();
    auto end = str.end();
    while (end > begin && std::isspace(end[-1]))
        --end;
    while (begin < end && std::isspace(begin[0]))
        ++begin;
    return std::string_view(&*begin, end - begin);
}

static HttpHeaders parse_headers(std::string_view raw_headers)
{
    HttpHeaders ret;
    size_t pos;
    while ((pos = raw_headers.find("\r\n")) != std::string::npos) {
        std::string_view line = raw_headers.substr(0, pos);
        raw_headers = raw_headers.substr(pos + 2);

        size_t colon = line.find(":");
        ret.emplace(line.substr(0, colon), trim_whitespace(line.substr(colon + 1)));
    }
    return ret;
}

static void success(emscripten_fetch_t* fetch)
{
    auto guard = util::make_scope_exit([&]() noexcept {
        emscripten_fetch_close(fetch);
    });
    std::unique_ptr<FetchState> state(reinterpret_cast<FetchState*>(fetch->userData));
    std::string packed_headers;
    packed_headers.resize(emscripten_fetch_get_response_headers_length(fetch));
    emscripten_fetch_get_response_headers(fetch, packed_headers.data(), packed_headers.size());
    state->completion_block(
        {fetch->status, 0, parse_headers(packed_headers), std::string(fetch->data, size_t(fetch->numBytes)), {}});
}

static void error(emscripten_fetch_t* fetch)
{
    auto guard = util::make_scope_exit([&]() noexcept {
        emscripten_fetch_close(fetch);
    });
    std::unique_ptr<FetchState> state(reinterpret_cast<FetchState*>(fetch->userData));
    state->completion_block({0, 0, {}, {}, {}});
}

void EmscriptenNetworkTransport::send_request_to_server(
    const Request& request, util::UniqueFunction<void(const Response&)>&& completion_block)
{
    auto state = std::make_unique<FetchState>(FetchState{request.body, std::move(completion_block)});

    emscripten_fetch_attr_t attr;
    emscripten_fetch_attr_init(&attr);
    attr.attributes = EMSCRIPTEN_FETCH_LOAD_TO_MEMORY;
    attr.onsuccess = success;
    attr.onerror = error;
    attr.timeoutMSecs = static_cast<unsigned long>(request.timeout_ms);

    if (state->request_body.size()) {
        attr.requestData = state->request_body.data();
        attr.requestDataSize = state->request_body.size();
    }

    std::vector<const char*> request_headers_buf;
    for (const auto& header : request.headers) {
        request_headers_buf.push_back(header.first.c_str());
        request_headers_buf.push_back(header.second.c_str());
    }
    request_headers_buf.push_back(nullptr);
    attr.requestHeaders = request_headers_buf.data();

    switch (request.method) {
        case HttpMethod::get:
            strncpy(attr.requestMethod, "GET", sizeof(attr.requestMethod));
            break;
        case HttpMethod::post:
            strncpy(attr.requestMethod, "POST", sizeof(attr.requestMethod));
            break;
        case HttpMethod::put:
            strncpy(attr.requestMethod, "PUT", sizeof(attr.requestMethod));
            break;
        case app::HttpMethod::del:
            strncpy(attr.requestMethod, "DELETE", sizeof(attr.requestMethod));
            break;
        case app::HttpMethod::patch:
            strncpy(attr.requestMethod, "PATCH", sizeof(attr.requestMethod));
            break;
    }

    attr.userData = state.release();
    emscripten_fetch(&attr, request.url.c_str());
}
} // namespace realm::_impl
