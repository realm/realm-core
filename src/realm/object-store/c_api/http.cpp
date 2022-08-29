////////////////////////////////////////////////////////////////////////////
//
// Copyright 2021 Realm Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or utilied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
////////////////////////////////////////////////////////////////////////////

#include <realm/object-store/c_api/types.hpp>
#include <realm/object-store/c_api/util.hpp>

namespace realm::c_api {
namespace {
using namespace realm::app;

static_assert(realm_http_request_method_e(HttpMethod::get) == RLM_HTTP_REQUEST_METHOD_GET);
static_assert(realm_http_request_method_e(HttpMethod::post) == RLM_HTTP_REQUEST_METHOD_POST);
static_assert(realm_http_request_method_e(HttpMethod::patch) == RLM_HTTP_REQUEST_METHOD_PATCH);
static_assert(realm_http_request_method_e(HttpMethod::put) == RLM_HTTP_REQUEST_METHOD_PUT);
static_assert(realm_http_request_method_e(HttpMethod::del) == RLM_HTTP_REQUEST_METHOD_DELETE);

class CNetworkTransport final : public GenericNetworkTransport {
    using Completion = realm::util::UniqueFunction<void(const Response&)>;

public:
    CNetworkTransport(UserdataPtr userdata, realm_http_request_func_t request_executor)
        : m_userdata(std::move(userdata))
        , m_request_executor(request_executor)
    {
    }

    static void on_response_completed(void* completion_data, const realm_http_response_t* response) noexcept
    {
        std::unique_ptr<Completion> completion(reinterpret_cast<Completion*>(completion_data));

        std::map<std::string, std::string> headers;
        for (size_t i = 0; i < response->num_headers; i++) {
            headers.emplace(response->headers[i].name, response->headers[i].value);
        }

        (*completion)({response->status_code, response->custom_status_code, std::move(headers),
                       std::string(response->body, response->body_size)});
    }

private:
    void send_request_to_server(Request&& request, Completion&& completion_block) final
    {
        auto completion_data = std::make_unique<Completion>(std::move(completion_block));

        std::vector<realm_http_header_t> c_headers;
        c_headers.reserve(request.headers.size());
        for (auto& header : request.headers) {
            c_headers.push_back({header.first.c_str(), header.second.c_str()});
        }

        realm_http_request_t c_request{realm_http_request_method_e(request.method),
                                       request.url.c_str(),
                                       request.timeout_ms,
                                       c_headers.data(),
                                       c_headers.size(),
                                       request.body.data(),
                                       request.body.size()};
        m_request_executor(m_userdata.get(), c_request, completion_data.release());
    }

    UserdataPtr m_userdata;
    realm_http_request_func_t m_request_executor;
};
} // namespace
} // namespace realm::c_api

RLM_API realm_http_transport_t* realm_http_transport_new(realm_http_request_func_t request_executor,
                                                         realm_userdata_t userdata, realm_free_userdata_func_t free)
{
    auto transport = std::make_shared<realm::c_api::CNetworkTransport>(realm::c_api::UserdataPtr{userdata, free},
                                                                       request_executor);
    return new realm_http_transport_t(std::move(transport));
}

RLM_API void realm_http_transport_complete_request(void* completion_data, const realm_http_response_t* response)
{
    realm::c_api::CNetworkTransport::on_response_completed(completion_data, response);
}
