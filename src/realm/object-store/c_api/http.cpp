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

static inline realm_http_method_e to_capi(HttpMethod method)
{
    switch (method) {
        case HttpMethod::get:
            return RLM_HTTP_METHOD_GET;
        case HttpMethod::post:
            return RLM_HTTP_METHOD_POST;
        case HttpMethod::patch:
            return RLM_HTTP_METHOD_PATCH;
        case HttpMethod::put:
            return RLM_HTTP_METHOD_PUT;
        case HttpMethod::del:
            return RLM_HTTP_METHOD_DELETE;
    };
    REALM_TERMINATE("Invalid http method."); // LCOV_EXCL_LINE
}

class CNetworkTransport : public GenericNetworkTransport {
public:
    CNetworkTransport(UserdataPtr userdata, realm_http_request_func_t request_executor)
        : m_userdata(std::move(userdata))
        , m_request_executor(request_executor)
    {
    }

protected:
    void send_request_to_server(const Request request,
                                std::function<void(const Response)> completion_block) override final
    {
        auto* completion_data = new std::function<void(const Response)>(std::move(completion_block));

        std::vector<realm_http_header_t> c_headers;
        for (auto& header : request.headers) {
            c_headers.push_back({header.first.c_str(), header.second.c_str()});
        }

        realm_http_request_t c_request{to_capi(request.method), request.url.c_str(),       request.timeout_ms,
                                       c_headers.data(),        c_headers.size(),          request.body.data(),
                                       request.body.size(),     request.uses_refresh_token};
        m_request_executor(m_userdata.get(), std::move(c_request), completion_data, &on_response_completed);
    }

private:
    static void on_response_completed(void* completion_data, const realm_http_response_t response)
    {
        auto& completion = reinterpret_cast<std::function<void(const Response)>&>(completion_data);

        std::map<std::string, std::string> headers;
        for (size_t i = 0; i < response.num_headers; i++) {
            headers.emplace(std::string(response.headers[i].name), std::string(response.headers[i].value));
        }

        completion({response.status_code, response.custom_status_code, std::move(headers),
                    std::string(response.body, response.body_size)});
        delete &completion;
    }

    UserdataPtr m_userdata;
    realm_http_request_func_t m_request_executor;
};
} // namespace
} // namespace realm::c_api

RLM_API realm_http_transport_t* realm_http_transport_new(void* userdata, realm_free_userdata_func_t free,
                                                         realm_http_request_func_t request_executor)
{
    realm_http_transport_t* transport = new realm_http_transport_t;
    transport->transport.reset(new realm::c_api::CNetworkTransport({userdata, free}, request_executor));
    return transport;
}