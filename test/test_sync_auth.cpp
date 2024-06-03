#include <thread>
#include <mutex>
#include <condition_variable>

#include <realm/binary_data.hpp>
#include <realm/sync/noinst/server/crypto_server.hpp>
#include <realm/sync/client.hpp>
#include <realm/sync/noinst/server/access_token.hpp>
#include <realm/sync/noinst/server/access_control.hpp>

#include "test.hpp"

using namespace realm;
using namespace realm::util;
using namespace realm::sync;

namespace {

#if !REALM_MOBILE

TEST(Sync_Auth_JWTAccessToken)
{
    AccessToken tok;
    AccessToken::ParseError error = AccessToken::ParseError::none;

    PKey pk1 = PKey::load_public(test_util::get_test_resource_path() + "test_pubkey2.pem");
    AccessControl ctrl(std::move(pk1));

    AccessToken::Verifier& verifier = ctrl.verifier();
    auto exampleJWT =
        "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9."
        "eyJhcHBJZCI6ImlvLnJlYWxtLkF1dGgiLCJhY2Nlc3MiOlsiZG93bmxvYWQiLCJ1cGxvYWQiXSwic3ViIjoiZGYyZjE4NjBjMTk1MjFiYjk0"
        "NjM0OTRjOTI1MTYyZjciLCJwYXRoIjoiL2RlZmF1bHQvX19wYXJ0aWFsL2RmMmYxODYwYzE5NTIxYmI5NDYzNDk0YzkyNTE2MmY3LzBlYzNj"
        "NjdlMTFjNzFkYmU1ZTgzYmZiNDE3MTViZmJlMGQ5ODNmODYiLCJzeW5jX2xhYmVsIjoiZGVmYXVsdCIsInNhbHQiOiIyY2FmZjhlMCIsImlh"
        "dCI6MTU2NDczNzY1NiwiZXhwIjo0NzIwNDExNjE1LCJhdWQiOiJyZWFsbSIsImlzcyI6InJlYWxtIiwianRpIjoiYmM3MTlkY2ItOTA2Ny00"
        "ZTQ4LWI1NmItYTQ3MzMxZDNmZDgxIn0.SGFUR8A-"
        "XXn2i7LFGcWuUlrfcPgUYRj58ZClZrjsW7NSiE1tI5zZSbrEL7vyTPtwbMbMe1qMgdoB1ZdSzt-HAB9RCIrRk40XlHw7flb8jk_"
        "q0hdqPnKbxEMz9wWzzUGOshXj2Yso1NVEX0q04k-ndpAODtuMDiU5T_3vF1czUFA-WXOMDr9dpX_Wn8KeEO0uOvb4_1AvDM_"
        "wK3RF5D9IsJGuvE2Sqbq5j2DPGCgTkBsTcKJPQPcgEDC270nSb9SfitzLEzxoQbhF9M82MQJqhfj4ZThImG6ed7hjUIqdgBFuyBQ4WaMQgPD"
        "vA5KRPYymC5owAHBmGht9wpUFzAbnBg";
    auto result = AccessToken::parseJWT(StringData(exampleJWT), tok, error, &verifier);

    CHECK(result);
    CHECK(error == AccessToken::ParseError::none);
    CHECK_EQUAL(tok.expires, 4720411615);
    CHECK_EQUAL(tok.identity, "df2f1860c19521bb9463494c925162f7");
    CHECK_EQUAL(tok.sync_label, "default");
}

#endif // !REALM_MOBILE

} // unnamed namespace
