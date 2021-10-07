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

TEST(Sync_Auth_JWTAccessToken)
{
    AccessToken tok;
    AccessToken::ParseError error = AccessToken::ParseError::none;

    PKey pk1 = PKey::load_public("test_pubkey2.pem");
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


TEST(Sync_Auth_JWTAccessTokenStitchFields)
{
    AccessToken tok;
    AccessToken::ParseError error = AccessToken::ParseError::none;

    PKey pk1 = PKey::load_public("stitch_public.pem");
    AccessControl ctrl(std::move(pk1));

    AccessToken::Verifier& verifier = ctrl.verifier();
    auto exampleJWT =
        "eyJhbGciOiJSUzI1NiIsImtpZCI6IjVkYTY0NzI2NTM5NWM0ZmY0NzE2ZmE4NyIsInR5cCI6IkpXVCJ9."
        "eyJhdWQiOiJyZWFsbSIsImV4cCI6NDU3MTE3ODYzOSwiaWF0Ijo0NTcxMTc4Mjc5LCJpc3MiOiJyZWFsbSIsInN0aXRjaF9kYXRhIjp7InJl"
        "YWxtX2FjY2VzcyI6WyJkb3dubG9hZCIsInVwbG9hZCIsIm1hbmFnZSJdLCJyZWFsbV9wYXRoIjoiLzVkYTY0NzI3NTM5NWM0ZmY0NzE2ZmE5"
        "My9leGFtcGxlIiwicmVhbG1fc3luY19sYWJlbCI6ImRlZmF1bHQifSwic3RpdGNoX2RldklkIjoiMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAw"
        "Iiwic3RpdGNoX2RvbWFpbklkIjoiNWRhNjQ3MjY1Mzk1YzRmZjQ3MTZmYTg2Iiwic3ViIjoiNWRhNjQ3Mjc1Mzk1YzRmZjQ3MTZmYTkzIiwi"
        "dHlwIjoiYWNjZXNzIiwidXNlcl9kYXRhIjpudWxsfQ."
        "dgoKeww6xSjvhmZ69muDOJGOkk1sFxq9sfdFF2ufq3z1oTFujp3g6AIaKy66qcx6zbHEx4Zv7Fy4ytGpIW30truAiTvEks2z_"
        "s6WHHUO2PEOygUruhnIHms2-Bw3MlTVn1cQHdIYK7F4AqT35Ds-9OVKWYPBMZnZt2AvIaBeESTNF-gOXKT0teAeM7PHkVzTow9I_"
        "G6aCTRZhBRLrGdlaScXoVTNUhZf-"
        "oxI7fmCcQYdZ4grulQgs40LxOnpGOxjnc9xiwoIsVsjsvju3qzqUt0Gg0tNjCAQtgdxn4XKXmx2THPnClxyeF67mn5IQ0QQy33EqLsETxgUD"
        "cZ17h62JQ";
    auto result = AccessToken::parseJWT(StringData(exampleJWT), tok, error, &verifier);

    CHECK(result);
    CHECK(error == AccessToken::ParseError::none);
    CHECK_EQUAL(tok.expires, 4571178639);
    CHECK_EQUAL(tok.sync_label, "default");
    CHECK_EQUAL(tok.path, "/5da647275395c4ff4716fa93/example");
    std::uint_least32_t admin_access =
        Privilege::Download | Privilege::Upload | Privilege::ModifySchema | Privilege::SetPermissions;
    CHECK_EQUAL(tok.access, admin_access);
}

} // unnamed namespace
