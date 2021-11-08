#include <cstring>
#include <sstream>
#include <iomanip>

#include <realm/sync/noinst/server/encrypt_fingerprint.hpp>
#include <realm/util/sha_crypto.hpp>

using namespace realm;

std::string encrypt::calculate_fingerprint(const util::Optional<std::array<char, 64>> encryption_key)
{
    unsigned char out_buf[32];
    if (encryption_key) {
        realm::util::sha256(encryption_key->data(), 64, out_buf);
    }
    else {
        realm::util::sha256(nullptr, 0, out_buf);
    }

    std::ostringstream os;
    for (int i = 0; i < 32; ++i) {
        os << std::setfill('0') << std::setw(2) << std::hex << int(out_buf[i]);
        if (i < 31)
            os << ":";
    }
    return os.str();
}

bool encrypt::verify_fingerprint(const std::string& fingerprint,
                                 const util::Optional<std::array<char, 64>> encryption_key)
{
    const std::string calculated_fingerprint = calculate_fingerprint(encryption_key);
    return fingerprint == calculated_fingerprint;
}
