#include <iostream>
#include <cstring>
#include <realm.hpp>
#include "hex_util.hpp"

using namespace realm;

int main(int argc, const char* argv[])
{
    if (argc > 1) {
        std::optional<util::EncryptionKey> key;
        std::string outfilename = "out.realm";
        for (int curr_arg = 1; curr_arg < argc; curr_arg++) {
            if (strcmp(argv[curr_arg], "--key") == 0) {
                std::array<uint8_t, 64> raw_key;
                hex_to_bin(argv[curr_arg + 1], reinterpret_cast<char*>(raw_key.data()));
                key = util::EncryptionKey(raw_key);
                curr_arg++;
            }
            else if (strcmp(argv[curr_arg], "--out") == 0) {
                outfilename = argv[curr_arg + 1];
                curr_arg++;
            }
            else {
                const std::string path = argv[curr_arg];
                std::cout << "Encrypting " << path << " into " << outfilename << std::endl;
                Group g(path);
                g.verify();
                g.write(outfilename, key);
            }
        }
    }
    else {
        std::cout << "Usage: realm-encrypt --key crypt_key [--out <outfilename>] <realmfile>" << std::endl;
    }

    return 0;
}
