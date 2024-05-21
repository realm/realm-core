#include <iostream>
#include <fstream>
#include <cstring>
#include <realm/util/file.hpp>
#include <realm/util/aes_cryptor.hpp>
#include <realm/util/encrypted_file_mapping.hpp>
#include "hex_util.hpp"

using namespace realm;

constexpr size_t block_size = 4096;

int main(int argc, const char* argv[])
{
    if (argc > 3) {
        util::EncryptionKey key;
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
                std::cout << "Decrypting " << path << " into " << outfilename << std::endl;
                std::ofstream out(outfilename);
                util::File file;
                file.open(path);
                file.set_encryption_key(key);
                auto size = (off_t)file.get_size();
                decltype(size) pos = 0;
                util::AESCryptor cryptor(key);
                cryptor.set_file_size(size);
                while (pos < size) {
                    char buf[block_size];
                    cryptor.try_read_block(file.get_descriptor(), pos, buf);
                    out.write(buf, block_size);
                    pos += block_size;
                }
            }
        }
    }
    else {
        std::cout << "Usage: realm-decrypt --key crypt_key [--out <outfilename>] <realmfile>" << std::endl;
    }

    return 0;
}
