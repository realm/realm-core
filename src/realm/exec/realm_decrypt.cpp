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
        const char* key_ptr = nullptr;
        char key[64];
        std::string outfilename = "out.realm";
        for (int curr_arg = 1; curr_arg < argc; curr_arg++) {
            if (strcmp(argv[curr_arg], "--key") == 0) {
                hex_to_bin(argv[curr_arg + 1], key);
                key_ptr = key;
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
                util::AESCryptor cryptor(key_ptr);
                cryptor.set_data_size(size);
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
