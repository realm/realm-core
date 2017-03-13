// This program generates the upper_lower array in unicode.cpp
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>
#include <cstring>

std::vector<std::string> tokenize(const char* src,
    char delim,
    bool want_empty_tokens = true)
{
    std::vector<std::string> tokens;

    if (src && *src != '\0') // defensive
        while (true) {
            const char* d = strchr(src, delim);
            size_t len = (d) ? d - src : strlen(src);

            if (len || want_empty_tokens)
                tokens.push_back(std::string(src, len)); // capture token

            if (d)
                src += len + 1;
            else
                break;
        }
    return tokens;
}

int hex2int(std::string hex)
{
    int x;
    std::stringstream ss;
    ss << std::hex << hex;
    ss >> x;
    return x;
}

int main()
{
    constexpr uint32_t last_unicode = 1023; // Last unicode we want to support. 1023 is the last greek unicode

    // You need to download this file from ftp://ftp.unicode.org/Public/UNIDATA/UnicodeData.txt
    std::ifstream infile("UnicodeData.txt");

    std::string line;
    int code = 0;

    std::cout << "static const uint32_t upper_lower[" << last_unicode << " + 1][2] = {";

    while (std::getline(infile, line))
    {
        std::istringstream iss(line);
        std::vector<std::string> v = tokenize(line.c_str(), ';');

        uint32_t in = hex2int(v[0]);
    gap:
        // The unicode characters in UnicodeData.txt can have gaps, i.e. increase by more than 1 for each new line. For skipped
        // characters (i.e. characters that have no case conversion that makes sense), we want to just return the same character
        // as we got as input. Signal that by a {0, 0}-entry.
        if (in > code)
            std::cout << "{0, 0}";
        else
            std::cout << "{" << (v[12] != "" ? "0x" + v[12] : "0") << ", " << (v[13] != "" ? "0x" + v[13] : "0") << "}";

        if (code != last_unicode)
            std::cout << ", ";

        if (code == last_unicode) {
            std::cout << "};\n";
            return 0;
        }

        if (code % 10 == 0)
            std::cout << "\n";

        code++;

        if (in > code - 1)
            goto gap;
    }

    return 0;
}

