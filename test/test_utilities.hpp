#ifndef TEST_UTILITIES_H
#define TEST_UTILITIES_H

namespace tightdb {
namespace test_util {

void replace_all(std::string& str, const std::string& from, const std::string& to);
bool equal_without_cr(std::string s1, std::string s2);

}
}

#endif
