#ifndef TEST_UTILITIES_H
#define TEST_UTILITIES_H

namespace tightdb {

namespace test_util {

void replace_all(std::string& str, const std::string& from, const std::string& to);
bool equal_without_cr(std::string s1, std::string s2);
template <class T> bool almost_equal(T a, T b, double epsilon = 0.0001)
{
    return a < b + epsilon && a > b - epsilon;
}

}
}

#endif
