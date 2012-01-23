#ifndef UTF8_H
#define UTF8_H

#include <string>
#include <Windows.h>

inline bool case_cmp(const char *constant_upper, const char *constant_lower, const char *source);
bool case_strstr(const char *constant_upper, const char *constant_lower, const char *source);
bool utf8case(const char *source, char *destination, int upper);
size_t case_prefix(const char *constant_upper, const char *constant_lower, const char *source);
bool utf8case_single(const char **source, char **destination, int upper);

#endif