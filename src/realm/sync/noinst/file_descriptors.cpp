#include <cstdio>
#include <cstdlib>
#include <string>
#include <iostream>
#include <sys/types.h>
#include <memory>

#ifdef REALM_LSOF_OUTPUT
#include <unistd.h>
#endif

#include <realm/sync/noinst/file_descriptors.hpp>

std::string realm::_impl::get_lsof_output()
{
#ifdef REALM_LSOF_OUTPUT
    pid_t pid = ::getpid();

    std::string cmd = "lsof -P -p " + std::to_string(pid) + " 2>&1";

    std::unique_ptr<FILE, void (*)(FILE*)> stream{popen(cmd.c_str(), "r"), [](FILE* f) {
                                                      pclose(f);
                                                  }};
    if (!stream)
        return "lsof failed";

    char buffer[1024];
    std::string result;
    while (fgets(buffer, 1024, stream.get()))
        result += buffer;

    return result;
#else
    return "";
#endif
}
