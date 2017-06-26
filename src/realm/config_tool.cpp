/*************************************************************************
 *
 * Copyright 2016 Realm Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **************************************************************************/

#include <cstddef>
#include <cstring>
#include <string>
#include <iostream>

#include <realm/util/features.h>
#include <realm/version.hpp>

namespace {

enum Func {
    func_EmitFlags,
    func_ShowVersion,
    func_ShowPrefix,
    func_ShowExecPrefix,
    func_ShowIncludedir,
    func_ShowBindir,
    func_ShowLibdir,
    func_ShowLibexecdir
};

bool emit_cflags = false;
bool emit_ldflags = false;

void clear_emit_flags()
{
    emit_cflags = false;
    emit_ldflags = false;
}

bool dirty = false;

void emit_flags(const char* str)
{
    if (dirty)
        std::cout << ' ';
    std::cout << str;
    dirty = true;
}

void flush()
{
    if (dirty)
        std::cout << '\n';
}

void emit_flags()
{
    if (emit_cflags) {
#ifdef REALM_DEBUG
        emit_flags("-DREALM_DEBUG");
#endif
    }


    if (emit_ldflags) {
#ifdef REALM_CONFIG_IOS
#ifdef REALM_DEBUG
        emit_flags("-lrealm-ios-dbg");
#else
        emit_flags("-lrealm-ios");
#endif
#else
#ifdef REALM_DEBUG
        emit_flags("-lrealm-dbg");
#else
        emit_flags("-lrealm");
#endif
#endif
    }

    flush();
}

} // anonymous namespace


int main(int argc, char* argv[])
{
    Func func = func_EmitFlags;

    // Process command line
    {
        bool empty = argc == 1;
        bool help = false;
        bool error = false;

        for (int i = 1; i < argc; ++i) {
            char* arg = argv[i];
            size_t size = strlen(arg);
            if (size < 2 || strncmp(arg, "--", 2) != 0) {
                error = true;
                continue;
            }

            if (strcmp(arg, "--help") == 0) {
                help = true;
                continue;
            }
            if (strcmp(arg, "--cflags") == 0) {
                func = func_EmitFlags;
                emit_cflags = true;
                continue;
            }
            if (strcmp(arg, "--libs") == 0) {
                func = func_EmitFlags;
                emit_ldflags = true;
                continue;
            }
            if (strcmp(arg, "--version") == 0) {
                func = func_ShowVersion;
                clear_emit_flags();
                continue;
            }
            if (strcmp(arg, "--prefix") == 0) {
                func = func_ShowPrefix;
                clear_emit_flags();
                continue;
            }
            if (strcmp(arg, "--exec-prefix") == 0) {
                func = func_ShowExecPrefix;
                clear_emit_flags();
                continue;
            }
            if (strcmp(arg, "--includedir") == 0) {
                func = func_ShowIncludedir;
                clear_emit_flags();
                continue;
            }
            if (strcmp(arg, "--bindir") == 0) {
                func = func_ShowBindir;
                clear_emit_flags();
                continue;
            }
            if (strcmp(arg, "--libdir") == 0) {
                func = func_ShowLibdir;
                clear_emit_flags();
                continue;
            }
            if (strcmp(arg, "--libexecdir") == 0) {
                func = func_ShowLibexecdir;
                clear_emit_flags();
                continue;
            }
            error = true;
            break;
        }

        if (empty || error || help) {
            const char* prog = argv[0];
            std::string msg = "Synopsis: " + std::string(prog) +
                              "\n\n"
                              "Options:\n"
                              "  --version     Show the version of Realm that this command was installed\n"
                              "                as part of\n"
                              "  --cflags      Output all pre-processor and compiler flags\n"
                              "  --libs        Output all linker flags\n"
                              "  --prefix      Show the Realm installation prefix\n"
                              "  --exec-prefix Show the Realm installation prefix for executables\n"
                              "  --includedir  Show the directory holding the Realm header files\n"
                              "  --bindir      Show the directory holding the Realm executables\n"
                              "  --libdir      Show the directory holding the Realm libraries\n"
                              "  --libexecdir  Show the directory holding the Realm executables to be run\n"
                              "                by programs rather than by users\n";
            if (error) {
                std::cerr << "ERROR: Bad command line.\n\n" << msg;
                return 1;
            }
            std::cout << msg;
            return 0;
        }
    }


    switch (func) {
        case func_EmitFlags:
            emit_flags();
            break;
        case func_ShowVersion:
            std::cout << REALM_VERSION_STRING "\n";
            break;
        case func_ShowPrefix:
            std::cout << REALM_INSTALL_PREFIX "\n";
            break;
        case func_ShowExecPrefix:
            std::cout << REALM_INSTALL_EXEC_PREFIX "\n";
            break;
        case func_ShowIncludedir:
            std::cout << REALM_INSTALL_INCLUDEDIR "\n";
            break;
        case func_ShowBindir:
            std::cout << REALM_INSTALL_BINDIR "\n";
            break;
        case func_ShowLibdir:
            std::cout << REALM_INSTALL_LIBDIR "\n";
            break;
        case func_ShowLibexecdir:
            std::cout << REALM_INSTALL_LIBEXECDIR "\n";
            break;
    }
}
