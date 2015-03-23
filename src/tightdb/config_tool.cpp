#include <cstddef>
#include <cstring>
#include <string>
#include <iostream>

#include <tightdb/util/features.h>

#define TO_STR(x) TO_STR2(x)
#define TO_STR2(x) #x

using namespace std;


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

bool emit_cflags  = false;
bool emit_ldflags = false;

void clear_emit_flags()
{
    emit_cflags  = false;
    emit_ldflags = false;
}

bool dirty = false;

void emit_flags(const char*str)
{
    if (dirty)
        cout << ' ';
    cout << str;
    dirty = true;
}

void flush()
{
    if (dirty)
        cout << '\n';
}

void emit_flags()
{
    if (emit_cflags) {
#ifdef REALM_HAVE_CONFIG
        emit_flags("-DREALM_HAVE_CONFIG");
#endif
#ifdef REALM_DEBUG
        emit_flags("-DREALM_DEBUG");
#endif
    }


    if (emit_ldflags) {
#ifdef REALM_CONFIG_IOS
#  ifdef REALM_DEBUG
        emit_flags("-lrealm-ios-dbg");
#  else
        emit_flags("-lrealm-ios");
#  endif
#else
#  ifdef REALM_DEBUG
        emit_flags("-lrealm-dbg");
#  else
        emit_flags("-ltightdb");
#  endif
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
        const char* prog = argv[0];
        --argc;
        ++argv;
        bool help  = false;
        bool error = false;
        int argc2 = 0;
        for (int i=0; i<argc; ++i) {
            char* arg = argv[i];
            size_t size = strlen(arg);
            if (size < 2 || strncmp(arg, "--", 2) != 0) {
                argv[argc2++] = arg;
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
        argc = argc2;

        if (argc != 0)
            error = true;

        if (error || help) {
            string msg =
                "Synopsis: "+string(prog)+"\n\n"
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
                cerr << "ERROR: Bad command line.\n\n" << msg;
                return 1;
            }
            cout << msg;
            return 0;
        }
    }


    switch (func) {
        case func_EmitFlags:
            emit_flags();
            break;
        case func_ShowVersion:
            cout << REALM_VERSION "\n";
            break;
        case func_ShowPrefix:
            cout << REALM_INSTALL_PREFIX "\n";
            break;
        case func_ShowExecPrefix:
            cout << REALM_INSTALL_EXEC_PREFIX "\n";
            break;
        case func_ShowIncludedir:
            cout << REALM_INSTALL_INCLUDEDIR "\n";
            break;
        case func_ShowBindir:
            cout << REALM_INSTALL_BINDIR "\n";
            break;
        case func_ShowLibdir:
            cout << REALM_INSTALL_LIBDIR "\n";
            break;
        case func_ShowLibexecdir:
            cout << REALM_INSTALL_LIBEXECDIR "\n";
            break;
    }
}
