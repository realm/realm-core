#include <cstddef>
#include <cstring>
#include <string>
#include <iostream>

#include <tightdb/config.h>

#define TO_STR(x) TO_STR2(x)
#define TO_STR2(x) #x

using namespace std;


namespace {

bool dirty = false;

void emit_flags(const char*str)
{
    if (dirty) cout << ' ';
    cout << str;
    dirty = true;
}

void flush()
{
    if (dirty) cout << '\n';
}

} // anonymous namespace



int main(int argc, char* argv[])
{
    bool emit_cflags  = false;
    bool emit_ldflags = false;

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
                emit_cflags = true;
                continue;
            }
            if (strcmp(arg, "--libs") == 0) {
                emit_ldflags = true;
                continue;
            }
            error = true;
            break;
        }
        argc = argc2;

        if (argc != 0) error = true;

        if (error || help) {
            string msg =
                "Synopsis: "+string(prog)+"\n\n"
                "Options:\n"
                "  --cflags  Output all pre-processor and compiler flags\n"
                "  --libs    Output all linker flags\n";
            if (error) {
                cerr << "ERROR: Bad command line.\n\n" << msg;
                return 1;
            }
            cout << msg;
            return 0;
        }
    }


    if (emit_cflags) {
#if defined USE_SSE3 || defined USE_SSE42
        emit_flags("-msse4.2");
#endif
#ifdef USE_SSE3
        emit_flags("-DUSE_SSE3");
#endif
#ifdef USE_SSE42
        emit_flags("-DUSE_SSE42");
#endif

#ifdef TIGHTDB_ENABLE_REPLICATION
        emit_flags("-DTIGHTDB_ENABLE_REPLICATION");
#endif

        if (MAX_LIST_SIZE != TIGHTDB_DEFAULT_MAX_LIST_SIZE) {
            emit_flags("-DMAX_LIST_SIZE=" TO_STR(MAX_LIST_SIZE));
        }

#ifdef TIGHTDB_DEBUG
        emit_flags("-DTIGHTDB_DEBUG");
#endif
    }


    if (emit_ldflags) {
#ifdef TIGHTDB_DEBUG
        emit_flags("-ltightdb-dbg");
#else
        emit_flags("-ltightdb");
#endif
    }

    flush();

    return 0;
}
