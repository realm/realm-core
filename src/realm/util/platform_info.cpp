#include <locale>
#include <sstream>
#include <vector>

#include <realm/util/features.h>
#include <realm/util/basic_system_errors.hpp>
#include <realm/util/platform_info.hpp>
#include <realm/util/assert.hpp>

#ifndef _WIN32
#include <unistd.h>
#include <sys/utsname.h>
#else
#include <windows.h>
#include <VersionHelpers.h>
#endif

using namespace realm;

#ifndef _WIN32

void util::get_platform_info(PlatformInfo& info)
{
    ::utsname info_2;
    int ret = ::uname(&info_2);
    if (REALM_UNLIKELY(ret == -1)) {
        int err = errno;
        std::error_code ec = make_basic_system_error_code(err);
        throw std::system_error{ec};
    }
    PlatformInfo info_3;

#if REALM_PLATFORM_APPLE
#if REALM_IOS
    info_3.osname = "iOS"; // Throws (copy)
#elif REALM_WATCHOS
    info_3.osname = "watchOS"; // Throws (copy)
#elif REALM_TVOS
    info_3.osname = "tvOS"; // Throws (copy)
#elif TARGET_OS_MAC
    info_3.osname = "macOS"; // Throws (copy)
#else
    info_3.osname = "Apple"; // Throws (copy)
#endif
#elif REALM_ANDROID
    info_3.osname = "Android"; // Throws (copy)
#elif defined __FreeBSD__
    info_3.osname = "FreeBSD"; // Throws (copy)
#elif defined __NetBSD__
    info_3.osname = "NetBSD"; // Throws (copy)
#elif defined __OpenBSD__
    info_3.osname = "OpenBSD"; // Throws (copy)
#elif __linux__
    info_3.osname = "Linux"; // Throws (copy)
#elif defined _POSIX_VERSION
    info_3.osname = "POSIX"; // Throws (copy)
#elif __unix__
    info_3.osname = "Unix"; // Throws (copy)
#else
    info_3.osname = "unknown"; // Throws (copy)
#endif

    info_3.sysname = info_2.sysname; // Throws (copy)
    info_3.release = info_2.release; // Throws (copy)
    info_3.version = info_2.version; // Throws (copy)
    info_3.machine = info_2.machine; // Throws (copy)

    info = std::move(info_3);
}

#else // defined _WIN32

void util::get_platform_info(PlatformInfo& info)
{
    util::PlatformInfo info_2;

    std::ostringstream out;
    out.imbue(std::locale::classic());

#if REALM_UWP

    info_2.sysname = "WindowsUniversal"; // Throws

    // FIXME: When we switch to C++17 use C++/WinRT to access
    // Windows.System.Profile.AnalyticsInfo to get the OS version and family
    // https://social.msdn.microsoft.com/Forums/vstudio/en-US/2d8a7dab-1bad-4405-b70d-768e4cb2af96/uwp-get-os-version-in-an-uwp-app?forum=wpdevelop
    info_2.osname = "Windows"; // Throws
    info_2.version = "10.0";   // Throws

#else // !REALM_UWP

    info_2.sysname = "Win32"; // Throws

    info_2.osname = "Windows"; // Throws
    if (IsWindowsServer())
        info_2.osname += " Server";

    const auto system = L"kernel32.dll";
    DWORD dummy;
    const DWORD cbInfo = GetFileVersionInfoSizeExW(FILE_VER_GET_NEUTRAL, system, &dummy);
    std::vector<char> buffer(cbInfo);
    GetFileVersionInfoExW(FILE_VER_GET_NEUTRAL, system, dummy, DWORD(buffer.size()), buffer.data());
    void* p = nullptr;
    UINT size = 0;
    VerQueryValueW(buffer.data(), L"\\", &p, &size);
    REALM_ASSERT(size >= sizeof(VS_FIXEDFILEINFO));
    REALM_ASSERT(p != nullptr);
    auto pFixed = static_cast<const VS_FIXEDFILEINFO*>(p);
    out << HIWORD(pFixed->dwFileVersionMS) << '.' << LOWORD(pFixed->dwFileVersionMS) << '.'
        << HIWORD(pFixed->dwFileVersionLS) << '.' << LOWORD(pFixed->dwFileVersionLS); // Throws

    info_2.version = out.str(); // Throws
    out.str({});

#endif // !REALM_UWP

    info_2.release = "unknown"; // Throws

    SYSTEM_INFO sysinfo;
    GetSystemInfo(&sysinfo);
    switch (sysinfo.wProcessorArchitecture) {
        case PROCESSOR_ARCHITECTURE_ARM:
            info_2.machine = "arm"; // Throws
            break;
#if defined(PROCESSOR_ARCHITECTURE_ARM64)
        case PROCESSOR_ARCHITECTURE_ARM64:
            info_2.machine = "arm64"; // Throws
            break;
#endif
        case PROCESSOR_ARCHITECTURE_INTEL:
            info_2.machine = "x86"; // Throws
            break;
        case PROCESSOR_ARCHITECTURE_AMD64:
            info_2.machine = "x86_64"; // Throws
            break;
        default:
            out << "unknown-" << sysinfo.wProcessorArchitecture; // Throws
            info_2.machine = out.str();                          // Throws
            break;
    }

    info = std::move(info_2);
}

#endif // defined _WIN32
