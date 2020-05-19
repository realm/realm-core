#include <cstddef>
#include <cstring>
#include <cstdlib>
#include <cerrno>
#include <memory>
#include <utility>
#include <algorithm>
#include <stdexcept>
#include <sstream>

#include <realm/util/features.h>

#if !(defined _WIN32) && !REALM_WATCHOS && !REALM_TVOS
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>
#include <fcntl.h>
#include <poll.h>
#define HAVE_SUPPORT_FOR_SPAWN 1
#endif

#ifdef _WIN32
#include <windows.h>
#else
extern char** environ;
#endif

#include <realm/util/assert.hpp>
#include <realm/util/buffer.hpp>
#include <realm/util/misc_ext_errors.hpp>
#include <realm/util/basic_system_errors.hpp>
#include <realm/util/system_process.hpp>

using namespace realm;
using namespace realm::util;


namespace {

#if HAVE_SUPPORT_FOR_SPAWN

// Set file descriptor flag FD_CLOEXEC if `value` is true, otherwise clear it.
//
// Note that this method of setting FD_CLOEXEC is subject to a race condition if
// another thread calls any of the exec functions concurrently. For that reason,
// this function should only be used when there is no better alternative. For
// example, Linux generally offers ways to set this flag atomically with the
// creation of a new file descriptor.
//
// `ec` untouched on success.
std::error_code set_cloexec_flag(int fd, bool value, std::error_code& ec) noexcept
{
    int flags = ::fcntl(fd, F_GETFD, 0);
    if (REALM_UNLIKELY(flags == -1)) {
        int err = errno;
        ec = make_basic_system_error_code(err);
        return ec;
    }
    flags &= ~FD_CLOEXEC;
    flags |= (value ? FD_CLOEXEC : 0);
    int ret = ::fcntl(fd, F_SETFD, flags);
    if (REALM_UNLIKELY(ret == -1)) {
        int err = errno;
        ec = make_basic_system_error_code(err);
        return ec;
    }
    return std::error_code(); // Success
}

// Set file descriptor flag FD_CLOEXEC. See set_cloexec_flag(int, bool,
// std::error_code&) for details. Throws std::system_error on failure.
inline void set_cloexec_flag(int fd, bool value = true)
{
    std::error_code ec;
    if (set_cloexec_flag(fd, value, ec))
        throw std::system_error(ec);
}


inline void checked_close(int fd) noexcept
{
    int ret = ::close(fd);
    // We can accept various errors from close(), but they must be ignored as
    // the file descriptor is closed in any case (not necessarily according to
    // POSIX, but we shall assume it anyway). `EBADF`, however, would indicate
    // an implementation bug, so we don't want to ignore that.
    REALM_ASSERT(ret != -1 || errno != EBADF);
}


class CloseGuard {
public:
    CloseGuard() noexcept {}
    explicit CloseGuard(int fd) noexcept
        : m_fd{fd}
    {
        REALM_ASSERT(fd != -1);
    }
    CloseGuard(CloseGuard&& cg) noexcept
        : m_fd{cg.release()}
    {
    }
    ~CloseGuard() noexcept
    {
        if (m_fd != -1)
            checked_close(m_fd);
    }
    void reset(int fd) noexcept
    {
        REALM_ASSERT(fd != -1);
        if (m_fd != -1)
            checked_close(m_fd);
        m_fd = fd;
    }
    operator int() const noexcept
    {
        return m_fd;
    }
    int release() noexcept
    {
        int fd = m_fd;
        m_fd = -1;
        return fd;
    }

private:
    int m_fd = -1;
};


std::size_t read_some(int fd, char* buffer, std::size_t size, std::error_code& ec) noexcept
{
    for (;;) {
        ssize_t ret = ::read(fd, buffer, size);
        if (ret == -1) {
            int err = errno;
            // Retry on interruption by system signal
            if (err == EINTR)
                continue;
            ec = make_basic_system_error_code(err); // Failure
            return 0;
        }
        if (REALM_UNLIKELY(ret == 0)) {
            ec = MiscExtErrors::end_of_input;
            return 0;
        }
        REALM_ASSERT(ret > 0);
        std::size_t n = std::size_t(ret);
        REALM_ASSERT(n <= size);
        ec = std::error_code(); // Success
        return n;
    }
}

std::size_t write_some(int fd, const char* data, std::size_t size, std::error_code& ec) noexcept
{
    for (;;) {
        ssize_t ret = ::write(fd, data, size);
        if (ret == -1) {
            int err = errno;
            // Retry on interruption by system signal
            if (err == EINTR)
                continue;
            ec = make_basic_system_error_code(err); // Failure
            return 0;
        }
        REALM_ASSERT(ret >= 0);
        std::size_t n = std::size_t(ret);
        REALM_ASSERT(n <= size);
        ec = std::error_code(); // Success
        return n;
    }
}


std::size_t read(int fd, char* buffer, std::size_t size, std::error_code& ec) noexcept
{
    char* begin = buffer;
    char* end = buffer + size;
    char* curr = begin;
    for (;;) {
        if (curr == end) {
            ec = std::error_code(); // Success
            break;
        }
        char* buffer_2 = curr;
        std::size_t size_2 = std::size_t(end - curr);
        std::size_t n = read_some(fd, buffer_2, size_2, ec);
        if (REALM_UNLIKELY(ec))
            break;
        REALM_ASSERT(n > 0);
        REALM_ASSERT(n <= size_2);
        curr += n;
    }
    std::size_t n = std::size_t(curr - begin);
    return n;
}


std::size_t write(int fd, const char* data, std::size_t size, std::error_code& ec) noexcept
{
    const char* begin = data;
    const char* end = data + size;
    const char* curr = begin;
    for (;;) {
        if (curr == end) {
            ec = std::error_code(); // Success
            break;
        }
        const char* data_2 = curr;
        std::size_t size_2 = std::size_t(end - curr);
        std::size_t n = write_some(fd, data_2, size_2, ec);
        if (REALM_UNLIKELY(ec))
            break;
        REALM_ASSERT(n > 0);
        REALM_ASSERT(n <= size_2);
        curr += n;
    }
    std::size_t n = std::size_t(curr - begin);
    return n;
}


::pid_t spawn(const char* path, char* const argv[], char* const envp[])
{
    ::pid_t child_pid = ::fork();
    if (REALM_UNLIKELY(child_pid == -1)) {
        int err = errno;
        std::error_code ec = util::make_basic_system_error_code(err);
        throw std::system_error{ec};
    }
    if (child_pid == 0) {
        // Child
        // FIXME: Consider using fexecve() when available
        ::execve(path, argv, envp);
        ::exit(127);
    }
    return child_pid;
}


bool is_valid_log_level(int value)
{
    using Level = Logger::Level;
    switch (Level(value)) {
        case Level::all:
        case Level::trace:
        case Level::debug:
        case Level::detail:
        case Level::info:
        case Level::warn:
        case Level::error:
        case Level::fatal:
        case Level::off:
            return true;
    }
    return false;
}


class LoggerFriend : Logger {
public:
    static void log(Logger& logger, Logger::Level level, const std::string& message)
    {
        Logger::do_log(logger, level, message); // Throws
    }
};


void parent_death_guard_thread(CloseGuard stop_pipe_read, CloseGuard death_pipe_read) noexcept
{
    pollfd pollfd_slots[2];
    pollfd& stop_pipe_slot = pollfd_slots[0];
    pollfd& death_pipe_slot = pollfd_slots[1];
    stop_pipe_slot = pollfd(); // Clear slot
    stop_pipe_slot.fd = stop_pipe_read;
    stop_pipe_slot.events = POLLRDNORM;
    death_pipe_slot = pollfd(); // Clear slot
    death_pipe_slot.fd = death_pipe_read;
    death_pipe_slot.events = POLLRDNORM;
    for (;;) {
        nfds_t nfds = 2;
        int timeout = -1;
        int ret = ::poll(pollfd_slots, nfds, timeout);
        if (REALM_LIKELY(ret != -1))
            break;
        int err = errno;
        REALM_ASSERT(err == EINTR);
    }

    if ((stop_pipe_slot.revents & POLLHUP) != 0)
        return;

    REALM_ASSERT((death_pipe_slot.revents & POLLHUP) != 0);
    std::abort();
}


#define SIG_CASE(sig)                                                                                                \
    case SIG##sig:                                                                                                   \
        return "SIG" #sig

const char* get_signal_name(int sig)
{
    switch (sig) {
        SIG_CASE(ABRT);
        SIG_CASE(ALRM);
        SIG_CASE(BUS);
        SIG_CASE(CHLD);
        SIG_CASE(CONT);
        SIG_CASE(FPE);
        SIG_CASE(HUP);
        SIG_CASE(ILL);
        SIG_CASE(INT);
        SIG_CASE(KILL);
        SIG_CASE(PIPE);
#ifdef SIGPOLL
        SIG_CASE(POLL);
#endif
        SIG_CASE(QUIT);
        SIG_CASE(SEGV);
        SIG_CASE(STOP);
        SIG_CASE(TERM);
        SIG_CASE(TSTP);
        SIG_CASE(TTIN);
        SIG_CASE(TTOU);
        SIG_CASE(USR1);
        SIG_CASE(USR2);
        SIG_CASE(PROF);
        SIG_CASE(SYS);
        SIG_CASE(TRAP);
        SIG_CASE(URG);
        SIG_CASE(VTALRM);
        SIG_CASE(XCPU);
        SIG_CASE(XFSZ);
    }
    return nullptr;
}

#endif // HAVE_SUPPORT_FOR_SPAWN

} // unnamed namespace


auto sys_proc::copy_local_environment() -> Environment
{
    Environment env;

#ifndef _WIN32

    for (char** i = environ; *i; ++i) {
        const char* j_1 = *i;
        const char* j_2 = std::strchr(j_1, '=');
        if (!j_2)
            throw std::runtime_error{"Environment entry without `=`"};
        std::string name{j_1, j_2};              // Throws
        std::string value{j_2 + 1};              // Throws
        env[std::move(name)] = std::move(value); // Throws
    }

#elif REALM_UWP

    if (true)
        throw std::runtime_error("Not yet implemented");

#else // defined _WIN32 && !REALM_UWP
    auto wstring_to_utf8 = [](wchar_t* w_str) {
        // First get the number of chars needed for output buffer
        int chars_num = WideCharToMultiByte(CP_UTF8, 0, w_str, -1, nullptr, 0, nullptr, nullptr);
        char* str = new char[chars_num];
        // Then convert
        WideCharToMultiByte(CP_UTF8, 0, w_str, -1, str, chars_num, nullptr, nullptr);
        std::string result{str};
        delete[] str;

        return result;
    };

    wchar_t* w_env = GetEnvironmentStringsW();
    if (REALM_UNLIKELY(!w_env))
        throw std::runtime_error("GetEnvironmentStringsA() failed");
    try {
        std::string env_2 = wstring_to_utf8(w_env);
        for (char* i = env_2.data(); *i; ++i) {
            char* j_1 = i;
            char* j_2 = std::strchr(j_1, '=');
            if (!j_2)
                throw std::runtime_error{"Environment entry without `=`"};
            i += std::strlen(i);
            std::string name{j_1, j_2};              // Throws
            std::string value{j_2 + 1, i};           // Throws
            env[std::move(name)] = std::move(value); // Throws
        }
        FreeEnvironmentStringsW(w_env);
    }
    catch (...) {
        FreeEnvironmentStringsW(w_env);
        throw;
    }

#endif // defined _WIN32 && !REALM_UWP

    return env;
}


#if HAVE_SUPPORT_FOR_SPAWN

class sys_proc::ChildHandle::Impl {
public:
    Impl(::pid_t pid, CloseGuard death_pipe_write, CloseGuard logger_pipe_read, Logger* logger) noexcept
        : m_pid{pid}
        , m_death_pipe_write{std::move(death_pipe_write)}
        , m_logger_pipe_read{std::move(logger_pipe_read)}
        , m_logger{logger}
    {
    }

    ExitInfo join();

private:
    const ::pid_t m_pid;

    const CloseGuard m_death_pipe_write;
    const CloseGuard m_logger_pipe_read;

    Logger* const m_logger;
};


auto sys_proc::ChildHandle::Impl::join() -> ExitInfo
{
    if (m_logger) {
        for (;;) {
            Logger::Level log_level = {};
            std::size_t message_size = {};
            auto& var_1 = log_level;
            auto& var_2 = message_size;
            const std::size_t header_size = sizeof var_1 + sizeof var_2;
            char header_buffer[header_size];
            std::error_code ec;
            read(m_logger_pipe_read, header_buffer, header_size, ec);
            if (REALM_UNLIKELY(ec)) {
                if (ec == MiscExtErrors::end_of_input)
                    break;
                throw std::system_error{ec};
            }
            const char* field_ptr_1 = header_buffer;
            const char* field_ptr_2 = header_buffer + sizeof var_1;
            char* var_1_ptr = reinterpret_cast<char*>(&var_1);
            char* var_2_ptr = reinterpret_cast<char*>(&var_2);
            std::copy(field_ptr_1, field_ptr_1 + sizeof var_1, var_1_ptr);
            std::copy(field_ptr_2, field_ptr_2 + sizeof var_2, var_2_ptr);
            std::string message;
            message.resize(message_size); // Throws
            read(m_logger_pipe_read, &message.front(), message_size, ec);
            if (REALM_UNLIKELY(ec)) {
                if (ec == MiscExtErrors::end_of_input)
                    break;
                throw std::system_error{ec};
            }
            if (!is_valid_log_level(int(log_level)))
                throw std::runtime_error("Bad log level");
            LoggerFriend::log(*m_logger, log_level, message); // Throws
        }
    }
    ExitInfo exit_info;
    for (;;) {
        int status = 0;
        int options = 0;
        ::pid_t pid = ::waitpid(m_pid, &status, options);
        if (REALM_UNLIKELY(pid == -1)) {
            int err = errno;
            if (err == EINTR) {
                // System call interrupted by signal, so try again.
                continue;
            }
            std::error_code ec = util::make_basic_system_error_code(err);
            throw std::system_error{ec};
        }
        REALM_ASSERT(pid == m_pid);
        if (WIFSIGNALED(status)) {
            int sig = WTERMSIG(status);
            exit_info.killed_by_signal = sig;
            exit_info.signal_name = get_signal_name(sig); // Throws
            break;
        }
        if (WIFEXITED(status)) {
            exit_info.status = WEXITSTATUS(status);
            break;
        }
    }
    return exit_info;
}

#else // !HAVE_SUPPORT_FOR_SPAWN

class sys_proc::ChildHandle::Impl {
public:
    ExitInfo join()
    {
        return {};
    }
};

#endif // !HAVE_SUPPORT_FOR_SPAWN


auto sys_proc::ChildHandle::join() -> ExitInfo
{
    return m_impl->join(); // Throws
}


sys_proc::ChildHandle::ChildHandle(ChildHandle&&) noexcept = default;


sys_proc::ChildHandle::~ChildHandle() noexcept
{
    if (Impl* impl = m_impl.release()) {
        impl->~Impl();
        std::unique_ptr<char[]> mem{reinterpret_cast<char*>(impl)};
    }
}


sys_proc::ChildHandle::ChildHandle(Impl* impl) noexcept
    : m_impl{impl}
{
}


bool sys_proc::is_spawn_supported() noexcept
{
#if HAVE_SUPPORT_FOR_SPAWN
    return true;
#else
    return false;
#endif
}


#if HAVE_SUPPORT_FOR_SPAWN

auto sys_proc::spawn(const std::string& path, const std::vector<std::string>& args, const Environment& env,
                     const SpawnConfig& config) -> ChildHandle
{
    Environment env_2;
    const Environment* env_3 = nullptr;

    // Setup parent death pipe
    CloseGuard death_pipe_read, death_pipe_write;
    if (config.parent_death_guard) {
        int pipe[2];
        int ret = ::pipe(pipe);
        if (REALM_UNLIKELY(ret == -1)) {
            int err = errno;
            std::error_code ec = make_basic_system_error_code(err);
            throw std::system_error(ec);
        }
        death_pipe_read.reset(pipe[0]);
        death_pipe_write.reset(pipe[1]);
        set_cloexec_flag(death_pipe_write); // Throws
        std::string str;
        {
            std::ostringstream out;
            out.imbue(std::locale::classic());
            out << death_pipe_read;
            str = out.str();
        }
        if (!env_3) {
            env_2 = env; // Throws (copy)
            env_3 = &env_2;
        }
        env_2["REALM_PARENT_DEATH_PIPE"] = std::move(str); // Throws
    }

    // Setup parent logger pipe
    CloseGuard logger_pipe_read, logger_pipe_write;
    if (config.logger) {
        int pipe[2];
        int ret = ::pipe(pipe);
        if (REALM_UNLIKELY(ret == -1)) {
            int err = errno;
            std::error_code ec = make_basic_system_error_code(err);
            throw std::system_error(ec);
        }
        logger_pipe_read.reset(pipe[0]);
        logger_pipe_write.reset(pipe[1]);
        set_cloexec_flag(logger_pipe_read); // Throws
        std::string str_1, str_2;
        {
            std::ostringstream out;
            out.imbue(std::locale::classic());
            out << logger_pipe_write;
            str_1 = out.str();
            out.str({});
            out << int(config.logger->level_threshold.get());
            str_2 = out.str();
        }
        if (!env_3) {
            env_2 = env; // Throws (copy)
            env_3 = &env_2;
        }
        env_2["REALM_PARENT_LOGGER_PIPE"] = std::move(str_1);  // Throws
        env_2["REALM_PARENT_LOGGER_LEVEL"] = std::move(str_2); // Throws
    }

    if (!env_3)
        env_3 = &env;

    util::AppendBuffer<char> buffer;
    buffer.append(path.data(), path.size() + 1); // Throws
    std::size_t num_args = args.size();
    std::size_t num_vars = env_3->size();
    std::size_t num_offsets = num_args + num_vars;
    std::unique_ptr<std::size_t[]> offsets = std::make_unique<std::size_t[]>(num_offsets); // Throws
    std::size_t i = 0;
    for (const std::string& arg : args) {
        offsets[i] = buffer.size();
        buffer.append(arg.data(), arg.size() + 1); // Throws
        ++i;
    }
    for (const auto& entry : *env_3) {
        const std::string& name = entry.first;
        const std::string& value = entry.second;
        if (name.find('=') != std::string::npos)
            throw std::runtime_error{"Bad environment variable name"};
        offsets[i] = buffer.size();
        buffer.append(name.data(), name.size());       // Throws
        buffer.append("=", 1);                         // Throws
        buffer.append(value.data(), value.size() + 1); // Throws
        ++i;
    }

    std::size_t num_pointers = 1 + num_args + 1 + num_vars + 1;
    std::unique_ptr<char*[]> pointers = std::make_unique<char*[]>(num_pointers); // Throws
    char** argv = pointers.get();
    char** envp = argv + 1 + num_args + 1;
    argv[0] = buffer.data();
    for (std::size_t i = 0; i < num_args; ++i)
        argv[1 + i] = buffer.data() + offsets[i];
    for (std::size_t i = 0; i < num_vars; ++i)
        envp[i] = buffer.data() + offsets[num_args + i];

    std::unique_ptr<char[]> mem = std::make_unique<char[]>(sizeof(ChildHandle::Impl)); // Throws

    ::pid_t pid = ::spawn(argv[0], argv, envp); // Throws

    ChildHandle::Impl* impl = new (mem.release())
        ChildHandle::Impl(pid, std::move(death_pipe_write), std::move(logger_pipe_read), config.logger);
    return {impl};
}

#else // !HAVE_SUPPORT_FOR_SPAWN

auto sys_proc::spawn(const std::string&, const std::vector<std::string>&, const Environment&, const SpawnConfig&)
    -> ChildHandle
{
    throw std::runtime_error("Not supported on this platform");
}

#endif // !HAVE_SUPPORT_FOR_SPAWN


#if HAVE_SUPPORT_FOR_SPAWN

sys_proc::ParentDeathGuard::ParentDeathGuard()
{
    const char* str = std::getenv("REALM_PARENT_DEATH_PIPE");
    if (!str)
        return;
    std::istringstream in(str); // Throws
    in.imbue(std::locale::classic());
    in.unsetf(std::ios_base::skipws);
    int fd = 0;
    in >> fd; // Throws
    if (!in || !in.eof())
        throw std::runtime_error("Environment variable `REALM_PARENT_DEATH_PIPE` has bad value");

    // Adopt the passed file handle
    CloseGuard death_pipe_read{fd};
    set_cloexec_flag(death_pipe_read); // Throws

    // Set up a stop pipe
    int stop_pipe[2];
    int ret = ::pipe(stop_pipe);
    if (REALM_UNLIKELY(ret == -1)) {
        int err = errno;
        std::error_code ec = make_basic_system_error_code(err);
        throw std::system_error(ec);
    }
    CloseGuard stop_pipe_read{stop_pipe[0]}, stop_pipe_write{stop_pipe[1]};
    set_cloexec_flag(stop_pipe_read);  // Throws
    set_cloexec_flag(stop_pipe_write); // Throws

    // Spawn off the guard thread
    auto func = [stop_pipe_read = std::move(stop_pipe_read), death_pipe_read = std::move(death_pipe_read)]() mutable {
        parent_death_guard_thread(std::move(stop_pipe_read), std::move(death_pipe_read));
    };
    m_thread = std::thread{std::move(func)}; // Throws

    m_stop_pipe_write = stop_pipe_write.release();
}


sys_proc::ParentDeathGuard::~ParentDeathGuard() noexcept
{
    if (!m_thread.joinable())
        return;
    REALM_ASSERT(m_stop_pipe_write != -1);
    // Wake up the guard thread and make it terminate, but without terminating
    // the process
    checked_close(m_stop_pipe_write);
    // Wait for the guard thread to terminate
    m_thread.join();
}

#else // !HAVE_SUPPORT_FOR_SPAWN

sys_proc::ParentDeathGuard::ParentDeathGuard() {}


sys_proc::ParentDeathGuard::~ParentDeathGuard() noexcept {}

#endif // !HAVE_SUPPORT_FOR_SPAWN


#if HAVE_SUPPORT_FOR_SPAWN

sys_proc::ParentLogger::ParentLogger()
{
    const char* str_1 = std::getenv("REALM_PARENT_LOGGER_PIPE");
    const char* str_2 = std::getenv("REALM_PARENT_LOGGER_LEVEL");
    if (!str_1 || !str_2)
        throw std::runtime_error("Parent process logger not available");
    std::istringstream in;
    in.imbue(std::locale::classic());
    in.unsetf(std::ios_base::skipws);
    in.str(str_1); // Throws
    int fd = 0;
    in >> fd; // Throws
    if (!in || !in.eof())
        throw std::runtime_error("Environment variable `REALM_PARENT_LOGGER_PIPE` has bad value");
    in.clear();
    in.str(str_2); // Throws
    int log_level = 0;
    in >> log_level; // Throws
    if (!in || !in.eof() || !is_valid_log_level(log_level))
        throw std::runtime_error("Environment variable `REALM_PARENT_LOGGER_LEVEL` has bad value");

    // Adopt the passed file handle
    CloseGuard pipe_write{fd};
    set_cloexec_flag(pipe_write); // Throws

    m_pipe_write = pipe_write.release();
    set_level_threshold(Level(log_level));
}


sys_proc::ParentLogger::~ParentLogger() noexcept
{
    REALM_ASSERT(m_pipe_write != -1);
    checked_close(m_pipe_write);
}


void sys_proc::ParentLogger::do_log(Level level, std::string message)
{
    std::size_t message_size = message.size();
    const auto& var_1 = level;
    const auto& var_2 = message_size;
    const std::size_t header_size = sizeof var_1 + sizeof var_2;
    char header_buffer[header_size];
    const char* var_1_ptr = reinterpret_cast<const char*>(&var_1);
    const char* var_2_ptr = reinterpret_cast<const char*>(&var_2);
    char* field_1_ptr = header_buffer;
    char* field_2_ptr = header_buffer + sizeof var_1;
    std::copy(var_1_ptr, var_1_ptr + sizeof var_1, field_1_ptr);
    std::copy(var_2_ptr, var_2_ptr + sizeof var_2, field_2_ptr);
    std::error_code ec;
    write(m_pipe_write, header_buffer, header_size, ec);
    if (REALM_UNLIKELY(ec))
        throw std::system_error{ec};
    write(m_pipe_write, message.data(), message_size, ec);
    if (REALM_UNLIKELY(ec))
        throw std::system_error{ec};
}

#else // !HAVE_SUPPORT_FOR_SPAWN

sys_proc::ParentLogger::ParentLogger()
{
    throw std::runtime_error("Parent process logger not available");
}


sys_proc::ParentLogger::~ParentLogger() noexcept {}


void sys_proc::ParentLogger::do_log(Level, std::string) {}

#endif // !HAVE_SUPPORT_FOR_SPAWN
