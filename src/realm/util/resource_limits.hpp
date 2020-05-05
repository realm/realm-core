#ifndef REALM_UTIL_RESOURCE_LIMITS_HPP
#define REALM_UTIL_RESOURCE_LIMITS_HPP

namespace realm {
namespace util {


enum class Resource {
    /// The maximum size, in bytes, of the core file produced when the memory
    /// image of this process is dumped. If the memory image is larger than the
    /// limit, the core file will not be created. Same as `RLIMIT_CORE` of
    /// POSIX.
    core_dump_size,

    /// The maximum CPU time, in seconds, available to this process. If the
    /// limit is exceeded, the process will be killed. Same as `RLIMIT_CPU` of
    /// POSIX.
    cpu_time,

    /// The maximum size, in bytes, of the data segment of this process. If the
    /// limit is exceede, std::malloc() will fail with `errno` equal to
    /// `ENOMEM`. Same as `RLIMIT_DATA` of POSIX.
    data_segment_size,

    /// The maximum size, in bytes, of a file that is modified by this
    /// process. If the limit is exceede, the process will be killed. Same as
    /// `RLIMIT_FSIZE` of POSIX.
    file_size,

    /// One plus the maximum file descriptor value that can be opened by this
    /// process. Same as `RLIMIT_NOFILE` of POSIX.
    num_open_files,

    /// The maximum size, in bytes, of the stack of the main thread of this
    /// process. If the limit is exceede, the process is killed. Same as
    /// `RLIMIT_STACK` of POSIX.
    stack_size,

    /// The maximum size, in bytes, of the process's virtual memory (address
    /// space). If the limit is exceeded due to heap allocation, std::malloc()
    /// will fail with `errno` equal to `ENOMEM`. If the limit is exceeded due
    /// to explicit memory mapping, mmap() will fail with `errno` equal to
    /// `ENOMEM`. If the limit is exceeded due to stack expansion, the process
    /// will be killed.  Same as `RLIMIT_AS` of POSIX.
    virtual_memory_size
};


bool system_has_rlimit(Resource) noexcept;


//@{
/// Get or set resouce limits. A negative value means 'unlimited', both when
/// getting and when setting.
long get_hard_rlimit(Resource);
long get_soft_rlimit(Resource);
void set_soft_rlimit(Resource, long value);
//@}


} // namespace util
} // namespace realm

#endif // REALM_UTIL_RESOURCE_LIMITS_HPP
