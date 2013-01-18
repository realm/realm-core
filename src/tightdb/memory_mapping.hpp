




struct MappedFile {
    open(path, mode);

    get_size() -> size;

    map(size, readl_only) -> addr;

    unmap(addr, size);

    remap(old_addr, old_size, new_size) -> new_addr;

private:
    int m_fd;
};


// When map is readonly need to be able to create a second map that is read/write.
struct WritableMapping {
    map(MappedFile, size) -> addr;

private:
};






struct MemoryMappedFile {
};



struct NamedSharedMemory {
  map()
private:
  int m_fd;
};



Could we get a corrupted state in the lock file if a process is killed at an inconvenient time?

We need a way to check if a lock file is fully initialized.

We need a way to check if a lock file needs reinitialization.

Should we at all try to remove the lock file (and the database file in case of in-memory)?




void create()
{
  m_path = db_path + ".lock";

 again:
  m_fd = open(m_path); // FIXME: Need close guard

  size = 0;

  {
    exclusive_file_lock l;
    if (l.try_lock(m_fd)) {
      // FIXME: Avoid reinitialization - not so easy!!!
      size = sizeof(...);
      ftruncate(m_fd, size);
      // Map
      if (not_fully_initialized || dirty)
      // Init
    }
  }

  {
    shared_file_lock l(m_fd);
    // FIXME: How to detect a not fully initialized file at this point??? I would like to set a flag in the mapped memory, but can I be sure that the update order is not reversed? The same question arises for changes in the database file. Is a memory barrier needed between the initialization and the setting of the flag?
    if (get_num_file_links(m_fd) == 0) goto again;
    if (size)
      if (get_file_size(m_fd) == 0) {
    // Get size
    // If size is zero,
    // Map

    l.release();
  }
}



void destroy()
{
  try {
    exclusive_file_lock l;
    if (l.try_lock(m_fd)) {
      remove(m_path);
    }
  }
  catch(...) {
    // Since we are in a destructor, we silently ignore a failed attempt to remove the coordination file.
  }
  close(m_fd); // FIXME: Failures here?
}





struct file {
    file()  TIGHTDB_NOEXCEPT: m_fd(-1) {}
    ~file() TIGHTDB_NOEXCEPT { close(); }

    void open(...) { ...; }
    void close() TIGHTDB_NOEXCEPT { ...; m_fd = -1; }

    // File must be open
    bool try_lock_exclusive();
    void lock_shared();
    void unlock();

    struct close_guard;
    struct unlock_guard;

private:
    int m_fd;
};

struct file::close_guard {
    close_guard(file& f) TIGHTDB_NOEXCEPT: m_file(&f) {}
    ~close_guard()  TIGHTDB_NOEXCEPT { if (m_file) m_file->close(); }
    release() TIGHTDB_NOEXCEPT { m_file = 0; }
private:
    file* m_file;
};

struct file::unlock_guard {
    unlock_guard(file& f) TIGHTDB_NOEXCEPT: m_file(&f) {}
    ~unlock_guard()  TIGHTDB_NOEXCEPT { if (m_file) m_file->unlock(); }
    release() TIGHTDB_NOEXCEPT { m_file = 0; }
private:
    file* m_file;
};

/* WinAPI:

OVERLAPPED dummy;
memset(&dummy, 0, sizeof dummy);
LockFileEx(file, (excl?LOCKFILE_EXCLUSIVE_LOCK:0), 0, 1, 0, &dummy); // Success if non-zero

UnlockFile(file, 0, 0, 1, 0); // Success if non-zero

*/

template<class T> struct mappable_file: file {
    mappable_file()  TIGHTDB_NOEXCEPT: m_addr(0) {}
    ~mappable_file() TIGHTDB_NOEXCEPT { close(); }

    T* map(std:size_t s = sizeof T) { ...; m_size = s; }
    void unmap() TIGHTDB_NOEXCEPT { ...; m_addr = 0; }

    void close() TIGHTDB_NOEXCEPT { unmap(); file::close(); }

private:
    T* m_addr;
    std::size_t m_size;
};


struct SharedData {
  pthread_mutex_t mutex;
  bool is_initialized; // Protected by the mutex
  // Other members protected by the mutex
};

void init()
{
    m_file.open(...); // Create it with sero size if it does not exist
    file::close_guard fcg(m_file);

    // First initialize just the mutex
    if (m_file.try_lock_exclusive()) {
        file::unlock_guard fulg(m_file);
        if (m_file.get_size() < sizeof ShareData) {
            m_file.resize(sizeof pthread_mutex_t);
            {
                SharedData* const shared_data = m_file.map(for_read_write, sizeof pthread_mutex_t);
                mappable_file::unmap_guard fumg(m_file);

                // Initialize mutex

                // FIXME: Must also initialize m_is_initialized if the resizing operation does not guarantee zero-fill.

                msync(MS_SYNC); // FIXME: Win-API???
            }
            m_file.resize(sizeof ShareData);
        }
    }

    m_file.lock_shared();
    file::unlock_guard fulg(m_file);

    SharedData* const shared_data = m_file.map(for_read_write);
    mappable_file::unmap_guard fumg(m_file);

    if (file_has_been_deleted(fd))   ; // FIXME: Could this be based simply on a flag in SharedData?

    // Initialize the rest of the file if we have to
    pthread_mutex_lock(&shared_data->mutex);
    if (!shared_data->m_is_initialized) {
        // Initialize other members
        shared_data->m_is_initialized = true;
    }
    pthread_mutex_lock(&shared_data->mutex);

    fumg.release(); // Don't unmap
    fulg.release(); // Don't unlock
    fcg.release(); // Don't close
}
