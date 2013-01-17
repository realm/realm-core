




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





struct SharedData {
  pthread_mutex_t mutex;
  bool is_initialized; // Protected by the mutex
  // Other members protected by the mutex
};

int main()
{
  SharedData* shared_data;
  int fd = open(...); // Create it with sero size if it does not exist

  // First initialize just the mutex
  {
    exclusive_file_lock efl;
    if (efl.try_lock(fd)) {
        if (get_file_size_using_stat(fd) < sizeof ShareData) {
            ftruncate(fd, sizeof pthread_mutex_t);
            shared_data = mmap(0, sizeof pthread_mutex_t, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
            // Initialize mutex
            msync(MS_SYNC);
            munmap(shared_data, sizeof pthread_mutex_t);
            ftruncate(fd, sizeof ShareData);
        }
    }
  }

  acquire_shared_file_lock_which_must_be_released_by_destructor();

  if (file_has_been_deleted(fd))

  shared_data = mmap(0, sizeof ShareData, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);

  // Initialize the rest of the file if we have to
  pthread_mutex_lock(&shared_data->mutex);
  if (!shared_data->is_initialized) {
    // Initialize other members
    shared_data->is_initialized = true;
  }
  pthread_mutex_lock(&shared_data->mutex);

  // Do stuff
}
