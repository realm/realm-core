/*************************************************************************
 *
 * TIGHTDB CONFIDENTIAL
 * __________________
 *
 *  [2011] - [2012] TightDB Inc
 *  All Rights Reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of TightDB Incorporated and its suppliers,
 * if any.  The intellectual and technical concepts contained
 * herein are proprietary to TightDB Incorporated
 * and its suppliers and may be covered by U.S. and Foreign Patents,
 * patents in process, and are protected by trade secret or copyright law.
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from TightDB Incorporated.
 *
 **************************************************************************/
#ifndef TIGHTDB_REPLICATION_HPP
#define TIGHTDB_REPLICATION_HPP

#include <algorithm>
#include <limits>
#include <exception>
#include <string>
#include <ostream>

#include <tightdb/util/assert.hpp>
#include <tightdb/util/tuple.hpp>
#include <tightdb/util/safe_int_ops.hpp>
#include <tightdb/util/unique_ptr.hpp>
#include <tightdb/util/buffer.hpp>
#include <tightdb/util/string_buffer.hpp>
#include <tightdb/util/file.hpp>
#include <tightdb/descriptor.hpp>
#include <tightdb/group.hpp>
#include <tightdb/group_shared.hpp>
#include <tightdb/impl/transact_log.hpp>

#include <iostream>


namespace tightdb {

// FIXME: Be careful about the possibility of one modification functions being called by another where both do transaction logging.

// FIXME: The current table/subtable selection scheme assumes that a TableRef of a subtable is not accessed after any modification of one of its ancestor tables.

// FIXME: Checking on same Table* requires that ~Table checks and nullifies on match. Another option would be to store m_selected_table as a TableRef. Yet another option would be to assign unique identifiers to each Table instance vial Allocator. Yet another option would be to explicitely invalidate subtables recursively when parent is modified.

/// Replication is enabled by passing an instance of an implementation
/// of this class to the SharedGroup constructor.
class Replication:
    public _impl::TransactLogConvenientEncoder,
    protected _impl::TransactLogStream
{
public:
    // Be sure to keep this type aligned with what is actually used in
    // SharedGroup.
    typedef uint_fast64_t version_type;
    typedef _impl::InputStream InputStream;
    typedef _impl::TransactLogParser TransactLogParser;
    class TransactLogApplier;
    class Interrupted; // Exception
    struct CommitLogEntry;
    class IndexTranslatorBase;
    class SimpleIndexTranslator;
    class SimpleInputStream;

    std::string get_database_path();

    /// Reset transaction logs. This call informs the commitlog subsystem of
    /// the initial version chosen as part of establishing a sharing scheme
    /// (also called a "session").
    /// Following a crash, the commitlog subsystem may hold multiple commitlogs
    /// for versions which are lost during the crash. When SharedGroup establishes
    /// a sharing scheme it will continue from the last version commited to
    /// the database.
    ///
    /// The call also indicates that the current thread (and current process)
    /// has exclusive access to the commitlogs, allowing them to reset
    /// synchronization variables. This can be beneficial on systems without
    /// proper support for robust mutexes.
    virtual void reset_log_management(version_type last_version);

    /// Cleanup, remove any log files
    virtual void stop_logging();

    /// The commitlog subsystem can be operated in either of two modes:
    /// server-synchronization mode and normal mode.
    /// When operating in server-synchronization mode.
    /// - the log files are persisted in a crash safe fashion
    /// - when a sharing scheme is established, the logs are assumed to exist already
    ///   (unless we are creating a new database), and an exception is thrown if they
    ///   are missing.
    /// - even after a crash which leaves the log files out of sync wrt to the database,
    ///   the log files can re-synchronized transparently
    /// When operating in normal-mode
    /// - the log files are not updated in a crash safe way
    /// - the log files are removed when the session ends
    /// - the log files are not assumed to be there when a session starts, but are
    ///   created on demand.
    virtual bool is_in_server_synchronization_mode();

    /// Called by SharedGroup during a write transaction, when readlocks are
    /// recycled, to keep the commit log management in sync with what versions
    /// can possibly be interesting in the future.
    virtual void set_last_version_seen_locally(version_type last_seen_version_number)
        TIGHTDB_NOEXCEPT;

    /// Get all transaction logs between the specified versions. The number of
    /// requested logs is exactly `to_version - from_version`. If this number is
    /// greater than zero, the first requested log is the one that brings the
    /// database from `from_version` to `from_version + 1`. References to the
    /// requested logs are stored in successive entries of `logs_buffer`. The
    /// calee retains ownership of the memory referenced by those entries, but
    /// the memory will remain accessible to the caller until they are declared
    /// stale by calls to 'set_last_version_seen_locally' and
    /// 'set_last_version_synced' *on any commitlog instance participating in
    /// the session*, OR until a new call to get_commit_entries(),
    /// apply_foreign_changeset() or commit_write_transact() is made *on the
    /// same commitlog instance*.
    ///
    /// The two variants differ in the type of data returned. Use the version
    /// with CommitLogEntry* if you need the additional data provided by that
    /// type (see below)
    virtual void get_commit_entries(version_type from_version, version_type to_version,
                                    BinaryData* logs_buffer) TIGHTDB_NOEXCEPT;

    virtual void get_commit_entries(version_type from_version, version_type to_version,
                                    CommitLogEntry* logs_buffer) TIGHTDB_NOEXCEPT;

    /// See set_client_file_ident() and set_sync_progress().
    virtual void get_sync_info(uint_fast64_t& client_file_ident, version_type& server_version,
                               version_type& client_version);

    /// Save the server assigned client file identifier to persistent
    /// storage. This is done in a way that ensures crash-safety. It is an error
    /// to set this identifier more than once. It is also an error to specify
    /// zero, as zero is not a valid identifier. This identifier is used as part
    /// of the synchronization mechanism.
    virtual void set_client_file_ident(uint_fast64_t);

    /// Save the synchronization progress to persistent storage, and as an
    /// atomic unit. This is done in a way that ensures
    /// crash-safety. Additionally, `client_version` has an effect on the
    /// process by which old history entries are discarded. See below for more
    /// on this.
    ///
    /// \param server_version The version number of a server version that was
    /// recently integrated by this client, or of a server version recently
    /// produced by integration on the server of a changeset from this client,
    /// and reported by the server to this client through an 'accept' message.
    ///
    /// \param client_version The version number of the last client version
    /// integrated by the server into `server_version`. All changesets produced
    /// after `client_version` are potentially needed when conflicting histories
    /// need to be merged during synchronization, so the Replication class
    /// promises to retain all history entries produced after
    /// `client_version`. That is, a history entry with a changeset that takes
    /// the group from version V to version W is guaranteed to be retained if W
    /// > `client_version`.
    ///
    /// It is an error to specify a client version that is less than the
    /// currently stored version, since there is no way to get discarded history
    /// back.
    virtual void set_sync_progress(version_type server_version,
                                   version_type client_version);

    /// Apply the specified changeset to the specified group as a single
    /// transaction, but only if that transaction can be based on the specified
    /// version. It is an error to specify a base version that is ahead of the
    /// current version of the group. Doing so will cause an exception to be
    /// thrown. Otherwise, if the current version is ahead of the specified base
    /// version (i.e., a conflict), this function returns 0. Otherwise it
    /// attempts to apply the changeset, and if that succeeds, it returns the
    /// new version produced by the transaction. Note that this will also have
    /// the effect of making the specified changeset available as a transaction
    /// log through this transaction log registry. The caller retains ownership
    /// of the specified changeset buffer.
    ///
    /// The specified shared group must have this replication instance set as
    /// its associated Replication instance. The effect of violating this rule
    /// is unspecified.
    ///
    /// \param server_version Not yet in used.
    ///
    /// FIXME This function, and several others, do not belong in the
    /// Replication class. The Replication interface is supposed to be just a
    /// sink that allows a SharedGroup to submit actions for replication. It is
    /// then up to the implementation of the Repication interface to define what
    /// replication means.
    virtual version_type apply_foreign_changeset(SharedGroup&, version_type base_version,
                                                 BinaryData changeset, uint_fast64_t timestamp,
                                                 uint_fast64_t peer_id, version_type peer_version,
                                                 std::ostream* apply_log = 0);
    virtual version_type get_last_peer_version(uint_fast64_t peer_id);

    /// Acquire permision to start a new 'write' transaction. This
    /// function must be called by a client before it requests a
    /// 'write' transaction. This ensures that the local shared
    /// database is up-to-date. During the transaction, all
    /// modifications must be posted to this Replication instance as
    /// calls to set_value() and friends. After the completion of the
    /// transaction, the client must call either
    /// commit_write_transact() or rollback_write_transact().
    ///
    /// \throw Interrupted If this call was interrupted by an
    /// asynchronous call to interrupt().
    void begin_write_transact(SharedGroup&);

    /// Commit the accumulated transaction log. The transaction log
    /// may not be committed if any of the functions that submit data
    /// to it, have failed or been interrupted. This operation will
    /// block until the local coordinator reports that the transaction
    /// log has been dealt with in a manner that makes the transaction
    /// persistent. This operation may be interrupted by an
    /// asynchronous call to interrupt().
    ///
    /// \throw Interrupted If this call was interrupted by an
    /// asynchronous call to interrupt().
    ///
    /// FIXME: In general the transaction will be considered complete
    /// even if this operation is interrupted. Is that ok?
    version_type commit_write_transact(SharedGroup&, version_type orig_version);

    /// Called by a client to discard the accumulated transaction
    /// log. This function must be called if a write transaction was
    /// successfully initiated, but one of the functions that submit
    /// data to the transaction log has failed or has been
    /// interrupted. It must also be called after a failed or
    /// interrupted call to commit_write_transact().
    void rollback_write_transact(SharedGroup&) TIGHTDB_NOEXCEPT;

    /// Interrupt any blocking call to a function in this class. This
    /// function may be called asyncronously from any thread, but it
    /// may not be called from a system signal handler.
    ///
    /// Some of the public function members of this class may block,
    /// but only when it it is explicitely stated in the documention
    /// for those functions.
    ///
    /// FIXME: Currently we do not state blocking behaviour for all
    /// the functions that can block.
    ///
    /// After any function has returned with an interruption
    /// indication, the only functions that may safely be called are
    /// rollback_write_transact() and the destructor. If a client,
    /// after having received an interruption indication, calls
    /// rollback_write_transact() and then clear_interrupt(), it may
    /// resume normal operation through this Replication instance.
    void interrupt() TIGHTDB_NOEXCEPT;

    /// May be called by a client to reset this replication instance
    /// after an interrupted transaction. It is not an error to call
    /// this function in a situation where no interruption has
    /// occured.
    void clear_interrupt() TIGHTDB_NOEXCEPT;

    /// Called by the local coordinator to apply a transaction log
    /// received from another local coordinator.
    ///
    /// \param apply_log If specified, and the library was compiled in
    /// debug mode, then a line describing each individual operation
    /// is writted to the specified stream.
    ///
    /// \throw BadTransactLog If the transaction log could not be
    /// successfully parsed, or ended prematurely.
    static void apply_transact_log(InputStream& transact_log, Group& target,
                                   std::ostream* apply_log = 0);
    static void apply_transact_log(InputStream& transact_log, Group& target,
                                   IndexTranslatorBase& translator, std::ostream* apply_log = 0);

    virtual ~Replication() TIGHTDB_NOEXCEPT {}

protected:
    Replication();

    virtual std::string do_get_database_path() = 0;

    /// As part of the initiation of a write transaction, this method
    /// is supposed to update `m_transact_log_free_begin` and
    /// `m_transact_log_free_end` such that they refer to a (possibly
    /// empty) chunk of free space.
    virtual void do_begin_write_transact(SharedGroup&) = 0;

    /// The caller guarantees that `m_transact_log_free_begin` marks
    /// the end of payload data in the transaction log.
    virtual version_type do_commit_write_transact(SharedGroup&, version_type orig_version) = 0;

    virtual void do_rollback_write_transact(SharedGroup&) TIGHTDB_NOEXCEPT = 0;

    virtual void do_interrupt() TIGHTDB_NOEXCEPT = 0;

    virtual void do_clear_interrupt() TIGHTDB_NOEXCEPT = 0;

    /// Must be called only from do_begin_write_transact(),
    /// do_commit_write_transact(), or do_rollback_write_transact().
    static Group& get_group(SharedGroup&) TIGHTDB_NOEXCEPT;

    /// Must be called only from do_begin_write_transact(),
    /// do_commit_write_transact(), or do_rollback_write_transact().
    static version_type get_current_version(SharedGroup&);

    friend class Group::TransactReverser;
};

/// Extended version of a commit log entry. The additional info is required for
/// Sync.
struct Replication::CommitLogEntry {
    /// When did it happen?
    uint64_t timestamp;

    /// Nonzero iff this changeset was submitted via apply_foreign_changeset().
    uint64_t peer_id;

    /// The last remote version that this commit reflects.
    version_type peer_version;

    /// The changeset.
    BinaryData log_data;
};

// re server_version: This field is written by Sync (if enabled) on commits which
// are foreign. It is carried over as part of a commit, allowing other threads involved
// with Sync to observet it. For local commits, the value of server_version is taken
// from any previous forewign commmit.

class Replication::IndexTranslatorBase {
public:
    virtual size_t translate_row_index(TableRef table, size_t row_ndx, bool* overwritten = null_ptr) = 0;
};

class Replication::SimpleIndexTranslator : public Replication::IndexTranslatorBase {
public:
    size_t translate_row_index(TableRef, size_t row_ndx, bool* overwritten) TIGHTDB_OVERRIDE
    {
        if (overwritten)
            *overwritten = false;
        return row_ndx;
    }
};


class Replication::SimpleInputStream: public Replication::InputStream {
public:
    SimpleInputStream(const char* data, std::size_t size):
        m_data(data),
        m_size(size)
    {
    }

    std::size_t next_block(const char*& begin, const char*& end) TIGHTDB_OVERRIDE
    {
        if (m_size == 0)
            return 0;
        std::size_t size = m_size;
        begin = m_data;
        end = m_data + size;
        m_size = 0;
        return size;
    }

private:
    const char* m_data;
    std::size_t m_size;
};


class Replication::Interrupted: public std::exception {
public:
    const char* what() const TIGHTDB_NOEXCEPT_OR_NOTHROW TIGHTDB_OVERRIDE
    {
        return "Interrupted";
    }
};


class TrivialReplication: public Replication {
public:
    ~TrivialReplication() TIGHTDB_NOEXCEPT {}

protected:
    typedef Replication::version_type version_type;

    TrivialReplication(const std::string& database_file);

    virtual void handle_transact_log(const char* data, std::size_t size,
                                     version_type new_version) = 0;

    static void apply_transact_log(const char* data, std::size_t size, SharedGroup& target,
                                   std::ostream* apply_log = 0);
    void prepare_to_write();

private:
    const std::string m_database_file;
    util::Buffer<char> m_transact_log_buffer;

    std::string do_get_database_path() TIGHTDB_OVERRIDE;
    void do_begin_write_transact(SharedGroup&) TIGHTDB_OVERRIDE;
    version_type do_commit_write_transact(SharedGroup&, version_type orig_version) TIGHTDB_OVERRIDE;
    void do_rollback_write_transact(SharedGroup&) TIGHTDB_NOEXCEPT TIGHTDB_OVERRIDE;
    void do_interrupt() TIGHTDB_NOEXCEPT TIGHTDB_OVERRIDE;
    void do_clear_interrupt() TIGHTDB_NOEXCEPT TIGHTDB_OVERRIDE;
    void transact_log_reserve(std::size_t n, char** new_begin, char** new_end) TIGHTDB_OVERRIDE;
    void transact_log_append(const char* data, std::size_t size, char** new_begin, char** new_end) TIGHTDB_OVERRIDE;
    void internal_transact_log_reserve(std::size_t, char** new_begin, char** new_end);

    std::size_t transact_log_size();

    friend class Group::TransactReverser;
};


// Implementation:

inline Replication::Replication():
    _impl::TransactLogConvenientEncoder(static_cast<_impl::TransactLogStream&>(*this))
{
}


inline std::string Replication::get_database_path()
{
    return do_get_database_path();
}

inline void Replication::reset_log_management(version_type)
{
}

inline bool Replication::is_in_server_synchronization_mode()
{
    return false;
}

inline void Replication::stop_logging()
{
}

inline void Replication::set_last_version_seen_locally(version_type) TIGHTDB_NOEXCEPT
{
}

inline void Replication::get_sync_info(uint_fast64_t&, version_type&, version_type&)
{
}

inline void Replication::set_client_file_ident(uint_fast64_t)
{
}

inline void Replication::set_sync_progress(version_type, version_type)
{
}

inline Replication::version_type
Replication::apply_foreign_changeset(SharedGroup&, version_type, BinaryData,
                                     uint_fast64_t, uint_fast64_t, version_type,
                                     std::ostream*)
{
    // Unimplemented!
    TIGHTDB_ASSERT(false);
    return false;
}

inline Replication::version_type
Replication::get_last_peer_version(uint_fast64_t)
{
    // Unimplemented!
    TIGHTDB_ASSERT(false);
    return 0;
}

inline void Replication::get_commit_entries(version_type, version_type, Replication::CommitLogEntry*)
    TIGHTDB_NOEXCEPT
{
    // Unimplemented!
    TIGHTDB_ASSERT(false);
}


inline void Replication::get_commit_entries(version_type, version_type, BinaryData*)
    TIGHTDB_NOEXCEPT
{
    // Unimplemented!
    TIGHTDB_ASSERT(false);
}

inline void Replication::begin_write_transact(SharedGroup& sg)
{
    do_begin_write_transact(sg);
    reset_selection_caches();
}

inline Replication::version_type
Replication::commit_write_transact(SharedGroup& sg, version_type orig_version)
{
    return do_commit_write_transact(sg, orig_version);
}

inline void Replication::rollback_write_transact(SharedGroup& sg) TIGHTDB_NOEXCEPT
{
    do_rollback_write_transact(sg);
}

inline void Replication::interrupt() TIGHTDB_NOEXCEPT
{
    do_interrupt();
}

inline void Replication::clear_interrupt() TIGHTDB_NOEXCEPT
{
    do_clear_interrupt();
}

inline TrivialReplication::TrivialReplication(const std::string& database_file):
    m_database_file(database_file)
{
}

inline std::size_t TrivialReplication::transact_log_size()
{
    return write_position() - m_transact_log_buffer.data();
}

inline void TrivialReplication::transact_log_reserve(std::size_t n, char** new_begin, char** new_end)
{
    internal_transact_log_reserve(n, new_begin, new_end);
}

inline void TrivialReplication::internal_transact_log_reserve(std::size_t n, char** new_begin, char** new_end)
{
    char* data = m_transact_log_buffer.data();
    std::size_t size = write_position() - data;
    m_transact_log_buffer.reserve_extra(size, n);
    data = m_transact_log_buffer.data(); // May have changed
    *new_begin = data + size;
    *new_end = data + m_transact_log_buffer.size();
}

} // namespace tightdb

#endif // TIGHTDB_REPLICATION_HPP
