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

#ifndef REALM_LANG_BIND_HELPER_HPP
#define REALM_LANG_BIND_HELPER_HPP

#include <cstddef>

#include <realm/table.hpp>
#include <realm/table_view.hpp>
#include <realm/group.hpp>
#include <realm/group_shared.hpp>

#include <realm/replication.hpp>

namespace realm {


/// These functions are only to be used by language bindings to gain
/// access to certain memebers that are othewise private.
///
/// \note Applications are not supposed to call any of these functions
/// directly.
///
/// All of the get_subtable_ptr() functions bind the table accessor pointer
/// before it is returned (bind_table_ptr()). The caller is then responsible for
/// making the corresponding call to unbind_table_ptr().
class LangBindHelper {
public:
    /// Increment the reference counter of the specified table accessor. This is
    /// done automatically by all of the functions in this class that return
    /// table accessor pointers, but if the binding/application makes a copy of
    /// such a pointer, and the copy needs to have an "independent life", then
    /// the binding/application must bind that copy using this function.
    static void bind_table_ptr(const Table*) noexcept;

    /// Decrement the reference counter of the specified table accessor. The
    /// binding/application must call this function for every bound table
    /// accessor pointer object, when that pointer object ends its life.
    static void unbind_table_ptr(const Table*) noexcept;

    /// Construct a new freestanding table. The table accessor pointer is bound
    /// by the callee before it is returned (bind_table_ptr()).
    static Table* new_table();

    /// Construct a new freestanding table as a copy of the specified one. The
    /// table accessor pointer is bound by the callee before it is returned
    /// (bind_table_ptr()).
    static Table* copy_table(const Table&);

    //@{

    /// These functions are like their namesakes in Group, but these bypass the
    /// construction of a smart-pointer object (TableRef). The table accessor
    /// pointer is bound by the callee before it is returned (bind_table_ptr()).

    static Table* get_table(Group&, TableKey key);
    static const Table* get_table(const Group&, TableKey key);

    static Table* get_table(Group&, StringData name);
    static const Table* get_table(const Group&, StringData name);

    static Table* add_table(Group&, StringData name, bool require_unique_name = true);
    static Table* get_or_add_table(Group&, StringData name, bool* was_added = nullptr);

    //@}

    using VersionID = DB::VersionID;

    /// \defgroup lang_bind_helper_transactions Continuous Transactions
    ///
    /// advance_read() is equivalent to terminating the current read transaction
    /// (SharedGroup::end_read()), and initiating a new one
    /// (SharedGroup::begin_read()), except that all subordinate accessors
    /// (Table, Row, Descriptor) will remain attached to the underlying objects,
    /// unless those objects were removed in the target snapshot. By default,
    /// the read transaction is advanced to the latest available snapshot, but
    /// see SharedGroup::begin_read() for information about \a version.
    ///
    /// promote_to_write() is equivalent to terminating the current read
    /// transaction (SharedGroup::end_read()), and initiating a new write
    /// transaction (SharedGroup::begin_write()), except that all subordinate
    /// accessors (Table, Row, Descriptor) will remain attached to the
    /// underlying objects, unless those objects were removed in the target
    /// snapshot.
    ///
    /// commit_and_continue_as_read() is equivalent to committing the current
    /// write transaction (SharedGroup::commit()) and initiating a new read
    /// transaction, which is bound to the snapshot produced by the write
    /// transaction (SharedGroup::begin_read()), except that all subordinate
    /// accessors (Table, Row, Descriptor) will remain attached to the
    /// underlying objects. commit_and_continue_as_read() returns the version
    /// produced by the committed transaction.
    ///
    /// rollback_and_continue_as_read() is equivalent to rolling back the
    /// current write transaction (SharedGroup::rollback()) and initiating a new
    /// read transaction, which is bound to the snapshot, that the write
    /// transaction was based on (SharedGroup::begin_read()), except that all
    /// subordinate accessors (Table, Row, Descriptor) will remain attached to
    /// the underlying objects, unless they were attached to object that were
    /// added during the rolled back transaction.
    ///
    /// If advance_read(), promote_to_write(), commit_and_continue_as_read(), or
    /// rollback_and_continue_as_read() throws, the associated group accessor
    /// and all of its subordinate accessors are left in a state that may not be
    /// fully consistent. Only minimal consistency is guaranteed (see
    /// AccessorConsistencyLevels). In this case, the application is required to
    /// either destroy the SharedGroup object, forcing all associated accessors
    /// to become detached, or take some other equivalent action that involves a
    /// complete accessor detachment, such as terminating the transaction in
    /// progress. Until then it is an error, and unsafe if the application
    /// attempts to access any of those accessors.
    ///
    /// The application must use SharedGroup::end_read() if it wants to
    /// terminate the transaction after advance_read() or promote_to_write() has
    /// thrown an exception. Likewise, it must use SharedGroup::rollback() if it
    /// wants to terminate the transaction after commit_and_continue_as_read()
    /// or rollback_and_continue_as_read() has thrown an exception.
    ///
    /// \param observer An optional custom replication instruction handler. The
    /// application may pass such a handler to observe the sequence of
    /// modifications that advances (or rolls back) the state of the Realm.
    ///
    /// \throw SharedGroup::BadVersion Thrown by advance_read() if the specified
    /// version does not correspond to a bound (or tethered) snapshot.
    ///
    //@{

    static void advance_read(DB&, VersionID = VersionID());
    template <class O>
    static void advance_read(DB&, O&& observer, VersionID = VersionID());
    static void promote_to_write(DB&);
    template <class O>
    static void promote_to_write(DB&, O&& observer);
    static DB::version_type commit_and_continue_as_read(DB&);
    static void rollback_and_continue_as_read(DB&);
    template <class O>
    static void rollback_and_continue_as_read(DB&, O&& observer);

    //@}

    /// Returns the name of the specified data type. Examples:
    ///
    /// <pre>
    ///
    ///   type_Int          ->  "int"
    ///   type_Bool         ->  "bool"
    ///   type_Float        ->  "float"
    ///   ...
    ///
    /// </pre>
    static const char* get_data_type_name(DataType) noexcept;

    static DB::version_type get_version_of_latest_snapshot(DB&);
};


// Implementation:

inline Table* LangBindHelper::new_table()
{
    typedef _impl::TableFriend tf;
    Allocator& alloc = Allocator::get_default();
    size_t ref = tf::create_empty_table(alloc); // Throws
    ArrayParent* parent = nullptr;
    size_t ndx_in_parent = 0;
    Table* table = tf::create_accessor(alloc, ref, parent, ndx_in_parent); // Throws
    bind_table_ptr(table);
    return table;
}


inline Table* LangBindHelper::get_table(Group& group, TableKey key)
{
    typedef _impl::GroupFriend gf;
    Table* table = &gf::get_table(group, key); // Throws
    return table;
}

inline const Table* LangBindHelper::get_table(const Group& group, TableKey key)
{
    typedef _impl::GroupFriend gf;
    const Table* table = &gf::get_table(group, key); // Throws
    return table;
}

inline Table* LangBindHelper::get_table(Group& group, StringData name)
{
    typedef _impl::GroupFriend gf;
    Table* table = gf::get_table(group, name); // Throws
    return table;
}

inline const Table* LangBindHelper::get_table(const Group& group, StringData name)
{
    typedef _impl::GroupFriend gf;
    const Table* table = gf::get_table(group, name); // Throws
    return table;
}

inline Table* LangBindHelper::add_table(Group& group, StringData name, bool require_unique_name)
{
    typedef _impl::GroupFriend gf;
    Table* table = &gf::add_table(group, name, require_unique_name); // Throws
    return table;
}

inline Table* LangBindHelper::get_or_add_table(Group& group, StringData name, bool* was_added)
{
    typedef _impl::GroupFriend gf;
    Table* table = &gf::get_or_add_table(group, name, was_added); // Throws
    return table;
}

inline void LangBindHelper::unbind_table_ptr(const Table*) noexcept
{
}

inline void LangBindHelper::bind_table_ptr(const Table*) noexcept
{
}

inline void LangBindHelper::advance_read(DB& sg, VersionID version)
{
    using sgf = _impl::SharedGroupFriend;
    _impl::NullInstructionObserver* observer = nullptr;
    sgf::advance_read(sg, observer, version); // Throws
}

template <class O>
inline void LangBindHelper::advance_read(DB& sg, O&& observer, VersionID version)
{
    using sgf = _impl::SharedGroupFriend;
    sgf::advance_read(sg, &observer, version); // Throws
}

inline void LangBindHelper::promote_to_write(DB& sg)
{
    using sgf = _impl::SharedGroupFriend;
    _impl::NullInstructionObserver* observer = nullptr;
    sgf::promote_to_write(sg, observer); // Throws
}

template <class O>
inline void LangBindHelper::promote_to_write(DB& sg, O&& observer)
{
    using sgf = _impl::SharedGroupFriend;
    sgf::promote_to_write(sg, &observer); // Throws
}

inline DB::version_type LangBindHelper::commit_and_continue_as_read(DB& sg)
{
    using sgf = _impl::SharedGroupFriend;
    return sgf::commit_and_continue_as_read(sg); // Throws
}

inline void LangBindHelper::rollback_and_continue_as_read(DB& sg)
{
    using sgf = _impl::SharedGroupFriend;
    _impl::NullInstructionObserver* observer = nullptr;
    sgf::rollback_and_continue_as_read(sg, observer); // Throws
}

template <class O>
inline void LangBindHelper::rollback_and_continue_as_read(DB& sg, O&& observer)
{
    using sgf = _impl::SharedGroupFriend;
    sgf::rollback_and_continue_as_read(sg, &observer); // Throws
}

inline DB::version_type LangBindHelper::get_version_of_latest_snapshot(DB& sg)
{
    using sgf = _impl::SharedGroupFriend;
    return sgf::get_version_of_latest_snapshot(sg); // Throws
}

} // namespace realm

#endif // REALM_LANG_BIND_HELPER_HPP
