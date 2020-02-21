////////////////////////////////////////////////////////////////////////////
//
// Copyright 2016 Realm Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
////////////////////////////////////////////////////////////////////////////

#ifndef REALM_OBJECT_STORE_GLOBAL_NOTIFIER_HPP
#define REALM_OBJECT_STORE_GLOBAL_NOTIFIER_HPP

#include "object_changeset.hpp"
#include "impl/collection_notifier.hpp"
#include "shared_realm.hpp"
#include "sync/sync_config.hpp"

namespace realm {
struct GlobalKey;
class SyncUser;

/// Used to listen for changes across all, or a subset of all Realms on a
/// particular sync server.
class GlobalNotifier {
public:
    class Callback;
    GlobalNotifier(std::unique_ptr<Callback>, std::string local_root_dir,
                   SyncConfig sync_config_template);
    ~GlobalNotifier();

    // Returns the target callback
    Callback& target();

    void start();

    class ChangeNotification;
    util::Optional<ChangeNotification> next_changed_realm();

private:
    class Impl;
    std::shared_ptr<Impl> m_impl;

    GlobalNotifier(std::shared_ptr<Impl> impl) : m_impl(std::move(impl)) {}
};

class GlobalNotifier::ChangeNotification {
public:
    // The virtual server path of the Realm which changed.
    std::string realm_path;

    // The kind of change which happened to the Realm.
    enum class Type {
        Change,
        Delete
    } type;

    // The Realm which changed, at the version immediately before the changes
    // made. `modifications` and `deletions` within the change sets are indices
    // in this Realm.
    // This will be nullptr for the initial notification of a Realm which
    // already existed when the GlobalNotifier was created.
    SharedRealm get_old_realm() const;

    // The Realm which changed, at the first version including the changes made.
    // `modifications_new` and `insertions` within the change sets are indices
    // in this Realm.
    SharedRealm get_new_realm() const;

    // The actual changes made, keyed on object name.
    // This will be empty if the Realm already existed before the
    // GlobalNotifier was started.
    std::unordered_map<std::string, ObjectChangeSet> const& get_changes() const;

    ~ChangeNotification();

    std::string serialize() const;
    ChangeNotification(std::string const&);

    ChangeNotification(ChangeNotification&&) = default;
    ChangeNotification& operator=(ChangeNotification&&) = default;
    ChangeNotification(ChangeNotification const&) = delete;
    ChangeNotification& operator=(ChangeNotification const&) = delete;

private:
    ChangeNotification(std::shared_ptr<GlobalNotifier::Impl> notifier,
                       std::string virtual_path,
                       GlobalKey realm_id,
                       Realm::Config config,
                       VersionID old_version, VersionID new_version);
    ChangeNotification(std::shared_ptr<GlobalNotifier::Impl> notifier,
                       std::string virtual_path,
                       GlobalKey realm_id);
    GlobalKey m_realm_id;
    Realm::Config m_config;
    VersionID m_old_version;
    VersionID m_new_version;
    std::shared_ptr<GlobalNotifier::Impl> m_notifier;
    mutable std::shared_ptr<Realm> m_old_realm;
    mutable std::shared_ptr<Realm> m_new_realm;
    mutable std::unordered_map<std::string, ObjectChangeSet> m_changes;
    mutable bool m_have_calculated_changes = false;

    ChangeNotification() = default;

    friend class GlobalNotifier;
};

class GlobalNotifier::Callback {
public:
    virtual ~Callback();

    /// Called when the initial download of the admin realm is complete and observation is beginning
    virtual void download_complete() = 0;

    /// Called when any error occurs within the global notifier
    virtual void error(std::exception_ptr) = 0;

    /// Called to determine whether the application wants to listen for changes
    /// to a particular Realm.
    ///
    /// The Realm name that is passed to the callback is hierarchical and takes
    /// the form of an absolute path (separated by forward slashes). This is a
    /// *virtual path*, i.e, it is not necesarily the file system path of the
    /// Realm on the server.
    ///
    /// If this function returns false, the global notifier will not observe
    /// the Realm.
    ///
    /// \param id A unique identifier for the Realm which will not be reused
    ///           even if multiple Realms are created for a single virtual path.
    /// \param name The name (virtual path) by which the server knows that
    /// Realm.
    virtual bool realm_available(StringData id, StringData virtual_path) = 0;

    /// Called when a new version is available in an observed Realm.
    virtual void realm_changed(GlobalNotifier*) = 0;
};

} // namespace realm

#endif // REALM_OBJECT_STORE_GLOBAL_NOTIFIER_HPP
