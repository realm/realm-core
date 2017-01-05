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

#include "admin_realm.hpp"
#include "results.hpp"
#include "shared_realm.hpp"
#include "sync/sync_config.hpp"

#include <realm/version_id.hpp>

#include <condition_variable>
#include <mutex>
#include <queue>
#include <unordered_map>

namespace realm {
namespace util {
    template<typename> class EventLoopSignal;
}

class SyncUser;
class SyncSession;

/// Used to listen for changes across all, or a subset of all Realms on a
/// particular sync server.
class GlobalNotifier : public std::enable_shared_from_this<GlobalNotifier>  {
public:
    class Callback;

    using ChangesetTransformer = sync::SyncHistory::ChangesetCooker;
    static std::shared_ptr<GlobalNotifier> shared_notifier(std::unique_ptr<Callback> callback, std::string local_root_dir,
                                                           std::string server_base_url, std::shared_ptr<SyncUser> user,
                                                           std::shared_ptr<ChangesetTransformer> transformer = nullptr);
    ~GlobalNotifier();

    // Start listening. No callbacks will be called until this function is
    // called. Must be called on the thread which notifications should be
    // delivered on.
    void start();

    // Stop calling callbacks until resume() is called. Must be called on the
    // notification thread.
    void pause();
    // Resume calling callbacks after a call to pause(). Must be called on the
    // notification thread.
    void resume();

    // Returns true if there are any Realms with calculated changes waiting to
    // be delivered.
    // Thread-safe, but probably only makes sense on the notification thread.
    bool has_pending();

    // Returns the target callback
    Callback *target() { return m_target.get(); }

    // RealmInfo is combination of realm_id and realm_path
    using RealmInfo = std::pair<std::string, std::string>;

    class ChangeNotification {
    public:
        RealmInfo realm_info;

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

        // The virtual server path of the Realm which changed.
        std::string get_path() const;

        // The actual changes made, keyed on object name.
        // This will be empty if the Realm already existed before the
        // GlobalNotifier was started.
        std::unordered_map<std::string, CollectionChangeSet> const& get_changes() const noexcept { return m_changes; }

        ChangeNotification(ChangeNotification&&) = default;
        ChangeNotification& operator=(ChangeNotification&&) = default;
        ChangeNotification(ChangeNotification const&) = delete;
        ChangeNotification& operator=(ChangeNotification const&) = delete;

    private:
        VersionID m_old_version;
        VersionID m_new_version;
        SharedRealm m_realm;
        std::unordered_map<std::string, CollectionChangeSet> m_changes;

        ChangeNotification(GlobalNotifier::RealmInfo, VersionID old_version, VersionID new_version, 
                           SharedRealm, std::unordered_map<std::string, CollectionChangeSet>);
        ChangeNotification() = default;

        friend class GlobalNotifier;
    };

    Realm::Config get_config(std::string realm_path);

private:
    GlobalNotifier(std::unique_ptr<Callback>, std::string local_root_dir,
                   std::string server_base_url, std::shared_ptr<SyncUser> user,
                   std::shared_ptr<ChangesetTransformer> transformer);

    AdminRealmListener m_admin;
    const std::unique_ptr<Callback> m_target;    
    const std::string m_server_base_url;
    std::shared_ptr<SyncUser> m_user;
    std::string m_regular_realms_dir;

    std::unordered_map<std::string, std::shared_ptr<_impl::RealmCoordinator>> m_listen_entries;
    std::unordered_map<std::string, std::string> m_realm_ids;

    std::shared_ptr<ChangesetTransformer> m_transformer = nullptr;

    std::mutex m_work_queue_mutex;
    std::condition_variable m_work_queue_cv;
    struct RealmToCalculate {
        RealmInfo info;
        std::shared_ptr<Realm> realm;
        VersionID target_version;
    };
    std::queue<RealmToCalculate> m_work_queue;
    std::thread m_work_thread;
    bool m_shutdown = false;

    std::mutex m_deliver_queue_mutex;
    std::queue<ChangeNotification> m_pending_deliveries;

    bool m_waiting = false;

    void register_realms(std::vector<RealmInfo>, bool all);
    void register_realm(RealmInfo info);

    void on_change();
    void calculate();

    struct SignalCallback {
        std::weak_ptr<GlobalNotifier> notifier;
        void (GlobalNotifier::*method)();
        void operator()()
        {
            if (auto alive = notifier.lock())
                (alive.get()->*method)();
        }
    };

    std::shared_ptr<util::EventLoopSignal<SignalCallback>> m_signal;
};

class GlobalNotifier::Callback {
public:
    virtual ~Callback();

    /// Called to determine whether the application wants to listen for changes
    /// to a particular Realm.
    ///
    /// The Realm name that is passed to the callback is hierarchical and takes
    /// the form of an absolute path (separated by forward slashes). This is a
    /// *virtual path*, i.e, it is not necesarily the file system path of the
    /// Realm on the server.
    ///
    /// \param name The name (virtual path) by which the server knows that
    /// Realm.
    virtual std::vector<bool> available(std::vector<GlobalNotifier::RealmInfo> realms,
                                        std::vector<bool> new_realms,
                                        bool all) = 0;

    /// Called when a Realm  has changed due to a transaction performed on behalf
    /// of the synchronization mechanism. This function is not called as a
    /// result of changing the Realm locally.
    ///
    /// This will also be called once for each Realm which already exists locally
    /// on disk when the notifier is started, even if there are no changes.
    ///
    /// \param new_realm The Realm which changed, with the changes applied.
    /// `modifications_new` and `insertions` within the change sets are indices
    /// in this Realm.
    ///
    /// \param changes The changes for each object type which changed. Will be
    /// empty only if this is a notification of a pre-existing Realm. Sync
    /// transactions which do not have any data changes do not produce a
    /// notification at all.
    virtual void realm_changed(GlobalNotifier::ChangeNotification changes) = 0;
};

} // namespace realm

#endif // REALM_OBJECT_STORE_GLOBAL_NOTIFIER_HPP
