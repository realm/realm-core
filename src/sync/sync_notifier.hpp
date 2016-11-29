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

#ifndef REALM_OS_SYNC_NOTIFIER_HPP
#define REALM_OS_SYNC_NOTIFIER_HPP

#include "sync_config.hpp"
#include "sync_session.hpp"
#include "sync_user.hpp"

#include <functional>
#include <memory>

namespace realm {

class SyncNotifier;

class SyncNotifierFactory {
public:
    virtual std::unique_ptr<SyncNotifier> make_notifier() = 0;
};

class SyncNotifier {
public:
	/// A user has successfully logged in.
	/// Arguments: user
	virtual void user_logged_in(std::shared_ptr<SyncUser>) const { }
	
	/// A user has successfully logged out.
	/// Arguments: user
	virtual void user_logged_out(std::shared_ptr<SyncUser>) const { }

	/// A session has successfully been bound to the Realm Object Server.
	/// Arguments: session
	virtual void session_bound_to_server(std::shared_ptr<SyncSession>) const { }

	/// A session has been destroyed.
	/// Arguments: the config for the session, the path to the session's Realm file
	virtual void session_destroyed(SyncConfig, const std::string&) const { }

	// TODO: enable later
	// /// A session might need to be reset.
	// /// Arguments: session, a closure which should be called if the session should be reset.
	// virtual void session_may_need_reset(std::shared_ptr<SyncSession>, std::function<void()>) const { }
	//
	// /// A session that needed to be reset was backed up.
	// /// Arguments: the name of the backup Realm file, TODO
	// virtual void session_reset_and_backed_up(const std::string&) const { }

	/// The metadata Realm was reset.
	/// Arguments: none
	virtual void metadata_realm_reset() const { }

	// TODO: enable later
	// /// A synced Realm was deleted.
	// /// Arguments: TODO
	// virtual void realm_deleted() const { }

	/// A user was deleted.
	/// Arguments: the identity of the deleted user
	virtual void user_deleted(const std::string&) const { }
};

}

#endif // REALM_OS_SYNC_NOTIFIER_HPP
