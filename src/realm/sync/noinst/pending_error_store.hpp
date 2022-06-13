/*************************************************************************
 *
 * Copyright 2022 Realm, Inc.
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

#pragma once

#include "realm/db.hpp"
#include "realm/sync/protocol.hpp"
#include "realm/util/logger.hpp"
namespace realm::sync {

class PendingErrorStore {
public:
    explicit PendingErrorStore(DBRef db, util::Logger* logger);

    PendingErrorStore(const PendingErrorStore&) = delete;
    PendingErrorStore& operator=(const PendingErrorStore&) = delete;

    std::vector<ProtocolErrorInfo> peek_pending_errors(const TransactionRef& tr,
                                                       sync::version_type before_server_version);
    void remove_pending_errors(sync::version_type before_server_version);

    void add_pending_error(const ProtocolErrorInfo& error_info);

private:
    DBRef m_db;
    util::Logger* m_logger;

    TableKey m_errors_table;
    TableKey m_rejected_updates_table;

    ColKey m_pending_until_server_version;
    ColKey m_error_code;
    ColKey m_error_message;
    ColKey m_log_url;
    ColKey m_recovery_mode_disabled;
    ColKey m_try_again;
    ColKey m_should_client_reset;
    ColKey m_rejected_updates;

    ColKey m_max_resumption_delay_interval;
    ColKey m_resumption_delay_interval;
    ColKey m_resumption_delay_backoff_multiplier;

    ColKey m_rejected_update_reason;
    ColKey m_rejected_update_pk;
    ColKey m_rejected_update_table;
};

} // namespace realm::sync
