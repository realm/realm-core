////////////////////////////////////////////////////////////////////////////
//
// Copyright 2015 Realm Inc.
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

#ifndef REALM_RESULTS_NOTIFIER_HPP
#define REALM_RESULTS_NOTIFIER_HPP

#include "collection_notifier.hpp"
#include "results.hpp"

#include <realm/db.hpp>

namespace realm {
namespace _impl {
class ResultsNotifier : public CollectionNotifier {
public:
    ResultsNotifier(Results& target);

    bool get_tableview(TableView& out);

private:
    std::unique_ptr<Query> m_query;
    DescriptorOrdering m_descriptor_ordering;
    bool m_target_is_in_table_order;

    // The TableView resulting from running the query. Will be detached unless
    // the query was (re)run since the last time the handover object was created
    TableView m_run_tv;
    std::unique_ptr<TableView> m_handover_tv;
    std::unique_ptr<TableView> m_delivered_tv;
    VersionID m_delivered_tv_version;

    // The table version from the last time the query was run. Used to avoid
    // rerunning the query when there's no chance of it changing.
    TableVersions m_last_seen_version;

    // The rows from the previous run of the query, for calculating diffs
    std::vector<int64_t> m_previous_rows;

    // The changeset calculated during run() and delivered in do_prepare_handover()
    CollectionChangeBuilder m_changes;
    TransactionChangeInfo* m_info = nullptr;
    bool m_results_were_used = true;

    bool need_to_run();
    void calculate_changes();
    void deliver(Transaction&) override;

    void run() override;
    void do_prepare_handover(Transaction&) override;
    bool do_add_required_change_info(TransactionChangeInfo& info) override;
    bool prepare_to_deliver() override;

    void release_data() noexcept override;
    void do_attach_to(Transaction& sg) override;
};

} // namespace _impl
} // namespace realm

#endif /* REALM_RESULTS_NOTIFIER_HPP */
