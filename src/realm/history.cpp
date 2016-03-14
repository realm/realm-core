#include <realm/impl/continuous_transactions_history.hpp>
#include <realm/binary_data.hpp>
#include <realm/group_shared.hpp>
#include <realm/replication.hpp>
#include <realm/history.hpp>

using namespace realm;


namespace {

class InRealmHistoryImpl:
        public TrivialReplication,
        private _impl::InRealmHistory {
public:
    using version_type = TrivialReplication::version_type;

    InRealmHistoryImpl(std::string realm_path):
        TrivialReplication(realm_path)
    {
    }

    void initialize(SharedGroup& sg) override
    {
        TrivialReplication::initialize(sg); // Throws
        using sgf = _impl::SharedGroupFriend;
        _impl::InRealmHistory::initialize(sgf::get_group(sg)); // Throws
    }

    void initiate_session(version_type) override
    {
        // No-op
    }

    void terminate_session() noexcept override
    {
        // No-op
    }

    version_type prepare_changeset(const char* data, size_t size,
                                   version_type orig_version) override
    {
        if (!is_history_updated())
            update_from_parent(orig_version); // Throws
        BinaryData changeset(data, size);
        version_type new_version = add_changeset(changeset); // Throws
        return new_version;
    }

    void finalize_changeset() noexcept override
    {
        // Since the history is in the Realm, the added changeset is
        // automatically finalized as part of the commit operation.
    }

    HistoryType get_history_type() const noexcept override
    {
        return hist_InRealm;
    }

    _impl::History* get_history() override
    {
        return this;
    }

    BinaryData get_uncommitted_changes() noexcept override
    {
        return TrivialReplication::get_uncommitted_changes();
    }
};

} // unnamed namespace


namespace realm {

std::unique_ptr<Replication> make_in_realm_history(const std::string& realm_path)
{
    return std::unique_ptr<InRealmHistoryImpl>(new InRealmHistoryImpl(realm_path)); // Throws
}

} // namespace realm
