#include <realm/sync/noinst/server/server_file_access_cache.hpp>

using namespace realm;
using namespace _impl;


void ServerFileAccessCache::proper_close_all()
{
    while (m_first_open_file)
        m_first_open_file->proper_close(); // Throws
}


void ServerFileAccessCache::access(Slot& slot)
{
    poll_core_metrics();
    if (slot.is_open()) {
        m_logger.trace("Using already open Realm file: %1", slot.realm_path); // Throws

        // Move to front
        REALM_ASSERT(m_first_open_file);
        if (&slot != m_first_open_file) {
            remove(slot);
            insert(slot); // At front
            m_first_open_file = &slot;
        }
        return;
    }

    // Close least recently accessed Realm file
    if (m_num_open_files == m_max_open_files) {
        REALM_ASSERT(m_first_open_file);
        Slot& least_recently_accessed = *m_first_open_file->m_prev_open_file;
        least_recently_accessed.proper_close(); // Throws
    }

    slot.open(); // Throws
}

void ServerFileAccessCache::poll_core_metrics()
{
#if REALM_METRICS
    if (m_metrics && m_first_open_file) {
        auto slot = m_first_open_file;
        do {
            if (slot->is_open() && slot->m_file) {
                std::shared_ptr<realm::metrics::Metrics> metrics = slot->m_file->shared_group->get_metrics();
                if (metrics) {
                    constexpr const char* query_metrics_prefix = "core.query";

                    std::string encoded_path;
                    auto get_encoded_path = [&encoded_path, &slot]() {
                        if (encoded_path.empty()) {
                            encoded_path = sync::Metrics::percent_encode(slot->virt_path);
                        }
                        return encoded_path;
                    };
                    if (metrics->num_query_metrics() > 0) {
                        // if users opt out of core query metrics, don't emit them, but still consume them from core.
                        auto query_info_list = metrics->take_queries();
                        if (!m_metrics->will_exclude(sync::MetricsOptions::Core_Query)) {
                            REALM_ASSERT(query_info_list);
                            for (auto& query_info : *query_info_list) {
                                std::stringstream out;
                                std::string desc = sync::Metrics::percent_encode(query_info.get_table_name() + ";") +
                                                   sync::Metrics::percent_encode(query_info.get_description());
                                double seconds = double(query_info.get_query_time_nanoseconds()) / 1e9;
                                out << query_metrics_prefix << ",path=" << get_encoded_path()
                                    << ",description=" << desc;
                                std::string key = out.str();
                                m_metrics->timing(key.c_str(), seconds);
                            }
                        }
                    }
                    if (metrics->num_transaction_metrics() > 0) {
                        // if users opt out of core transaction metrics, don't emit them, but still consume them from
                        // core.
                        auto transaction_info_list = metrics->take_transactions();
                        if (!m_metrics->will_exclude(sync::MetricsOptions::Core_Transaction)) {
                            REALM_ASSERT(transaction_info_list);
                            const char* transaction_metrics_prefix = "core.transaction";

                            bool write_transaction_metrics_enabled =
                                !m_metrics->will_exclude(sync::MetricsOptions::Core_Transaction_Write);
                            bool read_transaction_metrics_enabled =
                                !m_metrics->will_exclude(sync::MetricsOptions::Core_Transaction_Read);

                            std::stringstream stream;
                            for (auto& transaction_info : *transaction_info_list) {
                                const realm::metrics::TransactionInfo::TransactionType transaction_type =
                                    transaction_info.get_transaction_type();
                                if ((!write_transaction_metrics_enabled &&
                                     transaction_type ==
                                         realm::metrics::TransactionInfo::TransactionType::write_transaction) ||
                                    (!read_transaction_metrics_enabled &&
                                     transaction_type ==
                                         realm::metrics::TransactionInfo::TransactionType::read_transaction)) {
                                    continue; // user opts out
                                }
                                std::string transaction_type_string = "read";
                                get_encoded_path(); // ensure `encoded_path` is populated
                                if (transaction_type ==
                                    realm::metrics::TransactionInfo::TransactionType::write_transaction) {

                                    transaction_type_string = "write";
                                    stream.str({});
                                    stream << transaction_metrics_prefix << ".write.time,path=" << encoded_path;
                                    m_metrics->timing(stream.str().c_str(),
                                                      double(transaction_info.get_write_time_nanoseconds()) / 1e9);

                                    stream.str({});
                                    stream << transaction_metrics_prefix << ".fsync.time,path=" << encoded_path;
                                    m_metrics->timing(stream.str().c_str(),
                                                      double(transaction_info.get_fsync_time_nanoseconds()) / 1e9);

                                    stream.str({});
                                    stream << transaction_metrics_prefix << ".disk_size,path=" << encoded_path;
                                    m_metrics->gauge(stream.str().c_str(), double(transaction_info.get_disk_size()));

                                    stream.str({});
                                    stream << transaction_metrics_prefix << ".free_space,path=" << encoded_path;
                                    m_metrics->gauge(stream.str().c_str(), double(transaction_info.get_free_space()));

                                    stream.str({});
                                    stream << transaction_metrics_prefix << ".objects.count,path=" << encoded_path;
                                    m_metrics->gauge(stream.str().c_str(),
                                                     double(transaction_info.get_total_objects()));

                                    stream.str({});
                                    stream << transaction_metrics_prefix << ".versions.count,path=" << encoded_path;
                                    m_metrics->gauge(stream.str().c_str(),
                                                     double(transaction_info.get_num_available_versions()));
                                }
                                stream.str({});
                                stream << transaction_metrics_prefix
                                       << ".total.time,type="
                                          ""
                                       << transaction_type_string << ",path=" << encoded_path;
                                m_metrics->timing(stream.str().c_str(),
                                                  double(transaction_info.get_transaction_time_nanoseconds()) / 1e9);
                            }
                        }
                    }
                }
            }

            slot = slot->m_next_open_file;
        } while (slot != m_first_open_file);
    }
#endif
}

void ServerFileAccessCache::Slot::proper_close()
{
    if (is_open()) {
        m_cache.m_logger.detail("Closing Realm file: %1", realm_path); // Throws
        m_cache.poll_core_metrics();                                   // Throws
        do_close();
    }
}


void ServerFileAccessCache::Slot::open()
{
    REALM_ASSERT(!is_open());

    m_cache.m_logger.detail("Opening Realm file: %1", realm_path); // Throws

    m_file.reset(new File{*this}); // Throws

    m_cache.insert(*this);
    m_cache.m_first_open_file = this;
    ++m_cache.m_num_open_files;
}
