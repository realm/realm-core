#include <realm/object-store/c_api/types.hpp>
#include <realm/object-store/c_api/util.hpp>

namespace realm::c_api {
namespace {
struct ObjectNotificationsCallback {
    UserdataPtr m_userdata;
    realm_on_object_change_func_t m_on_change = nullptr;
    realm_callback_error_func_t m_on_error = nullptr;

    ObjectNotificationsCallback() = default;
    ObjectNotificationsCallback(ObjectNotificationsCallback&& other)
        : m_userdata(std::exchange(other.m_userdata, nullptr))
        , m_on_change(std::exchange(other.m_on_change, nullptr))
        , m_on_error(std::exchange(other.m_on_error, nullptr))
    {
    }

    void operator()(const CollectionChangeSet& changes, std::exception_ptr error)
    {
        // Note: As of Core 6, notification errors cannot happen.

        // LCOV_EXCL_START
        if (error) {
            if (m_on_error) {
                realm_async_error_t err{std::move(error)};
                m_on_error(m_userdata.get(), &err);
            }
        }
        // LCOV_EXCL_STOP
        else if (m_on_change) {
            realm_object_changes_t c{changes};
            m_on_change(m_userdata.get(), &c);
        }
    }
};

struct CollectionNotificationsCallback {
    UserdataPtr m_userdata;
    realm_on_collection_change_func_t m_on_change = nullptr;
    realm_callback_error_func_t m_on_error = nullptr;

    CollectionNotificationsCallback() = default;
    CollectionNotificationsCallback(CollectionNotificationsCallback&& other)
        : m_userdata(std::exchange(other.m_userdata, nullptr))
        , m_on_change(std::exchange(other.m_on_change, nullptr))
        , m_on_error(std::exchange(other.m_on_error, nullptr))
    {
    }

    void operator()(const CollectionChangeSet& changes, std::exception_ptr error)
    {
        // Note: As of Core 6, notification errors cannot happen.

        // LCOV_EXCL_START
        if (error) {
            if (m_on_error) {
                realm_async_error_t err{std::move(error)};
                m_on_error(m_userdata.get(), &err);
            }
        }
        // LCOV_EXCL_STOP
        else if (m_on_change) {
            realm_collection_changes_t c{changes};
            m_on_change(m_userdata.get(), &c);
        }
    }
};

KeyPathArray build_key_path_array(realm_key_path_array_t* key_path_array)
{
    KeyPathArray ret;
    if (key_path_array) {
        for (size_t i = 0; i < key_path_array->nb_elements; i++) {
            realm_key_path_t* key_path = key_path_array->paths + i;
            ret.emplace_back();
            KeyPath& kp = ret.back();
            for (size_t j = 0; j < key_path->nb_elements; j++) {
                realm_key_path_elem_t* path_elem = key_path->path_elements + j;
                kp.emplace_back(TableKey(path_elem->object), ColKey(path_elem->property));
            }
        }
    }
    return ret;
}

} // namespace

RLM_API realm_notification_token_t* realm_object_add_notification_callback(
    realm_object_t* obj, void* userdata, realm_free_userdata_func_t free, realm_key_path_array_t* key_path_array,
    realm_on_object_change_func_t on_change, realm_callback_error_func_t on_error, realm_scheduler_t*)
{
    return wrap_err([&]() {
        ObjectNotificationsCallback cb;
        cb.m_userdata = UserdataPtr{userdata, free};
        cb.m_on_change = on_change;
        cb.m_on_error = on_error;
        auto token = obj->add_notification_callback(std::move(cb), build_key_path_array(key_path_array));
        return new realm_notification_token_t{std::move(token)};
    });
}

RLM_API bool realm_object_changes_is_deleted(const realm_object_changes_t* changes)
{
    return !changes->deletions.empty();
}

RLM_API size_t realm_object_changes_get_num_modified_properties(const realm_object_changes_t* changes)
{
    return changes->columns.size();
}

RLM_API size_t realm_object_changes_get_modified_properties(const realm_object_changes_t* changes,
                                                            realm_property_key_t* out_properties, size_t max)
{
    if (!out_properties)
        return changes->columns.size();

    size_t i = 0;
    for (const auto& [col_key_val, index_set] : changes->columns) {
        if (i >= max) {
            break;
        }
        out_properties[i] = col_key_val;
        ++i;
    }
    return i;
}

RLM_API realm_notification_token_t* realm_list_add_notification_callback(
    realm_list_t* list, void* userdata, realm_free_userdata_func_t free, realm_key_path_array_t* key_path_array,
    realm_on_collection_change_func_t on_change, realm_callback_error_func_t on_error, realm_scheduler_t*)
{
    return wrap_err([&]() {
        CollectionNotificationsCallback cb;
        cb.m_userdata = UserdataPtr{userdata, free};
        cb.m_on_change = on_change;
        cb.m_on_error = on_error;
        auto token = list->add_notification_callback(std::move(cb), build_key_path_array(key_path_array));
        return new realm_notification_token_t{std::move(token)};
    });
}

RLM_API realm_notification_token_t* realm_set_add_notification_callback(
    realm_set_t* set, void* userdata, realm_free_userdata_func_t free, realm_key_path_array_t* key_path_array,
    realm_on_collection_change_func_t on_change, realm_callback_error_func_t on_error, realm_scheduler_t*)
{
    return wrap_err([&]() {
        CollectionNotificationsCallback cb;
        cb.m_userdata = UserdataPtr{userdata, free};
        cb.m_on_change = on_change;
        cb.m_on_error = on_error;
        auto token = set->add_notification_callback(std::move(cb), build_key_path_array(key_path_array));
        return new realm_notification_token_t{std::move(token)};
    });
}

RLM_API realm_notification_token_t* realm_dictionary_add_notification_callback(
    realm_dictionary_t* dict, void* userdata, realm_free_userdata_func_t free, realm_key_path_array_t* key_path_array,
    realm_on_collection_change_func_t on_change, realm_callback_error_func_t on_error, realm_scheduler_t*)
{
    return wrap_err([&]() {
        CollectionNotificationsCallback cb;
        cb.m_userdata = UserdataPtr{userdata, free};
        cb.m_on_change = on_change;
        cb.m_on_error = on_error;
        auto token = dict->add_notification_callback(std::move(cb), build_key_path_array(key_path_array));
        return new realm_notification_token_t{std::move(token)};
    });
}

RLM_API realm_notification_token_t* realm_results_add_notification_callback(
    realm_results_t* results, void* userdata, realm_free_userdata_func_t free, realm_key_path_array_t* key_path_array,
    realm_on_collection_change_func_t on_change, realm_callback_error_func_t on_error, realm_scheduler_t*)
{
    return wrap_err([&]() {
        CollectionNotificationsCallback cb;
        cb.m_userdata = UserdataPtr{userdata, free};
        cb.m_on_change = on_change;
        cb.m_on_error = on_error;
        auto token = results->add_notification_callback(std::move(cb), build_key_path_array(key_path_array));
        return new realm_notification_token_t{std::move(token)};
    });
}

RLM_API void realm_collection_changes_get_num_ranges(const realm_collection_changes_t* changes,
                                                     size_t* out_num_deletion_ranges,
                                                     size_t* out_num_insertion_ranges,
                                                     size_t* out_num_modification_ranges, size_t* out_num_moves)
{
    // FIXME: `std::distance()` has O(n) performance here, which seems ridiculous.

    if (out_num_deletion_ranges)
        *out_num_deletion_ranges = std::distance(changes->deletions.begin(), changes->deletions.end());
    if (out_num_insertion_ranges)
        *out_num_insertion_ranges = std::distance(changes->insertions.begin(), changes->insertions.end());
    if (out_num_modification_ranges)
        *out_num_modification_ranges = std::distance(changes->modifications.begin(), changes->modifications.end());
    if (out_num_moves)
        *out_num_moves = changes->moves.size();
}

RLM_API void realm_collection_changes_get_num_changes(const realm_collection_changes_t* changes,
                                                      size_t* out_num_deletions, size_t* out_num_insertions,
                                                      size_t* out_num_modifications, size_t* out_num_moves)
{
    // FIXME: This has O(n) performance, which seems ridiculous.

    if (out_num_deletions)
        *out_num_deletions = changes->deletions.count();
    if (out_num_insertions)
        *out_num_insertions = changes->insertions.count();
    if (out_num_modifications)
        *out_num_modifications = changes->modifications.count();
    if (out_num_moves)
        *out_num_moves = changes->moves.size();
}

static inline void copy_index_ranges(const IndexSet& index_set, realm_index_range_t* out_ranges, size_t max)
{
    size_t i = 0;
    for (auto [from, to] : index_set) {
        if (i >= max)
            return;
        out_ranges[i++] = realm_index_range_t{from, to};
    }
}

RLM_API void realm_collection_changes_get_ranges(
    const realm_collection_changes_t* changes, realm_index_range_t* out_deletion_ranges, size_t max_deletion_ranges,
    realm_index_range_t* out_insertion_ranges, size_t max_insertion_ranges,
    realm_index_range_t* out_modification_ranges, size_t max_modification_ranges,
    realm_index_range_t* out_modification_ranges_after, size_t max_modification_ranges_after,
    realm_collection_move_t* out_moves, size_t max_moves)
{
    if (out_deletion_ranges) {
        copy_index_ranges(changes->deletions, out_deletion_ranges, max_deletion_ranges);
    }
    if (out_insertion_ranges) {
        copy_index_ranges(changes->insertions, out_insertion_ranges, max_insertion_ranges);
    }
    if (out_modification_ranges) {
        copy_index_ranges(changes->modifications, out_modification_ranges, max_modification_ranges);
    }
    if (out_modification_ranges_after) {
        copy_index_ranges(changes->modifications_new, out_modification_ranges_after, max_modification_ranges_after);
    }
    if (out_moves) {
        size_t i = 0;
        for (auto [from, to] : changes->moves) {
            if (i >= max_moves)
                break;
            out_moves[i] = realm_collection_move_t{from, to};
            ++i;
        }
    }
}

static inline void copy_indices(const IndexSet& index_set, size_t* out_indices, size_t max)
{
    size_t i = 0;
    for (auto index : index_set.as_indexes()) {
        if (i >= max)
            return;
        out_indices[i] = index;
        ++i;
    }
}

RLM_API void realm_collection_changes_get_changes(const realm_collection_changes_t* changes, size_t* out_deletions,
                                                  size_t max_deletions, size_t* out_insertions, size_t max_insertions,
                                                  size_t* out_modifications, size_t max_modifications,
                                                  size_t* out_modifications_after, size_t max_modifications_after,
                                                  realm_collection_move_t* out_moves, size_t max_moves)
{
    if (out_deletions) {
        copy_indices(changes->deletions, out_deletions, max_deletions);
    }
    if (out_insertions) {
        copy_indices(changes->insertions, out_insertions, max_insertions);
    }
    if (out_modifications) {
        copy_indices(changes->modifications, out_modifications, max_modifications);
    }
    if (out_modifications_after) {
        copy_indices(changes->modifications_new, out_modifications_after, max_modifications_after);
    }
    if (out_moves) {
        size_t i = 0;
        for (auto [from, to] : changes->moves) {
            if (i >= max_moves)
                break;
            out_moves[i] = realm_collection_move_t{from, to};
            ++i;
        }
    }
}

} // namespace realm::c_api
