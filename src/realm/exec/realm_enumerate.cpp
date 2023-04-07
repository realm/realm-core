/*
 * Useage: realm-enumerate [--key crypt_key] [--threshold 0.xx] <realm-file-name>
 * Changes string columns which pass the threshold of unique values to enumerated columns
 * and compacts the Realm in place.
 */

#include <realm/db.hpp>
#include <realm/history.hpp>
#include <realm/object-store/shared_realm.hpp>
#include <realm/sort_descriptor.hpp>
#include <realm/table_view.hpp>
#include <realm/transaction.hpp>

#include <fstream>
#include <iostream>

static void enumerate_strings(realm::SharedRealm realm, double threshold)
{
    auto& group = realm->read_group();
    auto table_keys = group.get_table_keys();
    for (auto table_key : table_keys) {
        realm::TableRef t = group.get_table(table_key);
        size_t table_size = t->size();
        realm::util::format(std::cout, "Begin table '%1' of size %2:\n", t->get_name(), table_size);
        if (table_size == 0)
            continue;
        bool found_str_col = false;
        auto do_convert = [&realm, &t](realm::ColKey col) {
            auto start = std::chrono::steady_clock::now();
            std::cout << "[converting]" << std::flush;
            realm->begin_transaction();
            t->enumerate_string_column(col);
            realm->commit_transaction();
            std::chrono::duration<double> diff = std::chrono::steady_clock::now() - start;
            std::cout << " (" << diff.count() << " seconds)" << std::endl;
        };
        t->for_each_public_column([&](realm::ColKey col_key) {
            if (col_key.get_type() == realm::col_type_String && !col_key.is_collection()) {
                found_str_col = true;
                realm::util::format(std::cout, "\tcolumn '%1' ", t->get_column_name(col_key));
                std::cout << std::flush;
                if (t->is_enumerated(col_key)) {
                    std::cout << "[already enumerated]" << std::endl;
                }
                else if (t->get_primary_key_column() == col_key) {
                    std::cout << "[pk - skipping]" << std::endl;
                }
                else if (threshold >= 100) {
                    do_convert(col_key);
                }
                else if (threshold < 100 && threshold > 0) {
                    std::unique_ptr<realm::DescriptorOrdering> distinct =
                        std::make_unique<realm::DescriptorOrdering>();
                    distinct->append_distinct(realm::DistinctDescriptor({{col_key}}));
                    size_t uniques = t->where().count(*distinct.get());
                    double utilization = uniques / double(table_size);
                    realm::util::format(std::cout, "contains %1 unique values (%2%%) ", uniques, utilization * 100.0);
                    std::cout << std::flush;
                    if (utilization <= threshold / 100) {
                        do_convert(col_key);
                    }
                    else {
                        std::cout << "[skipping due to threshold]" << std::endl;
                    }
                }
                else {
                    std::cout << "[skipping due to threshold]" << std::endl;
                }
            }
            return realm::IteratorControl::AdvanceToNext;
        });
        if (!found_str_col) {
            std::cout << "\tNo string columns found." << std::endl;
        }
    }
}

int main(int argc, const char* argv[])
{
    if (argc > 1) {
        try {
            const char* key_ptr = nullptr;
            char key[64];
            double threshold = 0; // by default don't convert, just compact
            for (int curr_arg = 1; curr_arg < argc; curr_arg++) {
                if (strcmp(argv[curr_arg], "--key") == 0) {
                    std::ifstream key_file(argv[curr_arg + 1]);
                    key_file.read(key, sizeof(key));
                    key_ptr = key;
                    curr_arg++;
                }
                else if (strcmp(argv[curr_arg], "--threshold") == 0) {
                    threshold = strtod(argv[curr_arg + 1], nullptr);
                    curr_arg++;
                }
                else {
                    realm::util::format(std::cout, "File name '%1' for threshold %2%%\n", argv[curr_arg], threshold);
                    auto start = std::chrono::steady_clock::now();
                    realm::Realm::Config config;
                    config.path = argv[curr_arg];
                    if (key_ptr) {
                        config.encryption_key.resize(64);
                        memcpy(&config.encryption_key[0], &key_ptr[0], 64);
                    }
                    realm::SharedRealm realm;
                    try {
                        realm = realm::Realm::get_shared_realm(config);
                    }
                    catch (const realm::RealmFileException& e) {
                        std::cout << "trying to open as a sync Realm\n" << std::endl;
                        config.force_sync_history = true;
                        realm = realm::Realm::get_shared_realm(config);
                    }
                    enumerate_strings(realm, threshold);
                    realm->compact();
                    std::chrono::duration<double> diff = std::chrono::steady_clock::now() - start;
                    std::cout << "Done in " << diff.count() << " seconds." << std::endl;
                    std::cout << std::endl;
                    return 0;
                }
            }
        }
        catch (const std::exception& e) {
            std::cout << e.what() << std::endl;
        }
    }
    else {
        std::cout << "Usage: realm-enumerate [--key crypt_key] [--threshold 0.xx] <realmfile>" << std::endl;
        std::cout << "The optional crypt_key arg is a filename which contains the 64 byte key." << std::endl;
        std::cout
            << "The optional threshold is a number between [0, 100] indicating the percentage of unique strings "
               "below which columns will be converted. At a value of 100, all columns will be converted. "
               "For value of 50 only columns which have 50% or fewer unique values will be converted."
               "If not set, the threshold default is 0 which just compacts the file without converting anything."
            << std::endl;
    }

    return 0;
}
