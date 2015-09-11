/*************************************************************************
 *
 * Realm CONFIDENTIAL
 * __________________
 *
 *  [2011] - [2012] Realm Inc
 *  All Rights Reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Realm Incorporated and its suppliers,
 * if any.  The intellectual and technical concepts contained
 * herein are proprietary to Realm Incorporated
 * and its suppliers and may be covered by U.S. and Foreign Patents,
 * patents in process, and are protected by trade secret or copyright law.
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from Realm Incorporated.
 *
 **************************************************************************/
#ifndef REALM_REALM_HPP
#define REALM_REALM_HPP

class Snapshot;
class Transaction;

class Realm {
public:
    enum DurabilityLevel {
        durability_Full,
        durability_MemOnly,
        durability_Async    ///< Not yet supported on windows.
    };

    struct Config {
        bool is_read_only = false;
        bool no_create = false;
        DurabilityLevel durability = durability_Full;
        bool compact_file_if_possible = false;
        bool allow_file_format_upgrade = true;
    }

    // Delete it.
    virtual ~Realm();

    // Attach a Realm to a specific file with selected policies
    void open(const std::string& file, Config& cfg, const char* encryption_key = 0);

    // Close the Realm, detaching it from any file.
    void close();

    bool is_attached();

    // Get a snapshot
    std::shared_ptr<Snapshot> get_newest_snapshot();

    std::shared_ptr<Transaction> build_new_snapshot();

    // Check if a newer snapshot has become available
    bool newer_snapshot_available();
};


std::unique_ptr<Realm> make_realm();

#endif
