/*************************************************************************
 *
 * REALM CONFIDENTIAL
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


class Realm {
public:
    enum DurabilityLevel {
        durability_Full,
        durability_MemOnly,
        durability_Async    ///< Not yet supported on windows.
    };

    // Create a Realm which isn't attached to any file
    Realm();

    // Delete it.
    ~Realm();

    // Attach a Realm to a specific file with selected policies
    void open(const std::string& file, bool no_create = false,
              DurabilityLevel = durability_Full,
              const char* encryption_key = 0,
              bool compact_file_if_possible = false,
              bool allow_file_format_upgrade = true);

    // Close the Realm, detaching it from any Realm object.
    void close();

    bool is_attached();

private:
    class impl_Realm;
    std::unique_ptr<impl_Realm> m_impl;
};


#endif
