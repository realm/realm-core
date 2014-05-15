/*************************************************************************
 *
 * TIGHTDB CONFIDENTIAL
 * __________________
 *
 *  [2011] - [2014] TightDB Inc
 *  All Rights Reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of TightDB Incorporated and its suppliers,
 * if any.  The intellectual and technical concepts contained
 * herein are proprietary to TightDB Incorporated
 * and its suppliers and may be covered by U.S. and Foreign Patents,
 * patents in process, and are protected by trade secret or copyright law.
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from TightDB Incorporated.
 *
 **************************************************************************/
#ifndef TIGHTDB_COMMIT_LOG_HPP
#define TIGHTDB_COMMIT_LOG_HPP

#include <exception>

#include <tightdb/replication.hpp>
#include <tightdb/binary_data.hpp>

namespace tightdb {


class WriteLogRegistryInterface {
public:
    // keep this in sync with shared group.
    typedef uint_fast64_t version_type;
  
    // Add a commit for a given version:
    // The registry takes ownership of the buffer data.
    virtual void add_commit(version_type version, char* data, std::size_t sz) = 0;
    
    // The registry retains commit buffers for as long as there is a
    // registered interest:
    
    // Register an interest in commits following version 'from'
    virtual void register_interest(version_type from) = 0;
    
    // Register that you are no longer interested in commits following
    // version 'from'.
    virtual void unregister_interest(version_type from) = 0;

    // Fill an array with commits for a version range - ]from..to]
    // The array is allocated by the caller and must have at least 'to' - 'from' entries.
    // The caller retains ownership of the array of commits, but not of the
    // buffers pointed to by each commit in the array. Ownership of the
    // buffers remains with the WriteLogRegistry.
    virtual void get_commit_entries(version_type from, version_type to, BinaryData*) = 0;

    // This also unregisters interest in the same version range.
    virtual void release_commit_entries(version_type from, version_type to) = 0;
};


// Obtain the WriteLogRegistry for a specific filepath. Create it, if it doesn't exist.
WriteLogRegistryInterface* getWriteLogs(std::string filepath);

// Create a writelog collector and associate it with a filepath. You'll need one writelog
// collector for each shared group. Commits from writelog collectors for a specific filepath 
// may later be obtained through the WriteLogRegistry associated with said filepath.
Replication* makeWriteLogCollector(std::string filepath, 
                                   WriteLogRegistryInterface* registry);


} // namespace tightdb

#endif // TIGHTDB_COMMIT_LOG_HPP
