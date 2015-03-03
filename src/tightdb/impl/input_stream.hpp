/*************************************************************************
 *
 * TIGHTDB CONFIDENTIAL
 * __________________
 *
 *  [2011] - [2015] TightDB Inc
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

#ifndef TIGHTDB_IMPL_INPUT_STREAM_HPP
#define TIGHTDB_IMPL_INPUT_STREAM_HPP

namespace tightdb {
namespace _impl {

class InputStream {
public:
    /// \return the number of accessible bytes.
    /// A value of zero indicates end-of-input.
    /// For non-zero return value, \a begin and \a end are
    /// updated to reflect the start and limit of a
    /// contiguous memory chunk.
    virtual size_t next_block(const char*& begin, const char*& end) = 0;

    virtual ~InputStream() {}
};

}
}

#endif // TIGHTDB_IMPL_INPUT_STREAM_HPP
