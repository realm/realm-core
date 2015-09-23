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
#ifndef REALM_COLUMN_DATE_HPP
#define REALM_COLUMN_DATE_HPP

#include <vector>

#include <realm/column.hpp>

namespace realm {


    class DateColumn : public IntegerColumn {
    public:
        DateColumn(Allocator& alloc, ref_type ref_types, ref_type ref_dates, ref_type ref_extra) :
            m_types(alloc, ref_types),
            m_dates(alloc, ref_dates),
            m_dates_extra(alloc, ref_extra)
        {

        }

        // Should return upcoming UniversalDate class
        int64_t get(size_t index) { 
            return m_dates.get(0);
        }

        void add(int64_t v) {
            m_types.add(1);
            m_dates.add(v);
        }


    private:

        // Three payload columns. Note that `this` array inherits from IntegerColumn and must contain 3 RefTypes to
        // below payload columns. Todo, make create_column() that calls DateColumn() with these refs. See binary column for example

        IntegerColumn m_types;       // Flag telling the format of m_dates (C#, Java, ObjC, ...)
        IntegerColumn m_dates;       // Dates. Cast to double if from Swift/ObjC, else integer
        IntegerColumn m_dates_extra; // Used if one or more dates had been given as Java8's Instant, else empty
    };

}



#endif