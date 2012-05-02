#ifndef TIGHTDB_DATE_H
#define TIGHTDB_DATE_H

#include <ctime>

namespace tightdb {


class Date {
public:
    Date(std::time_t d): m_date(d) {}
    std::time_t GetDate() const { return m_date; }

private:
    std::time_t m_date;
};


} // namespace tightdb

#endif // TIGHTDB_DATE_H
