/*************************************************************************
 *
 * TIGHTDB CONFIDENTIAL
 * __________________
 *
 *  [2011] - [2012] TightDB Inc
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
#ifndef TIGHTDB_TEST_UTIL_BENCHMARK_RESULTS_HPP
#define TIGHTDB_TEST_UTIL_BENCHMARK_RESULTS_HPP

#include <vector>
#include <map>
#include <string>

namespace tightdb {
namespace test_util {


class BenchmarkResults {
public:
    BenchmarkResults(int max_lead_text_width, const char* results_file_stem = "results");
    ~BenchmarkResults();

    enum ChangeType {
        change_Percent,
        change_DropFactor,
        change_RiseFactor
    };

    void submit(double elapsed_seconds, const char* ident, const char* lead_text,
                ChangeType = change_Percent);

private:
    int m_max_lead_text_width;
    const char* const m_results_file_stem;

    struct Result {
        Result(double elapsed_seconds, const char* ident):
            m_elapsed_seconds(elapsed_seconds), m_ident(ident) {}
        double m_elapsed_seconds;
        const char* m_ident;
    };
    typedef std::vector<Result> Results;
    Results m_results;
    typedef std::map<std::string, double> BaselineResults;
    BaselineResults m_baseline_results;

    void try_load_baseline_results();
    void save_results();
};




// Implementation:

inline BenchmarkResults::BenchmarkResults(int max_lead_text_width, const char* results_file_stem):
    m_max_lead_text_width(max_lead_text_width), m_results_file_stem(results_file_stem)
{
    try_load_baseline_results();
}

inline BenchmarkResults::~BenchmarkResults()
{
    if (!m_results.empty())
        save_results();
}


} // namespace test_util
} // namespace tightdb

#endif // TIGHTDB_TEST_UTIL_BENCHMARK_RESULTS_HPP
