#include <ctime>
#include <cstring>
#include <algorithm>
#include <locale>
#include <sstream>
#include <fstream>
#include <iostream>
#include <iomanip>
#include <float.h> // DBL_MIN, DBL_MAX

#include <unistd.h> // link, unlink

#include <tightdb/util/file.hpp>

#include "timer.hpp"
#include "benchmark_results.hpp"

using namespace std;
using namespace tightdb;
using namespace test_util;


namespace {

string format_elapsed_time(double seconds)
{
    ostringstream out;
    Timer::format(seconds, out);
    return out.str();
}

string format_change_percent(double baseline, double seconds)
{
    ostringstream out;
    double percent = (seconds - baseline) / baseline * 100;
    out.precision(3);
    out.setf(ios_base::showpos);
    out << percent << "%";
    return out.str();
}

string format_drop_factor(double baseline, double seconds)
{
    ostringstream out;
    double factor = baseline / seconds;
    out.precision(3);
    out << factor << ":1";
    return out.str();
}

string format_rise_factor(double baseline, double seconds)
{
    ostringstream out;
    double factor = seconds / baseline;
    out.precision(3);
    out << "1:" << factor;
    return out.str();
}

string format_change(double baseline, double input, BenchmarkResults::ChangeType change_type)
{
    switch (change_type) {
        case BenchmarkResults::change_Percent:
            return format_change_percent(baseline, input);
        case BenchmarkResults::change_DropFactor:
            return format_drop_factor(baseline, input);
        case BenchmarkResults::change_RiseFactor:
            return format_rise_factor(baseline, input);
    }
    TIGHTDB_ASSERT(false);
    return string();
}

} // anonymous namespace

BenchmarkResults::Result::Result():
    min(DBL_MAX), max(DBL_MIN), total(0), rep(0)
{
}

double BenchmarkResults::Result::avg() const
{
    return total / rep;
}

void BenchmarkResults::submit_single(const char* ident, const char* lead_text,
    double seconds, ChangeType change_type)
{
    submit(ident, seconds);
    finish(ident, lead_text, change_type);
}

void BenchmarkResults::submit(const char* ident, double seconds)
{
    Results::iterator it = m_results.find(ident);
    if (it == m_results.end()) {
        it = m_results.insert(std::make_pair(ident, Result())).first;
    }
    Result& r = it->second;
    if (r.min > seconds) r.min = seconds;
    if (r.max < seconds) r.max = seconds;
    r.total += seconds;
    r.rep += 1;
}

void BenchmarkResults::finish(const std::string& ident, const std::string& lead_text, ChangeType change_type)
{
    /*
        OUTPUT FOR RESULTS WITHOUT BASELINE:
        Lead Text             min 0.0s     max 0.0s    avg 0.0s
        Lead Text 2           min 123.0s   max 32.0s   avg 1.0s

        OUTPUT FOR RESULTS WITH BASELINE:
        Lead Text             min 0.0s (+10%)   max 0.0s (-20%)   avg 0.0s (0%)
        Lead Text 2           min 0.0s (+10%)   max 0.0s (-20%)   avg 0.0s (0%)
    */

    BaselineResults::const_iterator baseline_iter = m_baseline_results.find(ident);

    std::ostream& out = std::cout;

    // Print Lead Text
    out.setf(ios_base::left, ios_base::adjustfield);
    m_max_lead_text_width = max(m_max_lead_text_width, int(lead_text.size()));
    std::string lead_text_2 = lead_text + ":";
    out << setw(m_max_lead_text_width + 1 + 3) << lead_text_2;

    Results::const_iterator it = m_results.find(ident);
    if (it == m_results.end()) {
        out << "(no measurements)" << endl;
        return;
    }

    const Result& r = it->second;

    const size_t time_width = 8;

    out.setf(ios_base::right, ios_base::adjustfield);
    if (baseline_iter != m_baseline_results.end()) {
        const Result& br = baseline_iter->second;
        out << "min " << setw(time_width) << format_elapsed_time(r.min)   << " (" << format_change(br.min, r.min, change_type) << ")     ";
        out << "max " << setw(time_width) << format_elapsed_time(r.max)   << " (" << format_change(br.max, r.max, change_type) << ")     ";
        out << "avg " << setw(time_width) << format_elapsed_time(r.avg()) << " (" << format_change(br.avg(), r.avg(), change_type) << ")     ";
        out << "reps " << r.rep;
    }
    else {
        out << "min " << setw(time_width) << format_elapsed_time(r.min)   << "     ";
        out << "max " << setw(time_width) << format_elapsed_time(r.max)   << "     ";
        out << "avg " << setw(time_width) << format_elapsed_time(r.avg()) << "     ";
        out << "reps " << r.rep;
    }
    out << endl;
}


void BenchmarkResults::try_load_baseline_results()
{
    string baseline_file = m_results_file_stem;
    baseline_file += ".baseline";
    if (util::File::exists(baseline_file)) {
        ifstream in(baseline_file.c_str());
        BaselineResults baseline_results;
        bool error = false;
        string line;
        while (getline(in, line)) {
            istringstream line_in(line);
            string ident;
            char space;
            Result r;
            line_in >> ident >> noskipws >> space >> skipws >> r.min >> space >> r.max >> space >> r.total >> r.rep;
            if (!line_in || !isspace(space, line_in.getloc()))
                error = true;
            if (!line_in.eof()) {
                line_in >> space;
                if (line_in.rdstate() != (ios_base::failbit | ios_base::eofbit))
                    error = true;
            }
            if (error)
                break;
            baseline_results[ident] = r;
        }
        if (error) {
            cerr << "WARNING: Failed to parse '"<<baseline_file<<"'\n";
        }
        else {
            m_baseline_results = baseline_results;
        }
    }
}


void BenchmarkResults::save_results()
{
    time_t now = time(0);
    localtime(&now);
    struct tm local;
    localtime_r(&now,  &local);
    ostringstream name_out;
    name_out << m_results_file_stem << ".";
    // Format: YYYYMMDD_hhmmss;
    name_out.fill('0');
    name_out << (1900 + local.tm_year) << ""
        "" << setw(2) << (1 + local.tm_mon) << ""
        "" << setw(2) << local.tm_mday << "_"
        "" << setw(2) << local.tm_hour << ""
        "" << setw(2) << local.tm_min << ""
        "" << setw(2) << local.tm_sec;
    string name = name_out.str();
    string csv_name = name + ".csv";
    {
        ofstream out(name.c_str());
        ofstream csv_out(csv_name.c_str());

        csv_out << "ident,min,max,avg,reps,total" << '\n';
        csv_out.setf(std::ios_base::fixed, std::ios_base::floatfield);

        typedef Results::const_iterator iter;
        for (iter it = m_results.begin(); it != m_results.end(); ++it) {
            const Result& r = it->second;

            out << it->first << ' ';
            out << r.min << " " << r.max << " " << r.total << " " << r.rep << '\n';

            csv_out << '"' << it->first << "\",";
            csv_out << r.min << ',' << r.max << ',' << r.avg() << ',' << r.rep << ',' << r.total << '\n';
        }
    }

    string baseline_file = m_results_file_stem;
    string latest_csv_file = m_results_file_stem + ".latest.csv";
    baseline_file += ".baseline";
    if (!util::File::exists(baseline_file)) {
        link(name.c_str(), baseline_file.c_str());
    }
    if (util::File::exists(latest_csv_file)) {
        unlink(latest_csv_file.c_str());
    }
    link(csv_name.c_str(), latest_csv_file.c_str());
}

