#include <ctime>
#include <cstring>
#include <algorithm>
#include <locale>
#include <sstream>
#include <fstream>
#include <iostream>
#include <iomanip>

#include <unistd.h> // link()

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
    double percent = (seconds - baseline) / baseline * 100;
    ostringstream out;
    out.precision(3);
    out.setf(ios_base::showpos);
    out << percent << "%";
    return out.str();
}

string format_drop_factor(double baseline, double seconds)
{
    double factor = baseline / seconds;
    ostringstream out;
    out.precision(3);
    out << factor << ":1";
    return out.str();
}

string format_rise_factor(double baseline, double seconds)
{
    double factor = seconds / baseline;
    ostringstream out;
    out.precision(3);
    out << "1:" << factor;
    return out.str();
}

} // anonymous namespace


void BenchmarkResults::submit(double elapsed_seconds, const char *ident, const char* lead_text,
                              ChangeType change_type)
{
    double baseline_seconds = 0;
    bool have_baseline = false;
    {
        typedef BaselineResults::const_iterator iter;
        iter i = m_baseline_results.find(ident);
        if (i != m_baseline_results.end()) {
            baseline_seconds = i->second;
            have_baseline = true;
        }
    }

    int separation_1 = 2;
    int separation_2 = 4;
    int separation_3 = 2;

    ostringstream out;
    out.setf(ios_base::left, ios_base::adjustfield);
    m_max_lead_text_width = max(m_max_lead_text_width, int(strlen(lead_text)));
    string lead_text_2 = lead_text;
    lead_text_2 += ":";
    out << setw(m_max_lead_text_width+1+separation_1) << lead_text_2;
    if (m_baseline_results.empty()) {
        out << format_elapsed_time(elapsed_seconds);
    }
    else {
        int time_width = 6;
        if (have_baseline) {
            out << setw(time_width+separation_2) << format_elapsed_time(baseline_seconds);
            out << setw(time_width+separation_3) << format_elapsed_time(elapsed_seconds);
            out << "(";
            switch (change_type) {
                case change_Percent:
                    out << format_change_percent(baseline_seconds, elapsed_seconds);
                    break;
                case change_DropFactor:
                    out << format_drop_factor(baseline_seconds, elapsed_seconds);
                    break;
                case change_RiseFactor:
                    out << format_rise_factor(baseline_seconds, elapsed_seconds);
                    break;
            }
            out << ")";
        }
        else {
            out << setw(time_width+separation_2) << "";
            out << format_elapsed_time(elapsed_seconds);
        }
    }
    cout << out.str() << endl;
    m_results.push_back(Result(elapsed_seconds, ident));
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
            char space = 0;
            double elapsed_seconds = 0;
            line_in >> ident >> noskipws >> space >> skipws >> elapsed_seconds;
            if (!line_in || !isspace(space, line_in.getloc()))
                error = true;
            if (!line_in.eof()) {
                line_in >> space;
                if (line_in.rdstate() != (ios_base::failbit | ios_base::eofbit))
                    error = true;
            }
            if (error)
                break;
            baseline_results[ident] = elapsed_seconds;
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
    {
        ofstream out(name.c_str());
        typedef Results::const_iterator iter;
        for (iter i = m_results.begin(); i != m_results.end(); ++i)
            out << i->m_ident << " " << i->m_elapsed_seconds << "\n";
    }

    string baseline_file = m_results_file_stem;
    baseline_file += ".baseline";
    if (!util::File::exists(baseline_file)) {
        int r = link(name.c_str(), baseline_file.c_str());
        static_cast<void>(r);
    }
}

