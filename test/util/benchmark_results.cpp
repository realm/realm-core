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

#include <realm/util/file.hpp>

#include "timer.hpp"
#include "benchmark_results.hpp"

using namespace realm;
using namespace test_util;


namespace {

std::string format_elapsed_time(double seconds)
{
    std::ostringstream out;
    Timer::format(seconds, out);
    return out.str();
}

std::string format_change_percent(double baseline, double seconds)
{
    std::ostringstream out;
    double percent = (seconds - baseline) / baseline * 100;
    out.precision(2);
    out.setf(std::ios_base::showpos);
    out << std::fixed;
    out << percent << "%";
    return out.str();
}

std::string format_drop_factor(double baseline, double seconds)
{
    std::ostringstream out;
    double factor = baseline / seconds;
    out.precision(3);
    out << factor << ":1";
    return out.str();
}

std::string format_rise_factor(double baseline, double seconds)
{
    std::ostringstream out;
    double factor = seconds / baseline;
    out.precision(3);
    out << "1:" << factor;
    return out.str();
}

std::string format_change(double baseline, double input, BenchmarkResults::ChangeType change_type)
{
    std::string str;
    switch (change_type) {
        case BenchmarkResults::change_Percent:
            str = format_change_percent(baseline, input);
            break;
        case BenchmarkResults::change_DropFactor:
            str = format_drop_factor(baseline, input);
            break;
        case BenchmarkResults::change_RiseFactor:
            str = format_rise_factor(baseline, input);
            break;
    }
    std::ostringstream os;
    os << '(' << str << ')';
    return os.str();
}

std::string pad_right(std::string str, size_t width, char padding = ' ')
{
    std::ostringstream ss;
    ss << std::setw(width);
    ss << std::setfill(padding);
    ss << std::left;
    ss << str;
    return ss.str();
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
    out.setf(std::ios_base::left, std::ios_base::adjustfield);
    m_max_lead_text_width = std::max(m_max_lead_text_width, int(lead_text.size()));
    std::string lead_text_2 = lead_text + ":";
    out << std::setw(m_max_lead_text_width + 1 + 3) << lead_text_2;

    Results::const_iterator it = m_results.find(ident);
    if (it == m_results.end()) {
        out << "(no measurements)" << std::endl;
        return;
    }

    const Result& r = it->second;

    const size_t time_width = 8;

    out.setf(std::ios_base::right, std::ios_base::adjustfield);
    if (baseline_iter != m_baseline_results.end()) {
        const Result& br = baseline_iter->second;
        out << "min " << std::setw(time_width) << format_elapsed_time(r.min)   << " " << pad_right(format_change(br.min, r.min, change_type), 15) << "     ";
        out << "max " << std::setw(time_width) << format_elapsed_time(r.max)   << " " << pad_right(format_change(br.max, r.max, change_type), 15) << "     ";
        out << "avg " << std::setw(time_width) << format_elapsed_time(r.avg()) << " " << pad_right(format_change(br.avg(), r.avg(), change_type), 15) << "     ";
        out << "reps " << r.rep;
    }
    else {
        out << "min " << std::setw(time_width) << format_elapsed_time(r.min)   << "     ";
        out << "max " << std::setw(time_width) << format_elapsed_time(r.max)   << "     ";
        out << "avg " << std::setw(time_width) << format_elapsed_time(r.avg()) << "     ";
        out << "reps " << r.rep;
    }
    out << std::endl;
}


void BenchmarkResults::try_load_baseline_results()
{
    std::string baseline_file = m_results_file_stem;
    baseline_file += ".baseline";
    if (util::File::exists(baseline_file)) {
        std::ifstream in(baseline_file.c_str());
        BaselineResults baseline_results;
        bool error = false;
        std::string line;
        int lineno = 1;
        while (getline(in, line)) {
            std::istringstream line_in(line);
            std::string ident;
            char space;
            Result r;
            if (line_in >> ident) {
                line_in >> space;
                double* numbers[] = {&r.min, &r.max, &r.total};
                for (size_t i = 0; i < 3; ++i) {
                    if (!(line_in >> *numbers[i])) {
                        std::cerr << "Expected number: line " << lineno << "\n";
                        error = true;
                        break;
                    }
                    if (!(line_in >> space)) {
                        std::cerr << "Expected space: line " << lineno << "\n";
                        error = true;
                        break;
                    }
                }
                if (!error) {
                    if (!(line_in >> r.rep)) {
                        std::cerr << "Expected integer: line " << lineno << "\n";
                        error = true;
                    }
                    if (!(line_in >> std::skipws)) {
                        std::cerr << "Expected whitespace: line " << lineno << '\n';
                        error = true;
                    }
                }
            }
            else {
                std::cerr << "Expected identifier: line " << lineno << "\n";
                error = true;
            }
            if (!line_in) {
                std::cerr << "Unknown error: line " << lineno << '\n';
                error = true;
            }
            if (error)
                break;
            baseline_results[ident] = r;
            ++lineno;
        }
        if (error) {
            std::cerr << "WARNING: Failed to parse '"<<baseline_file<<"'\n";
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
    std::ostringstream name_out;
    name_out << m_results_file_stem << ".";
    // Format: YYYYMMDD_hhmmss;
    name_out.fill('0');
    name_out << (1900 + local.tm_year) << ""
        "" << std::setw(2) << (1 + local.tm_mon) << ""
        "" << std::setw(2) << local.tm_mday << "_"
        "" << std::setw(2) << local.tm_hour << ""
        "" << std::setw(2) << local.tm_min << ""
        "" << std::setw(2) << local.tm_sec;
    std::string name = name_out.str();
    std::string csv_name = name + ".csv";
    {
        std::ofstream out(name.c_str());
        std::ofstream csv_out(csv_name.c_str());

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

    std::string baseline_file = m_results_file_stem;
    std::string latest_csv_file = m_results_file_stem + ".latest.csv";
    baseline_file += ".baseline";
    int r;
    if (!util::File::exists(baseline_file)) {
        r = link(name.c_str(), baseline_file.c_str());
    }
    if (util::File::exists(latest_csv_file)) {
        r = unlink(latest_csv_file.c_str());
    }
    r = link(csv_name.c_str(), latest_csv_file.c_str());
    static_cast<void>(r); // FIXME: Display if error
}

