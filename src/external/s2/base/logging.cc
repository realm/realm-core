// Copyright 2010 Google
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "logging.h"

#include <utility>

namespace s2_env {

LoggingEnv::~LoggingEnv() = default;

LogMessageSink::~LogMessageSink() = default;

LogMessage::LogMessage(int verbosity)
  : _sink(globalLoggingEnv().makeSink(verbosity)) { }
LogMessage::LogMessage(Severity severity)
  : _sink(globalLoggingEnv().makeSink(severity)) { }
LogMessage::LogMessage(Severity severity, const char* file, int line)
  : _sink(globalLoggingEnv().makeSink(severity, file, line)) { }

LogMessage::~LogMessage() = default;

struct DefaultLogSink : LogMessageSink {
    explicit DefaultLogSink(int verbosity) : DefaultLogSink(static_cast<LogMessage::Severity>(verbosity)) { }
    explicit DefaultLogSink(LogMessage::Severity severity, const char* file = nullptr, int line = 0)
        : _severity(severity) {
        _os << "s2 ";
        if (file)
            _os << file << ":" << line << " ";
    }

    ~DefaultLogSink() override {
        std::cout << "[" << static_cast<int>(_severity) << "]: " << _os.str() << std::endl;
    }

    std::ostream& stream() override { return _os; }

    LogMessage::Severity _severity;
    std::ostringstream _os;
};

struct DefaultLoggingEnv : LoggingEnv {
    DefaultLoggingEnv() = default;
    ~DefaultLoggingEnv() override {}

    bool shouldVLog(int verbosity) override {
        return static_cast<LogMessage::Severity>(verbosity) >= LogMessage::Severity::kWarning;
    }

    std::unique_ptr<LogMessageSink> makeSink(int verbosity) override {
        return std::make_unique<DefaultLogSink>(verbosity);
    }
    std::unique_ptr<LogMessageSink> makeSink(LogMessage::Severity severity) override {
        return std::make_unique<DefaultLogSink>(severity);
    }
    std::unique_ptr<LogMessageSink> makeSink(LogMessage::Severity severity,
                                                     const char* file, int line) override {
        return std::make_unique<DefaultLogSink>(severity, file, line);
    }
};

LoggingEnv& globalLoggingEnv() {
    static DefaultLoggingEnv p;
    return p;
}

}  // namespace s2_env
