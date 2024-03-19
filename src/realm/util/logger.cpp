/*************************************************************************
 *
 * Copyright 2016 Realm Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **************************************************************************/

#include <realm/util/logger.hpp>

#include <iostream>
#include <mutex>
#include <map>

namespace realm::util {

namespace {
auto& s_logger_mutex = *new std::mutex;
std::shared_ptr<util::Logger> s_default_logger;
} // anonymous namespace

size_t LogCategory::s_next_index = 0;
static std::map<std::string_view, LogCategory*> log_catagory_map;

LogCategory LogCategory::realm("Realm", nullptr);
LogCategory LogCategory::storage("Storage", &realm);
LogCategory LogCategory::transaction("Transaction", &storage);
LogCategory LogCategory::query("Query", &storage);
LogCategory LogCategory::object("Object", &storage);
LogCategory LogCategory::notification("Notification", &storage);
LogCategory LogCategory::sync("Sync", &realm);
LogCategory LogCategory::client("Client", &sync);
LogCategory LogCategory::session("Session", &client);
LogCategory LogCategory::changeset("Changeset", &client);
LogCategory LogCategory::network("Network", &client);
LogCategory LogCategory::reset("Reset", &client);
LogCategory LogCategory::server("Server", &sync);
LogCategory LogCategory::app("App", &realm);
LogCategory LogCategory::sdk("SDK", &realm);


LogCategory::LogCategory(std::string_view name, LogCategory* parent)
    : m_index(s_next_index++)
    , m_default_level(Logger::Level::info)
{
    if (parent) {
        m_name = parent->get_name() + ".";
        parent->m_children.push_back(this);
    }
    m_name += name;
    log_catagory_map.emplace(m_name, this);
}

LogCategory& LogCategory::get_category(std::string_view name)
{
    return *log_catagory_map.at(name); // Throws
}

std::vector<const char*> LogCategory::get_category_names()
{
    std::vector<const char*> ret;
    for (auto& it : log_catagory_map) {
        ret.push_back(it.second->get_name().c_str());
    }
    return ret;
}

void LogCategory::set_default_level_threshold(Level level)
{
    m_default_level.store(level);
    for (auto c : m_children) {
        c->set_default_level_threshold(level);
    }
    std::lock_guard lock(s_logger_mutex);
    if (s_default_logger)
        set_default_level_threshold(s_default_logger.get());
}

LogCategory::Level LogCategory::get_default_level_threshold() const noexcept
{
    return m_default_level.load(std::memory_order_relaxed);
}

void LogCategory::set_level_threshold(Logger* root, Level level) const
{
    root->set_level_threshold(m_index, level);
    for (auto c : m_children) {
        c->set_level_threshold(root, level);
    }
}

void LogCategory::set_default_level_threshold(Logger* root) const
{
    root->set_level_threshold(m_index, m_default_level.load(std::memory_order_relaxed));
    for (auto c : m_children) {
        c->set_default_level_threshold(root);
    }
}

void Logger::set_default_logger(std::shared_ptr<util::Logger> logger) noexcept
{
    std::lock_guard lock(s_logger_mutex);
    s_default_logger = logger;
}

std::shared_ptr<util::Logger>& Logger::get_default_logger() noexcept
{
    std::lock_guard lock(s_logger_mutex);
    if (!s_default_logger) {
        s_default_logger = std::make_shared<StderrLogger>();
    }

    return s_default_logger;
}

const char* Logger::get_level_prefix(Level level) noexcept
{
    switch (level) {
        case Level::off:
            [[fallthrough]];
        case Level::all:
            [[fallthrough]];
        case Level::trace:
            [[fallthrough]];
        case Level::debug:
            [[fallthrough]];
        case Level::detail:
            [[fallthrough]];
        case Level::info:
            break;
        case Level::warn:
            return "WARNING: ";
        case Level::error:
            return "ERROR: ";
        case Level::fatal:
            return "FATAL: ";
    }
    return "";
}

const std::string_view Logger::level_to_string(Level level) noexcept
{
    switch (level) {
        case Logger::Level::all:
            return "all";
        case Logger::Level::trace:
            return "trace";
        case Logger::Level::debug:
            return "debug";
        case Logger::Level::detail:
            return "detail";
        case Logger::Level::info:
            return "info";
        case Logger::Level::warn:
            return "warn";
        case Logger::Level::error:
            return "error";
        case Logger::Level::fatal:
            return "fatal";
        case Logger::Level::off:
            return "off";
    }
    REALM_ASSERT(false);
    return "";
}

void StderrLogger::do_log(const LogCategory& cat, Level level, const std::string& message)
{
    static Mutex mutex;
    LockGuard l(mutex);
    // std::cerr is unbuffered, so no need to flush
    std::cerr << cat.get_name() << " - " << get_level_prefix(level) << message << '\n'; // Throws
}

void StreamLogger::do_log(const LogCategory&, Level level, const std::string& message)
{
    m_out << get_level_prefix(level) << message << std::endl; // Throws
}

void ThreadSafeLogger::do_log(const LogCategory& category, Level level, const std::string& message)
{
    LockGuard l(m_mutex);
    Logger::do_log(*m_base_logger_ptr, category, level, message); // Throws
}

void PrefixLogger::do_log(const LogCategory& category, Level level, const std::string& message)
{
    Logger::do_log(m_chained_logger, category, level, m_prefix + message); // Throws
}

void LocalThresholdLogger::do_log(const LogCategory& category, Logger::Level level, std::string const& message)
{
    Logger::do_log(*m_chained_logger, category, level, message); // Throws
}
} // namespace realm::util
