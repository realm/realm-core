////////////////////////////////////////////////////////////////////////////
//
// Copyright 2016 Realm Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
////////////////////////////////////////////////////////////////////////////

#ifndef REALM_TEST_UTILS_HPP
#define REALM_TEST_UTILS_HPP

#include <catch2/catch_all.hpp>
#include <catch2/matchers/catch_matchers_all.hpp>
#include <realm/util/file.hpp>
#include <realm/util/optional.hpp>

#include <functional>
#include <filesystem>
#include <mutex>
#include <condition_variable>
namespace fs = std::filesystem;

namespace realm {
template <typename E>
class TestingStateMachine {
public:
    explicit TestingStateMachine(E initial_state)
        : m_cur_state(initial_state)
    {
    }

    E get()
    {
        std::lock_guard lock{m_mutex};
        return m_cur_state;
    }

    void transition_to(E new_state)
    {
        {
            std::lock_guard lock{m_mutex};
            m_cur_state = new_state;
        }
        m_cv.notify_one();
    }

    template <typename Func>
    void transition_with(Func&& func)
    {
        {
            std::lock_guard lock{m_mutex};
            std::optional<E> new_state = func(m_cur_state);
            if (!new_state) {
                return;
            }
            m_cur_state = *new_state;
        }
        m_cv.notify_one();
    }

    bool wait_for(E target, std::chrono::milliseconds period = std::chrono::seconds(15))
    {
        std::unique_lock lock{m_mutex};
        return m_cv.wait_for(lock, period, [&] {
            return m_cur_state == target;
        });
    }

private:
    std::mutex m_mutex;
    std::condition_variable m_cv;
    E m_cur_state;
};

template <typename MessageMatcher>
class ExceptionMatcher final : public Catch::Matchers::MatcherBase<Exception> {
public:
    ExceptionMatcher(ErrorCodes::Error code, MessageMatcher&& matcher)
        : m_code(code)
        , m_matcher(std::move(matcher))
    {
    }

    bool match(Exception const& ex) const override
    {
        return ex.code() == m_code && m_matcher.match(ex.what());
    }

    std::string describe() const override
    {
        return util::format("Exception(%1, \"%2\")", ErrorCodes::error_string(m_code), m_matcher.describe());
    }

private:
    ErrorCodes::Error m_code;
    MessageMatcher m_matcher;
};

template <>
class ExceptionMatcher<void> final : public Catch::Matchers::MatcherBase<Exception> {
public:
    ExceptionMatcher(ErrorCodes::Error code, std::string_view msg)
        : m_code(code)
        , m_message(msg)
    {
    }

    bool match(Exception const& ex) const override;
    std::string describe() const override;

private:
    ErrorCodes::Error m_code;
    std::string m_message;
};

class OutOfBoundsMatcher final : public Catch::Matchers::MatcherBase<OutOfBounds> {
public:
    OutOfBoundsMatcher(size_t index, size_t size, std::string_view msg)
        : m_index(index)
        , m_size(size)
        , m_message(msg)
    {
    }

    bool match(OutOfBounds const& ex) const override;
    std::string describe() const override;

private:
    size_t m_index, m_size;
    std::string m_message;
};

class LogicErrorMatcher final : public Catch::Matchers::MatcherBase<LogicError> {
public:
    LogicErrorMatcher(ErrorCodes::Error code)
        : m_code(code)
    {
    }

    bool match(LogicError const& ex) const override;
    std::string describe() const override;

private:
    ErrorCodes::Error m_code;
};

namespace _impl {
template <typename T>
ExceptionMatcher<T> make_exception_matcher(ErrorCodes::Error code, T&& matcher)
{
    return ExceptionMatcher<T>(code, std::move(matcher));
}
inline ExceptionMatcher<void> make_exception_matcher(ErrorCodes::Error code, const char* msg)
{
    return ExceptionMatcher<void>(code, msg);
}
inline ExceptionMatcher<void> make_exception_matcher(ErrorCodes::Error code, std::string_view msg)
{
    return ExceptionMatcher<void>(code, msg);
}
inline ExceptionMatcher<void> make_exception_matcher(ErrorCodes::Error code, const std::string& msg)
{
    return ExceptionMatcher<void>(code, msg);
}
inline ExceptionMatcher<void> make_exception_matcher(ErrorCodes::Error code, std::string&& msg)
{
    return ExceptionMatcher<void>(code, msg);
}
} // namespace _impl

std::ostream& operator<<(std::ostream&, const Exception&);

class Realm;
/// Open a Realm at a given path, creating its files.
bool create_dummy_realm(std::string path, std::shared_ptr<Realm>* out = nullptr);
std::vector<char> make_test_encryption_key(const char start = 0);
void catch2_ensure_section_run_workaround(bool did_run_a_section, std::string section_name,
                                          util::FunctionRef<void()> func);

std::string encode_fake_jwt(const std::string& in, util::Optional<int64_t> exp = {},
                            util::Optional<int64_t> iat = {});

std::string random_string(std::string::size_type length);
int64_t random_int();

bool chmod_supported(const std::string& path);
int get_permissions(const std::string& path);
void chmod(const std::string& path, int permissions);

} // namespace realm

#define REQUIRE_DIR_EXISTS(macro_path)                                                                               \
    do {                                                                                                             \
        CHECK(util::File::is_dir(macro_path) == true);                                                               \
    } while (0)

#define REQUIRE_DIR_PATH_EXISTS(macro_path)                                                                          \
    do {                                                                                                             \
        REQUIRE(util::File::is_dir((macro_path).string()));                                                          \
    } while (0)

#define REQUIRE_DIR_DOES_NOT_EXIST(macro_path)                                                                       \
    do {                                                                                                             \
        CHECK(util::File::exists(macro_path) == false);                                                              \
    } while (0)

#define REQUIRE_DIR_PATH_DOES_NOT_EXIST(macro_path)                                                                  \
    do {                                                                                                             \
        REQUIRE_FALSE(util::File::exists((macro_path).string()));                                                    \
    } while (0)

#define REQUIRE_REALM_EXISTS(macro_path)                                                                             \
    do {                                                                                                             \
        REQUIRE(util::File::exists(macro_path));                                                                     \
        REQUIRE(util::File::exists((macro_path) + ".lock"));                                                         \
        REQUIRE_DIR_EXISTS((macro_path) + ".management");                                                            \
    } while (0)

#define REQUIRE_REALM_DOES_NOT_EXIST(macro_path)                                                                     \
    do {                                                                                                             \
        REQUIRE(!util::File::exists(macro_path));                                                                    \
        REQUIRE(!util::File::exists((macro_path) + ".lock"));                                                        \
        REQUIRE_DIR_DOES_NOT_EXIST((macro_path) + ".management");                                                    \
    } while (0)

#define REQUIRE_THROWS_CONTAINING(expr, msg) REQUIRE_THROWS_WITH(expr, Catch::Matchers::ContainsSubstring(msg))

#define REQUIRE_EXCEPTION(expr, c, msg)                                                                              \
    REQUIRE_THROWS_MATCHES(expr, realm::Exception, _impl::make_exception_matcher(realm::ErrorCodes::c, msg))
#define REQUIRE_THROWS_OUT_OF_BOUNDS(expr, index, size, msg)                                                         \
    REQUIRE_THROWS_MATCHES(expr, OutOfBounds, OutOfBoundsMatcher(index, size, msg));
#define REQUIRE_THROW_LOGIC_ERROR_WITH_CODE(expr, err)                                                               \
    REQUIRE_THROWS_MATCHES(expr, LogicError, LogicErrorMatcher(err))

#define ENCODE_FAKE_JWT(in) realm::encode_fake_jwt(in)

#endif // REALM_TEST_UTILS_HPP
