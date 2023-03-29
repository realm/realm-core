#ifndef REALM_UTIL_CLI_ARGS_HPP
#define REALM_UTIL_CLI_ARGS_HPP

#include <vector>
#include <string_view>
#include <stdexcept>

namespace realm::util {

class CliFlag;
class CliArgumentParser {
public:
    void add_argument(CliFlag* flag);

    struct ParseResult {
        std::string_view program_name;
        std::vector<std::string_view> unmatched_arguments;
    };

    ParseResult parse(int argc, const char** argv);

private:
    std::vector<CliFlag*> m_flags;
};

class CliFlag {
public:
    virtual ~CliFlag() = default;

    explicit CliFlag(CliArgumentParser& parser, std::string_view name, char short_name = '\0')
        : m_name(name)
        , m_short_name(short_name)
    {
        parser.add_argument(this);
    }

    explicit operator bool() const noexcept
    {
        return m_found;
    }

    std::string_view name() const noexcept
    {
        return m_name;
    }
    char short_name() const noexcept
    {
        return m_short_name;
    }

protected:
    friend class CliArgumentParser;

    virtual void assign(std::string_view)
    {
        m_found = true;
    }

    virtual bool expects_value()
    {
        return false;
    }

private:
    bool m_found = false;
    std::string_view m_name;
    char m_short_name = '\0';
};

class CliArgument : public CliFlag {
public:
    using CliFlag::CliFlag;

    std::string_view value() const noexcept
    {
        return m_value;
    }

    template <typename T>
    T as() const;

protected:
    friend class CliArgumentParser;

    void assign(std::string_view value) override
    {
        CliFlag::assign(value);
        m_value = value;
    }

    bool expects_value() override
    {
        return true;
    }

private:
    std::string_view m_value;
};

class CliParseException : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

} // namespace realm::util

#endif // REALM_UTIL_CLI_ARGS_HPP
