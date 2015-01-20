#include <tightdb/exceptions.hpp>
#include <tightdb/version.hpp>

using namespace tightdb;

const char* ExceptionWithVersionInWhat::message() const TIGHTDB_NOEXCEPT_OR_NOTHROW
{
    const char* msg = what();
    size_t len = strlen(msg);
    static const char ver[] = TIGHTDB_VER_CHUNK;
    if (len > sizeof(ver)) {
        // Assume that what() actually included the version string.
        return msg + sizeof(ver);
    }
    return msg;
}

const char* Exception::version() const TIGHTDB_NOEXCEPT_OR_NOTHROW
{
    return TIGHTDB_VER_STRING;
}

RuntimeError::RuntimeError(const std::string& message):
    std::runtime_error(std::string(TIGHTDB_VER_CHUNK) + " " + message)
{
}

RuntimeError::RuntimeError(const RuntimeError& other):
    std::runtime_error(other)
{
}

const char* RuntimeError::message() const TIGHTDB_NOEXCEPT_OR_NOTHROW
{
    const char* msg = what();
    size_t len = strlen(msg);
    static const char ver[] = TIGHTDB_VER_CHUNK;
    if (len > sizeof(ver)) {
        // Assume that what() actually included the version string.
        return msg + sizeof(ver);
    }
    return msg;
}

const char* RuntimeError::version() const TIGHTDB_NOEXCEPT_OR_NOTHROW
{
    return TIGHTDB_VER_STRING;
}


const char* LogicError::get_message_for_error(LogicError::error_kind kind) TIGHTDB_NOEXCEPT_OR_NOTHROW
{
    switch (kind) {
        case LogicError::string_too_big:
            return TIGHTDB_VER_CHUNK " String too big";
        case LogicError::binary_too_big:
            return TIGHTDB_VER_CHUNK " Binary too big";
        case LogicError::table_name_too_long:
            return TIGHTDB_VER_CHUNK " Table name too long";
        case LogicError::column_name_too_long:
            return TIGHTDB_VER_CHUNK " Column name too long";
        case LogicError::table_index_out_of_range:
            return TIGHTDB_VER_CHUNK " Table index out of range";
        case LogicError::row_index_out_of_range:
            return TIGHTDB_VER_CHUNK " Row index out of range";
        case LogicError::column_index_out_of_range:
            return TIGHTDB_VER_CHUNK " Column index out of range";
        case LogicError::illegal_combination:
            return TIGHTDB_VER_CHUNK " Illegal combination";
        case LogicError::type_mismatch:
            return TIGHTDB_VER_CHUNK " Type mismatch";
        case LogicError::wrong_kind_of_table:
            return TIGHTDB_VER_CHUNK " Wrong kind of table";
        case LogicError::detached_accessor:
            return TIGHTDB_VER_CHUNK " Detached accessor";
        case LogicError::no_search_index:
            return TIGHTDB_VER_CHUNK " Column has no search index";
        case LogicError::no_primary_key:
            return TIGHTDB_VER_CHUNK " Table has no primary key";
        case LogicError::has_primary_key:
            return TIGHTDB_VER_CHUNK " Primary key already added";
        case LogicError::unique_constraint_violation:
            return TIGHTDB_VER_CHUNK " Unique constraint violation";
    }
    return TIGHTDB_VER_CHUNK " Unknown error";
}

const char* NoSuchTable::what() const TIGHTDB_NOEXCEPT_OR_NOTHROW
{
    return TIGHTDB_VER_CHUNK " No such table exists";
}

const char* TableNameInUse::what() const TIGHTDB_NOEXCEPT_OR_NOTHROW
{
    return TIGHTDB_VER_CHUNK " The specified table name is already in use";
}

const char* CrossTableLinkTarget::what() const TIGHTDB_NOEXCEPT_OR_NOTHROW
{
    return TIGHTDB_VER_CHUNK " Table is target of cross-table link columns";
}

const char* DescriptorMismatch::what() const TIGHTDB_NOEXCEPT_OR_NOTHROW
{
    return TIGHTDB_VER_CHUNK " Table descriptor mismatch";
}
