#include <realm/sync/noinst/server/crypto_server.hpp>

using namespace realm;
using namespace realm::sync;

struct PKey::Impl {};

PKey::PKey()
{
    throw std::runtime_error("PKey not implemented");
}

PKey::PKey(PKey&&) = default;
PKey& PKey::operator=(PKey&&) = default;

PKey::~PKey() = default;

PKey PKey::load_public(const std::string&)
{
    throw std::runtime_error("PKey not implemented");
}

PKey PKey::load_public(BinaryData)
{
    throw std::runtime_error("PKey not implemented");
}

PKey PKey::load_private(const std::string&)
{
    throw std::runtime_error("PKey not implemented");
}

PKey PKey::load_private(BinaryData)
{
    throw std::runtime_error("PKey not implemented");
}

bool PKey::can_sign() const noexcept
{
    return false;
}

bool PKey::can_verify() const noexcept
{
    return false;
}

void PKey::sign(BinaryData, util::Buffer<unsigned char>&) const {}

bool PKey::verify(BinaryData, BinaryData) const
{
    return false;
}
