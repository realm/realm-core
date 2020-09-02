#include <realm/collection.hpp>

namespace realm {

CollectionBase::CollectionBase(const Obj& owner, ColKey col_key)
    : m_obj(owner)
    , m_col_key(col_key)
{
    if (!((col_key.is_list() || col_key.is_dictionary()))) {
        throw LogicError(LogicError::collection_type_mismatch);
    }
    m_nullable = col_key.is_nullable();
}

CollectionBase::~CollectionBase() {}

ref_type CollectionBase::get_child_ref(size_t) const noexcept
{
    try {
        return to_ref(m_obj._get<int64_t>(m_col_key.get_index()));
    }
    catch (const KeyNotFound&) {
        return ref_type(0);
    }
}

} // namespace realm
