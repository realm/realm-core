
#ifndef REALM_NOINST_COMPACT_CHANGESETS_HPP
#define REALM_NOINST_COMPACT_CHANGESETS_HPP

#include <realm/sync/changeset.hpp>

namespace realm {
namespace _impl {

/// Compact changesets by removing redundant instructions.
///
/// Instructions considered for removal:
///   - Set
///   - CreateObject
///   - EraseObject
///
/// Instructions not (yet) considered for removal:
///   - AddInteger
///   - ContainerInsert
///   - ContainerSet
///
/// NOTE: All changesets are considered, in the sense that an instruction from
/// and earlier changeset being made redundant by a different instruction in a
/// later changeset will be removed. In other words, it is assumed that the
/// changesets will at least eventually all be applied (for instance as a client
/// receives a batched DOWNLOAD message).
///
/// NOTE: The input changesets are assumed to be in order of ascending versions,
/// but not necessarily in order of ascending timestamps. Callers must take care
/// to supply changesets in the same order in which they will be applied.
///
/// This function may throw exceptions due to the fact that it allocates memory.
///
/// This function is thread-safe, as long as its arguments are not modified by
/// other threads.
void compact_changesets(realm::sync::Changeset* changesets, size_t num_changesets);

} // namespace _impl
} // namespace realm

#endif // REALM_NOINST_COMPACT_CHANGESETS_HPP
