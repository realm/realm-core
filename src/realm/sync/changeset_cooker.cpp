#include <realm/sync/changeset_cooker.hpp>

using namespace realm;
using namespace sync;


bool TrivialChangesetCooker::cook_changeset(const Group&, const char* changeset, std::size_t changeset_size,
                                            util::AppendBuffer<char>& buffer)
{
    buffer.append(changeset, changeset_size); // Throws
    return true;                              // A cooked changeset was produced
}
