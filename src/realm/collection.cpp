#include <realm/collection.hpp>
#include <realm/bplustree.hpp>
#include <realm/array_key.hpp>

namespace realm::_impl {

size_t virtual2real(const std::vector<size_t>& vec, size_t ndx) noexcept
{
    for (auto i : vec) {
        if (i > ndx)
            break;
        ndx++;
    }
    return ndx;
}

size_t real2virtual(const std::vector<size_t>& vec, size_t ndx) noexcept
{
    // Subtract the number of tombstones below ndx.
    auto it = std::lower_bound(vec.begin(), vec.end(), ndx);
    auto n = it - vec.begin();
    return ndx - n;
}

void update_unresolved(std::vector<size_t>& vec, const BPlusTree<ObjKey>* tree)
{
    vec.clear();

    // Only do the scan if context flag is set.
    if (tree && tree->is_attached() && tree->get_context_flag()) {
        auto func = [&vec](BPlusTreeNode* node, size_t offset) {
            auto leaf = static_cast<BPlusTree<ObjKey>::LeafNode*>(node);
            size_t sz = leaf->size();
            for (size_t i = 0; i < sz; i++) {
                auto k = leaf->get(i);
                if (k.is_unresolved()) {
                    vec.push_back(i + offset);
                }
            }
            return false;
        };

        tree->traverse(func);
    }
}

void check_for_last_unresolved(BPlusTree<ObjKey>* tree)
{
    if (tree) {
        bool no_more_unresolved = true;
        size_t sz = tree->size();
        for (size_t n = 0; n < sz; n++) {
            if (tree->get(n).is_unresolved()) {
                no_more_unresolved = false;
                break;
            }
        }
        if (no_more_unresolved)
            tree->set_context_flag(false);
    }
}

} // namespace realm::_impl
