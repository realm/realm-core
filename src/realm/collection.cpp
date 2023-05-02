#include <realm/collection.hpp>
#include <realm/bplustree.hpp>
#include <realm/array_key.hpp>

namespace realm {

namespace _impl {
size_t virtual2real(const std::vector<size_t>& vec, size_t ndx) noexcept
{
    for (auto i : vec) {
        if (i > ndx)
            break;
        ndx++;
    }
    return ndx;
}

size_t virtual2real(const BPlusTree<ObjKey>* tree, size_t ndx) noexcept
{
    // Only translate if context flag is set.
    if (tree->get_context_flag()) {
        size_t adjust = 0;
        auto func = [&adjust, ndx](BPlusTreeNode* node, size_t offset) {
            auto leaf = static_cast<BPlusTree<ObjKey>::LeafNode*>(node);
            size_t sz = leaf->size();
            for (size_t i = 0; i < sz; i++) {
                if (i + offset == ndx) {
                    return IteratorControl::Stop;
                }
                auto k = leaf->get(i);
                if (k.is_unresolved()) {
                    adjust++;
                }
            }
            return IteratorControl::AdvanceToNext;
        };

        tree->traverse(func);
        ndx -= adjust;
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
            return IteratorControl::AdvanceToNext;
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

} // namespace _impl

Collection::~Collection() {}

std::pair<std::string, std::string> CollectionBase::get_open_close_strings(size_t link_depth,
                                                                           JSONOutputMode output_mode) const
{
    std::string open_str;
    std::string close_str;
    auto ck = get_col_key();
    Table* target_table = get_target_table().unchecked_ptr();
    auto type = ck.get_type();
    if (type == col_type_LinkList)
        type = col_type_Link;
    if (type == col_type_Link) {
        bool is_embedded = target_table->is_embedded();
        bool link_depth_reached = !is_embedded && (link_depth == 0);

        if (output_mode == output_mode_xjson_plus) {
            open_str = std::string("{ ") + (is_embedded ? "\"$embedded" : "\"$link");
            open_str += collection_type_name(ck, true);
            open_str += "\": ";
            close_str += " }";
        }

        if ((link_depth_reached && output_mode != output_mode_xjson) || output_mode == output_mode_xjson_plus) {
            open_str += "{ \"table\": \"" + std::string(target_table->get_name()) + "\", ";
            open_str += ((is_embedded || ck.is_dictionary()) ? "\"value" : "\"key");
            if (ck.is_collection())
                open_str += "s";
            open_str += "\": ";
            close_str += "}";
        }
    }
    else {
        if (output_mode == output_mode_xjson_plus) {
            if (ck.is_set()) {
                open_str = "{ \"$set\": ";
                close_str = " }";
            }
            else if (ck.is_dictionary()) {
                open_str = "{ \"$dictionary\": ";
                close_str = " }";
            }
        }
    }
    return {open_str, close_str};
}

} // namespace realm
