#include <realm/collection.hpp>
#include <realm/bplustree.hpp>
#include <realm/array_key.hpp>
#include <realm/array_string.hpp>
#include <realm/array_mixed.hpp>

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

void Collection::get_any(QueryCtrlBlock& ctrl, Mixed val, size_t index)
{
    auto path_size = ctrl.path.size() - index;
    PathElement& pe = ctrl.path[index];
    if (val.is_type(type_Dictionary) && (pe.is_key() || pe.is_all())) {
        auto ref = val.get_ref();
        if (!ref)
            return;
        Array top(ctrl.alloc);
        top.init_from_ref(ref);

        BPlusTree<StringData> keys(ctrl.alloc);
        keys.set_parent(&top, 0);
        keys.init_from_parent();
        size_t start = 0;
        if (size_t finish = keys.size()) {
            if (pe.is_key()) {
                start = keys.find_first(StringData(pe.get_key()));
                if (start == realm::not_found) {
                    if (pe.get_key() == "@keys") {
                        keys.for_all([&](auto& k) {
                            ctrl.matches.insert(k);
                            });
                    }
                    return;
                }
                finish = start + 1;
            }
            BPlusTree<Mixed> values(ctrl.alloc);
            values.set_parent(&top, 1);
            values.init_from_parent();
            for (; start < finish; start++) {
                val = values.get(start);
                if (path_size > 1) {
                    Collection::get_any(ctrl, val, index + 1);
                }
                else {
                    ctrl.matches.insert(val);
                }
            }
        }
    }
    else if (val.is_type(type_List) && (pe.is_ndx() || pe.is_all())) {
        auto ref = val.get_ref();
        if (!ref)
            return;
        ArrayMixed list(ctrl.alloc);
        list.init_from_ref(ref);
        if (size_t sz = list.size()) {
            size_t start = 0;
            size_t finish = sz;
            if (pe.is_ndx()) {
                start = pe.get_ndx();
                if (start == size_t(-1)) {
                    start = sz - 1;
                }
                if (start < sz) {
                    finish = start + 1;
                }
            }
            for (; start < finish; start++) {
                val = list.get(start);
                if (path_size > 1) {
                    Collection::get_any(ctrl, val, index + 1);
                }
                else {
                    ctrl.matches.insert(val);
                }
            }
        }
    }
}

std::pair<std::string, std::string> CollectionBase::get_open_close_strings(size_t link_depth,
                                                                           JSONOutputMode output_mode) const
{
    std::string open_str;
    std::string close_str;
    auto collection_type = get_collection_type();
    Table* target_table = get_target_table().unchecked_ptr();
    auto ck = get_col_key();
    auto type = ck.get_type();
    if (type == col_type_LinkList)
        type = col_type_Link;
    if (type == col_type_Link) {
        bool is_embedded = target_table->is_embedded();
        bool link_depth_reached = !is_embedded && (link_depth == 0);

        if (output_mode == output_mode_xjson_plus) {
            open_str = std::string("{ ") + (is_embedded ? "\"$embedded" : "\"$link");
            open_str += collection_type_name(collection_type, true);
            open_str += "\": ";
            close_str += " }";
        }

        if ((link_depth_reached && output_mode != output_mode_xjson) || output_mode == output_mode_xjson_plus) {
            open_str += "{ \"table\": \"" + std::string(target_table->get_name()) + "\", ";
            open_str += ((is_embedded || collection_type == CollectionType::Dictionary) ? "\"values" : "\"keys");
            open_str += "\": ";
            close_str += "}";
        }
    }
    else {
        if (output_mode == output_mode_xjson_plus) {
            switch (collection_type) {
                case CollectionType::List:
                    break;
                case CollectionType::Set:
                    open_str = "{ \"$set\": ";
                    close_str = " }";
                    break;
                case CollectionType::Dictionary:
                    open_str = "{ \"$dictionary\": ";
                    close_str = " }";
                    break;
            }
        }
    }
    return {open_str, close_str};
}

} // namespace realm
