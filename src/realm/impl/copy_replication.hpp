/*************************************************************************
 *
 * Copyright 2021 Realm Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **************************************************************************/

#ifndef REALM_COPY_REPLICATION_HPP
#define REALM_COPY_REPLICATION_HPP

#include <realm/replication.hpp>
#include <realm/transaction.hpp>

namespace realm::_impl {

class CopyReplication : public Replication {
public:
    CopyReplication(TransactionRef tr)
        : m_tr(tr)
    {
    }
    using version_type = _impl::History::version_type;

    void add_class(TableKey, StringData name, Table::Type table_type) override;
    void add_class_with_primary_key(TableKey, StringData name, DataType type, StringData pk_name, bool nullable,
                                    Table::Type table_type) override;
    void insert_column(const Table* t, ColKey col_key, DataType type, StringData name, Table* dest) override;
    void create_object_with_primary_key(const Table* t, ObjKey key, Mixed primary_key) override;
    void set(const Table* t, ColKey col_key, ObjKey key, Mixed value, _impl::Instruction) override;
    void list_clear(const CollectionBase& coll) override;
    void list_insert(const CollectionBase& coll, size_t idx, Mixed value, size_t) override;
    void set_insert(const CollectionBase& coll, size_t, Mixed value) override;
    void dictionary_insert(const CollectionBase& coll, size_t, Mixed key, Mixed value) override;

private:
    Table* get_table_in_destination_realm()
    {
        auto& dest = m_table_map[m_current.table];
        if (!dest) {
            dest = m_tr->get_table(m_current.table->get_name()).unchecked_ptr();
        }
        return dest;
    }
    ColKey get_colkey_in_destination_realm(ColKey c)
    {
        auto col_name = m_current.table->get_column_name(c);
        return get_table_in_destination_realm()->get_column_key(col_name);
    }
    void sync(const CollectionBase& coll)
    {
        auto t = coll.get_table();
        auto obj_key = coll.get_owner_key();
        sync(t.unchecked_ptr(), obj_key);
    }

    // Make m_current state match the input parameters
    void sync(const Table* t, ObjKey obj_key);

    // Returns link to target object - null if embedded object
    Mixed handle_link(ColKey col_key, Mixed val, util::FunctionRef<void(TableRef)> create_embedded_func);

    TransactionRef m_tr;
    struct State {
        // Table and object in source realm
        const Table* table = nullptr;
        ObjKey obj_key;
        // Corresponding object in destination realm
        Obj obj_in_destination;
    };
    State m_current;
    std::vector<State> m_states;
    std::map<const Table*, Table*> m_table_map;
};

} // namespace realm::_impl

#endif /* REALM_COPY_REPLICATION_HPP */
