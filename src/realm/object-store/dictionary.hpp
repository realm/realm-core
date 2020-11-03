////////////////////////////////////////////////////////////////////////////
//
// Copyright 2020 Realm Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
////////////////////////////////////////////////////////////////////////////

#ifndef REALM_OS_DICTIONARY_HPP_
#define REALM_OS_DICTIONARY_HPP_

#include <realm/object-store/collection.hpp>
#include <realm/dictionary.hpp>

namespace realm {
namespace object_store {

class Dictionary : public object_store::Collection {
public:
    Dictionary() noexcept;
    Dictionary(std::shared_ptr<Realm> r, const Obj& parent_obj, ColKey col);
    Dictionary(std::shared_ptr<Realm> r, const realm::Dictionary& list);
    ~Dictionary() override;

    template <typename T>
    void insert(StringData key, T value);

private:
    realm::Dictionary* m_dict;
};

template <typename T>
void Dictionary::insert(StringData key, T value)
{
    verify_in_transaction();
    m_dict->insert(key, value);
}

} // namespace object_store
} // namespace realm


#endif /* REALM_OS_DICTIONARY_HPP_ */
