/*************************************************************************
 *
 * Copyright 2022 Realm Inc.
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

#ifndef REALM_LINK_TRANSLATOR_HPP
#define REALM_LINK_TRANSLATOR_HPP

#include <realm/obj.hpp>

namespace realm {

// This construct is used when code needs to handle all
// possible link column types. Subclass and override all
// methods to handle each type. A good example of where
// this is useful is when following a backlink to its
// origin column and modifying the outgoing link from
// whatever container it came from.
class LinkTranslator {
public:
    LinkTranslator(Obj origin, ColKey origin_col_key);
    void run();
    virtual void on_list_of_links(LnkLst& list) = 0;
    virtual void on_list_of_mixed(Lst<Mixed>& list) = 0;
    virtual void on_set_of_links(LnkSet& set) = 0;
    virtual void on_set_of_mixed(Set<Mixed>& set) = 0;
    virtual void on_dictionary(Dictionary& dict) = 0;
    virtual void on_link_property(ColKey col) = 0;
    virtual void on_mixed_property(ColKey col) = 0;

protected:
    Obj m_origin_obj;
    ColKey m_origin_col_key;
};

} // namespace realm

#endif // REALM_LINK_TRANSLATOR_HPP
