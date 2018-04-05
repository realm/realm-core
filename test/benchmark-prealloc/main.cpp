/*************************************************************************
 *
 * Copyright 2016 Realm Inc.
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

#include <ctime>
#include <iostream>

#include <realm.hpp>
#include <realm/util/file.hpp>

using namespace realm;
using namespace realm::util;


namespace {

REALM_TABLE_2(Alpha, foo, Int, bar, Int)

} // anonymous namespace


#define DIR "/tmp"

int main()
{
    bool no_create = false;
    SharedGroupOptions::Durability dlevel = SharedGroupOptions::Durability::Full;

    File::try_remove(DIR "/benchmark-prealloc.realm");
    DB sg(DIR "/benchmark-prealloc.realm", no_create, {dlevel});

    File::try_remove(DIR "/benchmark-prealloc-interfere1.realm");
    DB sg_interfere1(DIR "/benchmark-prealloc-interfere1.realm", no_create, dlevel);

    File::try_remove(DIR "/benchmark-prealloc-interfere2.realm");
    DB sg_interfere2(DIR "/benchmark-prealloc-interfere2.realm", no_create, dlevel);

    File::try_remove(DIR "/benchmark-prealloc-interfere3.realm");
    DB sg_interfere3(DIR "/benchmark-prealloc-interfere3.realm", no_create, dlevel);

    int n_outer = 100;
    {
        time_t begin = time(0);

        int n_inner = 100;
        for (int i = 0; i < n_outer; ++i) {
            std::cerr << ".";
            for (int j = 0; j < n_inner; ++j) {
                {
                    WriteTransaction wt(sg);
                    Alpha::Ref t = wt.get_or_add_table<Alpha>("alpha");
                    for (int j = 0; j < 1000; ++j)
                        t->add(65536, 65536);
                    wt.commit();
                }
                // Interference
                for (int k = 0; k < 2; ++k) {
                    {
                        WriteTransaction wt(sg_interfere1);
                        Alpha::Ref t = wt.get_or_add_table<Alpha>("alpha");
                        for (int j = 0; j < 100; ++j)
                            t->add(65536, 65536);
                        wt.commit();
                    }
                    {
                        WriteTransaction wt(sg_interfere2);
                        Alpha::Ref t = wt.get_or_add_table<Alpha>("alpha");
                        for (int j = 0; j < 400; ++j)
                            t->add(65536, 65536);
                        wt.commit();
                    }
                    {
                        WriteTransaction wt(sg_interfere3);
                        Alpha::Ref t = wt.get_or_add_table<Alpha>("alpha");
                        for (int j = 0; j < 1600; ++j)
                            t->add(65536, 65536);
                        wt.commit();
                    }
                }
            }
        }
        std::cerr << "\n";

        time_t end = time(0);
        std::cerr << "Small write transactions per second = " << ((n_outer * n_inner * 7 / double(end - begin)))
                  << std::endl;
    }

    {
        time_t begin = time(0);

        int n_inner = 10;
        for (int i = 0; i < n_outer; ++i) {
            std::cerr << "x";
            for (int j = 0; j < n_inner; ++j) {
                {
                    WriteTransaction wt(sg);
                    Alpha::Ref t = wt.get_table<Alpha>("alpha");
                    t->column().foo += 1;
                    t->column().bar += 1;
                    wt.commit();
                }
            }
        }
        std::cerr << "\n";

        time_t end = time(0);
        std::cerr << "Large write transactions per second = " << ((n_outer * n_inner / double(end - begin)))
                  << std::endl;
    }
}
