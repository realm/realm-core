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

#include <realm.hpp>
#include <realm/group_shared.hpp>
#include <pthread.h>

#include <unistd.h>

using namespace realm;

struct thread_info {
    pthread_t thread_id;
    int       thread_num;
};


REALM_TABLE_3(People,
              name, String,
              age,  Int,
              hired, Bool)

REALM_TABLE_2(Books,
              title, String,
              author, String)


void* reader(void*)
{
    SharedGroup sg("test.realm");

    // Read transaction
    {
        const Group& g = sg.begin_read();
        Books::ConstRef t = g.get_table<Books>("books");
        std::cout << "Books: " << t->size() << std::endl;
        sg.end_read();
    }

    while (!sg.has_changed()) { // wait for an update
        sleep(2);
        std::cout << "No updates" << std::endl;
    }

    {
        const Group& g = sg.begin_read();
        Books::ConstRef t = g.get_table<Books>("books");
        std::cout << "Books: " << t->size() << std::endl;
        sg.end_read();
    }

    return NULL;
}

void* writer(void*)
{
    SharedGroup sg("test.realm");

    sleep(5);

    // Write transaction
    {
        std::cout << "Adding book" << std::endl;
        Group& g = sg.begin_write();
        Books::Ref t = g.get_table<Books>("books");
        t->add("Solaris", "Stanislaw Lem");
        sg.commit();
    }

    return NULL;
}

int main()
{
    pthread_attr_t attr;
    struct thread_info tinfo[2];
    void *res;

    pthread_attr_init(&attr);
    tinfo[0].thread_num = 1;
    tinfo[1].thread_num = 2;

    pthread_create(&tinfo[0].thread_id, &attr, &reader, &tinfo[0]);
    pthread_create(&tinfo[1].thread_id, &attr, &writer, &tinfo[1]);

    pthread_join(tinfo[0].thread_id, &res);
    pthread_join(tinfo[1].thread_id, &res);
}
