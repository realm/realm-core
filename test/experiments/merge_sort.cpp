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


/*
//56 ms
void merge_core(Array *a1, Array *a2, Array *res) {
    size_t wush = 0;
    int64_t v0, v1;
    size_t p0 = 0, p1 = 0; //p[2] = {0, 0};
    size_t s1 = a1->Size();
    size_t s2 = a2->Size();

    v0 = a1->Get<8>(p0);
    v1 = a2->Get<8>(p1);

    for(size_t i = 0; p0  < s1; i++) {


        if(v0 < v1) {
            wush += v0;
            p0++;
            v0 = a1->Get<8>(p0);

            if(v0 < v1) {
                wush += v0;
                p0++;
                v0 = a1->Get<8>(p0);
            }
            else {
                wush += v1;
                p1++;
                v1 = a2->Get<8>(p1);
            }



        }
        else {
            wush += v1;
            p1++;
            v1 = a2->Get<8>(p1);

            if(v0 < v1) {
                wush += v0;
                p0++;
                v0 = a1->Get<8>(p0);
            }
            else {
                wush += v1;
                p1++;
                v1 = a2->Get<8>(p1);
            }



        }



    }
    volatile size_t wush2 = wush;
}

*/


/*
// 37 ms
void merge_core(Array *a0, Array *a1, Array *res) {
    size_t wush = 0;
    tos = 0;
    int64_t v0, v1, vv0, vv1;
    size_t p0 = 0, p1 = 0; //p[2] = {0, 0};
    size_t s0 = a0->Size();
    size_t s1 = a1->Size();

    v0 = a0->Get<8>(p0);
    v1 = a1->Get<8>(p1);

    for(size_t i = 0; p0 + 1 < s0 && p1 + 1 < s1; i++) {
        if(v0 < v1) {
            vv0 = a1->Get<8>(++p0);
            if(v0 < vv0) {
                list[tos++] = v0;
                list[tos++] = vv0;
                v0 = a0->Get<8>(++p0);
            }
            else {
                list[tos++] = vv0;
                list[tos++] = v0;
                v0 = a0->Get<8>(++p0);
                v1 = a1->Get<8>(++p1);
            }
        }
        else {
            vv1 = a1->Get<8>(++p1);
            if(v1 < vv1) {
                list[tos++] = v1;
                list[tos++] = vv1;
                v0 = a0->Get<8>(++p0);
                v1 = a1->Get<8>(++p1);
            }
            else {
                list[tos++] = vv1;
                list[tos++] = v1;
                v1 = a1->Get<8>(++p1);
            }
        }

    }


    volatile size_t wush2 = wush;
}
*/


/*
// 50 ms
void merge_core(Array *a1, Array *a2, Array *res) {
    size_t wush = 0;
    int64_t v0, v1;
    size_t p0 = 0, p1 = 0; //p[2] = {0, 0};
    size_t s1 = a1->Size();
    size_t s2 = a2->Size();

    for(size_t i = 0; p0  < s1 && p1  < s2; i++) {

        v0 = a1->Get<32>(p0 >> 2);
        v1 = a2->Get<32>(p1 >> 2);

        if(v0 < v1) {
            // v0 < v1 implies v0 >> (64 - 8) <= v1 >> (64 - 8)
            wush += v0 >> (64 - 8);
            p0++;
            v0 <<= 8;
        }
        else {
            wush += v1 >> (64 - 8);
            p1++;
            v1 <<= 8;
        }


        if(v0 < v1) {
            // v0 < v1 implies v0 >> (64 - 8) <= v1 >> (64 - 8)
            wush += v0 >> (64 - 8);
            p0++;
            v0 <<= 8;
        }
        else {
            wush += v1 >> (64 - 8);
            p1++;
            v1 <<= 8;
        }


        if(v0 < v1) {
            // v0 < v1 implies v0 >> (64 - 8) <= v1 >> (64 - 8)
            wush += v0 >> (64 - 8);
            p0++;
            v0 <<= 8;
        }
        else {
            wush += v1 >> (64 - 8);
            p1++;
            v1 <<= 8;
        }


        if(v0 < v1) {
            // v0 < v1 implies v0 >> (64 - 8) <= v1 >> (64 - 8)
            wush += v0 >> (64 - 8);
            p0++;
            v0 <<= 8;
        }
        else {
            wush += v1 >> (64 - 8);
            p1++;
            v1 <<= 8;
        }
    }
    volatile size_t wush2 = wush;
}
*/


/*
// Branch free merge :)
// 130 ms
void merge_core(Array *a1, Array *a2, Array *res) {
    size_t wush = 0;
    Array *a[2] = {a1, a2};
    int64_t v = 0;
    int64_t p = 0;
    size_t s0 = a[0]->Size();
    size_t s1 = a[1]->Size();
    for(size_t i = 0; (p & 0xffff) < s0; i++) {

        v = a1->Get<8>(p & 0xffff);
        v = a2->Get<8>(p >> 16) << 16;
        int m = (v & 0xffff) < (v >> 16) ? 0 : 1; // cmovg
        wush += (v >> (m * 16) & 0xffff);
        p += 1 + (m * 0x10000);


    }

        volatile size_t wush2 = wush;
}
*/


/*
// Branch free merge :)
// 130 ms
void merge_core(Array *a1, Array *a2, Array *res) {
    size_t wush = 0;
    Array *a[2] = {a1, a2};
    int64_t v = 0;
    int64_t p = 0;
    size_t s0 = a[0]->Size();
    size_t s1 = a[1]->Size();
    for(size_t i = 0; (p & 0xffff) < s0; i++) {

        v = a1->Get<8>(p & 0xffff);
        v = a2->Get<8>(p >> 16);
        int m = (v & 0xffff) < (v >> 16) ? 0 : 1; // cmovg
        wush += (v >> (m * 16) & 0xffff);
        p += 1 + (m * 0x10000);
    }

        volatile size_t wush2 = wush;
}
*/


/*
// Branch free merge :)
void merge_core(Array *a1, Array *a2, Array *res) {
    size_t wush = 0;
    Array *a[2] = {a1, a2};
    int64_t v0, v1;
    int32_t p[2] = {0, 0};
    size_t s0 = a[0]->Size();
    size_t s1 = a[1]->Size();
    for(size_t i = 0; p[0]  < s0; i++) {

        v0 = a[0]->Get<8>(p[0]);
        v1 = a[1]->Get<8>(p[1]);
        int m = v0 < v1 ? 0 : 1; // cmovg
        wush += a[m]->Get<8>(p[m]);
//      p[m]++;
        *((int64_t *)p) += 1 + 0x100000000 * m; //bug

    }
}
*/


/*
// Each time two input lists are merged into a new list, the old ones can be destroyed to save
// memory. Set delete_old to true to do that.
Array *merge(Array *ArrayList, bool delete_old) {
    if(ArrayList->Size() == 1) {
        size_t ref = ArrayList->Get(0);
        Array *a = new Array(ref);
        return a;
    }

    Array Left, Right;
    size_t left = ArrayList->Size() / 2;
    for(size_t t = 0; t < left; t++)
        Left.Add(ArrayList->Get(t));
    for(size_t t = left; t < ArrayList->Size(); t++)
        Right.Add(ArrayList->Get(t));

    Array *l;
    Array *r;
    Array *res = new Array();

    // We merge left-half-first instead of bottom-up so that we access the same data in each call
    // so that it's in cache, at least for the first few iterations until lists get too long
    l = merge(&Left, true);
    r = merge(&Right, true);

    UnitTest::Timer timer;
    unsigned int g = -1;
    unsigned int ms;
    for(int j = 0; j < 20; j++) {
        timer.Start();

        for(int i = 0; i < 20000; i++)
            merge_core(l, r, res);
        ms = timer.GetTimeInMs();
        if (ms < g)
            g = ms;
        printf("%d ms\n", ms);
    }
    printf("\n%d ms\n", g);
    getchar();


    if(delete_old) {
//      l->Destroy();
//      r->Destroy();
//      delete(l);
//      delete(r);
    }

    return res;
}*/


/*
// Branch free merge :)
// 320 ms
void merge_core(Array *a1, Array *a2, Array *res) {
    size_t wush = 0;
    Array *a[2] = {a1, a2};
    int64_t v[2]; // todo, test non-subscripted v
    size_t p[2] = {0, 0};

    for(size_t i = 0; p[0] < a[0]->Size() && p[1] < a[1]->Size(); i++) {
        v[0] = a[0]->Get(p[0]);
        v[1] = a[1]->Get(p[1]);
        size_t m = v[0] < v[1] ? 0 : 1; // cmovg
        //res->Add(v[m]);
        wush += v[m];
        p[m]++;
    }

    for(size_t t = 0; t < 2; t++)
        for(; p[t] < a[t]->Size(); p[t]++)
            res->Add(a[t]->Get(p[t]));

    volatile size_t wush2 = wush;
}

*/

/*
// Branch free merge :)
// 396 ms
void merge_core(Array *a1, Array *a2, Array *res) {
    size_t wush = 0;
    Array *a[2] = {a1, a2};
    int64_t v0, v1;
    size_t p[2] = {0, 0};
    size_t s0 = a[0]->Size();
    size_t s1 = a[1]->Size();
    for(size_t i = 0; p[0]  < s0; i++) {

        v0 = a[0]->Get<8>(p[0]);
        v1 = a[1]->Get<8>(p[1]);
        int m = v0 < v1 ? 0 : 1; // cmovg
        wush += a[m]->Get<8>(p[m]);
        p[m]++;


    }



}
*/


/*
// 74 ms
void merge_core(Array *a1, Array *a2, Array *res) {
    size_t wush = 0;
    int64_t v0, v1;
    size_t p0 = 0, p1 = 0; //p[2] = {0, 0};
    size_t s1 = a1->Size();
    size_t s2 = a2->Size();

    for(size_t i = 0; p0  < s1 && p1 < s2; i++) {

        v0 = a1->Get<8>(p0);
        v1 = a2->Get<8>(p1);
        if(v0 < v1) {
            wush += v0;
            p0++;
        }
        else {
            wush += v1;
            p1++;
        }
    }
    volatile size_t wush2 = wush;
}
*/

/*
void merge_core(Array *a1, Array *a2, Array *res) {
    size_t wush = 0;
    int64_t v0, v1;
    size_t p0 = 0, p1 = 0; //p[2] = {0, 0};
    size_t s1 = a1->Size();
    size_t s2 = a2->Size();

    v0 = a1->Get<64>(p0);
    v1 = a2->Get<64>(p1);

    for(size_t i = 0; p0 < s1 && p1 < s2; i++) {
        if(v0 < v1) {
            wush += v0;
            p0++;
            v0 = a1->Get<8>(p0);
        }
        else {
            wush += v1;
            p1++;
            v1 = a2->Get<8>(p1);
        }

    }
}

*/

/*
// 45 ms
void merge_core(Array *a1, Array *a2, Array *res) {
    size_t wush = 0;
    int64_t v0, v1;
    size_t p0 = 0, p1 = 0; //p[2] = {0, 0};
    size_t s1 = a1->Size();
    size_t s2 = a2->Size();

    v0 = a1->Get<8>(p0);
    v1 = a2->Get<8>(p1);

    for(size_t i = 0; p0  < s1 && p1 < s2; i++) {


        if(v0 < v1) {
            wush += v0;
            p0++;
            v0 = a1->Get<8>(p0);
        }
        else {
            wush += v1;
            p1++;
            v1 = a2->Get<8>(p1);
        }


        if(v0 < v1) {
            wush += v0;
            p0++;
            v0 = a1->Get<8>(p0);
        }
        else {
            wush += v1;
            p1++;
            v1 = a2->Get<8>(p1);
        }


        if(v0 < v1) {
            wush += v0;
            p0++;
            v0 = a1->Get<8>(p0);
        }
        else {
            wush += v1;
            p1++;
            v1 = a2->Get<8>(p1);
        }


        if(v0 < v1) {
            wush += v0;
            p0++;
            v0 = a1->Get<8>(p0);
        }
        else {
            wush += v1;
            p1++;
            v1 = a2->Get<8>(p1);
        }

    }
    volatile size_t wush2 = wush;
}
*/

/*
// 61 ms,  8*unr = 46, 8*=61, 4*unrolled = 62
void merge_core(Array *a0, Array *a1, Array *res) {
    tos = 0;
    size_t wush = 0;
    uint64_t v0, v1;
    size_t p0 = 15, p1 = 0;
    size_t s0 = a0->Size();
    size_t s1 = a1->Size();

    for(size_t i = 0; p0 + 8 < s0 && p1 + 8 < s1; i++) {

        v0 = a0->Get<8>(p0 + 0) << 0*8 |
             a0->Get<8>(p0 + 1) << 1*8 |
             a0->Get<8>(p0 + 2) << 2*8 |
             a0->Get<8>(p0 + 3) << 3*8 |
             a0->Get<8>(p0 + 4) << 4*8 |
             a0->Get<8>(p0 + 5) << 5*8 |
             a0->Get<8>(p0 + 6) << 6*8 |
             a0->Get<8>(p0 + 7) << 7*8;

        v1 = a1->Get<8>(p1 + 0) << 0*8 |
             a1->Get<8>(p1 + 1) << 1*8 |
             a1->Get<8>(p1 + 2) << 2*8 |
             a1->Get<8>(p1 + 3) << 3*8 |
             a1->Get<8>(p1 + 4) << 4*8 |
             a1->Get<8>(p1 + 5) << 5*8 |
             a1->Get<8>(p1 + 6) << 6*8 |
             a1->Get<8>(p1 + 7) << 7*8;

        if(v0 < v1) {
            list[tos] = v0 >> 7*8;
            p0++;
            v1 <<= 8;
        }
        else {
            list[tos] = v1 >> 7*8;
            v0 <<= 8;
            p1++;
        }
        tos++;


        if(v0 < v1) {
            list[tos] = v0 >> 7*8;
            p0++;
            v1 <<= 8;
        }
        else {
            list[tos] = v1 >> 7*8;
            v0 <<= 8;
            p1++;
        }
        tos++;


        if(v0 < v1) {
            list[tos] = v0 >> 7*8;
            p0++;
            v1 <<= 8;
        }
        else {
            list[tos] = v1 >> 7*8;
            v0 <<= 8;
            p1++;
        }
        tos++;


        if(v0 < v1) {
            list[tos] = v0 >> 7*8;
            p0++;
            v1 <<= 8;
        }
        else {
            list[tos] = v1 >> 7*8;
            v0 <<= 8;
            p1++;
        }
        tos++;


        if(v0 < v1) {
            list[tos] = v0 >> 7*8;
            p0++;
            v1 <<= 8;
        }
        else {
            list[tos] = v1 >> 7*8;
            v0 <<= 8;
            p1++;
        }
        tos++;


        if(v0 < v1) {
            list[tos] = v0 >> 7*8;
            p0++;
            v1 <<= 8;
        }
        else {
            list[tos] = v1 >> 7*8;
            v0 <<= 8;
            p1++;
        }
        tos++;


        if(v0 < v1) {
            list[tos] = v0 >> 7*8;
            p0++;
            v1 <<= 8;
        }
        else {
            list[tos] = v1 >> 7*8;
            v0 <<= 8;
            p1++;
        }
        tos++;


        if(v0 < v1) {
            list[tos] = v0 >> 7*8;
            p0++;
            v1 <<= 8;
        }
        else {
            list[tos] = v1 >> 7*8;
            v0 <<= 8;
            p1++;
        }
        tos++;




    }
    volatile size_t wush2 = wush;
}
*/
