

/*
// This code snippet will generate the collation_order[] table in utf8.cpp::utf8_compare() method. See
// further documentation there.

const wchar_t end = 0x250;    // up to and including Latin Extended B
std::vector<wchar_t> vec;
std::locale l = std::locale("");
for (size_t t = 0; t < end; t++) {
    wstring s;
    s.push_back(wchar_t(t));
    size_t j;
    for (j = 0; j < vec.size(); j++) {
        wstring s2;
        s2.push_back(vec[j]);
        bool less = l(s, s2);
        if (less) {
            break;
        }
    }
    vec.insert(vec.begin() + j, wchar_t(t));
}

uint32_t inverse[end];
for (size_t t = 0; t < end; t++) {
    int v = vec[t];
    inverse[v] = t;
}

std::cout << "static const uint32_t collation_order[] = {\n";
for (size_t t = 0; t < end; t++) {
    std::cout << (t == 0 ? "" : ", ") << inverse[t];
    if (t != 0 && t % 100 == 0)
        cerr << "\n";
}
std::cout << "};";
*/


