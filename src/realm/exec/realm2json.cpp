#include <realm.hpp>
#include <iostream>

int main(int argc, char const* argv[])
{
    if (argc > 1) {
        realm::Group g(argv[1]);
        size_t link_depth = 0;
        if (argc > 2) {
            link_depth = strtol(argv[2], nullptr, 0);
        }
        g.to_json(std::cout, link_depth);
    }
    return 0;
}
