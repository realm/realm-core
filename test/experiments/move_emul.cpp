#include <iostream>
using namespace std;


namespace realm {

    struct Data {
        Data() { cout << "Data()\n"; }
        ~Data() { cout << "~Data()\n"; }
        Data* clone() const { return new Data(); }
    };


    struct CopyAndMove {
        CopyAndMove(): m_data(new Data()) {}
        ~CopyAndMove() { delete m_data; }

        CopyAndMove(const CopyAndMove& a): m_data(a.m_data->clone()) { cout << "Copy CopyAndMove (constructor)\n"; }
        CopyAndMove& operator=(CopyAndMove a) { delete m_data; m_data = a.m_data; a.m_data = 0; cout << "Move CopyAndMove (assign)\n"; return *this; }

        friend CopyAndMove move(CopyAndMove& a) { Data* d = a.m_data; a.m_data = 0; cout << "Move CopyAndMove (move)\n"; return CopyAndMove(d); }

    private:
        friend class ConstCopyAndMove;

        Data* m_data;

        CopyAndMove(Data* d): m_data(d) {}
    };


    struct ConstCopyAndMove {
        ConstCopyAndMove(): m_data(new Data()) {}
        ~ConstCopyAndMove() { delete m_data; }

        ConstCopyAndMove(const ConstCopyAndMove& a): m_data(a.m_data->clone()) { cout << "Copy ConstCopyAndMove (constructor)\n"; }
        ConstCopyAndMove& operator=(ConstCopyAndMove a) { delete m_data; m_data = a.m_data; a.m_data = 0; cout << "Move ConstCopyAndMove (assign)\n"; return *this; }

        ConstCopyAndMove(CopyAndMove a): m_data(a.m_data) { a.m_data = 0; cout << "Move CopyAndMove to ConstCopyAndMove (constructor)\n"; }
        ConstCopyAndMove& operator=(CopyAndMove a) { delete m_data; m_data = a.m_data; a.m_data = 0; cout << "Move CopyAndMove to ConstCopyAndMove (assign)\n"; return *this; }

        friend ConstCopyAndMove move(ConstCopyAndMove& a) { const Data* d = a.m_data; a.m_data = 0; cout << "Move ConstCopyAndMove (move)\n"; return ConstCopyAndMove(d); }

    private:
        const Data* m_data;

        ConstCopyAndMove(const Data* d): m_data(d) {}
    };

} // namespace realm



realm::CopyAndMove func(realm::CopyAndMove a) { return move(a); }

int main()
{
    realm::CopyAndMove x1, x2;
    cout << "---A---\n";
    x2 = x1;
    cout << "---B---\n";
    x2 = move(x1);

    cout << "---0---\n";
    realm::CopyAndMove a1;
    cout << "---1---\n";
    realm::CopyAndMove a2 = func(func(func(func(a1)))); // One genuine copy, and 'a1' is left untouched
    cout << "---2---\n";
    realm::CopyAndMove a3 = func(func(func(func(move(a2))))); // Zero genuine copies, and 'a2' is left truncated
    cout << "---3---\n";
    const realm::CopyAndMove a4(a3); // Copy
    cout << "---4---\n";
    realm::CopyAndMove a5(a4); // Copy from const
    cout << "---5---\n";
    static_cast<void>(a5);

    realm::ConstCopyAndMove b1(a1); // One genuine copy
    cout << "---6---\n";
    realm::ConstCopyAndMove b2(move(a1)); // Zero genuine copies, and 'a1' is left truncated
    cout << "---7---\n";
    realm::ConstCopyAndMove b3(a4); // One genuine copy from const
    cout << "---8---\n";
    realm::ConstCopyAndMove b4(func(func(func(func(a3))))); // One genuine copy, and 'a3' is left untouched
    cout << "---9---\n";
    realm::ConstCopyAndMove b5(func(func(func(func(move(a3)))))); // Zero genuine copies, and 'a3' is left truncated
    static_cast<void>(b1);
    static_cast<void>(b2);
    static_cast<void>(b3);
    static_cast<void>(b4);
    static_cast<void>(b5);

    return 0;
}
