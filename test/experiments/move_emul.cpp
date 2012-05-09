#include <iostream>
using namespace std;

// std::forward?
// Move emul works in C++11?
// Binding 'const A' to T in template<class T> foo(T&) C++03 / C++11?
// C++11 decltype and auto gives access to private type through return of value of that type from method? Yes
// Expand to const non-const parent scenario
// How to also support proper copying?

/*
template<class T> class rv: public T {
  rv();
  ~rv();
  rv(const rv&);
  void operator=(const rv&);
};
*/


struct A {
  A() {}

private:
  struct move { A* a; move(A* a): a(a) {} };
  struct copy { const A* a; copy(const A* a): a(a) {} };

  int i;

public:
  operator move() { return move(this); }
  operator copy() const { return copy(this); }
  A(move) { cout << "Move construct from non-const r-value\n"; }
  A& operator=(move) { cout << "Move assign from non-const r-value\n"; return *this; }
  A(A&) { cout << "Copy construct from non-const l-value\n"; }
  A(copy) { cout << "Copy construct from const value\n"; }
  A& operator=(A&) { cout << "Copy assign from non-const l-value\n"; return *this; }
  A& operator=(copy) { cout << "Copy assign from const value\n"; return *this; }

/*
  A(rv<A>&) {}
  A& operator=(rv<A>&) { return *this; }
  operator rv<A>&() { return static_cast<rv<A>&>(*this); }
*/
};


A func() { return A(); }

int main()
{
  A a = func();
cout << a.i << "A\n";
  A b(a);
cout << "B\n";

  return 0;
}
