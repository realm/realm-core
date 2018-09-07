#include <iostream>
#include <string>
void get_hello(std::string&);
int main() {
    std::string message;
    get_hello(message);
    std::cout << message << "\n";
    return 0;
}
