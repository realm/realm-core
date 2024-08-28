#include <util/sync/redirect_server.hpp>
#include <realm/util/future.hpp>
#include <realm/util/logger.hpp>

#include <future>
#include <iostream>
#include <memory>
#include <signal.h>

using namespace realm;

static std::shared_ptr<std::promise<void>> s_program_promise;
static std::shared_ptr<util::Logger> s_logger;

static void signalHandler(int signum)
{
    std::cerr << "Interrupt signal (" << signum << ") received.\n";

    // Only complete promise one time
    if (s_program_promise) {
        s_program_promise->set_value();
        s_program_promise.reset();
    }

    exit(signum);
}

static void print_usage(std::string_view executable, std::optional<std::string> error = std::nullopt)
{
    if (error) {
        std::cerr << executable << " failed: " << *error << std::endl;
    }
    std::cout << "usage: " << executable << " [-h|--help] [REDIRECT_URL [LISTEN_PORT]]\n" << std::endl;
    exit(error ? 1 : 0);
}

int main(int argc, char* argv[])
{
    // redir_server [-h|--help] [REDIRECT_URL [LISTEN_PORT]]
    int port = 0;
    std::string redir_url = "http://localhost:9090";

    // Parse the argument list
    constexpr int MAX_ARGS = 2;

    int arg_cnt = std::min(argc - 1, MAX_ARGS);
    std::string args[arg_cnt];
    for (int i = 0; i < arg_cnt; i++) {
        args[i] = argv[i + 1];
        if (args[i] == "-h" || args[i] == "--help") {
            print_usage(argv[0]);
        }
    }
    if (arg_cnt > 0) {
        redir_url = args[0];
        if (redir_url.empty()) {
            print_usage(argv[0], "REDIRECT_URL argument was empty");
        }
    }
    if (arg_cnt > 1) {
        port = atoi(args[1].c_str());
        if (port <= 0 || port > 0xFFFF) {
            print_usage(argv[0], util::format("invalid LISTEN_PORT value: %1", port));
        }
    }
    s_logger = std::make_shared<util::StderrLogger>(util::Logger::Level::debug);

    // register signal SIGINT, SIGTERM, and signal handler
    signal(SIGINT, signalHandler);
    signal(SIGTERM, signalHandler);

    s_program_promise = std::make_shared<std::promise<void>>();
    auto future = s_program_promise->get_future();

    try {
        auto server = sync::RedirectingHttpServer(redir_url, port, s_logger);
        std::cout << "=====================================================\n";
        std::cout << "* Server listen port: " << server.base_url() << "\n";
        std::cout << "* Redirect base url:  " << redir_url << "\n";
        std::cout << "=====================================================\n" << std::endl;
        future.get();
    }
    catch (std::exception e) {
        std::cerr << "Error running server: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}
