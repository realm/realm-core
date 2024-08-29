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
    std::cout << "usage: " << executable
              << " [-h|--help] [-r|--http-redirect] [-w|--ws-redirect] [REDIRECT_URL [LISTEN_PORT]]\n"
              << std::endl;
    exit(error ? 1 : 0);
}

int main(int argc, char* argv[])
{
    // redir_server [-h|--help] [REDIRECT_URL [LISTEN_PORT]]
    int port = 0;
    constexpr std::string_view default_url = "http://localhost:9090";
    bool http_redirect = false;
    bool websocket_redirect = false;
    std::string redir_url;
    int arg_stg_cnt = 0;
    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        if (arg[0] == '-') {
            if (arg == "-h" || arg == "--help") {
                print_usage(argv[0]);
            }
            else if (arg == "-r" || arg == "--http-redirect") {
                http_redirect = true;
            }
            else if (arg == "-w" || arg == "--ws-redirect") {
                websocket_redirect = true;
            }
            else {
                print_usage(argv[0], util::format("invalid argument: %1", argv));
            }
        }
        else {
            if (arg_stg_cnt == 0) {
                redir_url = arg;
                if (redir_url.empty()) {
                    print_usage(argv[0], "REDIRECT_URL cannot be empty");
                }
            }
            if (arg_stg_cnt == 1) {
                port = atoi(argv[i]);
                if (port <= 0 || port > 0xFFFF) {
                    print_usage(argv[0], util::format("invalid LISTEN_PORT value: %1", port));
                }
            }
            if (arg_stg_cnt > 1) {
                print_usage(argv[0], util::format("invalid argument: %1", arg));
            }
            arg_stg_cnt++;
        }
    }
    s_logger = std::make_shared<util::StderrLogger>(util::Logger::Level::debug);

    // register signal SIGINT, SIGTERM, and signal handler
    signal(SIGINT, signalHandler);
    signal(SIGTERM, signalHandler);

    s_program_promise = std::make_shared<std::promise<void>>();
    auto future = s_program_promise->get_future();
    if (redir_url.empty()) {
        redir_url = default_url;
    }

    try {
        auto server = sync::RedirectingHttpServer(redir_url, port, s_logger);
        server.force_http_redirect(http_redirect);
        server.force_websocket_redirect(websocket_redirect);
        std::cout << "=====================================================\n";
        std::cout << "* Listen port: " << server.base_url() << "\n";
        std::cout << "*  Server URL: " << server.server_url() << "\n";
        std::cout << "* Location details:\n";
        std::cout << "*      hostname: " << server.location_hostname() << (http_redirect ? " (redirecting)\n" : "\n");
        std::cout << "*   ws_hostname: " << server.location_wshostname()
                  << (websocket_redirect ? " (redirecting)\n" : "\n");
        std::cout << "=====================================================\n" << std::endl;
        future.get();
    }
    catch (std::exception e) {
        std::cerr << "Error running server: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}
