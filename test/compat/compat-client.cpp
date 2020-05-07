#include <thread>
#include <iostream>

#include <realm.hpp>
#include <realm/sync/client.hpp>
#include <realm/sync/history.hpp>

using version_type = realm::sync::ClientHistory::version_type;

const std::string token =
    "eyJpZGVudGl0eSI6InNvbWVvbmUiLCJhY2Nlc3MiOiBbImRvd25sb2FkIiwgInVwbG9hZCJdLCJ0aW1lc3RhbXAiOjEyMywiZXhwaXJlcyI6bnVs"
    "bCwiYXBwX2lkIjogImlvLnJlYWxtLkV4YW1wbGUifQo="
    ":"
    "F5AsGuW9QgkLJlCo2X1Sn/"
    "cBAESDZIoOethiTMdB5Ko7blpDNcG5gjJcC3mOUekOETwSTY0vK+qBF96a+"
    "Rvlw8XD3dlrL8Cex8ofmDRYhJQcB3EG3lb9HHhET7iIWfXbojhyinwE3ZHLPl3D0WwCbTHA4H6QY70qY88bJzziSRBR2pCCLAKMSfWBbdnQ98V/"
    "ASOKY4HZc8s7bl5021w6Zl3Stq63igrdst923Bt8NstHIerbpZDis8yPyJpc3CkQ9gfNLwlRHBC68f8yhVbR7JlCzfdSOT4o6+"
    "vqq54MbTNFHt9VJ5vZSuxrvBmafNkwFpMlnyuqDCkQL9OykoJjog==";

void error_handler(std::error_code /*ec*/, bool /*is_fatal*/, const std::string& detailed_message)
{
    std::cerr << "fail: " << detailed_message << std::endl;
    std::exit(1);
}

int main(int argc, char** argv)
{
    realm::sync::Client::Config cfg;
    cfg.reconnect_mode = realm::sync::Client::ReconnectMode::never;
    realm::sync::Client client(cfg);

    std::thread client_thread([&client] {
        client.run();
    });

    for (int argno = 1; argno < argc; argno++) {
        std::string realmfile = argv[argno];
        realm::sync::Session s(client, realmfile);
        s.set_error_handler(error_handler);
        s.bind("localhost", "/test", token, 7800);


        s.wait_for_download_complete_or_client_stopped();

        std::unique_ptr<realm::Replication> h = realm::sync::make_client_history(realmfile);
        std::unique_ptr<realm::SharedGroup> sg = std::make_unique<realm::SharedGroup>(*h);

        {
            realm::WriteTransaction t(*sg);
            if (!t.has_table("mytable")) {
                realm::TableRef table = t.add_table("mytable");
                table->add_column(realm::type_Int, "a");
                table->add_column(realm::type_Int, "b");

                auto idx = table->add_empty_row(10);
                for (int i = 0; i < 10; i++) {
                    table->set_int(0, idx + i, (idx + i) * 2 + 0);
                    table->set_int(1, idx + i, (idx + i) * 2 + 1);
                }
            }
            version_type new_version = t.commit();
            s.nonsync_transact_notify(new_version);
        }

        {
            realm::WriteTransaction t(*sg);
            realm::TableRef table = t.get_table("mytable");

            for (int i = 0; i < 10; i++) {
                table->set_int(0, i, i * 2 + 0);
                table->set_int(1, i, i * 2 + 1);
            }

            version_type new_version = t.commit();
            s.nonsync_transact_notify(new_version);
        }

        s.wait_for_upload_complete_or_client_stopped();
    }

    client.stop();
    client_thread.join();

    std::cout << "client ok" << std::endl;
    return 0;
}
