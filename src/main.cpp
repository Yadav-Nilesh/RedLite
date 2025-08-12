#include "../include/RedisServer.h"
#include "../include/RedisDatabase.h"
#include<iostream>
#include<thread>
#include <chrono>

int main(int argc, char *argv[]) {
    int port = 6379;
    if (argc >=2) port = std::stoi(argv[1]);

    if (RedisDatabase::getInstance().load("dump.redlite")) {
        std::cout << "Loading RedisDatabase successfully." << std::endl;
    }else {
        std::cout<< "No previous database found, starting with an empty database." << std::endl;
    }

    RedisServer server(port);

    // Background persistance: dump the database to disk every 5 mins(300 seconds)

    std:: thread persistanceThread([]() {
        while (true) {
            std::this_thread::sleep_for(std::chrono::seconds(300));
            if (!RedisDatabase::getInstance().dump("dump.redlite")) {
                std::cerr << "Failed to persist database." << std::endl;
            } else {
                std::cout << "Database persisted successfully to dump.redlite" << std::endl;
            }
        }
    });
    persistanceThread.detach();


    server.run();
    return 0;

}