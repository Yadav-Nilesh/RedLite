//
// Created by ajay on 18/6/25.
//

#include "../include/RedisServer.h"
#include "../include/RedisCommandHandler.h"
#include "../include/RedisDatabase.h"
#include <bits/stdc++.h>
#include <sys/socket.h>
#include<unistd.h>
#include<netinet/in.h>
#include<thread>
#include <csignal>
#include<signal.h>
using namespace std;

static RedisServer *globalServer =  nullptr;

void signalHandler(int signum) {
    if (globalServer) {
        cout<< "Received signal " << signum << ".. Shutting Down..."<<endl;
        globalServer->shutdown();
    }
    exit(signum);
}

void RedisServer::setupSignalHandler() {
    signal(SIGINT, signalHandler);
}

RedisServer::RedisServer(int port): port(port), server_socket(-1), running(true){
    globalServer = this;
    setupSignalHandler();
}

void RedisServer::shutdown() {
    running = false;
    if (server_socket != -1) {
        // persist database before shutdown;
        if (!RedisDatabase::getInstance().dump("dump.redlite")) {
            cerr << "Failed to persist database." << endl;
        } else {
            cout << "Database persisted successfully to dump.redlite" << endl;
        }
        close(server_socket);
    }
    cout<< "Server shutdown" << endl;
}

void RedisServer::run() {
    server_socket = socket(AF_INET, SOCK_STREAM, 0);
    if (server_socket <0) {
        cerr<< "Error creating socket" << endl;
        return;
    }
    int opt = 1;
    setsockopt(server_socket, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    sockaddr_in serverAddr{};
    serverAddr.sin_family = AF_INET;
    serverAddr.sin_addr.s_addr = INADDR_ANY;
    serverAddr.sin_port = htons(port);

    if (bind(server_socket, (sockaddr*)&serverAddr, sizeof(serverAddr)) < 0) {
        cerr<<"Error binding socket"<<endl;
        return;
    }
    if (listen(server_socket, 6) < 0) {
        cerr<<"Error listening on socket"<<endl;
        return;
    }

    cout<< "Server running on port " << port << endl;
    vector<std::thread> threads;
    RedisCommandHandler handler;

    while (running) {
        int client_socket = accept(server_socket, nullptr, nullptr);
        if (client_socket < 0) {
            cerr<< "Error accepting connection" << endl;
            break;
        }

        threads.emplace_back([client_socket, &handler]() {
                char buffer[1024];
                while (true) {
                    memset(buffer, 0, sizeof(buffer));
                    int bytes = recv(client_socket, buffer, sizeof(buffer)-1, 0);
                    if (bytes <= 0) break;
                    string request(buffer, bytes);
                    string response = handler.processCommand(request);
                    send(client_socket, response.c_str(), response.size(), 0);
                }
            close(client_socket);
        });
    }

    for (auto &thread : threads) {
        if (thread.joinable()) thread.join();
    }

    //Persist storage, before shutting down;
    if (!RedisDatabase::getInstance().dump("dump.redlite")) {
        cerr << "Failed to persist database." << endl;
    } else {
        cout << "Database persisted successfully to dump.redlite" << endl;
    }

}
