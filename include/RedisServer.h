//
// Created by ajay on 18/6/25.
//

#ifndef REDISSERVER_H
#define REDISSERVER_H
#include <atomic>
#include <string>


class RedisServer {
public:
    RedisServer(int port);
    void run();
    void shutdown();
private:
    int port;
    int server_socket;
    std::atomic<bool> running;


    void setupSignalHandler();
};




#endif
