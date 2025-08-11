//
// Created by ajay on 18/6/25.
//

 #include <string>

#ifndef REDISCOMMANDHANDLER_H
#define REDISCOMMANDHANDLER_H\

class RedisCommandHandler {
public:
    RedisCommandHandler();
    std::string processCommand(const std::string &command);
};

#endif //REDISCOMMANDHANDLER_H
