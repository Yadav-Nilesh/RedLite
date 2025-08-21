//
//
#include "../include/RedisCommandHandler.h"
#include<bits/stdc++.h>
#include "../include/RedisDatabase.h"
#include <string>
using namespace std;

void convertToUpperCase(string &str) {
    for (char &c : str) {
        if (c>='a' and c<='z') {
            c = (c-'a'+'A');
        }
    }
}

vector<string> parseRespCommand(const string &input) {
    vector<string> tokens;
    if (input.empty()) return tokens;

    if (input[0]!='*') {
        std::istringstream iss(input);
        string token;
        while (iss>>token) {
            tokens.push_back(token);
        }
        return tokens;
    }

    size_t pos = 0;
    if (input[pos]!='*') return tokens;
    pos++;
    size_t crlf = input.find("\r\n", pos);
    if (crlf==string::npos) return tokens;
    int numTokens = stoi(input.substr(pos, crlf-pos));
    pos = crlf + 2;

    for (int i=0;i<numTokens;i++) {
        if (pos>= input.size() or input[pos]!='$') break;
        pos++;

        crlf = input.find("\r\n", pos);
        if (crlf == string::npos) break;
        int len = stoi(input.substr(pos, crlf - pos));
        pos = crlf+2; // crlf -> length 2;
        if (pos + len > input.size()) break;
        string token = input.substr(pos, len);
        tokens.push_back(token);
        pos+= (len+2);
    }
    return tokens;
}

static std::string handlePing(const std::vector<std::string> &tokens, RedisDatabase &db) {
    return "+PONG\r\n";
}

static std::string handleEcho(const std::vector<std::string> &tokens, RedisDatabase &db) {
    if (tokens.size()<2) {
        return "-Error: Incorrect number of arguments\r\n";
    }
    return "+" + tokens[1] + "\r\n";
}

static std::string handleFlushAll(const std::vector<std::string> &tokens, RedisDatabase &db) {
    db.flushAll();
    return "+OK\r\n";
}


// key -value ops;
static std::string handleSet(const std::vector<std::string> &tokens, RedisDatabase &db) {
    if (tokens.size()<3) {
        return "-Error: Incorrect number of arguments\r\n";
    }
    db.set(tokens[1], tokens[2]);
    return "+OK\r\n";
}

static std::string handleGet(const std::vector<std::string> &tokens, RedisDatabase &db) {
    if (tokens.size() < 2) {
        return "-Error: Incorrect number of arguments\r\n";
    }
    string value;
    if (db.get(tokens[1], value)) {
        return "$" + std::to_string(value.size()) + "\r\n" + value + "\r\n";
    } else {
        return "$-1\r\n";
    }
}

static std::string handleKeys(const std::vector<std::string> &tokens, RedisDatabase &db) {
    std::vector<std::string> keys = db.keys();
    std::ostringstream response;
    response << "*" << keys.size() << "\r\n";
    for (const auto &key : keys) {
        response << "$" << key.size() << "\r\n" << key << "\r\n";
    }
    return response.str();
}

static std::string handleType(const std::vector<std::string> &tokens, RedisDatabase &db) {
    if (tokens.size() < 2) {
        return "-Error: Incorrect number of arguments\r\n";
    }
    return "+" + db.type(tokens[1]) + "\r\n";
}

static std::string handleDel(const std::vector<std::string> &tokens, RedisDatabase &db) {
    if (tokens.size() < 2) {
        return "-Error: Incorrect number of arguments\r\n";
    }
    bool deleted = db.del(tokens[1]);
    return ":" + std::to_string(deleted ? 1 : 0) + "\r\n";
}


static std::string handleExpire(const std::vector<std::string> &tokens, RedisDatabase &db) {
    if ( tokens.size() < 3) {
        return "-Error: Incorrect number of arguments\r\n";
    }
    try {

        if (db.expire(tokens[1], stoi(tokens[2]))) {
            return "+OK\r\n";
        }else {
            return "-Error: Key not found\r\n";
        }
    }catch (std::exception &e) {
        return "-Error: Invalid expiry time\r\n";
    }
}

static std::string handleRename(const std::vector<std::string> &tokens, RedisDatabase &db) {
    if (tokens.size() < 3) {
        return "-Error: Incorrect number of arguments\r\n";
    }
    if (db.rename(tokens[1], tokens[2])) {
        return "+OK\r\n";
    } else {
        return "-Error: Failed to rename key\r\n";
    }
}

static std::string  handleLlen(const std::vector<std::string> &tokens, RedisDatabase &db) {
    if (tokens.size()<2) {
        return "-Error: Incorrect number of arguments\r\n";
    }
    size_t len = db.llen(tokens[1]);
    return ":" + std::to_string(len) + "\r\n";

}

static std::string  handleLpush(const std::vector<std::string> &tokens, RedisDatabase &db) {
    if (tokens.size()<3) {
        return "-Error: Incorrect number of arguments\r\n";
    }
    const std::string &key = tokens[1];

    for (size_t i=2 ; i<tokens.size(); i++) {
        db.lpush(key, tokens[i]);
    }
    size_t len = db.llen(tokens[1]);
    return ":" + std::to_string(len) + "\r\n";
}

static std::string  handleRpush(const std::vector<std::string> &tokens, RedisDatabase &db) {
    if (tokens.size()<3) {
        return "-Error: Incorrect number of arguments\r\n";
    }

    const std::string &key = tokens[1];

    for (size_t i=2 ; i<tokens.size(); i++) {
        db.rpush(key, tokens[i]);
    }
    size_t len = db.llen(tokens[1]);
    return ":" + std::to_string(len) + "\r\n";
}

static std::string  handleLpop(const std::vector<std::string> &tokens, RedisDatabase &db) {
    if (tokens.size() <2) {
        return "-Error: Incorrect number of arguments\r\n";
    }
    std::string value;
    if (db.lpop(tokens[1], value)) {
        return "$" + std::to_string(value.size()) + "\r\n" + value + "\r\n";
    }
    return  "$-1\r\n";
}

static std::string  handleRpop(const std::vector<std::string> &tokens, RedisDatabase &db) {
    if (tokens.size() <2) {
        return "-Error: Incorrect number of arguments\r\n";
    }
    std::string value;
    if (db.rpop(tokens[1], value)) {
        return "$" + std::to_string(value.size()) + "\r\n" + value + "\r\n";
    }
    return  "$-1\r\n";
}

static std::string  handleLindex(const std::vector<std::string> &tokens, RedisDatabase &db) {
    if (tokens.size()<3) {
        return "-Error: Incorrect number of arguments\r\n";
    }
    try {
        int count = std::stoi(tokens[2]);
        std::string value;
        if (db.lindex(tokens[1], count, value)) {
            return "$" + std::to_string(value.size()) + "\r\n" + value + "\r\n";
        }else {
            return "$-1\r\n";
        }

    }catch (const std:: exception&) {
        return "-Error: Invalid index\r\n";
    }

}

static std::string  handleLrem(const std::vector<std::string> &tokens, RedisDatabase &db) {
    if (tokens.size() < 4) {
        return "-Error: Incorrect number of arguments\r\n";
    }
    try {
        int count  = std::stoi(tokens[2]);
        int removed = db.lrem(tokens[1], count, tokens[3]);
        return ":" + std::to_string(removed) + "\r\n";
    }catch (const std:: exception&) {
        return "-Error: Invalid count\r\n";
    }
}


static std::string  handleLset(const std::vector<std::string> &tokens, RedisDatabase &db) {
    if (tokens.size()<4) {
        return  "-Error: Incorrect number of arguments\r\n";
    }
    try {
        int index = std::stoi(tokens[2]);
        if (db.lset(tokens[1], index, tokens[3]))
                return "+OK\r\n";
        else
            return "-Error: index out of range\r\n";
    }catch (const std::exception&) {
        return "-Error: Invalid index\r\n";
    }
}

static std::string handleLget(const std::vector<std::string> &tokens, RedisDatabase &db) {
    if (tokens.size()<2) {
        return "-Error: Incorrect number of arguments\r\n";
    }
    std::vector<std::string> values = db.lget(tokens[1]);
    std:: ostringstream response;
    response << "*" << values.size() << "\r\n";
    for (auto &value: values) {
        response << "$" << value.size() << "\r\n" << value << "\r\n";
    }
    return response.str();
}

static std::string handleHset(const std::vector<std::string> &tokens, RedisDatabase &db) {
    if (tokens.size()<4) {
        return "-Error: Incorrect number of arguments\r\n";
    }
    db.hset(tokens[1], tokens[2], tokens[3]);
    return ":1\r\n";
}

static std::string handleHget(const std::vector<std::string> &tokens, RedisDatabase &db) {
    if (tokens.size()<3) {
        return "-Error: Incorrect number of arguments\r\n";
    }
    string value;
    if (db.hget(tokens[1], tokens[2], value)) {
        return "$"+ std::to_string(value.size()) + "\r\n" + value + "\r\n";
    }else {
        return "$-1\r\n";
    }
}


static std::string handleHexists(const std::vector<std::string> &tokens, RedisDatabase &db) {
    if (tokens.size()<3) {
        return "-Error: Incorrect number of arguments\r\n";
    }
    bool exists = db.hexists(tokens[1], tokens[2]);
    return ":" + std::to_string(exists ? 1 : 0) + "\r\n";
}

static std::string handleHdel(const std::vector<std::string> &tokens, RedisDatabase &db) {
    if (tokens.size()<3) {
        return "-Error: Incorrect number of arguments\r\n";
    }
    bool ans = db.hdel(tokens[1], tokens[2]);
    return ":" + std::to_string(ans ? 1 : 0) + "\r\n";

}

static std::string handleHgetall (const std::vector<std::string> &tokens, RedisDatabase &db) {
    if (tokens.size()<2) {
        return "-Error: Incorrect number of arguments\r\n";
    }

    auto hash = db.hgetall(tokens[1]);
    std:: ostringstream response;
    response << "*" << hash.size()*2 << "\r\n";
    for (const auto &item : hash) {
        response << "$" << item.first.size() << "\r\n" << item.first << "\r\n";
        response << "$" << item.second.size() << "\r\n" << item.second << "\r\n";
    }
    return response.str();
}

static std::string handleHkeys(const std::vector<std::string> &tokens, RedisDatabase &db) {
    if (tokens.size()<2) {
        return "-Error: Incorrect number of arguments\r\n";
    }
    auto keys = db.hkeys(tokens[1]);
    std::ostringstream response;
    response << "*" << keys.size() << "\r\n";
    for (const auto &key : keys) {
        response<< "$"<< key.size() << "\r\n" << key << "\r\n";
    }
    return response.str();
}

static std::string handleHvals(const std::vector<std::string> &tokens, RedisDatabase &db) {
    if (tokens.size()<2) {
        return "-Error: Incorrect number of arguments\r\n";
    }
    auto values = db.hvals(tokens[1]);
    std::ostringstream response;
    response << "*" << values.size() << "\r\n";
    for (const auto &val : values) {
        response<< "$"<< val.size() << "\r\n" << val << "\r\n";
    }
    return response.str();

}


static std::string handleHlen(const std::vector<std::string> &tokens, RedisDatabase &db) {
    if (tokens.size()<2) {
        return "-Error: Incorrect number of arguments\r\n";
    }

    size_t len = db.hlen(tokens[1]);
    return ":" + std::to_string(len) + "\r\n";
}

static std::string handleHmset(const std::vector<std::string> &tokens, RedisDatabase &db) {
    if (tokens.size()<4 || ((tokens.size()%2)==1)) {
        return "-Error: Incorrect number of arguments\r\n";
    }
    std::vector<std::pair<std::string, std::string>> fieldValues;

    for (size_t i=2; i<tokens.size(); i+=2) {
        fieldValues.emplace_back(tokens[i], tokens[i+1]);
    }
    db.hmset(tokens[1], fieldValues);

    return "+OK\r\n";
}



RedisCommandHandler::RedisCommandHandler() {}



string RedisCommandHandler::processCommand(const string &command) {
    vector<string> tokens = parseRespCommand(command);
    if (tokens.empty()) return "-Error: Empty Command\r\n";

    string cmd = tokens[0];
    // convert to upperCase;
    convertToUpperCase(cmd);
    RedisDatabase &db = RedisDatabase::getInstance();


    if (cmd == "PING") {
       return handlePing(tokens, db);
    }else if (cmd == "ECHO") {
        return handleEcho(tokens, db);
    }else if (cmd== "FLUSHALL") {
        return handleFlushAll(tokens, db);
    }else if (cmd == "SET") {
        return handleSet(tokens, db);
    }else if (cmd == "GET") {
        return handleGet(tokens, db);
    }else if (cmd == "KEYS") {
        return handleKeys(tokens, db);
    }else if (cmd=="TYPE") {
       return handleType(tokens, db);
    }else if (cmd == "DEL" or cmd == "UNLINK") {
        return handleDel(tokens, db);
    }else if (cmd == "EXPIRE") {
        return handleExpire(tokens, db);
    }else if (cmd == "RENAME") {
        return handleRename(tokens, db);
    }else if (cmd == "LLEN") {
        return handleLlen(tokens, db);
    }else if (cmd == "LPUSH") {
        return handleLpush(tokens, db);
    }else if (cmd == "RPUSH") {
        return handleRpush(tokens, db);
    }else if (cmd == "LPOP") {
        return handleLpop(tokens, db);
    }else if (cmd == "RPOP") {
        return handleRpop(tokens, db);
    }else if (cmd == "LREM") {
        return handleLrem(tokens, db);
    }else if (cmd == "LINDEX") {
        return handleLindex(tokens, db);
    }else if (cmd == "LSET") {
        return handleLset(tokens, db);
    }else if (cmd == "LGET") {
        return handleLget(tokens, db);
    }
    else if (cmd == "HSET") {
        return handleHset(tokens, db);
    }else if (cmd == "HGET") {
        return handleHget(tokens, db);
    }else if (cmd == "HEXISTS") {
        return handleHexists(tokens, db);
    }else if (cmd == "HDEL") {
        return handleHdel(tokens, db);
    }else if (cmd == "HGETALL") {
        return handleHgetall(tokens, db);
    }else if (cmd == "HKEYS") {
        return handleHkeys(tokens, db);
    }else if (cmd == "HVALS") {
        return handleHvals(tokens, db);
    }else if (cmd == "HLEN") {
        return handleHlen(tokens, db);
    }else if (cmd == "HMSET") {
        return handleHmset(tokens, db);
    }else {
        return "-Error: Unknown command\r\n";
    }

}