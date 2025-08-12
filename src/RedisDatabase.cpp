//
// Created by ajay on 19/6/25.
//
#include "../include/RedisDatabase.h"
#include <fstream>
#include <mutex>
#include <iostream>
#include<unordered_map>
#include <vector>
#include <string>
using namespace std;

RedisDatabase &RedisDatabase::getInstance() {
    static RedisDatabase instance;
    return instance;
}


bool RedisDatabase::flushAll() {
    std::lock_guard<std::mutex> lock(db_mutex);
    kv_store.clear();
    hash_store.clear();
    list_store.clear();
    return true;
}
bool RedisDatabase::set(const std::string &key, const std::string &value) {
    std::lock_guard<std::mutex> lock(db_mutex);
    kv_store[key] = value;
    return true;
}
bool RedisDatabase::get(const std::string &key,  std::string &value) {
    std::lock_guard<std::mutex> lock(db_mutex);
    purgeExpired();
    if (kv_store.find(key) != kv_store.end()) {
        value = kv_store[key];
        return true;
    }
    return false;
}
std::vector<std::string> RedisDatabase::keys() {
    std::lock_guard<std::mutex> lock(db_mutex);
    purgeExpired();
    std::vector<std::string> result;
    for (const auto &key : kv_store) {
        result.push_back(key.first);
    }
    for ( const auto &key : list_store) {
        result.push_back(key.first);
    }
    for (const auto &key : hash_store) {
        result.push_back(key.first);
    }
    return result;
}
bool RedisDatabase::del(const std::string &key) {
    std::lock_guard<std::mutex> lock(db_mutex);
    purgeExpired();
    bool done = false;
    done|= (kv_store.erase(key) > 0);
    done |= (list_store.erase(key) > 0);
    done |= (hash_store.erase(key) > 0);
    return done;
}
std::string RedisDatabase::type(const std::string &key) {
    std::lock_guard<std::mutex> lock(db_mutex);
    purgeExpired();
    if (kv_store.find(key) != kv_store.end()) {
        return "string";
    } else if (list_store.find(key) != list_store.end()) {
        return "list";
    } else if (hash_store.find(key) != hash_store.end()) {
        return "hash";
    }else {
        return "none";
    }
}
bool RedisDatabase::expire(const std::string &key, int seconds) {
    std::lock_guard<std::mutex> lock(db_mutex);
    purgeExpired();
    bool exists = false;
    if (kv_store.find(key) != kv_store.end()) exists = true;
    else if (list_store.find(key) != list_store.end()) exists = true;
    else if (hash_store.find(key) != hash_store.end()) exists = true;
    if (!exists) return false;
    expiry_map[key] = std::chrono::steady_clock::now() + std::chrono::seconds(seconds);
    return true;

}

void RedisDatabase::purgeExpired() {
    auto now = std::chrono::steady_clock::now();
    for (auto it = expiry_map.begin(); it != expiry_map.end();) {
        if (it->second < now) {
            kv_store.erase(it->first);
            list_store.erase(it->first);
            hash_store.erase(it->first);
            it = expiry_map.erase(it);
        } else {
            ++it;
        }
    }
}

bool RedisDatabase::rename(const std::string &old_key, const std::string &new_key) {
    std::lock_guard<std::mutex> lock(db_mutex);
    purgeExpired();
    bool found = false;
    auto it1 = kv_store.find(old_key);
    if (it1 != kv_store.end()) {
        kv_store[new_key] = it1->second;
        kv_store.erase(it1);
        found = true;
    }
    auto it2 = list_store.find(old_key);
    if (it2 != list_store.end()) {
        list_store[new_key] = it2->second;
        list_store.erase(it2);
        found = true;
    }
    auto it3 = hash_store.find(old_key);
    if (it3 != hash_store.end()) {
        hash_store[new_key] = it3->second;
        hash_store.erase(it3);
        found = true;
    }

    auto it4 = expiry_map.find(old_key);
    if (it4 != expiry_map.end()) {
        expiry_map[new_key] = it4->second;
        expiry_map.erase(it4);
        found = true;
    }

    return found;
}



size_t RedisDatabase::llen(const std::string &key) {
    std::lock_guard<std::mutex> lock(db_mutex);
    purgeExpired();
    auto it = list_store.find(key);
    if (it != list_store.end()) {
        return it->second.size();
    }

    return 0;
}
void RedisDatabase::lpush(const std::string &key, const std::string &value) {
    std::lock_guard<std::mutex> lock(db_mutex);
    purgeExpired();
    list_store[key].push_front(value);
}

void RedisDatabase::rpush(const std::string &key, const std::string &value) {
    std::lock_guard<std::mutex> lock(db_mutex);
    purgeExpired();
    list_store[key].push_back(value);
}
bool RedisDatabase::lpop(const std::string &key, std::string &value) {
    std::lock_guard<std::mutex> lock(db_mutex);
    purgeExpired();
    auto it = list_store.find(key);
    if (it!=list_store.end() and !it->second.empty()) {
        value = it->second.front();
        it->second.pop_front();
        return true;
    }
    return false;
}

bool RedisDatabase::rpop(const std::string &key, std::string &value) {
    std::lock_guard<std::mutex> lock(db_mutex);
    purgeExpired();
    auto it = list_store.find(key);
    if (it!=list_store.end() and !it->second.empty()) {
        value = it->second.back();
        it->second.pop_back();
        return true;
    }
    return false;
}
int RedisDatabase::lrem(const std::string &key, int count, const std::string &value) {
    std::lock_guard<std::mutex> lock(db_mutex);
    purgeExpired();
    auto it = list_store.find(key);
    int removed = 0;
    if (it == list_store.end()) return 0;

    auto &dq = it->second;;
    std::deque<std::string> filtered;

    if (count==0) {
        for (const auto &item : dq) {
            if (item==value) {
                removed++;
                continue;
            }
            filtered.push_back(item);
        }

    }else if (count<0) {
        for (auto it = dq.rbegin(); it!=dq.rend(); it++) {
            if (*it==value and removed < -count) {
                removed++;
                continue;
            }
            filtered.push_back(*it);
        }

    }else {
        for (const auto &item : dq) {
            if (item==value and removed < count) {
                removed++;
                continue;
            }
            filtered.push_back(item);
        }
    }

    dq = std::move(filtered);
    if (dq.empty()) {
        list_store.erase(it);
    }
    return removed;
}
bool RedisDatabase::lindex(const std::string &key, int index,  std::string &value) {
    std::lock_guard<std::mutex> lock(db_mutex);
    purgeExpired();
    auto it = list_store.find(key);
    if (it== list_store.end()) {
        return false;
    }
    const auto &dq = it->second;
    if (index<0)
        index = dq.size() + index;
    if (index< 0 or static_cast<size_t>(index) >= dq.size()) {
        return false;
    }

    value = dq[index];
    return true;

}
bool RedisDatabase::lset(const std::string &key, int index, const std::string &value) {
    std::lock_guard<std::mutex> lock(db_mutex);
    purgeExpired();
    auto it = list_store.find(key);
    if (it== list_store.end()) {
        return false;
    }

     auto &dq = it->second;
    if (index<0)
        index = dq.size() + index;
    if (index< 0 or static_cast<size_t>(index) >= dq.size()) {
        return false;
    }

    dq[index] = std::string(value);
    return true;
}

std::vector<std::string> RedisDatabase::lget(const std::string &key) {
    std::lock_guard<std::mutex> lock(db_mutex);
    purgeExpired();
    std::vector<std::string> result;
    auto it = list_store.find(key);
    if (it!=list_store.end()) {
        for (const auto &item : it->second) {
            result.push_back(item);
        }
    }
    return result;
}

bool RedisDatabase::hset(const std::string &key, const std::string &field, const std::string &value) {
    std::lock_guard<std::mutex> lock(db_mutex);
    purgeExpired();
    hash_store[key][field] = value;
    return true;
}

bool RedisDatabase::hget(const std::string &key, const std::string &field, std::string &value) {
    std::lock_guard<std::mutex> lock(db_mutex);
    purgeExpired();
    auto it = hash_store.find(key);
    if (it!= hash_store.end()) {
        auto f = it->second.find(field);
        if (f!=it->second.end()) {
            value = f->second;
            return true;
        }
    }
    return false;
}

bool RedisDatabase::hexists(const std::string &key, const std::string &field) {
    std::lock_guard<std::mutex> lock(db_mutex);
    purgeExpired();
    auto it = hash_store.find(key);
    if (it!=hash_store.end()) {
        if (it->second.find(field)!=it->second.end()) {
            return true;
        }
    }
    return false;
}

bool RedisDatabase::hdel(const std::string &key, const std::string &field) {
    std::lock_guard<std::mutex> lock(db_mutex);
    purgeExpired();
    auto it = hash_store.find(key);
    if (it!=hash_store.end()) {
        return it->second.erase(field) > 0;
    }
    return false;

}

std::unordered_map<std::string, std::string> RedisDatabase::hgetall(const std::string &key) {
    std::lock_guard<std::mutex> lock(db_mutex);
    purgeExpired();
    if (hash_store.find(key)!=hash_store.end()) {
        return hash_store[key];
    }
    return {};
}

std::vector<std::string> RedisDatabase::hkeys(const std::string &key) {
    std::lock_guard<std::mutex> lock(db_mutex);
    purgeExpired();
    std::vector<std::string> result;
    auto it = hash_store.find(key);
    if (it!=hash_store.end()) {
        for (const auto &pair: it->second) {
            result.push_back(pair.first);
        }
    }
    return result;
}

std::vector<std::string> RedisDatabase::hvals(const std::string &key) {
    std::lock_guard<std::mutex> lock(db_mutex);
    purgeExpired();
    std::vector<std::string> result;
    auto it = hash_store.find(key);
    if (it!=hash_store.end()) {
        for (const auto &pair: it->second) {
            result.push_back(pair.second);
        }
    }
    return result;

}
size_t RedisDatabase::hlen(const std::string &key) {
    std::lock_guard<std::mutex> lock(db_mutex);
    purgeExpired();
    auto it = hash_store.find(key);
    if (it!=hash_store.end()) {
        return it->second.size();
    }
    return 0;

}
bool RedisDatabase::hmset(const std::string &key, const std::vector<std::pair<std::string, std::string>> &fieldValues) {
    std::lock_guard<std::mutex> lock(db_mutex);
    purgeExpired();
    for (const auto &pair : fieldValues) {
        hash_store[key][pair.first] = pair.second;
    }

    return true;
}







bool RedisDatabase::dump(const std::string &filename) {
    // Implement the logic to dump the database to a file
    // For now, just return true to indicate success
    std::lock_guard<std::mutex> lock(db_mutex);
    purgeExpired();
    std::ofstream ofs(filename, std::ios::binary);
    if (!ofs) return false;

    for (const auto &kv : kv_store) {
        ofs <<"K "<< kv.first << " " << kv.second << "\n";
    }

    for (const auto &kv : list_store) {
        ofs <<"L "<< kv.first;
        for (const auto &item : kv.second) {
            ofs << " " << item;
        }
        ofs<< "\n";
    }

    for ( const auto &kv : hash_store) {
        ofs <<"H "<< kv.first ;
        for (const auto &item: kv.second) {
            ofs<< " " << item.first << ":" << item.second;
        }
        ofs<<"\n";
    }
    return true;
}

bool RedisDatabase::load(const std::string &filename) {
    std::lock_guard<std::mutex> lock(db_mutex);
    std::ifstream ifs(filename, std::ios::binary);
    if (!ifs) return false;
    kv_store.clear();
    list_store.clear();
    hash_store.clear();

    string line;
    while (std::getline(ifs, line)) {
        std:: istringstream iss(line);
        char type;
        iss>>type;
        if (type=='K') {
            string key, value;
            iss>>key>>value;
            kv_store[key] = value;
        }else if (type=='L') {
            string key; iss>>key;
            string item;
            deque<string> items;
            while (iss>>item) {
                items.push_back(item);
            }
            list_store[key] = items;
        }else if (type=='H'){
            string key; iss>>key;
            std::unordered_map<std::string, std::string> hash;
            string pair;
            while (iss>>pair) {
                auto pos = pair.find(':');
                if (pos != std::string::npos) {
                    string field = pair.substr(0, pos);
                    string value = pair.substr(pos + 1);
                    hash[field] = value;
                }
            }
            hash_store[key] = hash;

        }
    }

    return true;
}
