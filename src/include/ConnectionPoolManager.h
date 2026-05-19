#ifndef _ConnectionPoolManager_H
#define _ConnectionPoolManager_H
#include <mutex>
#include <unordered_map>
#include <string>

#include "ConnectionPool.h"

class ConnectionPoolManager
{
public:
    // 单例
    static ConnectionPoolManager& GetInstance();
    
    // 获取或创建连接池
    ConnectionPool* GetorCreatePool(const std::string& ip, uint16_t port);

    // 禁用拷贝
    ConnectionPoolManager(const ConnectionPoolManager&) = delete;
    ConnectionPoolManager& operator=(const ConnectionPoolManager&) = delete;
    
private:
    ConnectionPoolManager()=default;
    ~ConnectionPoolManager();

    std::mutex m_mutex;
    std::unordered_map<std::string,ConnectionPool*> m_pools;
};
#endif