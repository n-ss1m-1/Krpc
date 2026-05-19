#include "include/ConnectionPoolManager.h"

// 单例
ConnectionPoolManager& ConnectionPoolManager::GetInstance()
{
    LOG(INFO) << "获取连接池Manager单例" ;
    static ConnectionPoolManager instance;
    return instance;
}

// 获取或创建连接池
ConnectionPool* ConnectionPoolManager::GetorCreatePool(const std::string& ip, uint16_t port)
{
    LOG(INFO) << "获取/初始化连接池" ;
    std::string key = ip + ":" + std::to_string(port);
    std::lock_guard<std::mutex> lock(m_mutex);

    auto it = m_pools.find(key);
    //不存在则创建
    if(it == m_pools.end())
    {
        LOG(INFO) << "初始化新的连接池" ;
        m_pools[key] = new ConnectionPool(ip,port);
    }
    return m_pools[key];
}

ConnectionPoolManager::~ConnectionPoolManager()
{   
    LOG(INFO) << "释放所有连接池" ;
    std::lock_guard<std::mutex> lock(m_mutex);
    for(auto &pair : m_pools)
    {
        delete pair.second;
    }
    m_pools.clear();
}

