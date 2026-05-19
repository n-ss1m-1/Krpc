#ifndef _ConnectionPool_H
#define _ConnectionPool_H
#include <mutex>
#include <condition_variable>
#include <queue>
#include <string>
# include "KrpcLogger.h"

class ConnectionPool
{
public:
    ConnectionPool(const std::string& ip, uint16_t port,int max_size=10);
    ~ConnectionPool();
    
    //获取连接 / 没有则先创建连接
    int GetConnection();

    //返还连接
    void ReturnConnection(int fd);

    //销毁无效连接
    void RemoveConnection(int fd);
private:
    const std::string m_ip;
    uint16_t m_port;
    int max_size;
    int cur_size;
    bool m_stopped;

    std::queue<int> m_free_fds;
    std::mutex m_mutex;
    std::condition_variable m_cv;

    int CreateConnection();
};
#endif