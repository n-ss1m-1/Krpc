#include <unistd.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <string.h>
#include <time.h>

#include "include/ConnectionPool.h"

ConnectionPool::ConnectionPool(const std::string& ip, uint16_t port,int max_size):        //构造函数内不能写默认参数
            m_ip(ip),
            m_port(port),
            max_size(max_size),
            cur_size(0),
            m_stopped(false)
{
}
ConnectionPool::~ConnectionPool()
{
    {
        std::unique_lock<std::mutex> lock(m_mutex);
        m_stopped = true;
    }
    m_cv.notify_all();

    std::unique_lock<std::mutex> lock(m_mutex);
    m_cv.wait(lock,[this](){return cur_size==(int)m_free_fds.size();});      //等待所有连接返还
    
    while(!m_free_fds.empty())
    {
        close(m_free_fds.front());
        m_free_fds.pop();
    }
}

int ConnectionPool::CreateConnection()
{
    LOG(INFO) << "开始创建rpc连接" ;
    int fd=socket(AF_INET,SOCK_STREAM,0);
    if(fd == -1)
    {
        char errtxt[512] = {0};
        LOG(INFO) << "socket error" << strerror_r(errno, errtxt, sizeof(errtxt));  // 打印错误信息
        LOG(ERROR) << "socket error:" << errtxt;  // 记录错误日志
        return -1;
    }

    struct sockaddr_in server_addr;
    memset(&server_addr,0,sizeof(server_addr));
    server_addr.sin_family=AF_INET;
    server_addr.sin_port=htons(m_port);
    if(inet_pton(AF_INET,m_ip.c_str(),&server_addr.sin_addr) <= 0)
    {
        close(fd);
        char errtxt[512] = {0};
        LOG(INFO) << "inet_pton error" << strerror_r(errno, errtxt, sizeof(errtxt));  // 打印错误信息
        LOG(ERROR) << "inet_pton error" << errtxt;  // 记录错误日志
        return -1;
    }

    if(-1 == connect(fd,(struct sockaddr*)&server_addr,sizeof(server_addr)))
    {
        close(fd);
        char errtxt[512] = {0};
        LOG(INFO) << "connect error" << strerror_r(errno, errtxt, sizeof(errtxt));  // 打印错误信息
        LOG(ERROR) << "connect server error" << errtxt;  // 记录错误日志
        return -1;
    }

    // 设置fd请求超时
    struct timeval timeout;
    timeout.tv_sec=3;
    timeout.tv_usec=0;
    if(-1 == setsockopt(fd,SOL_SOCKET,SO_RCVTIMEO,&timeout,sizeof(timeout)))
    {
        close(fd);
        char errtxt[512] = {0};
        LOG(ERROR) << "set fd timeout error" <<strerror_r(errno, errtxt, sizeof(errtxt));
        return -1;
    }

    return fd;
}
int ConnectionPool::GetConnection()                 //!!!外部要处理-1
{
    std::unique_lock<std::mutex> lock(m_mutex);

    if(m_stopped) return -1;

    LOG(INFO) << "从连接池中获取rpc连接" ;
    //有空闲连接，直接取
    if(!m_free_fds.empty())
    {
        LOG(INFO) << "有空闲连接，直接取" ;
        int fd=m_free_fds.front();
        m_free_fds.pop();
        return fd;
    }
    //没有空闲连接，数量未达上限，创建新连接
    else if(cur_size<max_size)
    {
        LOG(INFO) << "没有空闲连接，数量未达上限，创建新连接" ;
        int fd=CreateConnection();
        if(fd!=-1) cur_size++;
        return fd;                          
    }
    //没有空闲连接，数量已达上限，等待
    LOG(INFO) << "没有空闲连接，数量已达上限，等待" ;
    m_cv.wait(lock,[this](){return !m_free_fds.empty() || m_stopped;});

    if(m_stopped) return -1;        //1.检查发现连接池关闭
    int fd=m_free_fds.front();      //2.等到了空闲连接
    m_free_fds.pop();
    return fd;
}
void ConnectionPool::ReturnConnection(int fd)
{
    // 验证fd是否有效
    int error = 0;
    socklen_t len = sizeof(error);
    if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &error, &len) == -1 || error != 0) 
    {
        RemoveConnection(fd);  // 坏fd直接移除
        return;
    }
    
    LOG(INFO) << "归还rpc连接到连接池" ;
    std::lock_guard<std::mutex> lock(m_mutex);
    m_free_fds.push(fd);
    m_cv.notify_one();
}
void ConnectionPool::RemoveConnection(int fd)
{
    LOG(INFO) << "移除无效的rpc连接" ;
    std::lock_guard<std::mutex> lock(m_mutex);
    close(fd);
    cur_size--;
    //此时关闭的是移出queue的fd，无需再次移出，但是cur_size需要减小
    m_cv.notify_all();          //通知析构函数检查条件
}

