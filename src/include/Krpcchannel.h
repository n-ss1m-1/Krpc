#ifndef _Krpcchannel_h_
#define _Krpcchannel_h_
// 此类是继承自google::protobuf::RpcChannel
// 目的是为了给客户端进行方法调用的时候，统一接收的
#include <google/protobuf/service.h>
#include "zookeeperutil.h"
#include "ConnectionPoolManager.h"

class KrpcChannel : public google::protobuf::RpcChannel     //继承
{
public:
    KrpcChannel(bool connectNow);
    virtual ~KrpcChannel()
    {
        if (m_clientfd >= 0) {
        m_pool->ReturnConnection(m_clientfd);
    }  
    }
    void CallMethod(const ::google::protobuf::MethodDescriptor *method,
                    ::google::protobuf::RpcController *controller,
                    const ::google::protobuf::Message *request,
                    ::google::protobuf::Message *response,
                    ::google::protobuf::Closure *done) override; // override可以验证是否是虚函数
private:
    int m_clientfd; // 存放客户端套接字
    std::string service_name;
    std::string method_name;
    std::string m_ip;           //rpc服务器ip
    uint16_t m_port;            //rpc服务器port
    int m_idx; // 用来划分服务器ip和port的下标
    ConnectionPool* m_pool;

    // 从ZooKeeper查询服务地址
    std::string QueryServiceHost(ZkClient *zkclient, std::string method_path, int &idx);
    // 新增：确保读取指定长度的数据，解决TCP拆包
    ssize_t recv_exact(int fd, char* buf, size_t size);
};
#endif
