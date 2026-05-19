#include "Krpcchannel.h"
#include "Krpcheader.pb.h"
#include "zookeeperutil.h"
#include "Krpcapplication.h"
#include "Krpccontroller.h"
#include "memory"
#include <errno.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <arpa/inet.h>
#include <time.h>
#include "KrpcLogger.h"


// 辅助函数：循环读取直到读够 size 字节
ssize_t KrpcChannel::recv_exact(int fd, char* buf, size_t size) {
    size_t total_read = 0;
    while (total_read < size) {
        ssize_t ret = recv(fd, buf + total_read, size - total_read, 0);
        if (ret == 0) return 0; // 对端关闭
        if (ret == -1) {
            if (errno == EINTR) continue; // 中断信号，继续读
            if (errno == EAGAIN || errno == EWOULDBLOCK) 
            {
                LOG(ERROR) << "recv timeout";
                return -2;     //超时
            }
            return -1; // 错误
        }
        total_read += ret;
    }
    return total_read;
}

// RPC调用的核心方法，负责将客户端的请求序列化并发送到服务端，同时接收服务端的响应
void KrpcChannel::CallMethod(const ::google::protobuf::MethodDescriptor *method,
                             ::google::protobuf::RpcController *controller,
                             const ::google::protobuf::Message *request,            //基类指针(Message*)指向子类对象(LoginRequest*)
                             ::google::protobuf::Message *response,
                             ::google::protobuf::Closure *done)   //！客户端 CallMethod：done 传进来，此处使用同步阻塞等响应，用不上done(虚函数模板导致必须这样写)
                                                                  //服务端 CallMethod：done 是回调，业务执行完调 done->Run() 发回响应
{
    // 不使用模拟持久连接
    // 通过[method]获取服务名和方法名 
    LOG(INFO) << "通过[method]获取服务名和方法名" ;
    const google::protobuf::ServiceDescriptor *sd = method->service();
    service_name = sd->name();      // 服务名
    method_name = method->name();   // 方法名

    // 客户端需要查询ZooKeeper，找到提供该服务的服务器地址
    ZkClient& zkCli = ZkClient::GetInstance();
    std::string method_path = "/" + service_name + "/" + method_name;  // 构造ZooKeeper路径
    std::string host_data;

    //查看是否有相应的缓存
    if (zkCli.HasCache(method_path))
    {
        //缓存有效，无需连接zk服务器
        LOG(INFO) << "缓存有效,无需连接zk服务器" ;
        host_data = zkCli.GetCache(method_path);    
        
        m_idx = host_data.find(":");  // 查找IP和端口的分隔符
        if (m_idx == -1) {  // 如果分隔符不存在
        LOG(ERROR) << method_path + " address is invalid!";  // 记录错误日志
        }    
    }
    else
    {
        //暂时没有相应的缓存
        static std::mutex g_cache_init_mutex;
        std::lock_guard<std::mutex> lock(g_cache_init_mutex);       //尝试获取锁来更新缓存
        //再次判断是否缓存有效
        if(zkCli.HasCache(method_path))
        {
            //缓存有效，无需连接zk服务器
            LOG(INFO) << "其他线程更新了缓存->缓存有效,无需连接zk服务器" ;
            host_data = zkCli.GetCache(method_path);    
            
            m_idx = host_data.find(":");  // 查找IP和端口的分隔符
            if (m_idx == -1) {  // 如果分隔符不存在
            LOG(ERROR) << method_path + " address is invalid!";  // 记录错误日志
            }  
        }
        else
        {
            LOG(INFO) << "缓存无效,连接到ZooKeeper服务器+查询提供相应服务的服务器ip和port" ;
            zkCli.Start();  // 连接ZooKeeper服务器
            host_data = QueryServiceHost(&zkCli, method_path, m_idx);  // 查询提供相应服务的服务器ip和port

            //将查询到的有效数据保存到缓存中
            if(host_data != " ") zkCli.SetCache(method_path,host_data);
        }
    }
    m_ip = host_data.substr(0, m_idx);  // 从查询结果中提取IP地址
    LOG(INFO) << "ip: " << m_ip;
    m_port = atoi(host_data.substr(m_idx + 1, host_data.size() - m_idx).c_str());  // 从查询结果中提取端口号
    LOG(INFO) << "port: " << m_port;


    // 尝试连接rpc服务器
    LOG(INFO) << "尝试连接rpc服务器" ;
    if (m_pool==nullptr) 
    {
        LOG(INFO) << "初始化连接池" ;
        m_pool = ConnectionPoolManager::GetInstance().GetorCreatePool(m_ip,m_port);
    }
    m_clientfd = m_pool->GetConnection();
    if (m_clientfd == -1) {
        controller->SetFailed("get connection from pool failed");
        LOG(ERROR) << "connect server error";  // 连接失败，记录错误日志
        return;
    } else {
        LOG(INFO) << "connect server success";  // 连接成功，记录日志
    }

    // 2. 序列化请求参数(request在Kcliend.cc中设置了参数 然后在此处序列化)
    std::string args_str;
    if (!request->SerializeToString(&args_str)) {                   //只要是 protobuf 的 message，生成的类就自动有序列化方法
        controller->SetFailed("serialize request fail");
        return;
    }

    // 3. 构建协议头
    Krpc::RpcHeader krpcheader;
    krpcheader.set_service_name(service_name);
    krpcheader.set_method_name(method_name);
    krpcheader.set_args_size(args_str.size());

    std::string rpc_header_str;
    if (!krpcheader.SerializeToString(&rpc_header_str)) {           //只要是 protobuf 的 message，生成的类就自动有序列化方法
        controller->SetFailed("serialize rpc header error!");
        return;
    }

    // 4. 打包数据发送
    // 格式：[4B Total Len] + [4B Header Len] + [Header] + [Args]
    
    uint32_t header_size = rpc_header_str.size();
    uint32_t total_len = 4 + header_size + args_str.size(); // Total Len 包含 HeaderLen(4) + Header + Body
    
    // 转网络字节序
    uint32_t net_total_len = htonl(total_len);
    uint32_t net_header_len = htonl(header_size);

    std::string send_rpc_str;
    send_rpc_str.reserve(4 + 4 + header_size + args_str.size());
    
    send_rpc_str.append((char*)&net_total_len, 4);
    send_rpc_str.append((char*)&net_header_len, 4);
    send_rpc_str.append(rpc_header_str);
    send_rpc_str.append(args_str);

    // 发送
    // send(fd,      // 往哪个socket发
    //     buf,      // 发什么数据
    //     len,      // 发多少字节
    //     0);       // flags，0表示默认
    if (-1 == send(m_clientfd, send_rpc_str.c_str(), send_rpc_str.size(), 0)) {
        m_pool->RemoveConnection(m_clientfd);
        m_clientfd = -1; // 重置
        controller->SetFailed("send error");
        return;
    }

    // 5. 接收响应(！阻塞等待服务器响应结果(不足之处->可以改进为异步回调or协程))
    // 格式：[4B Total Len] + [Response Data]
    
    // A. 先读4字节长度头
    uint32_t response_len = 0;
    auto ret1 = recv_exact(m_clientfd, (char*)&response_len, 4);
    if (ret1 == 0)
    {
        m_pool->RemoveConnection(m_clientfd);
        m_clientfd = -1;
        controller->SetFailed("server close connection");
        return;
    }
    if (ret1 == -2) {
        m_pool->RemoveConnection(m_clientfd);
        m_clientfd = -1;
        controller->SetFailed("recv timeout");
        return;
    }
    else if(ret1 != 4)
    {
        m_pool->RemoveConnection(m_clientfd);
        m_clientfd = -1;
        controller->SetFailed("recv response length error");
        return;
    }
    response_len = ntohl(response_len); // 转回主机字节序

    // B. 根据长度读取Body
    std::vector<char> recv_buf(response_len);
    auto ret2 = recv_exact(m_clientfd, recv_buf.data(), response_len);      //vector<char> 的 .data()方法：获取首元素的原始地址
    if (ret2 == 0)
    {
        m_pool->RemoveConnection(m_clientfd);
        m_clientfd = -1;
        controller->SetFailed("server close connection");
        return;
    }
    if (ret2 == -2) {
        m_pool->RemoveConnection(m_clientfd);
        m_clientfd = -1;
        controller->SetFailed("recv timeout");
        return;
    }
    else if(ret2 != (ssize_t)response_len)
    {
        m_pool->RemoveConnection(m_clientfd);
        m_clientfd = -1;
        controller->SetFailed("recv response body error");
        return;
    }

    // 6. 反序列化响应
    if (!response->ParseFromArray(recv_buf.data(), response_len)) {
        m_pool->RemoveConnection(m_clientfd);
        m_clientfd = -1;
        controller->SetFailed("parse response error");
        return;
    }

    //归还连接
    m_pool->ReturnConnection(m_clientfd);
    m_clientfd = -1;
}


// 从ZooKeeper查询服务地址
std::string KrpcChannel::QueryServiceHost(ZkClient *zkclient, std::string method_path, int &idx) {
    LOG(INFO) << "method_path: " << method_path;

    std::string host_data_1 = zkclient->GetData(method_path.c_str());  // 从ZooKeeper获取数据

    if (host_data_1 == "") {  // 如果未找到服务地址
        LOG(ERROR) << method_path + " is not exist!";  // 记录错误日志
        return " ";
    }

    idx = host_data_1.find(":");  // 查找IP和端口的分隔符
    if (idx == -1) {  // 如果分隔符不存在
        LOG(ERROR) << method_path + " address is invalid!";  // 记录错误日志
        return " ";
    }

    return host_data_1;  // 返回服务地址
}

// 构造函数，支持延迟连接
KrpcChannel::KrpcChannel(bool connectNow) : m_clientfd(-1), m_idx(0), m_pool(nullptr) {
    if (!connectNow) {  // 如果不需要立即连接
        return;
    }

    //！下列尝试直接连接的代码功能尚未完成：因为 ip 和 port 是从 ZooKeeper 查来的，而查询发生在 CallMethod 里，构造时还没查。
    /*改进方法：
    1.连接池：现在每次调用CallMethod()都重新与服务器建立连接，改成复用->省去每次 TCP 三次握手的开销
    2.ZooKeeper查询结果缓存：现在每次调用CallMethod()都查一次 ZooKeeper->直接读缓存(注意避免缓存失效)
    3.IO多路复用+非阻塞IO（最大的改进）：现在 recv_exact 阻塞整个线程->改成使用muduo自带的TcpClient
    */

    // 尝试连接服务器，最多重试3次
    // auto rt = newConnect(m_ip.c_str(), m_port);
    // int count = 3;  // 重试次数
    // while (!rt && count--) {
    //     rt = newConnect(m_ip.c_str(), m_port);
    // }
}