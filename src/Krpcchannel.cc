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
#include "KrpcLogger.h"

std::mutex g_data_mutx;  // 全局互斥锁，用于保护共享数据的线程安全


// 辅助函数：循环读取直到读够 size 字节
ssize_t KrpcChannel::recv_exact(int fd, char* buf, size_t size) {
    size_t total_read = 0;
    while (total_read < size) {
        ssize_t ret = recv(fd, buf + total_read, size - total_read, 0);
        if (ret == 0) return 0; // 对端关闭
        if (ret == -1) {
            if (errno == EINTR) continue; // 中断信号，继续读
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
    // 如果客户端socket未初始化
    if (-1 == m_clientfd) {  
        // [通过method]获取服务名和方法名
        const google::protobuf::ServiceDescriptor *sd = method->service();
        service_name = sd->name();      // 服务名
        method_name = method->name();   // 方法名

        // 客户端需要查询ZooKeeper，找到提供该服务的服务器地址
        ZkClient zkCli;
        zkCli.Start();  // 连接ZooKeeper服务器
        std::string host_data = QueryServiceHost(&zkCli, service_name, method_name, m_idx);  // 查询提供相应服务的服务器ip和port
        m_ip = host_data.substr(0, m_idx);  // 从查询结果中提取IP地址
        LOG(INFO) << "ip: " << m_ip;
        m_port = atoi(host_data.substr(m_idx + 1, host_data.size() - m_idx).c_str());  // 从查询结果中提取端口号
        LOG(INFO) << "port: " << m_port;

        // 尝试连接服务器
        auto rt = newConnect(m_ip.c_str(), m_port);
        if (!rt) {
            LOG(ERROR) << "connect server error";  // 连接失败，记录错误日志
            return;
        } else {
            LOG(INFO) << "connect server success";  // 连接成功，记录日志
        }
    }  // endif

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
        close(m_clientfd);
        m_clientfd = -1; // 重置
        controller->SetFailed("send error");
        return;
    }

    // 5. 接收响应(！阻塞等待服务器响应结果(不足之处->可以改进为异步回调or协程))
    // 格式：[4B Total Len] + [Response Data]
    
    // A. 先读4字节长度头
    uint32_t response_len = 0;
    if (recv_exact(m_clientfd, (char*)&response_len, 4) != 4) {
        close(m_clientfd);
        m_clientfd = -1;
        controller->SetFailed("recv response length error");
        return;
    }
    response_len = ntohl(response_len); // 转回主机字节序

    // B. 根据长度读取Body
    std::vector<char> recv_buf(response_len);
    if (recv_exact(m_clientfd, recv_buf.data(), response_len) != response_len) {    //vector<char> 的 .data()方法：获取首元素的原始地址
        close(m_clientfd);
        m_clientfd = -1;
        controller->SetFailed("recv response body error");
        return;
    }

    // 6. 反序列化响应
    if (!response->ParseFromArray(recv_buf.data(), response_len)) {
        close(m_clientfd);
        m_clientfd = -1;
        controller->SetFailed("parse response error");
        return;
    }
}

// 创建新的socket连接
bool KrpcChannel::newConnect(const char *ip, uint16_t port) {
    // 创建socket
    int clientfd = socket(AF_INET, SOCK_STREAM, 0);
    if (-1 == clientfd) {
        char errtxt[512] = {0};
        LOG(INFO) << "socket error" << strerror_r(errno, errtxt, sizeof(errtxt));  // 打印错误信息
        LOG(ERROR) << "socket error:" << errtxt;  // 记录错误日志
        return false;
    }

    // 设置服务器地址信息
    struct sockaddr_in server_addr;
    server_addr.sin_family = AF_INET;  // IPv4地址族
    server_addr.sin_port = htons(port);  // 端口号
    server_addr.sin_addr.s_addr = inet_addr(ip);  // IP地址

    // 尝试连接服务器
    if (-1 == connect(clientfd, (struct sockaddr *)&server_addr, sizeof(server_addr))) {
        close(clientfd);  // 连接失败，关闭socket
        char errtxt[512] = {0};
        LOG(INFO) << "connect error" << strerror_r(errno, errtxt, sizeof(errtxt));  // 打印错误信息
        LOG(ERROR) << "connect server error" << errtxt;  // 记录错误日志
        return false;
    }

    m_clientfd = clientfd;  // 保存socket文件描述符
    return true;
}

// 从ZooKeeper查询服务地址
std::string KrpcChannel::QueryServiceHost(ZkClient *zkclient, std::string service_name, std::string method_name, int &idx) {
    std::string method_path = "/" + service_name + "/" + method_name;  // 构造ZooKeeper路径
    LOG(INFO) << "method_path: " << method_path;

    std::unique_lock<std::mutex> lock(g_data_mutx);  // 加锁，保证线程安全
    std::string host_data_1 = zkclient->GetData(method_path.c_str());  // 从ZooKeeper获取数据
    lock.unlock();  // 解锁

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
KrpcChannel::KrpcChannel(bool connectNow) : m_clientfd(-1), m_idx(0) {
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
    auto rt = newConnect(m_ip.c_str(), m_port);
    int count = 3;  // 重试次数
    while (!rt && count--) {
        rt = newConnect(m_ip.c_str(), m_port);
    }

}