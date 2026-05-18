#include "zookeeperutil.h"
#include "Krpcapplication.h"
#include <mutex>
#include "KrpcLogger.h"
#include <condition_variable>

std::mutex cv_mutex;        // 全局锁，用于保护共享变量的线程安全
std::condition_variable cv; // 条件变量，用于线程间通信
bool is_connected = false;  // 标记ZooKeeper客户端是否连接成功

// 全局的watcher观察器，用于接收ZooKeeper服务器的通知(本质：回调函数)
void global_watcher(zhandle_t *zh, int type, int status, const char *path, void *watcherCtx) 
{
    //通过watcherCtx传入ZkClient的this指针(需要用这个this指针调用ReConnect())
    ZkClient* m_client = (ZkClient*)watcherCtx;

    if (type == ZOO_SESSION_EVENT) {  // 回调消息类型和会话相关的事件
        if (status == ZOO_CONNECTED_STATE) {  // ZooKeeper客户端和服务器连接成功
            std::lock_guard<std::mutex> lock(cv_mutex);  // 加锁保护
            is_connected = true;  // 标记连接成功
        }
        else if(status == ZOO_EXPIRED_SESSION_STATE)
        {
            m_client->ReConnect();
        }
    }
    cv.notify_all();  // 通知所有等待的线程
}
/*！此处watcher实现的局限：
可改进1：会话过期(ZOO_EXPIRED_SESSION_STATE)没有进行处理->会话过期后需要重新连接，否则后续所有 ZooKeeper 操作都会失败
可改进2：节点事件没有进行处理->
            type == ZOO_CREATED_EVENT   // 节点被创建
            type == ZOO_DELETED_EVENT   // 节点被删除
            type == ZOO_CHANGED_EVENT   // 节点数据变化
            type == ZOO_CHILD_EVENT     // 子节点变化
*/

// 构造函数，初始化ZooKeeper客户端句柄为空
ZkClient::ZkClient() : m_zhandle(nullptr) {}

// 析构函数，关闭ZooKeeper连接
ZkClient::~ZkClient() {
    if (m_zhandle != nullptr) {
        zookeeper_close(m_zhandle);  // 关闭ZooKeeper连接
    }
}

// 启动ZooKeeper客户端，连接ZooKeeper服务器
void ZkClient::Start() {
    // 从配置文件中读取ZooKeeper服务器的IP和端口
    std::string host = KrpcApplication::GetInstance().GetConfig().Load("zookeeperip");
    std::string port = KrpcApplication::GetInstance().GetConfig().Load("zookeeperport");
    std::string connstr = host + ":" + port;  // 拼接连接字符串

    /*
    zookeeper_mt：多线程版本
    ZooKeeper的API客户端程序提供了三个线程：
    1. API调用线程 (主线程)
    2. 网络I/O线程 (zookeeper_init 内部创建，负责和ZK服务器收发数据)
    3. watcher回调线程 (zookeeper_init 内部创建，负责触发回调函数)
    */

    // 使用zookeeper_init初始化一个ZooKeeper客户端对象，异步建立与服务器的连接
    // zhandle_t *zookeeper_init(
    // const char *host,          // ZK 地址：ip:port,ip:port
    // watcher_fn watcher,        // 监听回调函数
    // int recv_timeout,          // 超时时间（毫秒）
    // const clientid_t *clientid,// 客户端ID（一般填 NULL）
    // void *context,             // 传给回调的上下文
    // int flags                  // 标志（填 0）
    // );
    m_zhandle = zookeeper_init(connstr.c_str(), global_watcher, 1000, nullptr, this, 0);
    if (nullptr == m_zhandle) {  // 初始化失败
        LOG(ERROR) << "zookeeper_init error";
        exit(EXIT_FAILURE);  // 退出程序
    }

    // ！解决 zookeeper_init 异步连接的同步问题：
    // API线程调用zookeeper_init()后，在其内部创建网络I/O线程+watcher回调线程，然后立刻返回，此时连接尚未建立，需要通过watcher+mutex+cond进行同步。
    // 具体如下：
    //     1.API线程先持有mutex，然后释放锁-阻塞在cond上(is_connected=false)
    //     2.网络I/O线程负责与zookeeper服务器建立TCP连接，然后通知watcher回调线程
    //     3.然后watcher回调线程收到通知，然后触发watcher回调：判断连接状态，连接完成后设置is_connected=true，然后notify_all()唤醒所有阻塞在cond的线程，实现连接的同步。

    // 等待连接成功
    std::unique_lock<std::mutex> lock(cv_mutex);
    cv.wait(lock, [] { return is_connected; });  // 阻塞等待，直到连接成功
    LOG(INFO) << "zookeeper_init success";  // 记录日志，表示连接成功
}

//会话过期，zkclient重新连接zkserver
void ZkClient::ReConnect()
{
    LOG(ERROR) << "zookeeper session expired, reconnecting...";
    
    if(m_zhandle != nullptr)
    {
        zookeeper_close(m_zhandle);
        m_zhandle = nullptr;
    }

    //复用
    {
        std::lock_guard<std::mutex> lock(cv_mutex);
        is_connected = false;
    }
    Start();
    LOG(INFO) << "zookeeper reconnect success";
}

// 创建ZooKeeper节点
void ZkClient::Create(const char *path, const char *data, int datalen, int state) {
    char path_buffer[128];  // 用于存储创建的节点路径
    int bufferlen = sizeof(path_buffer);

    // 检查节点是否已经存在
    int flag = zoo_exists(m_zhandle, path, 0, nullptr);
    if (flag == ZNONODE) {  // 如果节点不存在
        // 创建指定的ZooKeeper节点
        flag = zoo_create(m_zhandle, path, data, datalen, &ZOO_OPEN_ACL_UNSAFE, state, path_buffer, bufferlen);
        if (flag == ZOK) {  // 创建成功
            LOG(INFO) << "znode create success... path:" << path;
        } else {  // 创建失败
            LOG(ERROR) << "znode create failed... path:" << path;
            exit(EXIT_FAILURE);  // 退出程序
        }
    }
}

// 获取ZooKeeper节点的数据
std::string ZkClient::GetData(const char *path) {
    char buf[64];  // 用于存储节点数据
    int bufferlen = sizeof(buf);

    // 获取指定节点的数据
    int flag = zoo_get(m_zhandle, path, 0, buf, &bufferlen, nullptr);
    if (flag != ZOK) {  // 获取失败
        LOG(ERROR) << "zoo_get error";
        return "";  // 返回空字符串
    } else {  // 获取成功
        return buf;  // 返回节点数据
    }
    return "";  // 默认返回空字符串
}