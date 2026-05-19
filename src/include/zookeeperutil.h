#ifndef _zookeeperutil_h_
#define _zookeeperutil_h_

#include<zookeeper/zookeeper.h>
#include<string>
#include<mutex>
#include<unordered_map>

//封装的zk客户端
class ZkClient
{
public:
    static ZkClient& GetInstance() {
        static ZkClient instance;  // C++11线程安全
        return instance;
    }


    //zkclient启动连接zkserver
    void Start();

    //(网络断开/zkserver重启/zkclient长时间无心跳)->会话过期：zkclient重新连接zkserver
    void ReConnect();

    //在zkserver中创建一个节点，根据指定的path
    void Create(const char* path,const char* data,int datalen,int state=0);
    
    //根据参数指定的znode节点路径，获取znode节点值
    std::string GetData(const char* path);

    //操作zk缓存的接口          const->支持传递临时变量
    bool HasCache(const std::string& path);
    std::string GetCache(const std::string& path);
    void SetCache(const std::string& path,const std::string& value);
    void DeleteCache(const std::string& path);

private:
    ZkClient();
    ~ZkClient();
    ZkClient(const ZkClient&) = delete;      // 禁止拷贝构造
    ZkClient& operator=(const ZkClient&) = delete;  // 禁止赋值

    //Zk的客户端句柄
    zhandle_t* m_zhandle;

    //缓存rpc服务器的ip:port
    static std::mutex m_cache_mutex;
    static std::unordered_map<std::string,std::string> m_host_cache;   //path -> ip:port
    //注意：
    //错误1：const char* 作为key，比较的是指针地址，不是字符串内容
    //错误2：没有设置成static成员，不是类共享的缓存和锁
};
#endif
