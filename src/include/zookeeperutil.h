#ifndef _zookeeperutil_h_
#define _zookeeperutil_h_

#include<zookeeper/zookeeper.h>
#include<string>

//封装的zk客户端
class ZkClient
{
public:
    ZkClient();
    ~ZkClient();

    //zkclient启动连接zkserver
    void Start();

    //(网络断开/zkserver重启/zkclient长时间无心跳)->会话过期：zkclient重新连接zkserver
    void ReConnect();

    //在zkserver中创建一个节点，根据指定的path
    void Create(const char* path,const char* data,int datalen,int state=0);
    
    //根据参数指定的znode节点路径，获取znode节点值
    std::string GetData(const char* path);
private:
    //Zk的客户端句柄
    zhandle_t* m_zhandle;
};
#endif
