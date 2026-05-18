#ifndef _Krpcapplication_H
#define _Krpcapplication_H
#include "Krpcconfig.h"
#include "Krpcchannel.h" 
#include  "Krpccontroller.h"
#include "KrpcLogger.h"
#include<mutex>
//Krpc基础类，负责框架的一些初始化操作
//KrpcApplication 就是个单例，唯一的作用是持有全局配置 m_config，让框架任何地方都能通过 KrpcApplication::GetConfig() 拿到配置项
class KrpcApplication
{
    public:
    static void Init(int argc,char **argv);
    static KrpcApplication & GetInstance();
    static void deleteInstance();
    static Krpcconfig& GetConfig();
    private:
    static Krpcconfig m_config;
    static KrpcApplication * m_application;//全局唯一单例访问对象
    static std::mutex m_mutex;
    KrpcApplication(){}
    ~KrpcApplication(){}
    KrpcApplication(const KrpcApplication&)=delete;
    KrpcApplication(KrpcApplication&&)=delete;
};
#endif 
