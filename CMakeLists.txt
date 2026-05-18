#设置最低版本和项目名称
cmake_minimum_required(VERSION 3.0)
project(Krpc)

#设置全局的C++标准
set(CMAKE_CXX_STANDARD 17)#在这里我设置了17，代表使用c++17标准
set(CMAKE_CXX_STANDARD_REQUIRED True)#CMAKE_CXX_STANDARD_REQUIRED表示强制使用指定的标准

#设置头文件目录，供所有子项目使用
#include_directories(${CMAKE_SOURCE_DIR}/src/include) #其中CMAKE_SOURCE_DIR表示当前目录

#查找porotbuf包
find_package(Protobuf REQUIRED)
include_directories(${Protobuf_INCLUDE_DIRS})#Protobuf_INCLUDE_DIRS表示protobuf头文件目录

# 你的头文件路径：/usr/include/zookeeper
include_directories(/usr/include/zookeeper)

# 你的库文件路径：/usr/lib/x86_64-linux-gnu/libzookeeper_mt.so
set(ZOOKEEPER_LIBRARIES /usr/lib/x86_64-linux-gnu/libzookeeper_mt.so)

# === 3. 强制启用同步 API ===
add_definitions(-DTHREADED)  # 必须保留！否则 zoo_exists 等函数无法调用

#设置全局链接库
set(LIBS
    protobuf
    pthread
    ${ZOOKEEPER_LIBRARIES}  # 直接注入硬编码路径
    muduo_net
    muduo_base
    glog
)
#添加子目录
add_subdirectory(src)
add_subdirectory(example)
