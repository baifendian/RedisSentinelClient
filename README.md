# SentinelClient
## 介绍
	sentinel-client使用Redis做单节点的数据存储，Sentinel做高可用服务的K-V存储集群。 

## 高可用方案简介
  高可用方案是基于Redis官方提供的Sentinel实现的.Sentinel的作用是对Redis实例的监控、通知、容灾备份，是Redis集群的管理工具。在一个Redis集群中Sentinel作为中心监控各个节点的工作状态，提升系统的高可用性。详细的官方文档见:http://redis.io/topics/sentinel

## 一致性Hash
  数据的分布式存储采用的是一致性Hash算法，这里做了些改动，对于客户存储的key，实际在redis中存储的结构是bid_key
  bid 是应用程序按照业务来做的逻辑划分
  key 是客户存储的key
  
## 环境依赖
	1.CentOS release 6.5
	2.libssl.so
	3.依赖的redis的配置项databases的值设置为1024
	
## Log
```
	在程序的执行目录下会生成log文件(./redis_client_log.txt)
	使用libkvdb.so出问题时请先查看./redis_client_log.txt
```

## Install All
```编译c++, java, python
	tar -zxvf sentinel-client.tar.gz
	cd sentinel-client
	make
	完成后在sentinel-client目录下生成libkvdb.so(c++), kvdb.jar(java) pykvdb.so(python)
```

## Install c++
```编译c++
	tar -zxvf sentinel-client.tar.gz
	cd sentinel-client/cpp
	make
	完成后在sentinel-client/cpp目录下生成libkvdb.so
	将libkvdb.so的目录加入到环境变量LD_LIBRARY_PATH中
```

## Install java
```编译java
	tar -zxvf sentinel-client.tar.gz
	cd sentinel-client/cpp && make
	cd sentinel-client/java/jni && make
	cd sentinel-client/java && ant
	完成后在sentinel-client/java目录下生成kvdb.jar
	由于kvdb.jar需要在/tmp目录下写临时的so文件,因此需要将/tmp目录加入环境变量LD_LIBRARY_PATH中
	export LD_LIBRARY_PATH=/tmp:${LD_LIBRARY_PATH}
```

## Install python
```编译python
	tar -zxvf sentinel-client.tar.gz
	cd sentinel-client/cpp && make
	cd sentinel-client/python && make
	完成后在sentinel-client/python目录下生成libkvdb.so和pykvdb.so
	将libkvdb.so和pykvdb.so的目录加入到环境变量LD_LIBRARY_PATH和PYTHON_PATH中
```

## TestAll
```test all
	cd sentinel-client
	make test
	注:需要将sentinel-client/cpp/3party目录加入到环境变量LD_LIBRARY_PATH中
```

## TestC++
```test c++
	cd sentinel-client/cpp
	make test
	注:需要将sentinel-client/cpp/3party目录加入到环境变量LD_LIBRARY_PATH中
```

## TestJava
```test java
	cd sentinel-client/java
	ant -f buildtest.xml
	java -jar kvdbtest.jar
```

## C++ Demo
```c++
// 实例化客户端
#include "RedisClient.h"
RedisClient client = RedisClient("{{sentinel ip list}}", "{{business id}}");
// 方法1：使用封装好的接口
// GET
string value = client.get("my_key");
// SET
bool valid = client.set("test_key", "test_value")

// 方法2：使用自定义命令的接口
Reply reply = client.RedisCommand(Command("GET")("my_key"));
if (reply.error()) {
  // 处理获取失败的逻辑 
}
string value = reply.str();
```
### Member functions of Reply
```
Reply
    \_type()      #获取返回值类型 
    \_str()       #获取字符处返回值 
    \_integer()   #获取整形返回值
    \_elements()  #获取list类型返回值 
    \_error()     #命令是否执行失败 
```

## Python Demo
```python
import pykvdb
client = pykvdb.newClient('{{sentinel ip list}}','{{your business id}}')
pykvdb.set(client, 'mykey', 'myvalue')
print pykvdb.get(client, 'mykey')
```
## 备注
  ae事件库中使用的zmalloc和Python的Impot_Module不兼容，使用时需要将zmalloc、zfree替换为malloc和free

## Java Demo
```Java 
import com.redis.sentinel.client.RedisClient;
  
RedisClient client = new RedisClient("127.0.0.1:26379", "item");
boolean ret = client.set("kkk", "vvv");
String v = client.get("kkk");
```	

## Proxy
```proxy

cd sentinel-client/proxy/
make
vi etc/kvproxy.ini
./bin/bfdproxy &
```
## 备注
 1.proxy是基于kvproxy扩展的对redis的支持
 2.proxy内部调用redis-sentinel-client的cpp客户端
 3.proxy支持原生的redis协议
 4.由于redis-sentinel-client的cpp客户端的使用了 business_id,所以如果使用proxy访问以前通过redis-sentinel-client写入的数据,则需要用户自己处理business_id.
 例:
```
	1.之前通过redis-sentinel-client写入数据:

	 RedisClient client("127.0.0.1:16379", "item");
	 client.set("kk", "vv");


	 现在通过hiredis访问proxy获取数据:

	 string business_id = "item_";
	 string key = business_id + "kk";
	 string command = string("GET ") + key;
	 redisReply *reply = (redisReply *)redisCommand(cc,command.c_str());

	2.之前通过redis-sentinel-client写入数据:
	 RedisClient client("127.0.0.1:16379");
	 client.set("kk", "vv");	
	 RedisClient 的构造函数的第二个参数的默认值为"item", 访问proxy时仍然需要对key加上 "item_"前缀
```