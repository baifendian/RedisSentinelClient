/* 
 * 使用RedisSentinel管理集群的配置信息
 * 1、在传入的sentinel地址中随机选取一个读取master节点信息，因为是同机房、
 *    同网段，所以暂不考虑在所有sentinel服务中选取延迟最小的一个这个逻辑。
 * 2、订阅master-switch事件，当集群信息有变化时，通知客户端更新集群信息
 *
 * Author:xu.yan@baifendian.com
 * Date:2014-04-04
 */
#ifndef REDIS_SENTINEL_MANAGER
#define REDIS_SENTINEL_MANAGER

#include <vector>
#include <algorithm>
#include <time.h>

#include "RedisClientPool.h"
#include "KetamaHasher.h"

#include "hiredis.h"
#include "hiredis_ae.h"
#include "async.h"

using namespace std;

namespace bfd
{
namespace redis
{

struct Sentinel
{
	string sentinelIP;
	int sentinelPort;
};

class RedisSentinelManager
{
public:
	RedisSentinelManager();
	~RedisSentinelManager();
	bool Init(const string& sentinels, const string& password);
	void SetKetamaHasher(Ketama *(&ketamaHasher)){m_KetamaHasher = ketamaHasher;};
	void Terminate();
//	bool CheckNewSentinel(const string& sentinel);
//	vector<RedisDB> AddSentinel(const string& sentinel);

	static void* monitorSentinel(void *arg);
	static void* AEThread(void *arg);

	map<string, RedisClientPool*>& servers()
	{
		return m_Servers;
	}
	;
	void UpdateServers(const string& master_name, const string& addr);

	bool CheckNewServer(const string& masterName);
	vector<RedisDB> AddServer(const string& masterName, const string& addr, int port);

	bool AddSentinelMonitor(const string& masterName, const string& addr, int port);

private:
	bool SubSentinelInfo(Sentinel& sen);
	map<string, RedisClientPool*> GetServers(Sentinel& sen);

private:
	redisAsyncContext* m_AsyncContext;
	pthread_mutex_t m_Mutex;

	string m_Password;
	map<string, RedisClientPool*> m_Servers;
	aeEventLoop *loop_;
	pthread_t m_AEThreadID;
	Ketama *m_KetamaHasher;

	vector<string> m_TotalSentinels;
	Sentinel m_PubSentinel;
};

} //redis
} //bfd
#endif
