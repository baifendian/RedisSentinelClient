#include "RedisSentinelManager.h"
#include "Utils.h"
#include "Log.h"
#include "ScopedLock.h"
#include <string.h>

namespace bfd
{
namespace redis
{
RedisSentinelManager::RedisSentinelManager():m_Mutex(PTHREAD_MUTEX_INITIALIZER),m_AEThreadID(0)
{
	// do nothing
	loop_ = aeCreateEventLoop(64);
}
RedisSentinelManager::~RedisSentinelManager()
{
	// do nothing
}

void RedisSentinelManager::Terminate()
{
	// do nothing
}

void* RedisSentinelManager::monitorSentinel(void *arg)
{

	RedisSentinelManager* ptr = reinterpret_cast<RedisSentinelManager*>(arg);

	while (1)
	{
		redisContext *c = redisConnect(ptr->m_PubSentinel.sentinelIP.c_str(), ptr->m_PubSentinel.sentinelPort);
		if ((c != NULL && c->err) || (c==NULL))
		{
			if (c != NULL)
			{
				redisFree(c);
				c = NULL;
			}
		    sleep(1);

			while (1)
			{
				vector<string> sentinels = ptr->m_TotalSentinels;
				size_t i=0;
				for (; i<sentinels.size(); i++)
				{
					vector<string> s_addr_port;
					s_addr_port = split(sentinels[i], ':');

					if (s_addr_port.size() != 2)
					{
						continue;
					}
					ptr->m_PubSentinel.sentinelIP = s_addr_port[0];
					ptr->m_PubSentinel.sentinelPort = atoi(s_addr_port[1].c_str());

					redisContext *c = redisConnect(ptr->m_PubSentinel.sentinelIP.c_str(), ptr->m_PubSentinel.sentinelPort);
					if (c != NULL && c->err)
					{
						redisFree(c);
						c = NULL;
						continue;
					}

					if (c == NULL) continue;

					if (c != NULL)
					{
						redisFree(c);
						c = NULL;
					}
					break;
				}

				if (i < sentinels.size())
				{
					stringstream stream;
					stream << "new ip=" << ptr->m_PubSentinel.sentinelIP << "; new port=" << ptr->m_PubSentinel.sentinelPort << endl;
					LOG(INFO, stream.str());
					ptr->SubSentinelInfo(ptr->m_PubSentinel);
					break;
				}
				sleep(1);
			}
		}

		if (c != NULL)
		{
			redisFree(c);
			c = NULL;
		}

		sleep(1);

	}
}

// ===================================================================
// 初始化
bool RedisSentinelManager::Init(const string& address, const string& password)
{
	m_Password = password;

	stringstream stream;
	stream << "RedisSentinelManager::Init address: " << address;
	LOG(INFO, stream.str());

	if (address.empty())
	{
		stringstream stream;
		stream << "address should not be empty";
		LOG(ERROR, stream.str());

		return false;
	}

	m_TotalSentinels = split(address, ',');

	srand((unsigned) time(NULL)); //生成种子
	int s_index = rand() % m_TotalSentinels.size();
	vector<string> s_addr_port;
	s_addr_port = split(m_TotalSentinels[s_index], ':');
	if (s_addr_port.size() != 2)
	{
		stringstream stream;
		stream << "selected addr_port is invalid, " << m_TotalSentinels[s_index];
		LOG(ERROR, stream.str());

		return false;
	}

	m_PubSentinel.sentinelIP = s_addr_port[0];
	m_PubSentinel.sentinelPort = atoi(s_addr_port[1].c_str());

	map<string, RedisClientPool*> servers = GetServers(m_PubSentinel);

	// 订阅所有频道，更新master地址信息
	SubSentinelInfo(m_PubSentinel);

	//更新配置,以及链接信息
	{
		ScopedLock lock(m_Mutex);
		m_Servers.swap(servers);
	}


	pthread_t monitorThreadID;

	int ret=pthread_create(&monitorThreadID,NULL, &monitorSentinel,this);
	if (ret != 0)
	{
		stringstream stream;
		stream << "Create pthread error!";
		LOG(ERROR, stream.str());

		exit(1);
	}

	return true;
}

map<string, RedisClientPool*> RedisSentinelManager::GetServers(Sentinel& sen)
{
	redisContext *c = redisConnect(sen.sentinelIP.c_str(), sen.sentinelPort);
	if (c != NULL && c->err)
	{
		stringstream stream;
		stream << "Connect sentinel error: " << c->errstr;
		LOG(ERROR, stream.str());

		// FIXME: 需要尝试另一个sentinel.建立错误直接返回NULL
		c = NULL;
		exit(1);
	}
	// 获取master节点信息
	redisReply * reply;
	reply = (redisReply*) redisCommand(c, "SENTINEL masters");
	if (reply->type != REDIS_REPLY_ARRAY)
	{
		stringstream stream;
		stream << "redis sentinel masters reply type is invalid.";
		LOG(ERROR, stream.str());
		exit(1);
	}

	map<string, RedisClientPool*> servers;
	for (size_t i = 0; i < reply->elements; ++i)
	{
		size_t j = 0;
		string ip;
		string port;
		string flags;
		string name;
		// 初始化 master的IP, PORT
		while (j < reply->element[i]->elements)
		{
			if (strcmp(reply->element[i]->element[j]->str, "ip") == 0)
			{
				ip = reply->element[i]->element[++j]->str;
				continue;
			}
			if (strcmp(reply->element[i]->element[j]->str, "port") == 0)
			{
				port = reply->element[i]->element[++j]->str;
				continue;
			}
			if (strcmp(reply->element[i]->element[j]->str, "flags") == 0)
			{
				flags = reply->element[i]->element[++j]->str;
				continue;
			}
			if (strcmp(reply->element[i]->element[j]->str, "name") == 0)
			{
				name = reply->element[i]->element[++j]->str;
				continue;
			}
			++j;
		}
		// 根据ODOWN判断节点是否挂掉
		if (flags.find_first_of("o_down") != std::string::npos)
		{
			stringstream stream;
			stream << "master: [" << ip << ":" << port << "] is ODOWN, flags: " << flags;
			LOG(ERROR, stream.str());

			continue;
		}
		//printf("reply[%zu]: %s:%s,flags:%s\n", i, ip.c_str(), port.c_str(), flags.c_str());
		servers.insert(make_pair(name, new RedisClientPool(ip, atoi(
				port.c_str()), m_Password)));
	}

	if (c != NULL)
	{
		redisFree(c);
		c = NULL;
	}
	if (reply != NULL)
	{
		freeReplyObject(reply);
		reply = NULL;
	}

	return servers;
}

void RedisSentinelManager::UpdateServers(const string& master_name,
		const string& new_addr)
{
	string ip;
	string port;
	vector<string> addr_vec;
	addr_vec = split(new_addr, ':');
	if (addr_vec.size() != 2)
	{
		stringstream stream;
		stream << "swich new addr faild, info is invalid: [" << new_addr << "]";
		LOG(ERROR, stream.str());
		return;
	}
	ip = addr_vec[0];
	port = addr_vec[1];

	ScopedLock lock(m_Mutex);
	m_Servers.erase(master_name);
	m_Servers.insert(make_pair(master_name, new RedisClientPool(ip, atoi(
			port.c_str()))));
	m_KetamaHasher->Init(m_Servers);

	stringstream stream;
	stream << "master:" << master_name << ", new_addr:" << new_addr;
	LOG(INFO, stream.str());
}
//--Async function
void subCallback(redisAsyncContext *c, void *r, void *priv)
{
	RedisSentinelManager *rsm = (RedisSentinelManager *) priv;
	redisReply *reply = (redisReply*) r;
	if (reply == NULL)
		return;
	if (reply->type == REDIS_REPLY_ARRAY)
	{
		size_t j = 0;
		while (j < reply->elements)
		{
			if (reply->element[j]->type == REDIS_REPLY_STRING && strcmp(
					reply->element[j]->str, "+switch-master") == 0)
			{
				string message = reply->element[++j]->str;

				stringstream stream;
				stream << "receive message: " << message;
				LOG(INFO, stream.str());

				vector<string> msg_vec;
				msg_vec = split(message, ' ');
				if (msg_vec.size() != 5)
				{
					stringstream stream;
					stream << "receive msg:[" << message << "] is invalid.";
					LOG(ERROR, stream.str());

					continue;
				}
				else
				{
					string master_name = msg_vec[0];
					string old_addr = msg_vec[1] + ":" + msg_vec[2];
					string new_addr = msg_vec[3] + ":" + msg_vec[4];
					rsm->UpdateServers(master_name, new_addr);
					break;
				}
				continue;
			}
			else if (reply->element[j]->type == REDIS_REPLY_STRING && strcmp(
					reply->element[j]->str, "+monitor") == 0)
			{
				//TODO
				string message = reply->element[++j]->str;

				stringstream stream;
				stream << "receive message: " << message;
				LOG(INFO, stream.str());

				vector<string> msg_vec;
				msg_vec = split(message, ' ');
				if (msg_vec.size() != 6)
				{
					stringstream stream;
					stream << "receive msg:[" << message << "] is invalid.";
					LOG(ERROR, stream.str());

					continue;
				}
				else
				{
					string master_name = msg_vec[1];
					string addr = msg_vec[2];
					int port = atoi(msg_vec[3].c_str());

					if (rsm->CheckNewServer(master_name))
					{
						rsm->AddServer(master_name, addr, port);
					}
					break;
				}
				continue;
			}
			++j;
		}
	}
}

void connectCallback(const redisAsyncContext *c, int status)
{
	if (status != REDIS_OK)
	{
		stringstream stream;
		stream << "Error: " << c->errstr;
		LOG(ERROR, stream.str());

		return;
	}

	stringstream stream;
	stream << "Connected callback...";
	LOG(INFO, stream.str());
}

void disconnectCallback(const redisAsyncContext *c, int status)
{
	if (status != REDIS_OK)
	{
		stringstream stream;
		stream << "Error: " << c->errstr;
		LOG(ERROR, stream.str());
		return;
	}

	stringstream stream;
	stream << "async disconnected...stopping event loop...";
	LOG(INFO, stream.str());
}

bool RedisSentinelManager::SubSentinelInfo(Sentinel& sen)
{
	m_AsyncContext = NULL;
//	loop_ = aeCreateEventLoop(64);
	m_AsyncContext = redisAsyncConnect(sen.sentinelIP.c_str(), sen.sentinelPort);
	if (m_AsyncContext->err)
	{
		stringstream stream;
		stream << "Error: " << m_AsyncContext->errstr;
		LOG(ERROR, stream.str());
		return false;
	}
	redisAeAttach(loop_, m_AsyncContext);
	redisAsyncSetConnectCallback(m_AsyncContext, connectCallback);
	redisAsyncSetDisconnectCallback(m_AsyncContext, disconnectCallback);
	redisAsyncCommand(m_AsyncContext, subCallback, this, "PSUBSCRIBE *");



	if (m_AEThreadID == 0)
	{
		int ret=pthread_create(&m_AEThreadID,NULL, &AEThread,this);
		if (ret != 0)
		{
			stringstream stream;
			stream << "Create pthread error!";
			LOG(ERROR, stream.str());

			exit(1);
		}
	}
	return true;
}

void* RedisSentinelManager::AEThread(void *arg)
{
	RedisSentinelManager* ptr = reinterpret_cast<RedisSentinelManager*>(arg);

	aeMain(ptr->loop_);
}

vector<RedisDB> RedisSentinelManager::AddServer(const string& masterName, const string& addr, int port)
{
	m_Servers[masterName] = new RedisClientPool(addr, port, m_Password);

	vector<RedisDB> dbs = m_KetamaHasher->Reset(m_Servers);

	stringstream stream;
	stream << "master:" << masterName << ", add new_addr:" << addr << ", port=" << port;
	LOG(INFO, stream.str());

	return dbs;
}

bool RedisSentinelManager::CheckNewServer(const string& masterName)
{
	if (m_Servers.count(masterName) > 0)
	{
		return false;
	}
	else
	{
		return true;
	}
}

bool RedisSentinelManager::AddSentinelMonitor(const string& masterName, const string& addr, int port)
{
	bool ret = true;

	stringstream streamcommand;
	streamcommand << "sentinel monitor " << masterName << " " << addr << " " << port << " 3";

	for (size_t i=0; i<m_TotalSentinels.size(); i++)
	{
		vector<string> s_addr_port;
		s_addr_port = split(m_TotalSentinels[i], ':');
		if (s_addr_port.size() != 2)
		{
			stringstream stream;
			stream << "selected addr_port is invalid, " << m_TotalSentinels[i];
			LOG(ERROR, stream.str());

			return false;
		}

		string sentinelIP = s_addr_port[0];
		int sentinelPort = atoi(s_addr_port[1].c_str());

		redisContext *c = redisConnect(sentinelIP.c_str(), sentinelPort);
		if (c != NULL && c->err)
		{
			stringstream stream;
			stream << "Connect sentinel error: " << c->errstr;
			LOG(ERROR, stream.str());

			// FIXME: 需要尝试另一个sentinel.建立错误直接返回NULL
			c = NULL;
			ret = false;
		}
		// 获取master节点信息
		redisReply * reply;
		reply = (redisReply*) redisCommand(c, streamcommand.str().c_str());
		if (reply->type != REDIS_REPLY_STRING)
		{
			stringstream stream;
			stream << "redis sentinel monitor reply type is invalid.";
			LOG(ERROR, stream.str());
			ret = false;
		}

		if (string(reply->str) != string("OK"))
		{
			ret = false;
		}

		if (c != NULL)
		{
			redisFree(c);
			c = NULL;
		}
		if (reply != NULL)
		{
			freeReplyObject(reply);
			reply = NULL;
		}

		if(!ret) return ret;
	}

	return true;
}

} //redis
} //bfd
