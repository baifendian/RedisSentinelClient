/**
 * @file
 * @brief
 *
 */

#ifndef REDIS_SENTINEL_CLIENT_REDISDB_H
#define REDIS_SENTINEL_CLIENT_REDISDB_H

#include "RedisClientPool.h"
#include "Reply.h"
#include "Command.h"
#include <map>

namespace bfd
{
namespace redis
{

struct KVMap
{
	KVMap():finish(false){};
	map<string, string> kvs;
	bool finish;
};

/**
 * @brief mget2
 */
class MgetAsyncResultMerger2
{
public:
	MgetAsyncResultMerger2():finish_(false){}
	// 用于mget异步调用，在callback中合并返回结果，每次mget生成一个这个对象
	int counter_;
	map<string, string> result_;
//	// condition variable to notify mget finish
//	std::condition_variable cv_;
	bool finish_;
	vector<bool> status_;
};

/**
 * @brief mget2
 */
class MgetAsyncRequestContext2
{
	// mget过程中，对每个server发起的请求的回调变量
public:
	MgetAsyncResultMerger2* merger_;
	vector<string> user_keylist_;
};

/**
 * @brief mget3
 */
class MgetAsyncResultMerger3
{
public:
	// 用于mget异步调用，在callback中合并返回结果，每次mget生成一个这个对象
	int counter_;
	KVMap *result_;
	vector<bool> status_;
};


/**
 * @brief mget3
 */
class MgetAsyncRequestContext3
{
	// mget过程中，对每个server发起的请求的回调变量
public:
	MgetAsyncResultMerger3* merger_;
	vector<string> user_keylist_;
};



class RedisDB
{
public:
	RedisDB();
	~RedisDB();

	const string GetRedisName() const
	{
		return m_RedisName;
	}

	const string GetRedisDBNo() const
	{
		return m_RedisDBNo;
	}

	RedisClientPool* GetConnPool() const
	{
		return m_ConnPool;
	}

	void SetRedisName(string redisName)
	{
		m_RedisName = redisName;
	}

	void SetRedisDBNo(string redisDBNo)
	{
		m_RedisDBNo = redisDBNo;
	}

	void SetConnPool(RedisClientPool* const (&connPool))
	{
		m_ConnPool = connPool;
	}

	Reply RedisCommand(vector<string> command);

	Reply RedisCommand(Command command);

	bool mget2(vector<string>& keys, MgetAsyncRequestContext2 *mget_async_rctxt,
			aeEventLoop *(&loop_));

	bool mget3(vector<string>& keys, MgetAsyncRequestContext3 *mget_async_rctxt,
			aeEventLoop *(&loop_));

	vector<string> keys(string prefix = "");

	RedisDB(const RedisDB& db)
	{
		this->m_RedisName = db.m_RedisName;
		this->m_RedisDBNo = db.m_RedisDBNo;
		this->m_ConnPool = db.m_ConnPool;
	}

	RedisDB& operator=(const RedisDB& db)
	{
		this->m_RedisName = db.m_RedisName;
		this->m_RedisDBNo = db.m_RedisDBNo;
		this->m_ConnPool = db.m_ConnPool;

		return *this;
	}

private:
	string m_RedisName;
	string m_RedisDBNo;
	RedisClientPool *m_ConnPool;



private:
	static void mgetCallback2(redisAsyncContext *c, void *r, void *privdata);
	static void mgetCallback3(redisAsyncContext *c, void *r, void *privdata);

	bool DoCommand(redisContext* (&redis), vector<string>& command, Reply& reply);
};

static bool operator<(const RedisDB& db1, const RedisDB& db2)
{
	if (&db1 == &db2) return false;

	if (db1.GetRedisName() < db2.GetRedisName())
	{
		return true;
	}
	else if (db1.GetRedisName() == db2.GetRedisName())
	{
		if (db1.GetRedisDBNo() < db2.GetRedisDBNo())
		{
			return true;
		}
		else if (db1.GetRedisDBNo() == db2.GetRedisDBNo())
		{
			return false;
		}
		else
		{
			return false;
		}
	}
	else
	{
		return false;
	}
}


}
}

#endif
