/**
 * @file
 * @brief
 *
 */

#ifndef REDIS_SENTINEL_CLIENT_REDISCLIENT_H
#define REDIS_SENTINEL_CLIENT_REDISCLIENT_H

#include <string>
#include <vector>
#include "Reply.h"
#include "Command.h"
#include "KetamaHasher.h"
#include "RedisSentinelManager.h"

using namespace std;

namespace bfd
{
namespace redis
{

/**
 * @brief 表示一个redis命令,redisDBNo为该命令所在的DB号
 *
 */
struct Comm
{
	string redisDBNo;
	vector<string> command;
};

class RedisClient
{
public:
	  RedisClient(const string& sentinel_addr, const string& bid="", const string& password="");
	  ~RedisClient();
	  vector<RedisDB> AddServer(const string& masterName, const string& addr, int port);
	  bool AddSentinelMonitor(const string& masterName, const string& addr, int port);

	  /**
	   * @brief key
	   */
	  bool exists(string key, bool ifBid = true);
	  int del(string key, bool ifBid = true);
	  int del(vector<string>& keys, bool ifBid = true);
	  string type(string key, bool ifBid = true);
	  bool expire(string key, int seconds, bool ifBid = true);

	  /**
	   * @brief string
	   */
	  bool set(string key, string value, bool ifBid = true);
	  bool setnx(string key, string value, bool ifBid = true);
	  bool setex(string key, string value, int seconds, bool ifBid = true);
	  string get(string key, bool ifBid = true);
	  string getset(string key, string value, bool ifBid = true);

	  /**
	   * @brief 同步
	   */
	  vector<string> mget(vector<string>& keys, bool ifBid = true);

	  /**
	   * @brief 半异步, 不支持多线程调用. 异步执行命令,等所有命令都返回结果后, mget2会return
	   */
	  vector<string> mget2(vector<string>& keys, bool ifBid = true);

	  /**
	   * @brief 全异步, 不支持多线程调用.异步执行命令,等所有命令都返回结果后, kvs.finish=true
	   */
	  bool mget3(vector<string>& keys, KVMap& kvs, bool ifBid = true);

	  bool mset(map<string, string>& keyvalues, bool ifBid = true);
	  int incr(string key, bool ifBid = true);
	  int decr(string key, bool ifBid = true);
	  int incrby(string key, int incr, bool ifBid = true);
	  int decrby(string key, int incr, bool ifBid = true);
	  long append(string key, string value, bool ifBid = true);

	  /**
	   * @brief list
	   */
	  int lpush(string key, string value, bool ifBid = true);
	  int rpush(string key, string value, bool ifBid = true);
	  int lpush(string key, vector<string> values, bool ifBid = true);
	  int rpush(string key, vector<string> values, bool ifBid = true);
	  int llen(string key, bool ifBid = true);
	  vector<string> lrange(string key, int start, int end, bool ifBid = true);
	  bool ltrim(string key, int start, int end, bool ifBid = true);
	  bool lset(string key, int index, string value, bool ifBid = true);
	  bool lrem(string key, int count, string value, bool ifBid = true);
	  string lpop(string key, bool ifBid = true);
	  string rpop(string key, bool ifBid = true);

	  /**
	   * @brief set
	   */
	  bool sadd(string key, string member, bool ifBid = true);
	  int sadd(string key, vector<string> members, bool ifBid = true);
	  bool srem(string key, string member, bool ifBid = true);
	  string spop(string key, bool ifBid = true);
	  string srandmember(string key, bool ifBid = true);
	  int scard(string key, bool ifBid = true);
	  bool sismember(string key, string member, bool ifBid = true);
	  vector<string> smembers(string key, bool ifBid = true);

	  /**
	   * @brief sorted set
	   */
	  bool zadd(string key, int score, string member, bool ifBid = true);
	  bool zrem(string key, string member, bool ifBid = true);
	  int zincrby(string key, int incr, string member, bool ifBid = true);
	  int zrank(string key, string member, bool ifBid = true);
	  int zrevrank(string key, string member, bool ifBid = true);
	  vector<string> zrange(string key, int start, int end, bool ifBid = true);
	  vector<string> zrevrange(string key, int start, int end, bool ifBid = true);
	  vector<string> zrangebyscore(string key, int min, int max, bool ifBid = true);
	  int zcount(string key, int min, int max, bool ifBid = true);
	  int zcard(string key, bool ifBid = true);
	  int zscore(string key, string member, bool ifBid = true);
	  int zremrangebyrank(string key, int min, int max, bool ifBid = true);
	  int zremrangebyscore(string key, int min, int max, bool ifBid = true);

	  /**
	   * @brief hash
	   */
	  bool hset(string key, string field, string value, bool ifBid = true);
	  string hget(string key, string field, bool ifBid = true);
	  vector<string> hmget(string key, vector<string>& field, bool ifBid = true);
	  bool hmset(string key, vector<string>& fields, vector<string>& values, bool ifBid = true);
	  int hincrby(string key, string field, int incr, bool ifBid = true);
	  bool hexists(string key, string field, bool ifBid = true);
	  bool hdel(string key, string field, bool ifBid = true);
	  int hlen(string key, bool ifBid = true);
	  vector<string> hkeys(string key, bool ifBid = true);
	  vector<string> hvals(string key, bool ifBid = true);
	  bool hgetall(string key, vector<string>& fields, vector<string>& values, bool ifBid = true);

	  /**
	   * @brief command
	   */
	  Reply RedisCommand(const vector<string>& command, bool ifBid = true);
	  Reply RedisCommand(Command& command, bool ifBid = true);
	  vector<Reply> RedisCommands(vector<Command>& commands, bool ifBid = true);
	  vector<Reply> RedisCommands(map<RedisClientPool*, vector<Comm> >& commands);

private:
	  Ketama *m_KetamaHasher;
	  RedisSentinelManager* m_ConfigPtr;
	  string m_BID;

	  aeEventLoop *loop_;

private:
	  static void* AEThread(void *arg);
};

}
}

#endif
