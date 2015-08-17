/**
 * @file
 * @brief
 *
 */

#ifndef REDIS_SENTINEL_CLIENT_KETAMA_H
#define REDIS_SENTINEL_CLIENT_KETAMA_H

#include <string>
#include <map>
#include <vector>
#include <stdint.h>
#include <sstream>
#include "RedisDB.h"

using namespace std;

namespace bfd
{
namespace redis
{

class Ketama
{
public:

	Ketama(const map<string, RedisClientPool*>& server_conn_map);
	~Ketama();

	RedisDB get(const std::string& key);

	void Init(const map<string, RedisClientPool*>& server_conn_map);

	vector<RedisDB> Reset(const map<string, RedisClientPool*>& server_conn_map);
private:




	void hash_config(const map<string, RedisClientPool*>& server_conn_map);

	RedisDB GetHashServer(const string& key);

	uint32_t hashKey(const std::string& key);

	string int2str(int i)
	{
		stringstream stream;
		stream << i;
		return stream.str();
	}
private:
	map<uint32_t, RedisDB> m_DBMap;
	vector<pair<uint32_t, RedisDB> > m_DBVec;
	pthread_mutex_t m_Mutex;


};
}
}

#endif
