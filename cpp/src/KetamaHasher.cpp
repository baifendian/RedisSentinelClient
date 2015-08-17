#include "KetamaHasher.h"
#include "MD5Util.h"
#include "ScopedLock.h"

using namespace MyUtil;
using namespace bfd::redis;


Ketama::Ketama(const map<string, RedisClientPool*>& server_conn_map) : m_Mutex(PTHREAD_MUTEX_INITIALIZER)
{
	Init(server_conn_map);
}

Ketama::~Ketama()
{

}

RedisDB Ketama::get(const std::string& key)
{
    if (m_DBVec.size() == 0) {
      exit(1);
    }

    return GetHashServer(key);
}

void Ketama::Init(const map<string, RedisClientPool*>& server_conn_map)
{
	ScopedLock lock(m_Mutex);
	m_DBMap.clear();
	m_DBVec.clear();

	hash_config(server_conn_map);

    for (map<uint32_t, RedisDB>::const_iterator it = m_DBMap.begin();
        it != m_DBMap.end(); ++it)
    {
    	m_DBVec.push_back(make_pair(it->first, it->second));
    }
}

vector<RedisDB> Ketama::Reset(const map<string, RedisClientPool*>& server_conn_map)
{
	vector<pair<uint32_t, RedisDB> > dbvec = m_DBVec;
	vector<RedisDB> ret;

	Init(server_conn_map);


    for (size_t i=0,j=0; i<dbvec.size(); i++)
    {
    	while (dbvec[i].first > m_DBVec[j].first)
    	{
    		j++;
    	}
    	if (dbvec[i].first == m_DBVec[j].first)
    	{
    		if (i==0 && j>0)
    		{
    			ret.push_back(dbvec[i].second);
    		}
    		else if ((i>0) && (dbvec[i-1].first != m_DBVec[j-1].first))
    		{
    			ret.push_back(dbvec[i].second);
    		}
    	}
    }

    if ((dbvec[0].first == m_DBVec[0].first) &&
    		(dbvec[dbvec.size()-1].first != m_DBVec[m_DBVec.size()-1].first))
    {
    	ret.push_back(dbvec[0].second);
    }


    return ret;
}

void Ketama::hash_config(const map<string, RedisClientPool*>& server_conn_map)
{
    for (map<string, RedisClientPool*>::const_iterator it = server_conn_map.begin();
        it != server_conn_map.end(); ++it)
    {
    	for (int i=0; i<1024; ++i)
    	{
    		RedisDB db;
    		db.SetRedisName(it->first);
    		db.SetRedisDBNo(int2str(i));
    		db.SetConnPool(it->second);

    		string strdb = it->first + ":" + int2str(i);

    		uint32_t hash = hashKey(strdb);
    		m_DBMap[hash] = db;
    	}
    }
}

RedisDB Ketama::GetHashServer(const string& key)
{
	ScopedLock lock(m_Mutex);

    uint32_t hash = hashKey(key);
    int left = 0;
    int right = m_DBVec.size() - 1;
    int end = right;
    int middle;
    while (left < right) {
      middle = left + (right - left) / 2;
      if (m_DBVec[middle].first < hash) {
        left = middle + 1;
        continue;
      } else if (m_DBVec[middle].first > hash) {
        right = middle;
        continue;
      } else {
        return m_DBVec[right].second;
      }
    }

    if (right == end && m_DBVec[right].first < hash) {
      right = 0;
    }
    return m_DBVec[right].second;
}

uint32_t Ketama::hashKey(const std::string& key)
{
    unsigned char results[16];
    MD5Util::stringToMD5(key, results);
    return ((uint32_t)(results[3] & 0xFF) << 24)
        | ((uint32_t)(results[2] & 0xFF) << 16)
        | ((uint32_t)(results[1] & 0xFF) << 8) | ((uint32_t)(results[0] & 0xFF));
}
