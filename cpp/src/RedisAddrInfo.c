#include <iostream>
#include <string>
#include <stdint.h>
#include "RedisAddrInfo.h"
#include "RedisClient.h"
#include "Utils.h"
#include "RedisClientPool.h"
using namespace bfd::redis;

RedisSentinelManager *g_manager = NULL;
Ketama *g_ketamaHasher = NULL;
std::string g_masterRedisIp;
uint32_t g_masterRedisPort = 0;
uint32_t Str2Uint32( const std::string & s )
{
    return (uint32_t)atol( s.c_str() );
}

void RedisAddrInit(char* addr)
{
    if(g_manager == NULL)
    {
        g_manager = new RedisSentinelManager();
        bool ret =  g_manager->Init(addr, "");
        if(!ret){
           std::cout<<"Init err "<<std::endl;
           exit(0);
           return;
        }
    }

    if(g_ketamaHasher == NULL)
    {
        g_ketamaHasher = new Ketama(g_manager->servers());

        g_manager->SetKetamaHasher(g_ketamaHasher);
    }
    RedisClientPool* pool = g_manager->GetFirstMasterPool();
    if(pool != NULL)
    {
        std::string redisAddr = pool->getId();
        std::string::size_type pos = redisAddr.find(":");
        if (std::string::npos == pos)
        {
            std::cout<<"invalid addr:%s"<<redisAddr<<std::endl;
            exit(0);
        }
        
        g_masterRedisIp = redisAddr.substr(0, pos);
        g_masterRedisPort = Str2Uint32(redisAddr.substr(pos+1));
        std::cout<<"redis ip:"<<g_masterRedisIp.c_str()<<"port:"<<g_masterRedisPort<<std::endl;   
         
    }
    else
    {
         std::cout<<"get redis addr err"<<std::endl;
         exit(0);
    }

}
void RedisAddrUnit()
{
    if(g_manager != NULL)
    {
        delete g_manager;
        g_manager = NULL;
    }
    if(g_ketamaHasher != NULL)
    {
        delete g_ketamaHasher;
        g_ketamaHasher = NULL;
    }
}
void  RefreshMasterRedisAddr()
{
    RedisClientPool* pool = g_manager->GetFirstMasterPool();
    if(pool != NULL)
    {
        std::string redisAddr = pool->getId();
        std::string::size_type pos = redisAddr.find(":");
        if (std::string::npos == pos)
        {
            std::cout<<"invalid addr:%s"<<redisAddr<<std::endl;
            return;
        }
        
        g_masterRedisIp = redisAddr.substr(0, pos);
        g_masterRedisPort = Str2Uint32(redisAddr.substr(pos+1));
        std::cout<<"redis ip:"<<g_masterRedisIp.c_str()<<"port:"<<g_masterRedisPort<<std::endl;       
    }
}
const char* GetMasterRedisIp()
{
    return g_masterRedisIp.c_str();
}
uint32_t GetMasterRedisPort()
{
    return g_masterRedisPort;
}