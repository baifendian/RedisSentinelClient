#ifndef _REDIS_ADDR_INFO_H__
#define _REDIS_ADDR_INFO_H__


#ifdef __cplusplus
extern "C" {
#endif
#include <stdint.h>
void RedisAddrInit(char* addr);
void RedisAddrUnit();
void  RefreshMasterRedisAddr();
const char* GetMasterRedisIp();
uint32_t GetMasterRedisPort();
#ifdef __cplusplus
}
#endif

#endif



