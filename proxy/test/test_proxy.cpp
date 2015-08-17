#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "hiredis.h"
#include "time.h"
#include "RedisClient.h"

using namespace bfd::redis;

using namespace std;

redisContext *c = NULL;;

void *func(void *arg)
{
	redisContext *cc;
    const char *hostname = "127.0.0.1";
    int port = 55669;

    struct timeval timeout = { 1, 500000 }; // 1.5 seconds
    cc = redisConnectWithTimeout(hostname, port, timeout);

    //------------set---------
	struct timeval start, end;
	gettimeofday(&start, NULL);

    for (int i=0;i<100000; i++)
    {
    	redisReply *reply = (redisReply *)redisCommand(cc,"SET kk vv");
    	freeReplyObject(reply);
    }
	gettimeofday(&end, NULL);

	long ttt = 1000000*(end.tv_sec-start.tv_sec)
                  +(end.tv_usec-start.tv_usec);

    cout << "set 100000 time=" <<ttt/1000 << " s!" <<endl;
    //========================
    //------------get---------
	gettimeofday(&start, NULL);

    for (int i=0;i<100000; i++)
    {
    	redisReply *reply = (redisReply *)redisCommand(cc,"GET kk");
    	freeReplyObject(reply);
    }
	gettimeofday(&end, NULL);

	ttt = 1000000*(end.tv_sec-start.tv_sec)
                  +(end.tv_usec-start.tv_usec);

    cout << "get 100000 time=" <<ttt/1000 << " s!" <<endl;

    //========================
    return ((void *)0);
}

int main()
{
	time_t t1, t2;
    t1 = time(NULL);

//    redisContext *c;
    redisReply *reply;
    const char *hostname = "127.0.0.1";
    int port = 55669;

    struct timeval timeout = { 1, 500000 }; // 1.5 seconds
    c = redisConnectWithTimeout(hostname, port, timeout);
    if (c == NULL || c->err) {
        if (c) {
            printf("Connection error: %s\n", c->errstr);
            redisFree(c);
        } else {
            printf("Connection error: can't allocate redis context\n");
        }
        exit(1);
    }

    int err;
    pthread_t id1, id2, id3;
	err = pthread_create(&id1, NULL, func, NULL); //创建线程

//	err = pthread_create(&id2, NULL, func, NULL); //创建线程

//	err = pthread_create(&id3, NULL, func, NULL); //创建线程

	pthread_join(id1, NULL);
//	pthread_join(id2, NULL);
//	pthread_join(id3, NULL);

    t2 = time(NULL);
    cout << "total 20000 time=" <<t2-t1 << " s!" <<endl;
	return 0;
}
