kvtest:
	g++ ./test_proxy.cpp -I /home/meng.wei/redis-2.8.19/deps/hiredis \
						-I /home/meng.wei/redis-sentinel-client/src/cpp/include \
						-I /home/meng.wei/redis-sentinel-client/src/cpp/RedisSentinel \
						-I /home/meng.wei/redis-sentinel-client/src/cpp/ \
						-I /usr/local/bfd/include \
						-I /usr/local/include/zookeeper \
						-L /home/meng.wei/redis-sentinel-client/release -lkvdb \
						-L /usr/local/bfd/lib64 -lboost_thread \
						-L /home/meng.wei/redis-2.8.19/deps/hiredis -lhiredis -std=c++0x -pthread -g -o kvtest
.PHONY:
clean:
	rm kvtest
