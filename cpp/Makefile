CXX = g++
AR = ar cr
OBJ_DIR = obj
SRC_DIR = src

INCPATH = -Iinclude
INCPATH += -I3party

LFLAGS = -L3party -lhiredis
LFLAGS += -L3party -lssl 
LFLAGS += -L3party -ljemalloc -pthread


TARGETSO = bin/libkvdb.so
TARGETA = bin/libkvdb.a

CXX_OBJS = $(OBJ_DIR)/RedisClient.o \
			$(OBJ_DIR)/RedisClientPool.o \
			$(OBJ_DIR)/RedisDB.o \
			$(OBJ_DIR)/RedisSentinelManager.o \
			$(OBJ_DIR)/Reply.o \
			$(OBJ_DIR)/KetamaHasher.o \
			$(OBJ_DIR)/ae.o \
			$(OBJ_DIR)/zmalloc.o \
			$(OBJ_DIR)/Command.o
			
$(OBJ_DIR)/%.o:$(SRC_DIR)/%.cpp
	$(CXX) -c -fPIC -o $@ $< $(INCPATH) $(LFLAGS)
	
$(OBJ_DIR)/%.o:$(SRC_DIR)/%.c
	$(CXX) -c -fPIC -o $@ $< $(INCPATH) $(LFLAGS)

			
$(TARGETSO):$(CXX_OBJS)
	$(CXX) -fPIC -shared -o $(TARGETSO) $(CXX_OBJS) $(LFLAGS)
	$(AR) $(TARGETA) $(CXX_OBJS)
 
.PHONY:clean
clean:
	-rm -f obj/*.o
	-rm -f bin/*

.PHONY:test
test:
	-rm -f bin/RedisClientTest
	make -f RedisClientTest.Makefile
	./bin/RedisClientTest
