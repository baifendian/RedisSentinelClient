CXX = g++
AR = ar cr
OBJ_DIR = .
SRC_DIR = .

INCPATH = -I.
INCPATH += -I../../cpp/include
INCPATH += -I../../cpp/3party
INCPATH += -I/usr/local/jdk1.7/include
INCPATH += -I/usr/local/jdk1.7/include/linux

LFLAGS = -L../lib -lkvdb


TARGETSO = ../lib/libjnikvdb.so

CXX_OBJS = $(OBJ_DIR)/com_redis_sentinel_client_RedisClient.o \
			$(OBJ_DIR)/JNI.o 
			
$(OBJ_DIR)/%.o:$(SRC_DIR)/%.cpp
	$(CXX) -c -fPIC -o $@ $< $(INCPATH) $(LFLAGS)
	
$(OBJ_DIR)/%.o:$(SRC_DIR)/%.c
	$(CXX) -c -fPIC -o $@ $< $(INCPATH) $(LFLAGS)

			
$(TARGETSO):$(CXX_OBJS)
	cp ../../cpp/bin/libkvdb.so ../lib
	$(CXX) -fPIC -shared -o $(TARGETSO) $(CXX_OBJS) $(LFLAGS)
 
.PHONY:clean
clean:
	-rm -f ./*.o
	-rm -f $(TARGETA)

