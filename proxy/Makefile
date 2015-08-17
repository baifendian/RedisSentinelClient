CXX = g++ -std=c++0x
AR = ar cr
OBJ_DIR = obj
SRC_DIR = src

INCPATH = -I.
INCPATH += -I../cpp/include
INCPATH += -I../cpp/3party
INCPATH += -I./deps/libevent2
INCPATH += -I./deps/libevent2/include

LFLAGS = -Llib -lkvdb
LFLAGS += -Llib -levent


TARGETSO = bin/bfdproxy

CXX_OBJS = $(OBJ_DIR)/kvproxy.o \
			$(OBJ_DIR)/extension.o \
			$(OBJ_DIR)/hash.o \
			$(OBJ_DIR)/serv.o \
			$(OBJ_DIR)/conn_pool.o \
			$(OBJ_DIR)/log.o \
			$(OBJ_DIR)/config.o \
			$(OBJ_DIR)/util.o 
			
			
$(OBJ_DIR)/%.o:$(SRC_DIR)/%.cpp
	$(CXX) -c -o $@ $< $(INCPATH) $(LFLAGS)
	
$(OBJ_DIR)/%.o:$(SRC_DIR)/%.c
	$(CXX) -c -o $@ $< $(INCPATH) $(LFLAGS)

			
$(TARGETSO):$(CXX_OBJS)
	cp ../cpp/bin/libkvdb.so lib
	$(CXX) -o $(TARGETSO) $(CXX_OBJS) $(LFLAGS)
 
.PHONY:clean
clean:
	-rm -f $(OBJ_DIR)/*.o
	-rm -f $(TARGETSO)

