CXX = g++
OBJ_DIR = obj
TEST_DIR = test

INCPATH = -Iinclude
INCPATH += -I3party

LFLAGS += -L3party -lgtest_main -lgtest -lpthread
LFLAGS += -Lbin -lkvdb


TARGET = bin/RedisClientTest

CXX_OBJS = $(OBJ_DIR)/RedisClientTest.o 
			
$(OBJ_DIR)/%.o:$(TEST_DIR)/%.cpp
	$(CXX) -c -fPIC -o $@ $< $(INCPATH) $(LFLAGS)
	
$(OBJ_DIR)/%.o:$(TEST_DIR)/%.c
	$(CXX) -c -fPIC -o $@ $< $(INCPATH) $(LFLAGS)

			
$(TARGET):$(CXX_OBJS)
	$(CXX) -o $(TARGET) $(CXX_OBJS) $(LFLAGS)
	
.PHONY:clean
clean:
	-rm -f bin/RedisClientTest
