CXX = g++
OBJ_DIR = obj
TOOLS_DIR = tools

INCPATH = -Iinclude
INCPATH += -I3party

LFLAGS += -L3party -lpthread
LFLAGS += -Lbin -lkvdb


TARGET = bin/RedisCommandTool

CXX_OBJS = $(OBJ_DIR)/RedisCommandTool.o 
			
$(OBJ_DIR)/%.o:$(TOOLS_DIR)/%.cpp
	$(CXX) -c -fPIC -o $@ $< $(INCPATH) $(LFLAGS)
	
$(OBJ_DIR)/%.o:$(TOOLS_DIR)/%.c
	$(CXX) -c -fPIC -o $@ $< $(INCPATH) $(LFLAGS)

			
$(TARGET):$(CXX_OBJS)
	$(CXX) -o $(TARGET) $(CXX_OBJS) $(LFLAGS)
	
.PHONY:clean
clean:
	-rm -f bin/RedisCommandTool
