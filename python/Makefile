CXX = g++
AR = ar cr
OBJ_DIR = obj
SRC_DIR = src

INCPATH = -I.
INCPATH += -I../cpp/include
INCPATH += -I../cpp/3party

LFLAGS = -Llib -lkvdb


TARGETSO = pykvdb.so

CXX_OBJS = $(OBJ_DIR)/pykvdb.o 
			
$(OBJ_DIR)/%.o:$(SRC_DIR)/%.cpp
	$(CXX) -c -fPIC -o $@ $< $(INCPATH) $(LFLAGS)
	
$(OBJ_DIR)/%.o:$(SRC_DIR)/%.c
	$(CXX) -c -fPIC -o $@ $< $(INCPATH) $(LFLAGS)

			
$(TARGETSO):$(CXX_OBJS)
	cp ../cpp/bin/libkvdb.so lib
	$(CXX) -fPIC -shared -o $(TARGETSO) $(CXX_OBJS) $(LFLAGS)
	cp lib/libkvdb.so .
 
.PHONY:clean
clean:
	-rm -f $(OBJ_DIR)/*.o
	-rm -f $(TARGETSO)
	-rm -f lib/*
	-rm -f libkvdb.so

