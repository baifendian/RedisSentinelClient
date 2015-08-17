
all:
	cd cpp && make
	cd cpp && make -f RedisCommandTool.Makefile
	cd cpp && make -f RedistributeTool.Makefile
	cp cpp/bin/libkvdb.so .
	cd java/jni && make
	cd java && ant
	cp java/kvdb.jar .
	cd python && make
	cp python/pykvdb.so .

.PHONY:test
test:all
	cd cpp && make test
	cd java && ant -f buildtest.xml
	cd java && java -jar kvdbtest.jar
	
clean:
	cd cpp && make clean
	cd java/jni && make clean
	cd python && make clean
	-rm -f libkvdb.so
	-rm -f kvdb.jar
	-rm -f pykvdb.so
	