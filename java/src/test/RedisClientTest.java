package test;

import java.util.ArrayList;
import java.util.List;

import com.redis.sentinel.client.RedisClient;
import com.redis.sentinel.client.Reply;

public class RedisClientTest {
	
	RedisClient client = new RedisClient("127.0.0.1:26379", "item", "");
	
	public void testSet() {
		
		boolean ret = client.set("kkk", "vvv");
		if(!ret) {
			System.out.println("test set faild !");
		} else {
			System.out.println("test set OK !");
		}
		
	}
	
	public void testGet() {
		String v = client.get("kkk");
		if (!v.equals("vvv")) {
			System.out.println("test get faild");
		} else {
			System.out.println("test get OK");
		}			
	}
	
	public void testmget() {
		List<String> values = client.mget("kkk");
		
		if (values.size() != 1) {
			System.out.println("test mget faild");
		} else {
			System.out.println("test mget OK");
		}
	}
	
	public void testmget2() {
		List<String> values = client.mget2("kkk");
		
		if (values.size() != 1) {
			System.out.println("test mget2 faild");
		} else {
			System.out.println("test mget2 OK");
		}
	}
	
	public void testmset() {
		boolean ret = client.mset("111", "222", "333", "444");
		if (!ret) {
			System.out.println("test mset faild");
		} else {
			System.out.println("test mset OK");
		}
	}
	public void testdel() {
		boolean ret = client.del("kkk");
		if (!ret) {
			System.out.println("test del faild");
		} else {
			System.out.println("test del OK");
		}
	}
	public void testmdel() {
		boolean ret = client.del("111", "333");
		if (!ret) {
			System.out.println("test dels faild");
		} else {
			System.out.println("test dels OK");
		}
	}
	public void testlpush() {
		boolean ret = client.lpush("555", "666", "777", "888");
		if (!ret) {
			System.out.println("test lpush faild");
		} else {
			System.out.println("test lpush OK");
		}
	}
	public void testlrange() {
		List<String> values = client.lrange("555", 1, 2);
		if (values.size() != 2) {
			System.out.println("test lrange faild");
		} else {
			System.out.println("test lrange OK");
		}
	}
	public void testzadd() {
		client.del("keyzadd");
		int ret = client.zadd("keyzadd", 1, "zaddvalue");
		if (ret != 1) {
			System.out.println("test zadd faild");
		} else {
			System.out.println("test zadd OK");
		}
	}
	public void testzrange() {
		List<String> values = client.zrange("keyzadd", 0, 1);
		if (values.size() != 1) {
			System.out.println("test zrange faild");
		} else {
			System.out.println("test zrange OK");
		}
	}
	public void testRedisCommand() {
		Reply rret = client.RedisCommand("set 888 111");
		if (!rret.getStr_().equals("OK")) {
			System.out.println("test RedisCommand faild");
		} else {
			System.out.println("test RedisCommand OK");
		}
	}
	
	//=================================
	
	public void testexists() {
		boolean ret = client.exists("888");
		if (!ret) {
			System.out.println("test exists faild");
		} else {
			System.out.println("test exists OK");
		}
	}
	
	public void testtype() {
		String ret = client.type("888");
		if (!ret.equals("string")) {
			System.out.println("test type faild");
		} else {
			System.out.println("test type OK");
		}
	}
	
	public void testsetnx() {
		boolean ret = client.setnx("888", "111");
		if (ret) {
			System.out.println("test setnx faild");
		} else {
			System.out.println("test setnx OK");
		}
	}
	
	public void testsetex() {
		boolean ret = client.setex("888", "111", 100);
		if (ret) {
			System.out.println("test setex faild");
		} else {
			System.out.println("test setex OK");
		}
	}
	
	public void testgetset() {
		String ret = client.getset("888", "222");
		if (!ret.equals("111")) {
			System.out.println("test getset faild");
		} else {
			System.out.println("test getset OK");
		}
	}
	
	public void testincr() {
		int ret = client.incr("888");
		if (ret != 223) {
			System.out.println("test incr faild");
		} else {
			System.out.println("test incr OK");
		}
	}
	
	public void testdecr() {
		int ret = client.decr("888");
		if (ret != 222) {
			System.out.println("test decr faild");
		} else {
			System.out.println("test decr OK");
		}
	}
	
	public void testincrby() {
		int ret = client.incrby("888", 1);
		if (ret != 223) {
			System.out.println("test incrby faild");
		} else {
			System.out.println("test incrby OK");
		}
	}
	
	public void testdecrby() {
		int ret = client.decrby("888", 1);
		if (ret != 222) {
			System.out.println("test decrby faild");
		} else {
			System.out.println("test decrby OK");
		}
	}
	
	public void testappend() {
		long ret = client.append("888", "111");
		if (ret != 6) {
			System.out.println("test append faild");
		} else {
			System.out.println("test append OK");
		}
	}
	
	public void testrpush() {
		int ret = client.rpush("rpush", "rpushv");
		if (ret != 1) {
			System.out.println("test rpush faild");
		} else {
			System.out.println("test rpush OK");
		}
	}
	
	public void testllen() {
		int ret = client.llen("rpush");
		if (ret != 1) {
			System.out.println("test llen faild");
		} else {
			System.out.println("test llen OK");
		}
	}
	
	public void testltrim() {
		boolean ret = client.ltrim("rpush", 0, 1);
		if (!ret) {
			System.out.println("test ltrim faild");
		} else {
			System.out.println("test ltrim OK");
		}
	}
	
	public void testlset() {
		boolean ret = client.lset("rpush", 0, "rpushv");
		if (!ret) {
			System.out.println("test lset faild");
		} else {
			System.out.println("test lset OK");
		}
	}
	
	public void testlrem() {
		boolean ret = client.lrem("rpush", 1, "rpushv");
		if (!ret) {
			System.out.println("test lrem faild");
		} else {
			System.out.println("test lrem OK");
		}
	}
	
	public void testlpop() {
		client.lpush("lpush", "lpushv");
		String ret = client.lpop("lpush");
		if (!ret.equals("lpushv")) {
			System.out.println("test lpop faild");
		} else {
			System.out.println("test lpop OK");
		}
		
	}
	
	public void testrpop() {
		client.lpush("lpush", "lpushv");
		String ret = client.rpop("lpush");
		if (!ret.equals("lpushv")) {
			System.out.println("test rpop faild");
		} else {
			System.out.println("test rpop OK");
		}
	}
	
	public void testsadd() {
		client.del("sadd");
		boolean ret = client.sadd("sadd", "saddv");
		if (!ret) {
			System.out.println("test sadd faild");
		} else {
			System.out.println("test sadd OK");
		}
	}
	
	public void testsrem() {
		boolean ret = client.srem("sadd", "saddv");
		if (!ret) {
			System.out.println("test srem faild");
		} else {
			System.out.println("test srem OK");
		}
	}
	
	public void testspop() {
		client.sadd("sadd", "saddv");
		String ret = client.spop("sadd");
		if (!ret.equals("saddv")) {
			System.out.println("test spop faild");
		} else {
			System.out.println("test spop OK");
		}
	}
	
	public void testsrandmember() {
		client.sadd("sadd", "saddv");
		String ret = client.srandmember("sadd");
		if (!ret.equals("saddv")) {
			System.out.println("test srandmember faild");
		} else {
			System.out.println("test srandmember OK");
		}
	}
	
	public void testscard() {
		int ret = client.scard("sadd");
		if (ret != 1) {
			System.out.println("test scard faild");
		} else {
			System.out.println("test scard OK");
		}
	}
	
	public void testsismember() {
		boolean ret = client.sismember("sadd", "saddv");
		if (!ret) {
			System.out.println("test sismember faild");
		} else {
			System.out.println("test sismember OK");
		}
	}
	
	public void testsmembers() {
		List<String> ret = client.smembers("sadd");
		if (ret.size() != 1) {
			System.out.println("test smembers faild");
		} else {
			System.out.println("test smembers OK");
		}
	}
	
	public void testzrem() {
		client.zadd("zadd", 1, "zaddv");
		boolean ret = client.zrem("zadd", "zaddv");
		if (!ret) {
			System.out.println("test zrem faild");
		} else {
			System.out.println("test zrem OK");
		}
	}
	
	public void testzincrby() {
		client.zadd("zadd", 1, "zaddv");
		int ret = client.zincrby("zadd", 1, "zaddv");
		if (ret != 2) {
			System.out.println("test zincrby faild");
		} else {
			System.out.println("test zincrby OK");
		}
	}
	
	public void testzrank() {
		int ret = client.zrank("zadd", "zaddv");
		if (ret != 0) {
			System.out.println("test zrank faild");
		} else {
			System.out.println("test zrank OK");
		}
	}
	
	public void testzrevrank() {
		int ret = client.zrevrank("zadd", "zaddv");
		if (ret != 0) {
			System.out.println("test zrevrank faild");
		} else {
			System.out.println("test zrevrank OK");
		}
	}
	
	public void testzrevrange() {
		List<String> ret = client.zrevrange("zadd", 0, 1);
		if (ret.size() != 1) {
			System.out.println("test zrevrange faild");
		} else {
			System.out.println("test zrevrange OK");
		}
	}
	
	public void testzrangebyscore() {
		List<String> ret = client.zrangebyscore("zadd", 1, 2);
		if (ret.size() != 1) {
			System.out.println("test testzrangebyscore faild");
		} else {
			System.out.println("test testzrangebyscore OK");
		}
	}
	
	public void testzcount() {
		int ret = client.zcount("zadd", 1, 2);
		if (ret != 1) {
			System.out.println("test zcount faild");
		} else {
			System.out.println("test zcount OK");
		}
	}
	
	public void testzcard() {
		int ret = client.zcard("zadd");
		if (ret != 1) {
			System.out.println("test zcard faild");
		} else {
			System.out.println("test zcard OK");
		}
	}
	
	public void testzscore() {
		int ret = client.zscore("zadd", "zaddv");
		if (ret != 2) {
			System.out.println("test zscore faild");
		} else {
			System.out.println("test zscore OK");
		}
	}
	
	public void testzremrangebyrank() {
		int ret = client.zremrangebyrank("zadd", 0, 1);
		if (ret != 1) {
			System.out.println("test zremrangebyrank faild");
		} else {
			System.out.println("test zremrangebyrank OK");
		}
	}
	
	public void testzremrangebyscore() {
		client.zadd("zadd", 1, "zaddv");
		int ret = client.zremrangebyscore("zadd", 1, 2);
		if (ret != 1) {
			System.out.println("test zremrangebyscore faild");
		} else {
			System.out.println("test zremrangebyscore OK");
		}
	}
	
	public void testhset() {
		boolean ret = client.hset("hset", "hfield", "hvalue");
		if (!ret) {
			System.out.println("test hset faild");
		} else {
			System.out.println("test hset OK");
		}
	}
	
	public void testhmget() {
		List<String> ret = client.hmget("hset", "hfield");
		if (ret.size() != 1) {
			System.out.println("test hmget faild");
		} else {
			System.out.println("test hmget OK");
		}
	}
	
	public void testhincrby() {
		client.hset("hset", "hfield", "11");
		int ret = client.hincrby("hset", "hfield", 1);
		if(ret != 12) {
			System.out.println("test hincrby faild");
		} else {
			System.out.println("test hincrby OK");
		}
	}
	
	public void testhexists() {
		boolean ret = client.hexists("hset", "hfield");
		if(!ret) {
			System.out.println("test hexists faild");
		} else {
			System.out.println("test hexists OK");
		}
	}
	

	
	public void testhlen() {
		int ret = client.hlen("hset");
		if (ret != 1) {
			System.out.println("test hlen faild");
		} else {
			System.out.println("test hlen OK");
		}
	}
	
	public void testhkeys() {
		List<String> ret = client.hkeys("hset");
		if (ret.size() != 1) {
			System.out.println("test hkeys faild");
		} else {
			System.out.println("test hkeys OK");
		}
	}
	
	public void testhvals() {
		List<String> ret = client.hvals("hset");
		if (ret.size() != 1) {
			System.out.println("test hvals faild");
		} else {
			System.out.println("test hvals OK");
		}
	}
	
	public void testhdel() {
		boolean ret = client.hdel("hset", "hfield");
		if (!ret) {
			System.out.println("test hdel faild");
		} else {
			System.out.println("test hdel OK");
		}
	}
	
	public static void main(String[] args) {
		
		RedisClientTest test = new RedisClientTest();
		System.out.println("start test");
		test.testSet();
		test.testGet();
		test.testmget();
		test.testmget2();
		test.testmset();
		test.testdel();
		test.testmdel();
		test.testlpush();
		test.testlrange();
		test.testzadd();
		test.testzrange();
		test.testRedisCommand();
		
		test.testexists();
		test.testtype();
		test.testsetnx();
		test.testsetex();
		test.testgetset();
		test.testincr();
		test.testdecr();
		test.testincrby();
		test.testdecrby();
		test.testappend();
		test.testrpush();
		test.testllen();
		test.testltrim();
		test.testlset();
		test.testlrem();
		test.testlpop();
		test.testrpop();
		test.testsadd();
		test.testsrem();
		test.testspop();
		test.testsrandmember();
		test.testscard();
		test.testsismember();
		test.testsmembers();
		test.testzrem();
		test.testzincrby();
		test.testzrank();
		test.testzrevrank();
		test.testzrevrange();
		test.testzrangebyscore();
		test.testzcount();
		test.testzcard();
		test.testzscore();
		test.testzremrangebyrank();
		test.testzremrangebyscore();
		
		test.testhset();
		test.testhmget();
		test.testhincrby();
		test.testhexists();
		test.testhlen();
		test.testhkeys();
		test.testhvals();
		test.testhdel();
		System.out.println("end test");
	}
}

