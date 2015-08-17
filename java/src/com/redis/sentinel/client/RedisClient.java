package com.redis.sentinel.client;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class RedisClient {
	
	public RedisClient(String Sentinel_addr, String Business_id, String password) {
		cinit(Sentinel_addr, Business_id, password);
	}
	
	public boolean exists(String key) {
		return cexists(key);
	}

	public String type(String key) {
		return ctype(key);
	}
	
	public boolean setnx(String key, String value) {
		return csetnx(key, value);
	}
	
	public boolean setex(String key, String value, int seconds) {
		return csetex(key, value, seconds);
	}
	
	public String getset(String key, String value) {
		return cgetset(key, value);
	}
	
	public int incr(String key) {
		return cincr(key);
	}
	
	public int decr(String key) {
		return cdecr(key);
	}
	
	public int incrby(String key, int incr) {
		return cincrby(key, incr);
	}
	
	public int decrby(String key, int decr) {
		return cdecrby(key, decr);
	}
	
	public long append(String key , String value) {
		return cappend(key, value);
	}
	
	public int rpush(String key, String value) {
		return crpush(key, value);
	}
	
	public int llen(String key) {
		return cllen(key);
	}
	
	public boolean ltrim(String key, int start, int end) {
		return cltrim(key, start, end);
	}
	
	public boolean lset(String key, int index, String value) {
		return clset(key, index, value);
	}
	
	public boolean lrem(String key, int count, String value) {
		return clrem(key, count, value);
	}
	
	public String lpop(String key) {
		return clpop(key);
	}
	
	public String rpop(String key) {
		return crpop(key);
	}
	
	public boolean sadd(String key, String member) {
		return csadd(key, member);
	}
	
	public boolean srem(String key, String member) {
		return csrem(key, member);
	}
	
	public String spop(String key) {
		return cspop(key);
	}
	
	public String srandmember(String key) {
		return csrandmember(key);
	}
	
	public int scard(String key) {
		return cscard(key);
	}
	
	public boolean sismember(String key, String member) {
		return csismember(key, member);
	}
	
	public List<String> smembers(String key) {
		return csmembers(key);
	}

	public boolean zrem(String key, String member) {
		return czrem(key, member);
	}
	
	public int zincrby(String key, int incr, String member) {
		return czincrby(key, incr, member);
	}
	
	public int zrank(String key, String member) {
		return czrank(key, member);
	}
	
	public int zrevrank(String key, String member) {
		return czrevrank(key, member);
	}
	
	public List<String> zrevrange(String key, int start, int end) {
		return czrevrange(key, start, end);
	}
	
	public List<String> zrangebyscore(String key, int min, int max) {
		return czrangebyscore(key, min, max);
	}
	
	public int zcount(String key, int min, int max) {
		return czcount(key, min, max);
	}
	
	public int zcard(String key) {
		return czcard(key);
	}
	
	public int zscore(String key, String member) {
		return czscore(key, member);
	}
	
	public int zremrangebyrank(String key, int min, int max) {
		return czremrangebyrank(key, min, max);
	}
	
	public int zremrangebyscore(String key, int min, int max) {
		return czremrangebyscore(key, min, max);
	}
	
	public boolean hset(String key, String field, String value) {
		return chset(key, field, value);
	}
	
	public List<String> hmget(String key, String... fields) {
		return chmget(key, fields);
	}
	
	public int hincrby(String key, String field, int incr) {
		return chincrby(key, field, incr);
	}
	
	public boolean hexists(String key, String field) {
		return chexists(key, field);
	}
	
	public boolean hdel(String key, String field) {
		return chdel(key, field);
	}
	
	public int hlen(String key) {
		return chlen(key);
	}
	
	public List<String> hkeys(String key) {
		return chkeys(key);
	}
	
	public List<String> hvals(String key) {
		return chvals(key);
	}

	
	public byte[] getBytes(byte[] key) {
		return cgetBytes(key);
	}
	
	public boolean setBytes(byte[] key, byte[] value) {
		return csetBytes(key, value);
	}
	
	public String get(final String key) {
		return cget(key);
	}
	
	public List<String> mget(final String... keys) {
		return cmget(keys);
	}
	
	public List<String> mget2(final String... keys) {
		return cmget2(keys);
	}
	
	public boolean set(final String key, String value) {
		return cset(key, value);
	}
	
	public boolean set(final String key, String value, long expireTime) {
		return cset(key, value, expireTime);
	}
	
	public boolean mset(final String... keysvalues) {
		return cmset(keysvalues);
	}
	
	public boolean del(final String... keys) {
		return cdel(keys);
	}
	
	public boolean del(String key) {
		return cdel(key);
	}
	
	public boolean lpush(final String key, final String... strings) {
		return clpush(key, strings);
	}
	
	public List<String> lrange(final String key, final int start, final int end) {
		return clrange(key, start, end);
	}
	
	public List<byte[]> lrange(byte[] key, int start, int end) {
		return clrange(key, start, end);
	}
	
	public String hget(final String key, final String field) {
		return chget(key, field);
	}
	
	public int zadd(final String key, final int score, final String member) {
		return czadd(key, score, member);
	}
	
	public List<String> zrange(final String key, final int start, final int end) {
		return czrange(key, start, end);
	}
	
	private Reply StringToReply(final String str) {
		Reply rep = new Reply();
		
		String[] strs = str.split(";"); 
		
		if (strs.length > 0)
			rep.setType_(type_t.values()[Integer.parseInt(strs[0])-1]);
		if (strs.length > 1)
			rep.setStr_(strs[1]);
		if (strs.length > 2)
			rep.setError_(Boolean.parseBoolean(strs[2]));
		if  (strs.length > 3)
			rep.setInteger_(Integer.parseInt(strs[3]));
		
		if (strs.length > 4)
		{
			for (int i=4; i<strs.length; i+=4) {
				Reply tmprep = new Reply();
				
				rep.setType_(type_t.values()[Integer.parseInt(strs[i])-1]);
				rep.setStr_(strs[i+1]);
				rep.setError_(Boolean.parseBoolean(strs[i+2]));
				rep.setInteger_(Integer.parseInt(strs[i+3]));
				
				rep.AddElements(tmprep);
			}
		}
		
		return rep;
	}
	
	public Reply RedisCommand(final String command) {
		String str = cRedisCommand(command);
		
		return StringToReply(str);
	}
	
	
	//JNI接口
	
	static { 

		try {
			InputStream jnisoinput = Class.class.getResource("/libjnikvdb.so").openStream();
//			File jniso = File.createTempFile("libjnikvdb", ".so");
			File jniso = new File("/tmp/libjnikvdb.so");
			FileOutputStream jnisooutput = new FileOutputStream(jniso);
			
			int jnii;
			byte[] jnibuf = new byte[1024];
			while ((jnii=jnisoinput.read(jnibuf))!=-1) {
				jnisooutput.write(jnibuf, 0, jnii);
			}
			
			jnisoinput.close();
			jnisooutput.close();
			jniso.deleteOnExit();
			
			InputStream soinput = Class.class.getResource("/libkvdb.so").openStream();
//			File so = File.createTempFile("libkvdb", ".so");
			File so = new File("/tmp/libkvdb.so");
			FileOutputStream sooutput = new FileOutputStream(so);
			
			int i;
			byte[] buf = new byte[1024];
			while ((i=soinput.read(buf))!=-1) {
				sooutput.write(buf, 0, i);
			}
			
			soinput.close();
			sooutput.close();
			so.deleteOnExit();
						
			System.load(so.toString());
			System.load(jniso.toString());
			
		}catch (Exception e) {
			System.err.println("load jni error");
		}
	}; 
		
	private native static void cinit(String Sentinel_addr, String Business_id, String password);
	
	private native static String cget(final String key);
	
	private native static ArrayList cmget(final String... keys);
	
	private native static ArrayList cmget2(final String... keys);
	
	private native static boolean cset(final String key, String value);
	
	private native static boolean cmset(final String... keysvalues);
	
	private native static boolean cdel(final String... keys);
	
	private native static boolean cdel(String key);
	
	private native static boolean clpush(final String key, final String... strings);
	
	private native static ArrayList clrange(final String key, final int start, final int end);
	
	private native static String chget(final String key, final String field);
	
	private native static int czadd(final String key, final int score, final String member);
	
	private native static ArrayList czrange(final String key, final int start, final int end);
	
	private native static String cRedisCommand(final String command);
	
	private native static byte[] cgetBytes(byte[] key);
	
	private native static boolean csetBytes(byte[] key, byte[] value);
	
	private native static boolean cset(final String key, String value, long expireTime);
	
	private native static List<byte[]> clrange(byte[] key, int start, int end);
	
	
	//000
	private native static boolean cexists(String key);
	
	private native static String ctype(String key);
	
	private native static boolean csetnx(String key, String value);
	
	private native static String cgetset(String key, String value);
	
	private native static int cincr(String key);
	
	private native static int cdecr(String key);
	
	private native static int cincrby(String key, int incr);
	
	private native static int cdecrby(String key, int decr);
	
	private native static long cappend(String key , String value);
	
	private native static int crpush(String key, String value);
	
	private native static int cllen(String key);
	
	private native static boolean cltrim(String key, int start, int end);
	
	private native static boolean clset(String key, int index, String value);
	
	private native static boolean clrem(String key, int count, String value);
	
	private native static String clpop(String key);
	
	private native static String crpop(String key);
	
	private native static boolean csadd(String key, String member);
	
	private native static boolean csrem(String key, String member);
	
	private native static String cspop(String key);
	
	private native static String csrandmember(String key);
	
	private native static int cscard(String key);
	
	private native static boolean csismember(String key, String member);
	
	private native static List<String> csmembers(String key);
	
	private native static boolean czrem(String key, String member);
	
	private native static int czincrby(String key, int incr, String member);
	
	private native static int czrank(String key, String member);
	
	private native static int czrevrank(String key, String member);
	
	private native static List<String> czrevrange(String key, int start, int end);
	
	private native static List<String> czrangebyscore(String key, int min, int max);
	
	private native static int czcount(String key, int min, int max);
	
	private native static int czcard(String key);
	
	private native static int czscore(String key, String member);
	
	private native static int czremrangebyrank(String key, int min, int max);
	
	private native static int czremrangebyscore(String key, int min, int max);
	
	private native static boolean chset(String key, String field, String value);
	
	private native static List<String> chmget(String key, String... fields);
	
	private native static int chincrby(String key, String field, int incr);
	
	private native static boolean chexists(String key, String field);
	
	private native static boolean chdel(String key, String field);
	
	private native static int chlen(String key);
	
	private native static List<String> chkeys(String key);
	
	private native static List<String> chvals(String key);
	
	private native static boolean csetex(String key, String value, int seconds);
	
};
