#include "com_redis_sentinel_client_RedisClient.h"
#include "JNI.h"
#include "RedisClient.h"
#include <iostream>

using namespace std;
using namespace bfd::redis;

RedisClient *client = NULL;

void split(std::string& s, std::string delim,std::vector<std::string>& ret)
{
    size_t last = 0;
    size_t index=s.find_first_of(delim,last);
    while (index!=std::string::npos)
    {
        ret.push_back(s.substr(last,index-last));
        last=index+1;
        index=s.find_first_of(delim,last);
    }
    if (index-last>0)
    {
        ret.push_back(s.substr(last,index-last));
    }
}

void ReplyToString(Reply& reply, std::string &rep)
{
	stringstream stream;
	stream << reply.type() << ";"
			<< reply.str() << ";"
			<< reply.error() << ";"
			<< reply.integer() << ";";

	std::vector<Reply> elements = reply.elements();

	for (size_t i=0; i<elements.size(); i++)
	{
		stream << elements[i].type() << ";"
				<< elements[i].str() << ";"
				<< elements[i].error() << ";"
				<< elements[i].integer() << ";";
	}

	rep = stream.str();
}


void ReplyToString(vector<Reply>& replys, std::vector<std::string>& reps)
{
	for (size_t i=0; i<replys.size(); i++)
	{
		std::string rep;
		ReplyToString(replys[i], rep);
		reps.push_back(rep);
	}
}
/*
 * Class:     com_redis_sentinel_client_RedisClient
 * Method:    cinit
 * Signature: (Ljava/lang/String;Ljava/lang/String;)V
 */
JNIEXPORT void JNICALL Java_com_redis_sentinel_client_RedisClient_cinit
  (JNIEnv *env, jclass, jstring sentinel_addr, jstring bid, jstring password)
{
	const char* addr = env->GetStringUTFChars(sentinel_addr, NULL);
	const char* busiID = env->GetStringUTFChars(bid, NULL);
	const char* pass = env->GetStringUTFChars(password, NULL);
	std::string sentinelAddr = string(addr);
	std::string businessID = string(busiID);
	std::string strpass = string(pass);

	env->ReleaseStringUTFChars(sentinel_addr, addr);
	env->ReleaseStringUTFChars(bid, busiID);
	env->ReleaseStringUTFChars(password, pass);

	if (client == NULL)
	{
		client = new RedisClient(sentinelAddr, businessID, strpass);
	}
}

/*
 * Class:     com_redis_sentinel_client_RedisClient
 * Method:    cget
 * Signature: (Ljava/lang/String;)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_com_redis_sentinel_client_RedisClient_cget
  (JNIEnv *env, jclass, jstring jKey)
{
	const char* ckey = env->GetStringUTFChars(jKey, NULL);
	std::string key = string(ckey);
	env->ReleaseStringUTFChars(jKey, ckey);

	std::string value = client->get(key);

	jstring jvalue = env->NewStringUTF(value.c_str());

	return jvalue;
}

/*
 * Class:     com_redis_sentinel_client_RedisClient
 * Method:    cmget
 * Signature: ([Ljava/lang/String;)Ljava/util/ArrayList;
 */
JNIEXPORT jobject JNICALL Java_com_redis_sentinel_client_RedisClient_cmget
  (JNIEnv *env, jclass, jobjectArray jkeys)
{
	std::vector<std::string> ckeys;

	int size=env->GetArrayLength(jkeys);

	for (int i=0; i<size; i++)
	{

		jstring jkey = (jstring)env->GetObjectArrayElement(jkeys, i);

		const char* pkey = env->GetStringUTFChars(jkey, NULL);
		std::string ckey(pkey);

		ckeys.push_back(ckey);

		env->ReleaseStringUTFChars(jkey, pkey);
		env->DeleteLocalRef(jkey);
	}

	vector<string> values = client->mget(ckeys);

	return JNI::VectorToJList(env, values);
}

/*
 * Class:     com_redis_sentinel_client_RedisClient
 * Method:    cmget2
 * Signature: ([Ljava/lang/String;)Ljava/util/ArrayList;
 */
JNIEXPORT jobject JNICALL Java_com_redis_sentinel_client_RedisClient_cmget2
  (JNIEnv *env, jclass, jobjectArray jkeys)
{
	std::vector<std::string> ckeys;

	int size=env->GetArrayLength(jkeys);

	for (int i=0; i<size; i++)
	{

		jstring jkey = (jstring)env->GetObjectArrayElement(jkeys, i);

		const char* pkey = env->GetStringUTFChars(jkey, NULL);
		std::string ckey(pkey);

		ckeys.push_back(ckey);

		env->ReleaseStringUTFChars(jkey, pkey);
		env->DeleteLocalRef(jkey);
	}

	vector<string> values = client->mget2(ckeys);

	return JNI::VectorToJList(env, values);
}

/*
 * Class:     com_redis_sentinel_client_RedisClient
 * Method:    cset
 * Signature: (Ljava/lang/String;Ljava/lang/String;)Z
 */
JNIEXPORT jboolean JNICALL Java_com_redis_sentinel_client_RedisClient_cset__Ljava_lang_String_2Ljava_lang_String_2
  (JNIEnv *env, jclass, jstring jKey, jstring jValue)
{
	const char* ckey = env->GetStringUTFChars(jKey, NULL);
	const char* cvalue = env->GetStringUTFChars(jValue, NULL);
	std::string key(ckey);
	std::string value(cvalue);

	env->ReleaseStringUTFChars(jKey, ckey);
	env->ReleaseStringUTFChars(jValue, cvalue);

	return client->set(key, value);
}

/*
 * Class:     com_redis_sentinel_client_RedisClient
 * Method:    cmset
 * Signature: ([Ljava/lang/String;)Z
 */
JNIEXPORT jboolean JNICALL Java_com_redis_sentinel_client_RedisClient_cmset
  (JNIEnv *env, jclass, jobjectArray jKeys)
{
	map<string, string> kvMap;
	std::string key = "";
	std::string value = "";

	int size=env->GetArrayLength(jKeys);

	for (int i=0; i<size; i++)
	{
		jstring jkey = (jstring)env->GetObjectArrayElement(jKeys, i);

		const char* ckv = env->GetStringUTFChars(jkey, NULL);
		std::string kv(ckv);
		env->ReleaseStringUTFChars(jkey, ckv);

		if (i%2 == 0)
		{
			key = kv;
		}
		else
		{
			value = kv;
			kvMap[key] = value;
		}
	}

	return client->mset(kvMap);
}

/*
 * Class:     com_redis_sentinel_client_RedisClient
 * Method:    cdel
 * Signature: ([Ljava/lang/String;)Z
 */
JNIEXPORT jboolean JNICALL Java_com_redis_sentinel_client_RedisClient_cdel___3Ljava_lang_String_2
  (JNIEnv *env, jclass, jobjectArray jKeys)
{
	std::vector<std::string> ckeys;

	int size=env->GetArrayLength(jKeys);

	for (int i=0; i<size; i++)
	{
		jstring jkey = (jstring)env->GetObjectArrayElement(jKeys, i);

		const char* ckey = env->GetStringUTFChars(jkey, NULL);
		std::string key(ckey);

		env->ReleaseStringUTFChars(jkey, ckey);
		env->DeleteLocalRef(jkey);

		ckeys.push_back(key);
	}

	return client->del(ckeys);
}

/*
 * Class:     com_redis_sentinel_client_RedisClient
 * Method:    cdel
 * Signature: (Ljava/lang/String;)Z
 */
JNIEXPORT jboolean JNICALL Java_com_redis_sentinel_client_RedisClient_cdel__Ljava_lang_String_2
  (JNIEnv *env, jclass, jstring jKey)
{
	const char* ckey = env->GetStringUTFChars(jKey, NULL);
	std::string key(ckey);
	env->ReleaseStringUTFChars(jKey, ckey);

	return client->del(key);
}

/*
 * Class:     com_redis_sentinel_client_RedisClient
 * Method:    clpush
 * Signature: (Ljava/lang/String;[Ljava/lang/String;)Z
 */
JNIEXPORT jboolean JNICALL Java_com_redis_sentinel_client_RedisClient_clpush
  (JNIEnv *env, jclass, jstring jKey, jobjectArray jValues)
{
	const char* ckey = env->GetStringUTFChars(jKey, NULL);
	std::string key(ckey);
	env->ReleaseStringUTFChars(jKey, ckey);

	int size=env->GetArrayLength(jValues);

	for (int i=0; i<size; i++)
	{
		jstring jvalue = (jstring)env->GetObjectArrayElement(jValues, i);

		const char* cvalue = env->GetStringUTFChars(jvalue, NULL);
		std::string value(cvalue);

		env->ReleaseStringUTFChars(jvalue, cvalue);
		env->DeleteLocalRef(jvalue);

		bool ret = client->lpush(key, value);

		if (!ret) return ret;
	}

	return true;
}

/*
 * Class:     com_redis_sentinel_client_RedisClient
 * Method:    clrange
 * Signature: (Ljava/lang/String;II)Ljava/util/ArrayList;
 */
JNIEXPORT jobject JNICALL Java_com_redis_sentinel_client_RedisClient_clrange__Ljava_lang_String_2II
  (JNIEnv *env, jclass, jstring jKey, jint jstart, jint jend)
{
	const char* ckey = env->GetStringUTFChars(jKey, NULL);
	std::string key(ckey);
	env->ReleaseStringUTFChars(jKey, ckey);

	int start = jstart;
	int end = jend;

	std::vector<std::string> cvalues = client->lrange(key, start, end);

	return JNI::VectorToJList(env, cvalues);
}

/*
 * Class:     com_redis_sentinel_client_RedisClient
 * Method:    chget
 * Signature: (Ljava/lang/String;Ljava/lang/String;)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_com_redis_sentinel_client_RedisClient_chget
  (JNIEnv *env, jclass, jstring jKey, jstring jfield)
{
	const char* ckey = env->GetStringUTFChars(jKey, NULL);
	const char* cfield = env->GetStringUTFChars(jfield, NULL);
	std::string key(ckey);
	std::string field(cfield);
	env->ReleaseStringUTFChars(jKey, ckey);
	env->ReleaseStringUTFChars(jfield, cfield);

	std::string value = client->hget(key, field);

	jstring jvalue = env->NewStringUTF(value.c_str());

	return jvalue;
}

/*
 * Class:     com_redis_sentinel_client_RedisClient
 * Method:    czadd
 * Signature: (Ljava/lang/String;ILjava/lang/String;)I
 */
JNIEXPORT jint JNICALL Java_com_redis_sentinel_client_RedisClient_czadd
  (JNIEnv *env, jclass, jstring jKey, jint jScore, jstring jMember)
{
	const char* ckey = env->GetStringUTFChars(jKey, NULL);
	std::string key(ckey);
	env->ReleaseStringUTFChars(jKey, ckey);

	int score = jScore;
	const char* cmember = env->GetStringUTFChars(jMember, NULL);
	std::string member(cmember);

	return client->zadd(key, score, member);
}

/*
 * Class:     com_redis_sentinel_client_RedisClient
 * Method:    czrange
 * Signature: (Ljava/lang/String;II)Ljava/util/ArrayList;
 */
JNIEXPORT jobject JNICALL Java_com_redis_sentinel_client_RedisClient_czrange
  (JNIEnv *env, jclass, jstring jKey, jint jStart, jint jEnd)
{
	std::vector<std::string> values;

	const char* ckey = env->GetStringUTFChars(jKey, NULL);
	std::string key(ckey);
	env->ReleaseStringUTFChars(jKey, ckey);

	int start = jStart;
	int end = jEnd;

	values = client->zrange(key, start, end);

	return JNI::VectorToJList(env, values);
}

/*
 * Class:     com_redis_sentinel_client_RedisClient
 * Method:    cRedisCommand
 * Signature: (Ljava/lang/String;)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_com_redis_sentinel_client_RedisClient_cRedisCommand__Ljava_lang_String_2
  (JNIEnv *env, jclass, jstring jCommand)
{
	const char* ccommand = env->GetStringUTFChars(jCommand, NULL);
	std::string comm(ccommand);
	env->ReleaseStringUTFChars(jCommand, ccommand);

	std::vector<std::string> command;
	split(comm, std::string(" "), command);

	Reply ret = client->RedisCommand(command);

	stringstream stream;
	stream << ret.type() << ";"
			<< ret.str() << ";"
			<< ret.error() << ";"
			<< ret.integer() << ";";

	std::vector<Reply> elements = ret.elements();

	for (size_t i=0; i<elements.size(); i++)
	{
		stream << elements[i].type() << ";"
				<< elements[i].str() << ";"
				<< elements[i].error() << ";"
				<< elements[i].integer() << ";";
	}

	jstring reply = env->NewStringUTF(stream.str().c_str());

	return reply;
}


/*
 * Class:     com_redis_sentinel_client_RedisClient
 * Method:    cgetBytes
 * Signature: ([B)[B
 */
JNIEXPORT jbyteArray JNICALL Java_com_redis_sentinel_client_RedisClient_cgetBytes
  (JNIEnv *env, jclass, jbyteArray jkey)
{
	char *ckey = (char*)env->GetByteArrayElements(jkey, 0);
	int len = env->GetArrayLength(jkey);

	string strkey(ckey, len);

	std::string value = client->get(strkey);

	jbyteArray jarrRV =env->NewByteArray(value.size());
	jbyte *jby =env->GetByteArrayElements(jarrRV, 0);
	memcpy(jby, value.c_str(), value.size());

	env->SetByteArrayRegion(jarrRV, 0,value.size(), jby);

	env->ReleaseByteArrayElements(jkey, (jbyte*)ckey, len);

	return jarrRV;
}

/*
 * Class:     com_redis_sentinel_client_RedisClient
 * Method:    csetBytes
 * Signature: ([B[B)Z
 */
JNIEXPORT jboolean JNICALL Java_com_redis_sentinel_client_RedisClient_csetBytes
  (JNIEnv *env, jclass, jbyteArray jkey, jbyteArray jvalue)
{
	char *ckey = (char*)env->GetByteArrayElements(jkey, 0);
	int keylen = env->GetArrayLength(jkey);

	char *cvalue = (char*)env->GetByteArrayElements(jvalue, 0);
	int valuelen = env->GetArrayLength(jvalue);

	string strkey(ckey, keylen);
	string strvalue(cvalue, valuelen);

	env->ReleaseByteArrayElements(jkey, (jbyte*)ckey, keylen);
	env->ReleaseByteArrayElements(jvalue, (jbyte*)cvalue, valuelen);

	return client->set(strkey, strvalue);
}

/*
 * Class:     com_redis_sentinel_client_RedisClient
 * Method:    cset
 * Signature: (Ljava/lang/String;Ljava/lang/String;J)Z
 */
JNIEXPORT jboolean JNICALL Java_com_redis_sentinel_client_RedisClient_cset__Ljava_lang_String_2Ljava_lang_String_2J
  (JNIEnv *env, jclass, jstring jKey, jstring jValue, jlong expireTime)
{
	const char* ckey = env->GetStringUTFChars(jKey, NULL);
	const char* cvalue = env->GetStringUTFChars(jValue, NULL);
	std::string key(ckey);
	std::string value(cvalue);
	long expire = expireTime;

	env->ReleaseStringUTFChars(jKey, ckey);
	env->ReleaseStringUTFChars(jValue, cvalue);

	return client->set(key, value, expire);
}

/*
 * Class:     com_redis_sentinel_client_RedisClient
 * Method:    clrange
 * Signature: ([BII)Ljava/util/List;
 */
JNIEXPORT jobject JNICALL Java_com_redis_sentinel_client_RedisClient_clrange___3BII
  (JNIEnv *env, jclass, jbyteArray jkey, jint jstart, jint jend)
{
	char *ckey = (char*)env->GetByteArrayElements(jkey, 0);
	int keylen = env->GetArrayLength(jkey);

	string strkey(ckey, keylen);
	int start = jstart;
	int end = jend;

	env->ReleaseByteArrayElements(jkey, (jbyte*)ckey, keylen);

	vector<string> values = client->lrange(strkey, start, end);

	return JNI::VectorToJListByte(env, values);
}

/*
 * Class:     com_redis_sentinel_client_RedisClient
 * Method:    cexists
 * Signature: (Ljava/lang/String;)Z
 */
JNIEXPORT jboolean JNICALL Java_com_redis_sentinel_client_RedisClient_cexists
  (JNIEnv *env, jclass, jstring jkey)
{
	string key = JNI::JStrToStr(env, jkey);

	return client->exists(key);
}

/*
 * Class:     com_redis_sentinel_client_RedisClient
 * Method:    ctype
 * Signature: (Ljava/lang/String;)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_com_redis_sentinel_client_RedisClient_ctype
  (JNIEnv *env, jclass, jstring jkey)
{
	string key = JNI::JStrToStr(env, jkey);

	string type = client->type(key);

	return JNI::StrToJStr(env, type);
}

/*
 * Class:     com_redis_sentinel_client_RedisClient
 * Method:    csetnx
 * Signature: (Ljava/lang/String;)Z
 */
JNIEXPORT jboolean JNICALL Java_com_redis_sentinel_client_RedisClient_csetnx
  (JNIEnv *env, jclass, jstring jkey, jstring jvalue)
{
	string key = JNI::JStrToStr(env, jkey);
	string value = JNI::JStrToStr(env, jvalue);

	return client->setnx(key, value);
}

/*
 * Class:     com_redis_sentinel_client_RedisClient
 * Method:    cgetset
 * Signature: (Ljava/lang/String;)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_com_redis_sentinel_client_RedisClient_cgetset
  (JNIEnv *env, jclass, jstring jkey, jstring jvalue)
{
	string key = JNI::JStrToStr(env, jkey);
	string value = JNI::JStrToStr(env, jvalue);

	string oldval = client->getset(key, value);

	return JNI::StrToJStr(env, oldval);
}

/*
 * Class:     com_redis_sentinel_client_RedisClient
 * Method:    cincr
 * Signature: (Ljava/lang/String;)I
 */
JNIEXPORT jint JNICALL Java_com_redis_sentinel_client_RedisClient_cincr
  (JNIEnv *env, jclass, jstring jkey)
{
	string key = JNI::JStrToStr(env, jkey);

	return client->incr(key);
}

/*
 * Class:     com_redis_sentinel_client_RedisClient
 * Method:    cdecr
 * Signature: (Ljava/lang/String;)I
 */
JNIEXPORT jint JNICALL Java_com_redis_sentinel_client_RedisClient_cdecr
  (JNIEnv *env, jclass, jstring jkey)
{
	string key = JNI::JStrToStr(env, jkey);

	return client->decr(key);
}

/*
 * Class:     com_redis_sentinel_client_RedisClient
 * Method:    cincrby
 * Signature: (Ljava/lang/String;)I
 */
JNIEXPORT jint JNICALL Java_com_redis_sentinel_client_RedisClient_cincrby
  (JNIEnv *env, jclass, jstring jkey, jint jincr)
{
	string key = JNI::JStrToStr(env, jkey);
	int incr = jincr;

	return client->incrby(key, incr);
}

/*
 * Class:     com_redis_sentinel_client_RedisClient
 * Method:    cdecrby
 * Signature: (Ljava/lang/String;)I
 */
JNIEXPORT jint JNICALL Java_com_redis_sentinel_client_RedisClient_cdecrby
  (JNIEnv *env, jclass, jstring jkey, jint jdecr)
{
	string key = JNI::JStrToStr(env, jkey);
	int decr = jdecr;

	return client->decrby(key, decr);
}

/*
 * Class:     com_redis_sentinel_client_RedisClient
 * Method:    cappend
 * Signature: (Ljava/lang/String;Ljava/lang/String;)J
 */
JNIEXPORT jlong JNICALL Java_com_redis_sentinel_client_RedisClient_cappend
  (JNIEnv *env, jclass, jstring jkey, jstring jvalue)
{
	string key = JNI::JStrToStr(env, jkey);
	string value = JNI::JStrToStr(env, jvalue);

	return client->append(key, value);
}

/*
 * Class:     com_redis_sentinel_client_RedisClient
 * Method:    crpush
 * Signature: (Ljava/lang/String;Ljava/lang/String;)I
 */
JNIEXPORT jint JNICALL Java_com_redis_sentinel_client_RedisClient_crpush
  (JNIEnv *env, jclass, jstring jkey, jstring jvalue)
{
	string key = JNI::JStrToStr(env, jkey);
	string value = JNI::JStrToStr(env, jvalue);

	return client->rpush(key, value);
}

/*
 * Class:     com_redis_sentinel_client_RedisClient
 * Method:    cllen
 * Signature: (Ljava/lang/String;)I
 */
JNIEXPORT jint JNICALL Java_com_redis_sentinel_client_RedisClient_cllen
  (JNIEnv *env, jclass, jstring jkey)
{
	string key = JNI::JStrToStr(env, jkey);

	return client->llen(key);
}

/*
 * Class:     com_redis_sentinel_client_RedisClient
 * Method:    cltrim
 * Signature: (Ljava/lang/String;II)Z
 */
JNIEXPORT jboolean JNICALL Java_com_redis_sentinel_client_RedisClient_cltrim
  (JNIEnv *env, jclass, jstring jkey, jint jstart, jint jend)
{
	string key = JNI::JStrToStr(env, jkey);
	int start = jstart;
	int end = jend;

	return client->ltrim(key, start, end);
}

/*
 * Class:     com_redis_sentinel_client_RedisClient
 * Method:    clset
 * Signature: (Ljava/lang/String;)Z
 */
JNIEXPORT jboolean JNICALL Java_com_redis_sentinel_client_RedisClient_clset
  (JNIEnv *env, jclass, jstring jkey, jint jindex, jstring jvalue)
{
	string key = JNI::JStrToStr(env, jkey);
	int index = jindex;
	string value = JNI::JStrToStr(env, jvalue);

	return client->lset(key, index, value);
}

/*
 * Class:     com_redis_sentinel_client_RedisClient
 * Method:    clrem
 * Signature: (Ljava/lang/String;ILjava/lang/String;)Z
 */
JNIEXPORT jboolean JNICALL Java_com_redis_sentinel_client_RedisClient_clrem
  (JNIEnv *env, jclass, jstring jkey, jint jcount, jstring jvalue)
{
	string key = JNI::JStrToStr(env, jkey);
	int count = jcount;
	string value = JNI::JStrToStr(env, jvalue);

	return client->lrem(key, count, value);
}

/*
 * Class:     com_redis_sentinel_client_RedisClient
 * Method:    clpop
 * Signature: (Ljava/lang/String;)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_com_redis_sentinel_client_RedisClient_clpop
  (JNIEnv *env, jclass, jstring jkey)
{
	string key = JNI::JStrToStr(env, jkey);

	string value = client->lpop(key);

	return JNI::StrToJStr(env, value);
}

/*
 * Class:     com_redis_sentinel_client_RedisClient
 * Method:    crpop
 * Signature: (Ljava/lang/String;)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_com_redis_sentinel_client_RedisClient_crpop
  (JNIEnv *env, jclass, jstring jkey)
{
	string key = JNI::JStrToStr(env, jkey);

	string value = client->rpop(key);

	return JNI::StrToJStr(env, value);
}

/*
 * Class:     com_redis_sentinel_client_RedisClient
 * Method:    csadd
 * Signature: (Ljava/lang/String;Ljava/lang/String;)Z
 */
JNIEXPORT jboolean JNICALL Java_com_redis_sentinel_client_RedisClient_csadd
  (JNIEnv *env, jclass, jstring jkey, jstring jmember)
{
	string key = JNI::JStrToStr(env, jkey);
	string member = JNI::JStrToStr(env, jmember);

	return client->sadd(key, member);
}

/*
 * Class:     com_redis_sentinel_client_RedisClient
 * Method:    csrem
 * Signature: (Ljava/lang/String;Ljava/lang/String;)Z
 */
JNIEXPORT jboolean JNICALL Java_com_redis_sentinel_client_RedisClient_csrem
  (JNIEnv *env, jclass, jstring jkey, jstring jmember)
{
	string key = JNI::JStrToStr(env, jkey);
	string member = JNI::JStrToStr(env, jmember);

	return client->srem(key, member);
}

/*
 * Class:     com_redis_sentinel_client_RedisClient
 * Method:    cspop
 * Signature: (Ljava/lang/String;)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_com_redis_sentinel_client_RedisClient_cspop
  (JNIEnv *env, jclass, jstring jkey)
{
	string key = JNI::JStrToStr(env, jkey);

	string member = client->spop(key);

	return JNI::StrToJStr(env, member);
}

/*
 * Class:     com_redis_sentinel_client_RedisClient
 * Method:    csrandmember
 * Signature: (Ljava/lang/String;)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_com_redis_sentinel_client_RedisClient_csrandmember
  (JNIEnv *env, jclass, jstring jkey)
{
	string key = JNI::JStrToStr(env, jkey);

	string member = client->srandmember(key);

	return JNI::StrToJStr(env, member);
}

/*
 * Class:     com_redis_sentinel_client_RedisClient
 * Method:    cscard
 * Signature: (Ljava/lang/String;)I
 */
JNIEXPORT jint JNICALL Java_com_redis_sentinel_client_RedisClient_cscard
  (JNIEnv *env, jclass, jstring jkey)
{
	string key = JNI::JStrToStr(env, jkey);

	return client->scard(key);
}

/*
 * Class:     com_redis_sentinel_client_RedisClient
 * Method:    csismember
 * Signature: (Ljava/lang/String;Ljava/lang/String;)Z
 */
JNIEXPORT jboolean JNICALL Java_com_redis_sentinel_client_RedisClient_csismember
  (JNIEnv *env, jclass, jstring jkey, jstring jmember)
{
	string key = JNI::JStrToStr(env, jkey);
	string member = JNI::JStrToStr(env, jmember);

	return client->sismember(key, member);
}

/*
 * Class:     com_redis_sentinel_client_RedisClient
 * Method:    csmembers
 * Signature: (Ljava/lang/String;)Ljava/util/List;
 */
JNIEXPORT jobject JNICALL Java_com_redis_sentinel_client_RedisClient_csmembers
  (JNIEnv *env, jclass, jstring jkey)
{
	string key = JNI::JStrToStr(env, jkey);

	vector<string> members = client->smembers(key);

	return JNI::VectorToJList(env, members);
}

/*
 * Class:     com_redis_sentinel_client_RedisClient
 * Method:    czrem
 * Signature: (Ljava/lang/String;Ljava/lang/String;)Z
 */
JNIEXPORT jboolean JNICALL Java_com_redis_sentinel_client_RedisClient_czrem
  (JNIEnv *env, jclass, jstring jkey, jstring jmember)
{
	string key = JNI::JStrToStr(env, jkey);
	string member = JNI::JStrToStr(env, jmember);

	return client->zrem(key, member);
}

/*
 * Class:     com_redis_sentinel_client_RedisClient
 * Method:    czincrby
 * Signature: (Ljava/lang/String;ILjava/lang/String;)I
 */
JNIEXPORT jint JNICALL Java_com_redis_sentinel_client_RedisClient_czincrby
  (JNIEnv *env, jclass, jstring jkey, jint jincr, jstring jmember)
{
	string key = JNI::JStrToStr(env, jkey);
	int incr = jincr;
	string member = JNI::JStrToStr(env, jmember);

	return client->zincrby(key, incr, member);
}

/*
 * Class:     com_redis_sentinel_client_RedisClient
 * Method:    czrank
 * Signature: (Ljava/lang/String;Ljava/lang/String;)I
 */
JNIEXPORT jint JNICALL Java_com_redis_sentinel_client_RedisClient_czrank
  (JNIEnv *env, jclass, jstring jkey, jstring jmember)
{
	string key = JNI::JStrToStr(env, jkey);
	string member = JNI::JStrToStr(env, jmember);

	return client->zrank(key, member);
}

/*
 * Class:     com_redis_sentinel_client_RedisClient
 * Method:    czrevrank
 * Signature: (Ljava/lang/String;Ljava/lang/String;)I
 */
JNIEXPORT jint JNICALL Java_com_redis_sentinel_client_RedisClient_czrevrank
(JNIEnv *env, jclass, jstring jkey, jstring jmember)
{
	string key = JNI::JStrToStr(env, jkey);
	string member = JNI::JStrToStr(env, jmember);

	return client->zrevrank(key, member);
}

/*
 * Class:     com_redis_sentinel_client_RedisClient
 * Method:    czrevrange
 * Signature: (Ljava/lang/String;II)Ljava/util/List;
 */
JNIEXPORT jobject JNICALL Java_com_redis_sentinel_client_RedisClient_czrevrange
  (JNIEnv *env, jclass, jstring jkey, jint jstart, jint jend)
{
	string key = JNI::JStrToStr(env, jkey);
	int start = jstart;
	int end = jend;

	vector<string> members = client->zrevrange(key, start, end);

	return JNI::VectorToJList(env, members);
}

/*
 * Class:     com_redis_sentinel_client_RedisClient
 * Method:    czrangebyscore
 * Signature: (Ljava/lang/String;II)Ljava/util/List;
 */
JNIEXPORT jobject JNICALL Java_com_redis_sentinel_client_RedisClient_czrangebyscore
  (JNIEnv *env, jclass, jstring jkey, jint jstart, jint jend)
{
	string key = JNI::JStrToStr(env, jkey);
	int start = jstart;
	int end = jend;

	vector<string> members = client->zrangebyscore(key, start, end);

	return JNI::VectorToJList(env, members);
}

/*
 * Class:     com_redis_sentinel_client_RedisClient
 * Method:    czcount
 * Signature: (Ljava/lang/String;II)I
 */
JNIEXPORT jint JNICALL Java_com_redis_sentinel_client_RedisClient_czcount__Ljava_lang_String_2II
  (JNIEnv *env, jclass, jstring jkey, jint jmin, jint jmax)
{
	string key = JNI::JStrToStr(env, jkey);
	int min = jmin;
	int max = jmax;

	return client->zcount(key, min, max);
}


/*
 * Class:     com_redis_sentinel_client_RedisClient
 * Method:    czcard
 * Signature: (Ljava/lang/String;)I
 */
JNIEXPORT jint JNICALL Java_com_redis_sentinel_client_RedisClient_czcard
  (JNIEnv *env, jclass, jstring jkey)
{
	string key = JNI::JStrToStr(env, jkey);

	return client->zcard(key);
}

/*
 * Class:     com_redis_sentinel_client_RedisClient
 * Method:    czscore
 * Signature: (Ljava/lang/String;Ljava/lang/String;)I
 */
JNIEXPORT jint JNICALL Java_com_redis_sentinel_client_RedisClient_czscore
  (JNIEnv *env, jclass, jstring jkey, jstring jmember)
{
	string key = JNI::JStrToStr(env, jkey);
	string member = JNI::JStrToStr(env, jmember);

	return client->zscore(key, member);
}

/*
 * Class:     com_redis_sentinel_client_RedisClient
 * Method:    czremrangebyrank
 * Signature: (Ljava/lang/String;II)I
 */
JNIEXPORT jint JNICALL Java_com_redis_sentinel_client_RedisClient_czremrangebyrank
  (JNIEnv *env, jclass, jstring jkey, jint jmin, jint jmax)
{
	string key = JNI::JStrToStr(env, jkey);
	int min = jmin;
	int max = jmax;

	return client->zremrangebyrank(key, min, max);
}

/*
 * Class:     com_redis_sentinel_client_RedisClient
 * Method:    czremrangebyscore
 * Signature: (Ljava/lang/String;II)I
 */
JNIEXPORT jint JNICALL Java_com_redis_sentinel_client_RedisClient_czremrangebyscore
(JNIEnv *env, jclass, jstring jkey, jint jmin, jint jmax)
{
	string key = JNI::JStrToStr(env, jkey);
	int min = jmin;
	int max = jmax;

	return client->zremrangebyscore(key, min, max);
}

/*
 * Class:     com_redis_sentinel_client_RedisClient
 * Method:    chset
 * Signature: (Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;)Z
 */
JNIEXPORT jboolean JNICALL Java_com_redis_sentinel_client_RedisClient_chset
  (JNIEnv *env, jclass, jstring jkey, jstring jfield, jstring jvalue)
{
	string key = JNI::JStrToStr(env, jkey);
	string field = JNI::JStrToStr(env, jfield);
	string value = JNI::JStrToStr(env, jvalue);

	return client->hset(key, field, value);
}

/*
 * Class:     com_redis_sentinel_client_RedisClient
 * Method:    chmget
 * Signature: (Ljava/lang/String;[Ljava/lang/String;)Ljava/util/List;
 */
JNIEXPORT jobject JNICALL Java_com_redis_sentinel_client_RedisClient_chmget
  (JNIEnv *env, jclass, jstring jkey, jobjectArray jfields)
{
	string key = JNI::JStrToStr(env, jkey);
	vector<string> fields = JNI::JArrayToVec(env, jfields);

	vector<string> values = client->hmget(key, fields);

	return JNI::VectorToJList(env, values);
}

/*
 * Class:     com_redis_sentinel_client_RedisClient
 * Method:    chincrby
 * Signature: (Ljava/lang/String;Ljava/lang/String;I)I
 */
JNIEXPORT jint JNICALL Java_com_redis_sentinel_client_RedisClient_chincrby
  (JNIEnv *env, jclass, jstring jkey, jstring jfield, jint jincr)
{
	string key = JNI::JStrToStr(env, jkey);
	string field = JNI::JStrToStr(env, jfield);
	int incr = jincr;

	return client->hincrby(key, field, incr);
}

/*
 * Class:     com_redis_sentinel_client_RedisClient
 * Method:    chexists
 * Signature: (Ljava/lang/String;Ljava/lang/String;)Z
 */
JNIEXPORT jboolean JNICALL Java_com_redis_sentinel_client_RedisClient_chexists
  (JNIEnv *env, jclass, jstring jkey, jstring jfield)
{
	string key = JNI::JStrToStr(env, jkey);
	string field = JNI::JStrToStr(env, jfield);

	return client->hexists(key, field);
}

/*
 * Class:     com_redis_sentinel_client_RedisClient
 * Method:    chdel
 * Signature: (Ljava/lang/String;Ljava/lang/String;)Z
 */
JNIEXPORT jboolean JNICALL Java_com_redis_sentinel_client_RedisClient_chdel
  (JNIEnv *env, jclass, jstring jkey, jstring jfield)
{
	string key = JNI::JStrToStr(env, jkey);
	string field = JNI::JStrToStr(env, jfield);

	return client->hdel(key, field);
}

/*
 * Class:     com_redis_sentinel_client_RedisClient
 * Method:    chlen
 * Signature: (Ljava/lang/String;)I
 */
JNIEXPORT jint JNICALL Java_com_redis_sentinel_client_RedisClient_chlen
  (JNIEnv *env, jclass, jstring jkey)
{
	string key = JNI::JStrToStr(env, jkey);

	return client->hlen(key);
}

/*
 * Class:     com_redis_sentinel_client_RedisClient
 * Method:    chkeys
 * Signature: (Ljava/lang/String;)Ljava/util/List;
 */
JNIEXPORT jobject JNICALL Java_com_redis_sentinel_client_RedisClient_chkeys
  (JNIEnv *env, jclass, jstring jkey)
{
	string key = JNI::JStrToStr(env, jkey);

	vector<string> fields = client->hkeys(key);

	return JNI::VectorToJList(env, fields);
}

/*
 * Class:     com_redis_sentinel_client_RedisClient
 * Method:    chvals
 * Signature: (Ljava/lang/String;)Ljava/util/List;
 */
JNIEXPORT jobject JNICALL Java_com_redis_sentinel_client_RedisClient_chvals
  (JNIEnv *env, jclass, jstring jkey)
{
	string key = JNI::JStrToStr(env, jkey);

	vector<string> values = client->hvals(key);

	return JNI::VectorToJList(env, values);
}

/*
 * Class:     com_redis_sentinel_client_RedisClient
 * Method:    csetex
 * Signature: (Ljava/lang/String;Ljava/lang/String;I)Z
 */
JNIEXPORT jboolean JNICALL Java_com_redis_sentinel_client_RedisClient_csetex
  (JNIEnv *env, jclass, jstring jkey, jstring jvalue, jint jseconds)
{
	string key = JNI::JStrToStr(env, jkey);
	string value = JNI::JStrToStr(env, jvalue);
	int seconds = jseconds;

	return client->setex(key, value, seconds);
}

