#include "RedisClient.h"
#include "Utils.h"
#include "Log.h"
#include <exception>

using namespace bfd::redis;

struct AsyncInfo
{
	RedisDB db;
	vector<string> keys;
	MgetAsyncRequestContext2 mget_async_rctxt;
};





RedisClient::RedisClient(const string& sentinel_addr, const string& bid, const string& password):m_KetamaHasher(NULL),m_ConfigPtr(NULL)
{
	if (m_ConfigPtr == NULL)
	{
		m_ConfigPtr = new RedisSentinelManager();
		bool ret = m_ConfigPtr->Init(sentinel_addr, password);
		if (!ret)
		{
			cout << "Connect sentinel error!" << endl;
			throw new exception();
		}
	}

	if (m_KetamaHasher == NULL)
	{
		m_KetamaHasher = new Ketama(m_ConfigPtr->servers());
	}

	m_ConfigPtr->SetKetamaHasher(m_KetamaHasher);

	m_BID = bid;

	loop_ = aeCreateEventLoop(64);

	pthread_t AEThreadID;

	int ret=pthread_create(&AEThreadID,NULL, &AEThread,this);
	if (ret != 0)
	{
		stringstream stream;
		stream << "Create pthread error!";
		LOG(ERROR, stream.str());

		exit(1);
	}
}

RedisClient::~RedisClient()
{
	if (m_ConfigPtr != NULL)
	{
		delete m_ConfigPtr;
		m_ConfigPtr = NULL;
	}

	if (m_KetamaHasher != NULL)
	{
		delete m_KetamaHasher;
		m_KetamaHasher = NULL;
	}
}

bool RedisClient::expire(string key, int seconds, bool ifBid)
{
	Reply rep = RedisCommand(Command("EXPIRE")(key)(int2string(seconds)), ifBid);

	return rep.integer()==1;
}

bool RedisClient::exists(string key, bool ifBid)
{
	Reply rep = RedisCommand(Command("EXISTS")(key), ifBid);

	return rep.integer()==1;

}

int RedisClient::del(string key, bool ifBid)
{
	Reply rep = RedisCommand(Command("DEL")(key), ifBid);

	return rep.integer();
}

int RedisClient::del(vector<string>& keys, bool ifBid)
{
	Command command("DEL");
	Reply rep;
	for (size_t i=0; i<keys.size(); i++)
	{
		rep = RedisCommand(command(keys[i]), ifBid);
	}

	return rep.integer();
}

string RedisClient::type(string key, bool ifBid)
{
	Reply rep = RedisCommand(Command("TYPE")(key), ifBid);

	return rep.str();
}

bool RedisClient::set(string key, string value, bool ifBid)
{
	Reply rep = RedisCommand(Command("SET")(key)(value), ifBid);

	return rep.str() == "OK";
}

bool RedisClient::setnx(string key, string value, bool ifBid)
{
	Reply rep = RedisCommand(Command("SETNX")(key)(value), ifBid);

	return rep.integer() == 1;
}

bool RedisClient::setex(string key, string value, int seconds, bool ifBid)
{
	Reply rep = RedisCommand(Command("SETEX")(key)(int2string(seconds))(value), ifBid);

	return rep.str() == "OK";
}

string RedisClient::get(string key, bool ifBid)
{
	Reply rep = RedisCommand(Command("GET")(key), ifBid);

	if (rep.error())
	{
		return "";
	}
	else
	{
		return rep.str();
	}
}

string RedisClient::getset(string key, string value, bool ifBid)
{
	Reply rep = RedisCommand(Command("GETSET")(key)(value), ifBid);

	if (rep.error())
	{
		return "";
	}
	else
	{
		return rep.str();
	}
}

vector<string> RedisClient::mget(vector<string>& keys, bool ifBid)
{
	vector<string> values;

	for (size_t i=0; i<keys.size(); i++)
	{
		string value = get(keys[i], ifBid);
		values.push_back(value);
	}

	return values;
}

vector<string> RedisClient::mget2(vector<string>& keys, bool ifBid)
{
	vector<string> values;

	map<string, string> kvmap;

	map<RedisDB, vector<string>  > client_map;

	vector<string> innerKey;

	for(size_t i=0; i<keys.size(); i++)
	{
		if (ifBid && m_BID!="")
		{
			innerKey.push_back(m_BID + "_" + keys[i]);
		}
		else
		{
			innerKey.push_back(keys[i]);
		}
	}

	for (size_t i=0; i<innerKey.size(); i++)
	{
		RedisDB db = m_KetamaHasher->get(innerKey[i]);

		client_map[db].push_back(innerKey[i]);
	}

	MgetAsyncResultMerger2 merger;
	merger.counter_ = client_map.size();
	merger.status_.resize(client_map.size());

	vector<AsyncInfo> infos;
	infos.resize(client_map.size());
	map<RedisDB, vector<string>  >::iterator iter = client_map.begin();
	for (size_t i=0; iter!=client_map.end(); ++iter, ++i)
	{
		infos[i].db = iter->first;
		infos[i].keys = iter->second;
		infos[i].mget_async_rctxt.merger_ = &merger;
		infos[i].mget_async_rctxt.user_keylist_ = iter->second;
	}

	for (size_t i=0; i<infos.size(); ++i)
	{
		bool ret = infos[i].db.mget2(infos[i].keys, &(infos[i].mget_async_rctxt), loop_);
		merger.status_[i] = ret;
	}

	size_t badmget = 0;
	for (size_t i=0; i<client_map.size(); i++)
	{
		if (!merger.status_[i]) badmget++;
	}

	while (1)
	{
		if (merger.finish_ || (badmget==merger.counter_))
		{
			kvmap.swap(merger.result_);

			for (size_t i=0; i<innerKey.size(); i++)
			{
				values.push_back(kvmap[innerKey[i]]);
			}
			return values;
		}
	}
}

bool RedisClient::mget3(vector<string>& keys, KVMap& kvs, bool ifBid)
{
	map<RedisDB, vector<string>  > client_map;

	vector<string> innerKey;

	for(size_t i=0; i<keys.size(); i++)
	{
		if (ifBid && m_BID!="")
		{
			innerKey.push_back(m_BID + "_" + keys[i]);
		}
		else
		{
			innerKey.push_back(keys[i]);
		}
	}

	for (size_t i=0; i<innerKey.size(); i++)
	{
		RedisDB db = m_KetamaHasher->get(innerKey[i]);

		client_map[db].push_back(innerKey[i]);
	}

	MgetAsyncResultMerger3 *merger = new MgetAsyncResultMerger3;
	merger->counter_ = client_map.size();
	merger->status_.resize(client_map.size());
	merger->result_ = &kvs;

	vector<AsyncInfo> infos;
	infos.resize(client_map.size());
	map<RedisDB, vector<string>  >::iterator iter = client_map.begin();
	for (size_t i=0; iter!=client_map.end(); ++iter, ++i)
	{
		infos[i].db = iter->first;
		infos[i].keys = iter->second;
		infos[i].mget_async_rctxt.merger_ = NULL;
		infos[i].mget_async_rctxt.user_keylist_ = iter->second;
	}

	for (size_t i=0; i<infos.size(); ++i)
	{
		MgetAsyncRequestContext3 *context = new MgetAsyncRequestContext3;
		context->merger_ = merger;
		context->user_keylist_ = infos[i].keys;

		bool ret = infos[i].db.mget3(infos[i].keys, context, loop_);
		merger->status_[i] = ret;
	}

	//mget3为异步调用, new的资源将在callback中释放
	return true;
}

bool RedisClient::mset(map<string, string>& keyvalues, bool ifBid)
{
	map<string, string>::iterator it = keyvalues.begin();
	for (; it!=keyvalues.end(); it++)
	{
		bool ret = set(it->first, it->second, ifBid);
		if (!ret) return false;
	}

	return true;

}

int RedisClient::incr(string key, bool ifBid)
{
	Reply rep = RedisCommand(Command("INCR")(key), ifBid);

	return (rep.error()) ? 0 : rep.integer();
}

int RedisClient::decr(string key, bool ifBid)
{
	Reply rep = RedisCommand(Command("DECR")(key), ifBid);

	return (rep.error()) ? 0 : rep.integer();
}

int RedisClient::incrby(string key, int incr, bool ifBid)
{

	Reply rep = RedisCommand(Command("INCRBY")(key)(int2string(incr)), ifBid);

	return (rep.error()) ? 0 : rep.integer();
}

int RedisClient::decrby(string key, int incr, bool ifBid)
{
	Reply rep = RedisCommand(Command("DECRBY")(key)(int2string(incr)), ifBid);

	return (rep.error()) ? 0 : rep.integer();
}

long RedisClient::append(string key, string value, bool ifBid)
{
	Reply rep = RedisCommand(Command("APPEND")(key)(value), ifBid);

	return rep.integer();
}

int RedisClient::lpush(string key, string value, bool ifBid)
{
	Reply rep = RedisCommand(Command("LPUSH")(key)(value), ifBid);

	return rep.integer();
}

int RedisClient::rpush(string key, string value, bool ifBid)
{
	Reply rep = RedisCommand(Command("RPUSH")(key)(value), ifBid);

	return rep.integer();
}

int RedisClient::lpush(string key, vector<string> values, bool ifBid)
{
	Command comm("LPUSH");
	comm(key);
	for (size_t i=0; i<values.size(); i++)
	{
		comm(values[i]);
	}

	Reply rep = RedisCommand(comm, ifBid);

	return rep.integer();
}

int RedisClient::rpush(string key, vector<string> values, bool ifBid)
{
	Command comm("RPUSH");
	comm(key);
	for (size_t i=0; i<values.size(); i++)
	{
		comm(values[i]);
	}

	Reply rep = RedisCommand(comm, ifBid);

	return rep.integer();
}


int RedisClient::llen(string key, bool ifBid)
{
	Reply rep = RedisCommand(Command("LLEN")(key), ifBid);

	return rep.integer();
}

vector<string> RedisClient::lrange(string key, int start, int end, bool ifBid)
{
	vector<string> values;
	Reply rep = RedisCommand(Command("LRANGE")(key)(int2string(start))(int2string(end)), ifBid);

	for (size_t i=0; i<rep.elements().size(); i++)
	{
		values.push_back(rep.elements()[i].str());
	}

	return values;
}

bool RedisClient::ltrim(string key, int start, int end, bool ifBid)
{
	Reply rep = RedisCommand(Command("LTRIM")(key)(int2string(start))(int2string(end)), ifBid);

	return rep.str()==string("OK");
}

bool RedisClient::lset(string key, int index, string value, bool ifBid)
{
	Reply rep = RedisCommand(Command("LSET")(key)(int2string(index))(value), ifBid);

	return rep.str()==string("OK");
}

bool RedisClient::lrem(string key, int count, string value, bool ifBid)
{
	Reply rep = RedisCommand(Command("LREM")(key)(int2string(count))(value), ifBid);

	return rep.integer();
}

string RedisClient::lpop(string key, bool ifBid)
{
	Reply rep = RedisCommand(Command("LPOP")(key), ifBid);

	if (rep.error())
	{
		return "";
	}
	else
	{
		return rep.str();
	}
}

string RedisClient::rpop(string key, bool ifBid)
{
	Reply rep = RedisCommand(Command("RPOP")(key), ifBid);

	if (rep.error())
	{
		return "";
	}
	else
	{
		return rep.str();
	}
}

bool RedisClient::sadd(string key, string member, bool ifBid)
{
	Reply rep = RedisCommand(Command("SADD")(key)(member), ifBid);

	return rep.integer()==1;
}

int RedisClient::sadd(string key, vector<string> members, bool ifBid)
{
	Command comm("SADD");
	comm(key);
	for (size_t i=0; i<members.size(); i++)
	{
		comm(members[i]);
	}

	Reply rep = RedisCommand(comm, ifBid);

	return rep.integer();
}

bool RedisClient::srem(string key, string member, bool ifBid)
{
	Reply rep = RedisCommand(Command("SREM")(key)(member), ifBid);

	return rep.integer()==1;
}

string RedisClient::spop(string key, bool ifBid)
{
	Reply rep = RedisCommand(Command("SPOP")(key), ifBid);

	if (rep.error())
	{
		return "";
	}
	else
	{
		return rep.str();
	}
}

string RedisClient::srandmember(string key, bool ifBid)
{
	Reply rep = RedisCommand(Command("SRANDMEMBER")(key), ifBid);

	if (rep.error())
	{
		return "";
	}
	else
	{
		return rep.str();
	}
}

int RedisClient::scard(string key, bool ifBid)
{
	Reply rep = RedisCommand(Command("SCARD")(key), ifBid);

	return rep.integer();
}

bool RedisClient::sismember(string key, string member, bool ifBid)
{
	Reply rep = RedisCommand(Command("SISMEMBER")(key)(member), ifBid);

	return rep.integer()==1;
}

vector<string> RedisClient::smembers(string key, bool ifBid)
{
	vector<string> values;
	Reply rep = RedisCommand(Command("SMEMBERS")(key), ifBid);

	for (size_t i=0; i<rep.elements().size(); i++)
	{
		values.push_back(rep.elements()[i].str());
	}

	return values;
}

bool RedisClient::zadd(string key, int score, string member, bool ifBid)
{
	Reply rep = RedisCommand(Command("ZADD")(key)(int2string(score))(member), ifBid);

	return rep.integer()==1;
}

bool RedisClient::zrem(string key, string member, bool ifBid)
{
	Reply rep = RedisCommand(Command("ZREM")(key)(member), ifBid);

	return rep.integer()==1;
}

int RedisClient::zincrby(string key, int incr, string member, bool ifBid)
{
	Reply rep = RedisCommand(Command("ZINCRBY")(key)(int2string(incr))(member), ifBid);

	return string2int(rep.str());
}

int RedisClient::zrank(string key, string member, bool ifBid)
{
	Reply rep = RedisCommand(Command("ZRANK")(key)(member), ifBid);

	return rep.integer();
}

int RedisClient::zrevrank(string key, string member, bool ifBid)
{
	Reply rep = RedisCommand(Command("ZREVRANK")(key)(member), ifBid);

	return rep.integer();
}

vector<string> RedisClient::zrange(string key, int start, int end, bool ifBid)
{
	vector<string> values;
	Reply rep = RedisCommand(Command("ZRANGE")(key)(int2string(start))(int2string(end)), ifBid);

	for (size_t i=0; i<rep.elements().size(); i++)
	{
		values.push_back(rep.elements()[i].str());
	}

	return values;
}

vector<string> RedisClient::zrevrange(string key, int start, int end, bool ifBid)
{
	vector<string> values;
	Reply rep = RedisCommand(Command("ZREVRANGE")(key)(int2string(start))(int2string(end)), ifBid);

	for (size_t i=0; i<rep.elements().size(); i++)
	{
		values.push_back(rep.elements()[i].str());
	}

	return values;
}

vector<string> RedisClient::zrangebyscore(string key, int min, int max, bool ifBid)
{
	vector<string> values;
	Reply rep = RedisCommand(Command("ZRANGEBYSCORE")(key)(int2string(min))(int2string(max)), ifBid);

	for (size_t i=0; i<rep.elements().size(); i++)
	{
		values.push_back(rep.elements()[i].str());
	}

	return values;
}

int RedisClient::zcount(string key, int min, int max, bool ifBid)
{
	Reply rep = RedisCommand(Command("ZCOUNT")(key)(int2string(min))(int2string(max)), ifBid);

	return rep.integer();
}

int RedisClient::zcard(string key, bool ifBid)
{
	Reply rep = RedisCommand(Command("ZCARD")(key), ifBid);

	return rep.integer();
}

int RedisClient::zscore(string key, string member, bool ifBid)
{
	Reply rep = RedisCommand(Command("ZSCORE")(key)(member), ifBid);

	return string2int(rep.str());
}

int RedisClient::zremrangebyrank(string key, int min, int max, bool ifBid)
{
	Reply rep = RedisCommand(Command("ZREMRANGEBYRANK")(key)(int2string(min))(int2string(max)), ifBid);

	return rep.integer();
}

int RedisClient::zremrangebyscore(string key, int min, int max, bool ifBid)
{
	Reply rep = RedisCommand(Command("ZREMRANGEBYSCORE")(key)(int2string(min))(int2string(max)), ifBid);

	return rep.integer();
}

bool RedisClient::hset(string key, string field, string value, bool ifBid)
{
	Reply rep = RedisCommand(Command("HSET")(key)(field)(value), ifBid);

	return rep.integer();
}

string RedisClient::hget(string key, string field, bool ifBid)
{
	Reply rep = RedisCommand(Command("HGET")(key)(field), ifBid);

	if (rep.error())
	{
		return "";
	}
	else
	{
		return rep.str();
	}
}

vector<string> RedisClient::hmget(string key, vector<string>& fields, bool ifBid)
{
	vector<string> values;
	Command command("HMGET");
	command(key);
	for (size_t i=0; i<fields.size(); i++)
	{
		command(fields[i]);
	}

	Reply rep = RedisCommand(command, ifBid);

	for (size_t i=0; i<rep.elements().size(); i++)
	{
		values.push_back(rep.elements()[i].str());
	}

	return values;
}

bool RedisClient::hmset(string key, vector<string>& fields, vector<string>& values, bool ifBid)
{
	if (fields.size() != values.size()) return false;
	if (fields.empty()) return false;

	Command command("HMSET");
	command(key);

	for (size_t i=0; i<fields.size(); i++)
	{
		command(fields[i])(values[i]);
	}

	Reply rep = RedisCommand(command, ifBid);

	return rep.str()==string("OK");
}

int RedisClient::hincrby(string key, string field, int incr, bool ifBid)
{
	Reply rep = RedisCommand(Command("HINCRBY")(key)(field)(int2string(incr)), ifBid);

	return rep.integer();
}

bool RedisClient::hexists(string key, string field, bool ifBid)
{
	Reply rep = RedisCommand(Command("HEXISTS")(key)(field), ifBid);

	return rep.integer()==1;
}

bool RedisClient::hdel(string key, string field, bool ifBid)
{
	Reply rep = RedisCommand(Command("HDEL")(key)(field), ifBid);

	return rep.integer()==1;
}

int RedisClient::hlen(string key, bool ifBid)
{
	Reply rep = RedisCommand(Command("HLEN")(key), ifBid);

	return rep.integer();
}

vector<string> RedisClient::hkeys(string key, bool ifBid)
{
	vector<string> keys;
	Reply rep = RedisCommand(Command("HKEYS")(key), ifBid);

	for (size_t i=0; i<rep.elements().size(); i++)
	{
		keys.push_back(rep.elements()[i].str());
	}

	return keys;
}

vector<string> RedisClient::hvals(string key, bool ifBid)
{
	vector<string> values;
	Reply rep = RedisCommand(Command("HVALS")(key), ifBid);

	for (size_t i=0; i<rep.elements().size(); i++)
	{
		values.push_back(rep.elements()[i].str());
	}

	return values;
}

bool RedisClient::hgetall(string key, vector<string>& fields, vector<string>& values, bool ifBid)
{
	Reply rep = RedisCommand(Command("HGETALL")(key), ifBid);

	if (rep.error()) return false;

	for (size_t i=1; i<rep.elements().size(); i+=2)
	{
		fields.push_back(rep.elements()[i-1].str());
		values.push_back(rep.elements()[i].str());
	}

	return true;
}

Reply RedisClient::RedisCommand(const vector<string>& command, bool ifBid)
{
	if (command.size() < 2)
	{
		Reply reply;
		reply.SetErrorMessage("Command length should gt 2");
		return reply;
	}

	vector<string> innerCommand;

	for(size_t i=0; i<command.size(); i++)
	{
		if (i==1 && ifBid && m_BID!="")
		{
			innerCommand.push_back(m_BID + "_" + command[i]);
			continue;
		}
		innerCommand.push_back(command[i]);
	}

	RedisDB db = m_KetamaHasher->get(innerCommand[1]);

	return db.RedisCommand(innerCommand);
}

Reply RedisClient::RedisCommand(Command& command, bool ifBid)
{
	return RedisCommand(command.args(), ifBid);
}

vector<Reply> RedisClient::RedisCommands(vector<Command>& commands, bool ifBid)
{
	vector<Reply> replys;

	map<RedisClientPool*, vector<Comm> > innerCommand;

	for (size_t i=0; i<commands.size(); i++)
	{
		vector<string> command = commands[i].args();

		if(command.size() < 2)
		{
			return replys;
		}


		string innerKey = command[1];
		if(ifBid && m_BID!="")
		{
			innerKey = m_BID + "_" + command[1];
		}

		command[1] = innerKey;

		RedisDB db = m_KetamaHasher->get(innerKey);

		Comm comm;
		comm.redisDBNo = db.GetRedisDBNo();
		comm.command = command;

		RedisClientPool *p = db.GetConnPool();
		innerCommand[p].push_back(comm);
	}

	return RedisCommands(innerCommand);
}

vector<Reply> RedisClient::RedisCommands(map<RedisClientPool*, vector<Comm> >& commands)
{
	vector<Reply> replys;

	map<RedisClientPool*, vector<Comm> >::iterator iter = commands.begin();
	for (; iter!=commands.end(); ++iter)
	{
		if (!iter->first)
		{
			fprintf(stderr, "client pool is null!!!");
			Reply reply;
			reply.SetErrorMessage("Can not fetch client from pool!!!");
			replys.push_back(reply);
			continue;
		}

		for (size_t i=0; i<(iter->second).size(); ++i)
		{

			string dbno = (iter->second)[i].redisDBNo;
			vector<string> command = (iter->second)[i].command;

			vector<const char*> argv;
			vector<size_t> arglen;
			for (size_t j = 0; i < command.size(); ++i)
			{
				argv.push_back(command[i].c_str());
				arglen.push_back(command[i].size());
			}

			redisContext* redis = iter->first->borrowItem();


			if (!redis)
			{
				fprintf(stderr, "contetx is NULL!!");
				Reply reply;
				reply.SetErrorMessage("context is NULL !!");
				replys.push_back(reply);
				continue;
			}
			redisReply *reply = NULL;

			string selectCommand = string("select ") + dbno;

			int ret = redisAppendCommand(redis, selectCommand.c_str());
			if (ret != REDIS_OK)
			{
				redisFree(redis);
				redis = NULL;
				redis = iter->first->create();
				if (redis == NULL)
				{
					LOG(ERROR, "reconnect faild!");
					Reply reply;
					reply.SetErrorMessage("reconnect faild!");
					replys.push_back(reply);
					continue;
				}
				ret = redisAppendCommand(redis, selectCommand.c_str());
			}

			if (ret != REDIS_OK)
			{
				redisFree(redis);
				Reply reply;
				reply.SetErrorMessage("Do Select Command faild.");
				LOG(ERROR, "Do Select Command faild.");
				replys.push_back(reply);
				continue;
			}

			ret = redisAppendCommandArgv(redis, argv.size(), &argv[0], &arglen[0]);

			if (ret != REDIS_OK)
			{
				redisFree(redis);
				Reply reply;
				reply.SetErrorMessage("Do Command faild.");
				LOG(ERROR, "Do Command faild.");
				replys.push_back(reply);
				continue;
			}

			if (REDIS_OK != redisGetReply(redis, (void**) &reply))
			{
				redisFree(redis);
				Reply reply;
				reply.SetErrorMessage("Do Select Command faild.");
				LOG(ERROR, "Do Select Command faild.");
				replys.push_back(reply);
				continue;
			}

			if (string(reply->str) != string("OK"))
			{
				redisFree(redis);
				Reply reply;
				reply.SetErrorMessage(
					"Do Select Command faild, please check redis config parameter databases, it must >= 1024!");
				LOG(ERROR, "Do Select Command faild, please check redis config parameter databases, it must >= 1024!");
				exit(1);
			}

			if (REDIS_OK != redisGetReply(redis, (void**) &reply))
			{
				redisFree(redis);
				Reply reply;
				reply.SetErrorMessage("Do Command faild.");
				LOG(ERROR, "Do Command faild.");
				replys.push_back(reply);
				continue;
			}

			iter->first->returnItem(redis);
			Reply retrep = Reply(reply);
			freeReplyObject(reply);
			replys.push_back(retrep);
		}
	}
	return replys;
}

void* RedisClient::AEThread(void *arg)
{
	RedisClient* ptr = reinterpret_cast<RedisClient*>(arg);

	aeMain(ptr->loop_);
}

vector<RedisDB> RedisClient::AddServer(const string& masterName, const string& addr, int port)
{
	return m_ConfigPtr->AddServer(masterName, addr, port);
}

bool RedisClient::AddSentinelMonitor(const string& masterName, const string& addr, int port)
{
	return m_ConfigPtr->AddSentinelMonitor(masterName, addr, port);
}


