#include "RedisDB.h"
#include "Log.h"
#include "ScopedLock.h"

using namespace bfd::redis;

static pthread_mutex_t  m_Mutex = PTHREAD_MUTEX_INITIALIZER;

RedisDB::RedisDB()
{
	m_ConnPool = NULL;
}

RedisDB::~RedisDB()
{

}

bool RedisDB::DoCommand(redisContext* (&redis), vector<string>& command, Reply& reply)
{
	vector<const char*> argv;
	vector<size_t> arglen;
	for (size_t i = 0; i < command.size(); ++i)
	{
		argv.push_back(command[i].c_str());
		arglen.push_back(command[i].size());
	}

	redisReply *rep = NULL;

	string selectCommand = string("select ") + m_RedisDBNo;

	int ret = redisAppendCommand(redis, selectCommand.c_str());
	if (ret != REDIS_OK)
	{
		if (redis != NULL)
		{
			redisFree(redis);
			redis = NULL;
		}

		redis = m_ConnPool->create();
		if (redis == NULL)
		{
			LOG(ERROR, "reconnect faild!");
			reply.SetErrorMessage("reconnect faild!");
			return false;
		}
		ret = redisAppendCommand(redis, selectCommand.c_str());
	}

	if (ret != REDIS_OK)
	{
		if (redis != NULL)
		{
			redisFree(redis);
			redis = NULL;
		}
		reply.SetErrorMessage("Do Select Command faild.");
		LOG(ERROR, "Do Select Command faild.");
		return false;
	}

	ret = redisAppendCommandArgv(redis, argv.size(), &argv[0], &arglen[0]);

	if (ret != REDIS_OK)
	{
		if (redis != NULL)
		{
			redisFree(redis);
			redis = NULL;
		}
		reply.SetErrorMessage("Do Command faild.");
		LOG(ERROR, "Do Command faild.");
		return false;
	}

	if (REDIS_OK != redisGetReply(redis, (void**) &rep))
	{
		if (redis != NULL)
		{
			redisFree(redis);
			redis = NULL;
		}
		reply.SetErrorMessage("Do Select Command faild.");
		LOG(ERROR, "Do Select Command faild.");
		return false;
	}

	if (string(rep->str) != string("OK"))
	{
		if (redis != NULL)
		{
			redisFree(redis);
			redis = NULL;
		}
		reply.SetErrorMessage(
				"Do Select Command faild, please check redis config parameter databases, it must >= 1024!");
		LOG(ERROR, "Do Select Command faild, please check redis config parameter databases, it must >= 1024!");
		return false;
	}

	if (REDIS_OK != redisGetReply(redis, (void**) &rep))
	{
		if (redis != NULL)
		{
			redisFree(redis);
			redis = NULL;
		}
		reply.SetErrorMessage("Do Command faild.");
		LOG(ERROR, "Do Command faild.");
		return false;
	}

	reply = Reply(rep);
	if (rep != NULL)
	{
		freeReplyObject(rep);
		rep = NULL;
	}

	return true;
}

Reply RedisDB::RedisCommand(Command command)
{
	return RedisCommand(command.args());
}

Reply RedisDB::RedisCommand(vector<string> command)
{
	if (command.size() < 2)
	{
		Reply reply;
		reply.SetErrorMessage("Command length should gt 2");
		return reply;
	}

	if (!m_ConnPool)
	{
		fprintf(stderr, "client pool is null!!!");
		Reply reply;
		reply.SetErrorMessage("Can not fetch client from pool!!!");
		return reply;
	}

	//----

	redisContext* redis = m_ConnPool->borrowItem();

	if (!redis)
	{
		fprintf(stderr, "contetx is NULL!!");
		Reply reply;
		reply.SetErrorMessage("context is NULL !!");
		return reply;
	}

	Reply reply;

	bool ret = DoCommand(redis, command, reply);

	if (!ret)
	{
		if (redis != NULL)
		{
			redisFree(redis);
			redis = NULL;
		}
		redis = m_ConnPool->create();
		if (redis == NULL)
		{
			LOG(ERROR, "reconnect faild!");
			Reply reply;
			reply.SetErrorMessage("reconnect faild!");
			return reply;
		}
		ret = DoCommand(redis, command, reply);
	}

	if (ret)
	{
		m_ConnPool->returnItem(redis);
	}
	else
	{
		if (redis != NULL)
		{
			redisFree(redis);
			redis = NULL;
		}
	}

	return reply;
	//----

//	vector<const char*> argv;
//	vector<size_t> arglen;
//	for (size_t i = 0; i < command.size(); ++i)
//	{
//		argv.push_back(command[i].c_str());
//		arglen.push_back(command[i].size());
//	}
//
//	redisContext* redis = m_ConnPool->borrowItem();
//
//
//	if (!redis)
//	{
//		fprintf(stderr, "contetx is NULL!!");
//		Reply reply;
//		reply.SetErrorMessage("context is NULL !!");
//		return reply;
//	}
//	redisReply *reply = NULL;
//
//	string selectCommand = string("select ") + m_RedisDBNo;
//
//	int ret = redisAppendCommand(redis, selectCommand.c_str());
//	if (ret != REDIS_OK)
//	{
//		redisFree(redis);
//		redis = NULL;
//		redis = m_ConnPool->create();
//		if (redis == NULL)
//		{
//			LOG(ERROR, "reconnect faild!");
//			Reply reply;
//			reply.SetErrorMessage("reconnect faild!");
//			return reply;
//		}
//		ret = redisAppendCommand(redis, selectCommand.c_str());
//	}
//
//	if (ret != REDIS_OK)
//	{
//		redisFree(redis);
//		Reply reply;
//		reply.SetErrorMessage("Do Select Command faild.");
//		LOG(ERROR, "Do Select Command faild.");
//		return reply;
//	}
//
//	ret = redisAppendCommandArgv(redis, argv.size(), &argv[0], &arglen[0]);
//
//	if (ret != REDIS_OK)
//	{
//		redisFree(redis);
//		Reply reply;
//		reply.SetErrorMessage("Do Command faild.");
//		LOG(ERROR, "Do Command faild.");
//		return reply;
//	}
//
//	if (REDIS_OK != redisGetReply(redis, (void**) &reply))
//	{
//		redisFree(redis);
//		Reply reply;
//		reply.SetErrorMessage("Do Select Command faild.");
//		LOG(ERROR, "Do Select Command faild.");
//		return reply;
//	}
//
//	if (string(reply->str) != string("OK"))
//	{
//		redisFree(redis);
//		Reply reply;
//		reply.SetErrorMessage(
//				"Do Select Command faild, please check redis config parameter databases, it must >= 1024!");
//		LOG(ERROR, "Do Select Command faild, please check redis config parameter databases, it must >= 1024!");
//		exit(1);
//	}
//
//	if (REDIS_OK != redisGetReply(redis, (void**) &reply))
//	{
//		redisFree(redis);
//		Reply reply;
//		reply.SetErrorMessage("Do Command faild.");
//		LOG(ERROR, "Do Command faild.");
//		return reply;
//	}
//
//	m_ConnPool->returnItem(redis);
//	Reply retrep = Reply(reply);
//	freeReplyObject(reply);
//	return retrep;
}

bool RedisDB::mget2(vector<string>& keys,
		MgetAsyncRequestContext2 *mget_async_rctxt, aeEventLoop *(&loop_))
{

	cout << "mget2 dbno=" << m_RedisDBNo << ", key=" << keys[0] << endl;
	vector<string> command;
	command.push_back("MGET");
	for (size_t i = 0; i < keys.size(); ++i)
	{
		command.push_back(keys[i]);
	}

	vector<const char*> argv;
	vector<size_t> arglen;
	for (size_t i = 0; i < command.size(); ++i)
	{
		argv.push_back(command[i].c_str());
		arglen.push_back(command[i].size());
	}

	redisAsyncContext* async_context = m_ConnPool->borrowItemAsync(loop_);
	if (async_context == NULL)
	{
		LOG(ERROR, "contetx is NULL!!");
		m_ConnPool->returnItemAsync(async_context); // FIXME: add returnItem

		return false;
	}

	string selectCommand = string("select ") + m_RedisDBNo;

	redisAsyncCommand(async_context, NULL, NULL, selectCommand.c_str());

	int rc = redisAsyncCommandArgv(async_context, &mgetCallback2,
			mget_async_rctxt, //FIXME: callback, privdata
			argv.size(), &argv[0], &arglen[0]);

	if (rc == REDIS_ERR)
	{
		if (!m_ConnPool->ReconnectAsync(async_context, loop_))
		{
			LOG(ERROR, "reconnect faild, ");
			m_ConnPool->returnItemAsync(async_context); // FIXME: add returnItem

			return false;
		}

		rc = redisAsyncCommandArgv(async_context, &mgetCallback2, mget_async_rctxt, argv.size(),
				&argv[0], &arglen[0]);
	}

	if (rc == REDIS_ERR)
	{
		stringstream stream;
		stream << "run command error(" << rc << "): " << async_context->errstr;
		LOG(ERROR, stream.str());
		m_ConnPool->returnItemAsync(async_context); // FIXME: add returnItem

		return false;
	}

	m_ConnPool->returnItemAsync(async_context);

	return true;
}

void RedisDB::mgetCallback2(redisAsyncContext *c, void *r, void *privdata)
{
	// mget异步调用的回调函数
	// 合并结果数据到merger, 在最后一个mget回调处理完成后，通过cv通知
	redisReply *reply = (redisReply*) r;
	MgetAsyncRequestContext2 *rctxt = (MgetAsyncRequestContext2*) privdata;
	//  printf("result count: %d\n", reply->elements);
	for (size_t i = 0; i < reply->elements; ++i)
	{
		string value = std::string(reply->element[i]->str,
				reply->element[i]->len);
		string user_key = rctxt->user_keylist_[i];
		//    printf("callback, key:(%s), value(%s), counter(%d)\n", user_key.c_str(), value.c_str(), rctxt->merger_->counter_);
		rctxt->merger_->result_.insert(make_pair(user_key, value));
	}
	// update counter and notify
	rctxt->merger_->counter_--;
	if (rctxt->merger_->counter_ <= 0)
	{
		rctxt->merger_->finish_ = true;
	}

}

bool RedisDB::mget3(vector<string>& keys, MgetAsyncRequestContext3 *mget_async_rctxt,
			aeEventLoop *(&loop_))
{

	cout << "mget2 dbno=" << m_RedisDBNo << ", key=" << keys[0] << endl;
	vector<string> command;
	command.push_back("MGET");
	for (size_t i = 0; i < keys.size(); ++i)
	{
		command.push_back(keys[i]);
	}

	vector<const char*> argv;
	vector<size_t> arglen;
	for (size_t i = 0; i < command.size(); ++i)
	{
		argv.push_back(command[i].c_str());
		arglen.push_back(command[i].size());
	}

	redisAsyncContext* async_context = m_ConnPool->borrowItemAsync(loop_);
	if (async_context == NULL)
	{
		LOG(ERROR, "contetx is NULL!!");
		m_ConnPool->returnItemAsync(async_context); // FIXME: add returnItem

		return false;
	}

	string selectCommand = string("select ") + m_RedisDBNo;

	redisAsyncCommand(async_context, NULL, NULL, selectCommand.c_str());

	int rc = redisAsyncCommandArgv(async_context, &mgetCallback3,
			mget_async_rctxt, //FIXME: callback, privdata
			argv.size(), &argv[0], &arglen[0]);

	if (rc == REDIS_ERR)
	{
		if (!m_ConnPool->ReconnectAsync(async_context, loop_))
		{
			LOG(ERROR, "reconnect faild, ");
			m_ConnPool->returnItemAsync(async_context); // FIXME: add returnItem

			return false;
		}

		rc = redisAsyncCommandArgv(async_context, &mgetCallback3, mget_async_rctxt, argv.size(),
				&argv[0], &arglen[0]);
	}

	if (rc == REDIS_ERR)
	{
		stringstream stream;
		stream << "run command error(" << rc << "): " << async_context->errstr;
		LOG(ERROR, stream.str());
		m_ConnPool->returnItemAsync(async_context); // FIXME: add returnItem

		return false;
	}

	m_ConnPool->returnItemAsync(async_context);

	return true;
}

void RedisDB::mgetCallback3(redisAsyncContext *c, void *r, void *privdata)
{
	// mget异步调用的回调函数
	// 合并结果数据到merger, 在最后一个mget回调处理完成后，通过cv通知
	redisReply *reply = (redisReply*) r;
	MgetAsyncRequestContext3 *rctxt = (MgetAsyncRequestContext3*) privdata;
	//  printf("result count: %d\n", reply->elements);
	for (size_t i = 0; i < reply->elements; ++i)
	{
		string value = std::string(reply->element[i]->str,
				reply->element[i]->len);
		string user_key = rctxt->user_keylist_[i];
		//    printf("callback, key:(%s), value(%s), counter(%d)\n", user_key.c_str(), value.c_str(), rctxt->merger_->counter_);
		rctxt->merger_->result_->kvs.insert(make_pair(user_key, value));
	}

	int badmget = 0;

	for (size_t i=0; i<rctxt->merger_->status_.size(); i++)
	{
		if(!rctxt->merger_->status_[i]) badmget++;
	}

	// update counter and notify
	rctxt->merger_->counter_--;
	if (rctxt->merger_->counter_ <= 0 || (rctxt->merger_->counter_ == badmget))
	{
		rctxt->merger_->result_->finish = true;
		rctxt->merger_->result_ = NULL;

		ScopedLock lock(m_Mutex);
		if (rctxt->merger_ != NULL)
		{
			delete rctxt->merger_;
			rctxt->merger_ = NULL;
		}
	}

	rctxt->merger_ = NULL;
	if (rctxt != NULL)
	{
		delete rctxt;
		rctxt = NULL;
	}
}

vector<string> RedisDB::keys(string prefix)
{
	vector<string> keys;

	if (!m_ConnPool)
	{
		fprintf(stderr, "client pool is null!!!");
		Reply reply;
		reply.SetErrorMessage("Can not fetch client from pool!!!");
		LOG(ERROR, "client pool is null!!!");
		return keys;
	}

	vector<string> command;
	command.push_back("KEYS");
	if (prefix != "")
	{
		command.push_back(prefix + "*");
	}
	else
	{
		command.push_back("*");
	}

	vector<const char*> argv;
	vector<size_t> arglen;
	for (size_t i = 0; i < command.size(); ++i)
	{
		argv.push_back(command[i].c_str());
		arglen.push_back(command[i].size());
	}

	redisContext* redis = m_ConnPool->borrowItem();
#ifdef DEBUG
	fprintf(stdout, "real key: %s, host:%s \n", inner_key.c_str(), p->getId().c_str());
#endif

	if (!redis)
	{
		fprintf(stderr, "contetx is NULL!!");
		Reply reply;
		reply.SetErrorMessage("context is NULL !!");
		LOG(ERROR, "contetx is null!!!");
		return keys;
	}
	redisReply *reply = NULL;

	string selectCommand = string("select ") + m_RedisDBNo;

	int ret = redisAppendCommand(redis, selectCommand.c_str());
	if (ret != REDIS_OK)
	{
		if (redis != NULL)
		{
			redisFree(redis);
			redis = NULL;
		}
		redis = m_ConnPool->create();
		if (redis == NULL)
		{
			LOG(ERROR, "reconnect faild!");
			Reply reply;
			reply.SetErrorMessage("reconnect faild!");
			LOG(ERROR, "reconnect faild!");
			return keys;
		}
		ret = redisAppendCommand(redis, selectCommand.c_str());
	}

	if (ret != REDIS_OK)
	{
		if (redis != NULL)
		{
			redisFree(redis);
			redis = NULL;
		}
		Reply reply;
		reply.SetErrorMessage("Do Select Command faild.");
		LOG(ERROR, "Do Select Command faild.");
		return keys;
	}

	ret = redisAppendCommandArgv(redis, argv.size(), &argv[0], &arglen[0]);

	if (ret != REDIS_OK)
	{
		if (redis != NULL)
		{
			redisFree(redis);
			redis = NULL;
		}
		Reply reply;
		reply.SetErrorMessage("Do Command faild.");
		LOG(ERROR, "Do Command faild.");
		return keys;
	}

	if (REDIS_OK != redisGetReply(redis, (void**) &reply))
	{
		if (redis != NULL)
		{
			redisFree(redis);
			redis = NULL;
		}
		Reply reply;
		reply.SetErrorMessage("Do Select Command faild.");
		LOG(ERROR, "Do Select Command faild.");
		return keys;
	}

	if (string(reply->str) != string("OK"))
	{
		if (redis != NULL)
		{
			redisFree(redis);
			redis = NULL;
		}
		Reply reply;
		reply.SetErrorMessage(
				"Do Select Command faild, please check redis config parameter databases, it must >= 1024!");
		LOG(ERROR, "Do Select Command faild, please check redis config parameter databases, it must >= 1024!");
		return keys;
	}

	if (REDIS_OK != redisGetReply(redis, (void**) &reply))
	{
		if (redis != NULL)
		{
			redisFree(redis);
			redis = NULL;
		}
		Reply reply;
		reply.SetErrorMessage("Do Command faild.");
		LOG(ERROR, "Do Command faild.");
		return keys;
	}

	m_ConnPool->returnItem(redis);
	Reply retrep = Reply(reply);
	freeReplyObject(reply);

	for (size_t i=0; i<retrep.elements().size(); i++)
	{
		keys.push_back(retrep.elements()[i].str());
	}

	return keys;
}

