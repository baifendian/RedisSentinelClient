#include <strings.h>
#include <sstream>
#include <time.h>
#include <vector>
#include <string>
#include "kvproxy.h"

bool KvProxy::is_async = false;

static pthread_once_t once = PTHREAD_ONCE_INIT;
static pthread_key_t redisClientKey;
pthread_mutex_t onecelock = PTHREAD_MUTEX_INITIALIZER;

vector<string> commands;

static void RedisClientDestructor(void *redisClientPtr)
{
	delete static_cast<RedisClient*>(redisClientPtr);
}

static void CreateRedisClientKey(void)
{
	int ret = pthread_key_create(&redisClientKey, RedisClientDestructor);
	if (ret != 0) {
		std::stringstream errorMsg;
		errorMsg << __FUNCTION__ << ": pthread_key_create: Failed to create key. "
			<< "Error number: " << ret << ".";

        log_fatal("pthread_key_create: Failed to create key. Error number: %d." , ret);
        cerr << errorMsg.str() << endl;
        exit(EXIT_FAILURE);
	}
}

RedisClient& KvProxy::GetRedisClient()
{
	pthread_mutex_lock(&onecelock);
	int ret = pthread_once(&once, CreateRedisClientKey);
	if (ret != 0) {
		pthread_mutex_unlock(&onecelock);
		std::stringstream errorMsg;
		errorMsg << __FUNCTION__ << ": pthread_once: Failed to create key. "
			<< "Error number: " << ret << ".";

        log_fatal("pthread_once: Failed to create key. Error number: %d." , ret);
        cerr << errorMsg.str() << endl;
        exit(EXIT_FAILURE);
	}
	RedisClient* redisClientPtr = static_cast<RedisClient*>(pthread_getspecific(redisClientKey));
	if (redisClientPtr == NULL) {
		redisClientPtr = new RedisClient(m_sentinelAddr);
		ret = pthread_setspecific(redisClientKey, redisClientPtr);
		if (ret != 0) {
			pthread_mutex_unlock(&onecelock);
			std::stringstream errorMsg;
			errorMsg << __FUNCTION__ << ": pthread_once: Failed to set thread key. "
				<< "Error number: " << ret << ".";

	        log_fatal("pthread_once: Failed to set thread key. Error number: %d." , ret);
	        cerr << errorMsg.str() << endl;
	        exit(EXIT_FAILURE);
		}
	}
	pthread_mutex_unlock(&onecelock);
	return *redisClientPtr;
}

KvProxy::KvProxy(int count, bool cpu_affinity) : Server(count, cpu_affinity){
    int timeout;

    st_limit = ST_LIMIT;
    st_req = 0;
    st_conn = 0;
    thread_count = count;
    recv_timeout = TIMEOUT;
    timeout = Config::getConfInt("kvproxy","connect_timeout");
    if(timeout != 0){
        connect_timeout = timeout * 1000;
    }
    timeout = Config::getConfInt("kvproxy","send_timeout");
    if(timeout != 0){
        send_timeout = timeout * 1000;
    }
    timeout = Config::getConfInt("kvproxy","recv_timeout");
    if(timeout != 0){
        recv_timeout = timeout * 1000;
    }
    
    max_packet = Config::getConfInt("kvproxy","max_packet");
    if(max_packet <= 0){
        max_packet = MAX_PACKET;
    }
    
    failover_threshold = Config::getConfInt("kvproxy","failover_threshold");
    if(failover_threshold <= 0){
        failover_threshold = FAILOVER_THRESHOLD;
    }
    
    failover_interval = Config::getConfInt("kvproxy","failover_interval");
    if(failover_interval <= 0){
        failover_interval = FAILOVER_INTERVAL;
    }
    
    async_size = Config::getConfInt("kvproxy","async_size");
    if(async_size <= 0){
        async_size = ASYNC_SIZE;
    }
    
    conn_pool = ConnPool::getInstance();

    for(int id = 0; id <= thread_count; id++){
        req_buf[id] = malloc(max_packet * 1024 * 1024); 
        req_buf_len[id] = 0;
        req_buf_size[id] = max_packet * 1024 * 1024;
        resp_buf[id] = malloc(max_packet * 1024 * 1024); 
        resp_buf_len[id] = 0;
        resp_buf_size[id] = max_packet * 1024 * 1024;
        backend_buf[id] = malloc(max_packet * 1024 * 1024); 
        backend_buf_len[id] = 0;
        backend_buf_size[id] = max_packet * 1024 * 1024;
        client_buf[id] = malloc(max_packet * 1024 * 1024); 
        client_buf_len[id] = 0;
        client_buf_size[id] = max_packet * 1024 * 1024;
    }

    m_sentinelAddr = Config::getConfStr("kvproxy","sentinel");
    if (m_sentinelAddr == "")
    {
    	cout << "Can't get sentinel address!" << endl;
    	exit(EXIT_FAILURE);
    }

    commands.push_back("exists");	//---0
    commands.push_back("del");		//---1
    commands.push_back("type");		//---2
    commands.push_back("expire");	//---3
    commands.push_back("set");		//---4
    commands.push_back("setnx");	//---5
    commands.push_back("get");		//---6
    commands.push_back("getset");	//---7
    commands.push_back("mget");		//---8
    commands.push_back("mset");		//---9
    commands.push_back("incr");		//---10
    commands.push_back("decr");		//---11
    commands.push_back("incrby");	//---12
    commands.push_back("decrby");	//---13
    commands.push_back("append");	//---14
    commands.push_back("lpush");	//---15
    commands.push_back("rpush");	//---16
    commands.push_back("llen");		//---17
    commands.push_back("lrange");	//---18
    commands.push_back("ltrim");	//---19
    commands.push_back("lset");		//---20
    commands.push_back("lrem");		//---21
    commands.push_back("lpop");		//---22
    commands.push_back("rpop");		//---23
    commands.push_back("sadd");		//---24
    commands.push_back("srem");		//---25
    commands.push_back("spop");		//---26
    commands.push_back("srandmember");//---27
    commands.push_back("scard");	//---28
    commands.push_back("sismember");//---29
    commands.push_back("smembers");	//---30
    commands.push_back("zadd");		//---31
    commands.push_back("zrem");		//---32
    commands.push_back("zincrby");	//---33
    commands.push_back("zrank");	//---34
    commands.push_back("zrevrank");	//---35
    commands.push_back("zrange");	//---36
    commands.push_back("zrevrange");//---37
    commands.push_back("zrangebyscore");//---38
    commands.push_back("zcount");	//---39
    commands.push_back("zcard");	//---40
    commands.push_back("zscore");	//---41
    commands.push_back("zremrangebyrank");//---42
    commands.push_back("zremrangebyscore");//---43
    commands.push_back("hset");		//---44
    commands.push_back("hget");		//---45
    commands.push_back("hmget");	//---46
    commands.push_back("hmset");	//---47
    commands.push_back("hincrby");	//---48
    commands.push_back("hexists");	//---49
    commands.push_back("hdel");		//---50
    commands.push_back("hlen");		//---51
    commands.push_back("hkeys");	//---52
    commands.push_back("hvals");	//---53
    commands.push_back("hgetall");	//---54



}

KvProxy::~KvProxy(){
    for(int id = 0; id <= thread_count; id++){
        free(req_buf[id]); 
        free(resp_buf[id]); 
        free(backend_buf[id]);
    }
}

void KvProxy::initVar(){
    map<uint32_t, pair<string,uint32_t> >::iterator it;
    for(it = host_alias.begin(); it != host_alias.end(); it++){
        host_offline[it->first] = false;
        st_cont_fail[it->first] = 0;
        st_fail[it->first] = 0;
    }
}

map<uint32_t, pair<string,uint32_t> >  KvProxy::getHostAlias(){
    return host_alias;
}

map<uint32_t, bool>  KvProxy::getHostOffline(){
    return host_offline;
}

ConnPool * KvProxy::getConnPool(){
    return conn_pool;
}

int KvProxy::getFailOverInterval(){
    return failover_interval;
}

int KvProxy::getFailOverThreshold(){
    return failover_threshold;
}

map<uint32_t, uint32_t> KvProxy::getStContFail(){
    return st_cont_fail;
}

int KvProxy::getThreadCount(){
    return thread_count;
}

string KvProxy::getStatus(){
    string content;
    string str_req_failed;
    int int_req_failed = 0;
    string str_req_cont_failed;
    int int_req_cont_failed = 0;
    string host;
    int port;
    string str_st_req;
    string str_host_offline;
    int int_host_offline = 0;
    map<uint32_t, uint32_t>::iterator it; 
    map<uint32_t, bool>::iterator it_of;

    if(st_req >= ST_LIMIT){
        str_st_req = int2str(st_req) + "+";
    }else{
        str_st_req = int2str(st_req);
    }

    content = "\nNumber of processed requests [" + str_st_req + "]\n";
    for(it = st_fail.begin(); it != st_fail.end(); it++){
        host = host_alias[it->first].first;
        port = host_alias[it->first].second;
        str_req_failed += "  - " + host + ":" + int2str(port) + " [" + int2str(it->second) + "]\n"; 
        int_req_failed += it->second;
    }
    for(it = st_cont_fail.begin(); it != st_cont_fail.end(); it++){
        host = host_alias[it->first].first;
        port = host_alias[it->first].second;
        str_req_cont_failed += "  - " + host + ":" + int2str(port) + " [" + int2str(it->second) + "]\n"; 
        int_req_cont_failed += it->second;
    }
    for(it_of = host_offline.begin(); it_of != host_offline.end(); it_of++){
        if(it_of->second == false){
            continue;
        }
        host = host_alias[it_of->first].first;
        port = host_alias[it_of->first].second;
        str_host_offline += "  - " + host + ":" + int2str(port) + " [offline]\n";
        int_host_offline++;
    }

    //todo concurrency
    if(st_conn < 0){
        st_conn = 0;
    }

    content += "Number of requests failed [" + int2str(int_req_failed) + "]\n";
    content += str_req_failed;
    content += "Number of continuous requests failed [" + int2str(int_req_cont_failed) + "]\n";
    content += str_req_cont_failed;
    content += "All of offline hosts ["+ int2str(int_host_offline) +"] \n";
    content += str_host_offline;
    content += "Number of client connection [" + int2str(st_conn) + "]\n";
    content += "Number of backend connection [" + int2str(conn_pool->getConnectionSize()) + "]\n";
    content += "Read timeout of backend connection [" + int2str(recv_timeout/1000) + "ms]\n";
    content += "Size of async queue [" + int2str(async_req.size()) + "]\n";
    content += "\n";
    return content;
}

void KvProxy::setHosts(string type, string ext_name, uint32_t thread_count){
    map<pair<string,uint32_t>, pair<uint32_t,uint32_t> > hosts_tmp;
    string group_name;
    map<string, string> host_group;
    map<string, string>::iterator g_it;
    map<int, string> split_vals;
    string host;
    string host_port;
    uint32_t port = 0;
    uint32_t index;
    uint32_t weight;
    uint32_t alias_index;
   
    group_name = Config::getConfStr(ext_name, type);
    if(group_name != ""){
        host_group = Config::getConfStr(group_name);
    }

    for(g_it = host_group.begin(); g_it != host_group.end(); g_it++){
        split_vals = splitmap(g_it->first, ':');
        if(split_vals.size() != 2){
            log_fatal("host group in config is invalid: %s" , g_it->first.c_str());
            cerr << "host group in config is invalid: " + g_it->first << endl;
            exit(EXIT_FAILURE);
        }else{
            host = split_vals[0];
            port = str2int(split_vals[1].c_str());
        }
        split_vals = splitmap(g_it->second, ':');
        if(split_vals.size() != 2){
            log_fatal("host group in config is invalid: %s" , g_it->second.c_str());
            cerr << "host group in config is invalid:" + g_it->second << endl;
            exit(EXIT_FAILURE);
        }else{
            index = str2int(split_vals[0].c_str());
            weight = str2int(split_vals[1].c_str());
        }
        
        host_port = host + ":" + int2str(port);
        alias_index = setHostAlias(host, port);
        host_infos[alias_index] = make_pair(index,weight);
        hosts_tmp[make_pair(host_port,alias_index)] = make_pair(index,weight);
        conn_pool->initConnection(host, port, thread_count);
        if(type == "hosts"){
            hosts_default.insert(alias_index);
        }else if(type == "hosts_backup"){
            hosts_backup.insert(alias_index);
        }else if(type == "hosts_read"){
            hosts_read.insert(alias_index);
        } 
    }
    if(type == "hosts" && hosts_tmp.size() == 0){
        log_fatal("hosts in config is empty");
        cerr << "host in config is empty " << endl;
        exit(EXIT_FAILURE);
    }else if(type == "hosts"){
        hash.setHosts(hosts_tmp);
    }

}

uint32_t KvProxy::setHostAlias(string host, uint32_t port){
    static uint32_t index = 0;
    map<uint32_t, pair<string,uint32_t> >::iterator it;
    pair<string, uint32_t> host_port;
    host_port.first = host;
    host_port.second = port;
    for(it = host_alias.begin(); it != host_alias.end(); it++){
        if(it->second.first == host && it->second.second == port){
            return it->first;
        }
    }
    index++;
    host_alias[index] = host_port;
    return index;
}

bool KvProxy::failover(uint32_t alias_index, bool is_del){
    bool ret = false;
    string host_port;
    uint32_t index;
    uint32_t weight;

    if(is_del == true){
        if(host_offline[alias_index] == true){
             return true;
        } 
        if(host_alias.find(alias_index) != host_alias.end()){
            host_port = host_alias[alias_index].first + ":" + int2str(host_alias[alias_index].second);
        }else{
            return false;
        } 
        if(hosts_default.find(alias_index) != hosts_default.end()){
            ret = hash.delHost(alias_index);
            log_warn("%s offline in hosts, %s", host_port.c_str(), bool2str(ret).c_str());
        }
        if(hosts_read.find(alias_index) != hosts_read.end()){
            ret = hash_read.delHost(alias_index);
            log_warn("%s offline in hosts read, %s", host_port.c_str(), bool2str(ret).c_str()); 
        }
        if(hosts_backup.find(alias_index) != hosts_backup.end()){
            hash_backup.delHost(alias_index);
            log_warn("%s offline in hosts backup, %s", host_port.c_str(), bool2str(ret).c_str());
        }
        host_offline[alias_index] = true;
    }else{
        if(host_offline[alias_index] == false){
            return true;
        }
        if(host_alias.find(alias_index) != host_alias.end()){
            host_port = host_alias[alias_index].first + ":" + int2str(host_alias[alias_index].second);
        }else{
            return false;
        }
        if(host_infos.find(alias_index) != host_infos.end()){
           index = host_infos[alias_index].first;
           weight = host_infos[alias_index].second;
        }else{
            return false;
        }
        if(hosts_default.find(alias_index) != hosts_default.end()){
            ret = hash.addHost(host_port, alias_index, index, weight); 
            log_warn("%s online in hosts, %s", host_port.c_str(), bool2str(ret).c_str());
        }
        if(hosts_read.find(alias_index) != hosts_read.end()){
            ret = hash_read.addHost(host_port, alias_index, index, weight); 
            log_warn("%s online in hosts read, %s", host_port.c_str(), bool2str(ret).c_str());
        }
        if(hosts_backup.find(alias_index) != hosts_backup.end()){
            ret = hash_backup.addHost(host_port, alias_index, index, weight); 
            log_warn("%s online in hosts backup, %s", host_port.c_str(), bool2str(ret).c_str());
        }
        
        host_offline[alias_index] = false;
    }
    return true;
}

void KvProxy::countFail(bool is_fail, uint32_t alias_index){ 
    if(is_fail == true){
        if(st_fail[alias_index] < st_limit){
            st_fail[alias_index]++;
            st_cont_fail[alias_index]++;
        }
    }else if(st_cont_fail[alias_index] != 0){
        st_cont_fail[alias_index] = 0;
    }
}

void * checkHealthThread(void* args){
    string host;
    uint32_t port;
    uint32_t alias_index;
    int conn_fd;
    int failover_interval;
    int failover_threshold;
    int thread_count;
    map<uint32_t, pair<string,uint32_t> > host_alias;
    map<uint32_t, bool> host_offline;
    map<uint32_t,bool>::iterator it;
    map<uint32_t, uint32_t> st_cont_fail;
    map<uint32_t, uint32_t>::iterator it_fail;
    ConnPool * conn_pool;
    KvProxy * obj = (KvProxy *)args;
    
    failover_interval = obj->getFailOverInterval();
    failover_threshold = obj->getFailOverThreshold();
    host_alias = obj->getHostAlias();
    conn_pool = obj->getConnPool();
    thread_count = obj->getThreadCount();

    while(1){
        sleep(failover_interval);
        //start to check online host which is failed
        st_cont_fail = obj->getStContFail();
        for(it_fail = st_cont_fail.begin(); it_fail != st_cont_fail.end(); it_fail++){
            if((int)it_fail->second >= failover_threshold){
                obj->failover(it_fail->first, true);      
            } 
        }
        //start to check offline host
        host_offline = obj->getHostOffline();
        for(it = host_offline.begin(); it != host_offline.end(); it++){
            if(it->second == false){
                continue;
            }
            alias_index = it->first;
            if(host_alias.find(alias_index) == host_alias.end()){
                continue;
            }
            host = host_alias[alias_index].first;
            port = host_alias[alias_index].second;
            conn_fd = conn_pool->createConnection(host, port); 
            if(conn_fd > 0){
                obj->failover(alias_index, false);
                obj->countFail(false, alias_index);
                conn_pool->closeConnection(conn_fd);
            }
        }
    }
}

void KvProxy::createCheckHealthThread(){
    int rt;
    pthread_attr_t attr;

    pthread_attr_init(&attr);  
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);  
    rt = pthread_create( &th_check_health, &attr, checkHealthThread, this );
    if( rt != 0 ){
		cerr << "fail to create thread for check health!" << endl;
        log_fatal("fail to create thread for check health");
		exit(EXIT_FAILURE);
    }
}

string KvProxy::RedisCommand(vector<string>& command)
{
	convertStr(command[0]);
//	for (size_t i=0; i<command.size(); i++)
//	{
//		cout << "command=" << command[i] <<",";
//	}
//	cout << endl;
	size_t index = 0;
	for (; index<commands.size(); index++)
	{
		if(command[0] == commands[index])
		{
			break;
		}
	}

	if (index == commands.size())
	{
		return "-error command!\r\n";
	}

	string ret = "";
	Reply rep;
	Command comm;


	switch(index)
	{
	case 0:		//exist
		if (command.size() != 2) return "-error command!\r\n";
		rep = GetRedisClient().RedisCommand(Command("EXISTS")(command[1]));
		ret = ReplyToString(rep);
		break;
	case 1:		//del
		if (command.size() != 2) return "-error command!\r\n";
		rep = GetRedisClient().RedisCommand(Command("DEL")(command[1]));
		ret = ReplyToString(rep);
		break;
	case 2:		//type
		if (command.size() != 2) return "-error command!\r\n";
		rep = GetRedisClient().RedisCommand(Command("TYPE")(command[1]));
		ret = ReplyToString(rep);
		break;
	case 3:		//expire
		if (command.size() != 3) return "-error command!\r\n";
		rep = GetRedisClient().RedisCommand(Command("EXPIRE")(command[1])(command[2]));
		ret = ReplyToString(rep);
		break;
	case 4:		//set
		if (command.size() != 3) return "-error command!\r\n";
		rep = GetRedisClient().RedisCommand(Command("SET")(command[1])(command[2]));
		ret = ReplyToString(rep);
		break;
	case 5:		//setnx
		if (command.size() != 3) return "-error command!\r\n";
		rep = GetRedisClient().RedisCommand(Command("SETNX")(command[1])(command[2]));
		ret = ReplyToString(rep);
		break;
	case 6:		//get
		if (command.size() != 2) return "-error command!\r\n";
		rep = GetRedisClient().RedisCommand(Command("GET")(command[1]));
		ret = ReplyToString(rep);
		break;
	case 7:		//getset
		rep = GetRedisClient().RedisCommand(Command("GETSET")(command[1]));
		ret = ReplyToString(rep);
		break;
	case 8:		//mget
		return "-error command!\r\n";
		break;
	case 9:		//mset
		return "-error command!\r\n";
		break;
	case 10:	//incr
		if (command.size() != 2) return "-error command!\r\n";
		rep = GetRedisClient().RedisCommand(Command("INCR")(command[1]));
		ret = ReplyToString(rep);
		break;
	case 11:	//decr
		if (command.size() != 2) return "-error command!\r\n";
		rep = GetRedisClient().RedisCommand(Command("DECR")(command[1]));
		ret = ReplyToString(rep);
		break;
	case 12:	//incrby
		if (command.size() != 3) return "-error command!\r\n";
		rep = GetRedisClient().RedisCommand(Command("INCRBY")(command[1])(command[2]));
		ret = ReplyToString(rep);
		break;
	case 13:	//decrby
		if (command.size() != 3) return "-error command!\r\n";
		rep = GetRedisClient().RedisCommand(Command("DECRBY")(command[1])(command[2]));
		ret = ReplyToString(rep);
		break;
	case 14:	//append
		if (command.size() != 3) return "-error command!\r\n";
		rep = GetRedisClient().RedisCommand(Command("APPEND")(command[1])(command[2]));
		ret = ReplyToString(rep);
		break;
	case 15:	//lpush
		if (command.size() != 3) return "-error command!\r\n";
		rep = GetRedisClient().RedisCommand(Command("LPUSH")(command[1])(command[2]));
		ret = ReplyToString(rep);
		break;
	case 16:	//rpush
		if (command.size() != 3) return "-error command!\r\n";
		rep = GetRedisClient().RedisCommand(Command("RPUSH")(command[1])(command[2]));
		ret = ReplyToString(rep);
		break;
	case 17:	//llen
		if (command.size() != 2) return "-error command!\r\n";
		rep = GetRedisClient().RedisCommand(Command("LLEN")(command[1]));
		ret = ReplyToString(rep);
		break;
	case 18:	//lrange
		if (command.size() != 4) return "-error command!\r\n";
		rep = GetRedisClient().RedisCommand(Command("LRANGE")(command[1])(command[2])(command[3]));
		ret = ReplyToString(rep);
		break;
	case 19:	//ltrim
		if (command.size() != 4) return "-error command!\r\n";
		rep = GetRedisClient().RedisCommand(Command("LTRIM")(command[1])(command[2])(command[3]));
		ret = ReplyToString(rep);
		break;
	case 20:	//lset
		if (command.size() != 4) return "-error command!\r\n";
		rep = GetRedisClient().RedisCommand(Command("LSET")(command[1])(command[2])(command[3]));
		ret = ReplyToString(rep);
		break;
	case 21:	//lrem
		if (command.size() != 4) return "-error command!\r\n";
		rep = GetRedisClient().RedisCommand(Command("LREM")(command[1])(command[2])(command[3]));
		ret = ReplyToString(rep);
		break;
	case 22:	//lpop
		if (command.size() != 2) return "-error command!\r\n";
		rep = GetRedisClient().RedisCommand(Command("LPOP")(command[1]));
		ret = ReplyToString(rep);
		break;
	case 23:	//rpop
		if (command.size() != 2) return "-error command!\r\n";
		rep = GetRedisClient().RedisCommand(Command("RPOP")(command[1]));
		ret = ReplyToString(rep);
		break;
	case 24:	//sadd
		if (command.size() != 3) return "-error command!\r\n";
		rep = GetRedisClient().RedisCommand(Command("SADD")(command[1])(command[2]));
		ret = ReplyToString(rep);
		break;
	case 25:	//srem
		if (command.size() != 3) return "-error command!\r\n";
		rep = GetRedisClient().RedisCommand(Command("SREM")(command[1])(command[2]));
		ret = ReplyToString(rep);
		break;
	case 26:	//spop
		if (command.size() != 2) return "-error command!\r\n";
		rep = GetRedisClient().RedisCommand(Command("SPOP")(command[1]));
		ret = ReplyToString(rep);
		break;
	case 27:	//srandmember
		if (command.size() != 2) return "-error command!\r\n";
		rep = GetRedisClient().RedisCommand(Command("SRANDMEMBER")(command[1]));
		ret = ReplyToString(rep);
		break;
	case 28:	//scard
		if (command.size() != 2) return "-error command!\r\n";
		rep = GetRedisClient().RedisCommand(Command("SCARD")(command[1]));
		ret = ReplyToString(rep);
		break;
	case 29:	//sismember
		if (command.size() != 3) return "-error command!\r\n";
		rep = GetRedisClient().RedisCommand(Command("SISMEMBER")(command[1])(command[2]));
		ret = ReplyToString(rep);
		break;
	case 30:	//smembers
		if (command.size() != 2) return "-error command!\r\n";
		rep = GetRedisClient().RedisCommand(Command("SMEMBERS")(command[1]));
		ret = ReplyToString(rep);
		break;
	case 31:	//zadd
		if (command.size() != 4) return "-error command!\r\n";
		rep = GetRedisClient().RedisCommand(Command("ZADD")(command[1])(command[2])(command[3]));
		ret = ReplyToString(rep);
		break;
	case 32:	//zrem
		if (command.size() != 3) return "-error command!\r\n";
		rep = GetRedisClient().RedisCommand(Command("ZREM")(command[1])(command[2]));
		ret = ReplyToString(rep);
		break;
	case 33:	//zincrby
		if (command.size() != 4) return "-error command!\r\n";
		rep = GetRedisClient().RedisCommand(Command("ZINCRBY")(command[1])(command[2])(command[3]));
		ret = ReplyToString(rep);
		break;
	case 34:	//zrank
		if (command.size() != 3) return "-error command!\r\n";
		rep = GetRedisClient().RedisCommand(Command("ZRANK")(command[1])(command[2]));
		ret = ReplyToString(rep);
		break;
	case 35:	//zrevrank
		if (command.size() != 3) return "-error command!\r\n";
		rep = GetRedisClient().RedisCommand(Command("ZREVRANK")(command[1])(command[2]));
		ret = ReplyToString(rep);
		break;
	case 36:	//zrange
		if (command.size() != 4) return "-error command!\r\n";
		rep = GetRedisClient().RedisCommand(Command("ZRANGE")(command[1])(command[2])(command[3]));
		ret = ReplyToString(rep);
		break;
	case 37:	//zrevrange
		if (command.size() != 4) return "-error command!\r\n";
		rep = GetRedisClient().RedisCommand(Command("ZREVRANGE")(command[1])(command[2])(command[3]));
		ret = ReplyToString(rep);
		break;
	case 38:	//zrangebyscore
		if (command.size() != 4) return "-error command!\r\n";
		rep = GetRedisClient().RedisCommand(Command("ZRANGEBYSCORE")(command[1])(command[2])(command[3]));
		ret = ReplyToString(rep);
		break;
	case 39:	//zcount
		if (command.size() != 4) return "-error command!\r\n";
		rep = GetRedisClient().RedisCommand(Command("ZCOUNT")(command[1])(command[2])(command[3]));
		ret = ReplyToString(rep);
		break;
	case 40:	//zcard
		if (command.size() != 2) return "-error command!\r\n";
		rep = GetRedisClient().RedisCommand(Command("ZCARD")(command[1]));
		ret = ReplyToString(rep);
		break;
	case 41:	//zscore
		if (command.size() != 3) return "-error command!\r\n";
		rep = GetRedisClient().RedisCommand(Command("ZSCORE")(command[1])(command[2]));
		ret = ReplyToString(rep);
		break;
	case 42:	//zremrangebyrank
		if (command.size() != 4) return "-error command!\r\n";
		rep = GetRedisClient().RedisCommand(Command("ZREMRANGEBYRANK")(command[1])(command[2])(command[3]));
		ret = ReplyToString(rep);
		break;
	case 43:	//zremrangebyscore
		if (command.size() != 4) return "-error command!\r\n";
		rep = GetRedisClient().RedisCommand(Command("ZREMRANGEBYSCORE")(command[1])(command[2])(command[3]));
		ret = ReplyToString(rep);
		break;
	case 44:	//hset
		if (command.size() != 4) return "-error command!\r\n";
		rep = GetRedisClient().RedisCommand(Command("HSET")(command[1])(command[2])(command[3]));
		ret = ReplyToString(rep);
		break;
	case 45:	//hget
		if (command.size() != 3) return "-error command!\r\n";
		rep = GetRedisClient().RedisCommand(Command("HGET")(command[1])(command[2]));
		ret = ReplyToString(rep);
		break;
	case 46:	//hmget
		if (command.size() < 3) return "-error command!\r\n";
		comm("HMGET");

		for (size_t i=1; i<command.size(); i++)
		{
			comm(command[i]);
		}
		rep = GetRedisClient().RedisCommand(comm);
		ret = ReplyToString(rep);
		break;
	case 47:	//hmset
		if (command.size() < 4) return "-error command!\r\n";
		comm("HMSET");

		for (size_t i=1; i<command.size(); i++)
		{
			comm(command[i]);
		}

		rep = GetRedisClient().RedisCommand(comm);
		ret = ReplyToString(rep);
		break;
	case 48:	//hincrby
		if (command.size() != 4) return "-error command!\r\n";
		rep = GetRedisClient().RedisCommand(Command("HINCRBY")(command[1])(command[2])(command[3]));
		ret = ReplyToString(rep);
		break;
	case 49:	//hexists
		if (command.size() != 3) return "-error command!\r\n";
		rep = GetRedisClient().RedisCommand(Command("HEXISTS")(command[1])(command[2]));
		ret = ReplyToString(rep);
		break;
	case 50:	//hdel
		if (command.size() != 3) return "-error command!\r\n";
		rep = GetRedisClient().RedisCommand(Command("HDEL")(command[1])(command[2]));
		ret = ReplyToString(rep);
		break;
	case 51:	//hlen
		if (command.size() != 2) return "-error command!\r\n";
		rep = GetRedisClient().RedisCommand(Command("HLEN")(command[1]));
		ret = ReplyToString(rep);
		break;
	case 52:	//hkeys
		if (command.size() != 2) return "-error command!\r\n";
		rep = GetRedisClient().RedisCommand(Command("HKEYS")(command[1]));
		ret = ReplyToString(rep);
		break;
	case 53:	//hvals
		if (command.size() != 2) return "-error command!\r\n";
		rep = GetRedisClient().RedisCommand(Command("HVALS")(command[1]));
		ret = ReplyToString(rep);
		break;
	case 54:	//hgetall
		if (command.size() != 2) return "-error command!\r\n";
		rep = GetRedisClient().RedisCommand(Command("HGETALL")(command[1]));
		ret = ReplyToString(rep);
		break;
	default:
		ret = "-error command!\r\n";
	}
	return ret;
}

void KvProxy::readEvent(Conn *conn){
    char cmd[7] = "status";
    ext_ret_t ext_ret; 
    char buf[FAST_BUFF_SIZE];
    char * buf_ptr;
    int ret;
    int buf_len;
    int cur_conn_fd = conn->getFd();
    int thread_index = conn->getThread()->index;
    
    req_group_t req_group;
    req_group_t req_group_backup;
    req_group_t req_group_backup_sync;
    req_group_t::iterator group_it;
    req_ptr_list_t::iterator list_it;
    req_group_async_t req_group_backup_async;
    req_group_async_t::iterator async_group_it;
    comm_t async_item;

    string resp_cont;
    string resp_cont_sync;

    //read request data
    buf_len = conn->getReadBufLen();
  
    if(buf_len <= FAST_BUFF_SIZE){
        buf_ptr = buf;
    }else if(buf_len <= req_buf_size[thread_index]){
        buf_ptr = (char *)req_buf[thread_index];
    }else{
        conn->clearReadBuf(buf_len);
        return;
    }
    
    buf_len = conn->CopyReadBuf(buf_ptr, buf_len);
    log_debug("[%d]read size %d", cur_conn_fd, buf_len);

    vector<string> command;

    char *ptr = buf_ptr;

	struct timeval start, end;
	gettimeofday(&start, NULL);


    stringstream stream;
    string tmp(buf_ptr, buf_len);

    cout << "readbuf='" << tmp << "'" << endl;

    stream << tmp;

    if (stream.str().size() > 0)
    {
    	string str;
    	stream >> str;
    	if (str[0] == '*')
    	{
    		int lineNum = atoi(str.substr(1).c_str());

    		while (lineNum>0)
    		{
    			stream >> str;

    			if (str[0] == '$')
    			{
    				int size = atoi(str.substr(1).c_str());
//    				stream >> str;

    				stream.seekg(2, std::ios::cur); //去掉 \r\n
    				str = "";
    				str.resize(size + 2); //增加后面的\r\n
    				stream.read(&(str[0]), size+2);

    				str = str.substr(0, size); //去掉 \r\n

    				lineNum--;
    				if (str.size() == size)
    				{
    					command.push_back(str);
    				}
    			}
    		}
    	}
    }


    string retstr;

    if (command.size() > 0)
    {
    	retstr = RedisCommand(command);
    }
    else
    {
    	retstr = "-error command!\r\n";
    }

    if (retstr.empty())
    {
    	retstr = "-error\r\n";
    }

    if (retstr.size()>0 && retstr[0]=='-')
    {
    	log_info("retstr=%s\r\n", retstr.c_str());
    	stringstream comm;
    	for (size_t i=0; i<command.size(); i++)
    	{
    		comm << command[i] << " ";
    	}
    	log_info("command=%s\r\n", comm.str().c_str());
    }



	int send_len = 0;
	const char *msg = retstr.c_str();
	int msg_len = retstr.size();
    while(1){
    	int ret_len = send(cur_conn_fd, msg, msg_len, MSG_WAITALL);
		if(ret_len > 0){
			send_len += ret_len;
			if (send_len >= msg_len){
				break;
			}
		}
    }
    conn->clearReadBuf(buf_len);

    bzero(buf_ptr, buf_len);
    position[thread_index] = 0;


    return ;
}

void KvProxy::writeEvent(Conn *conn){
}

void KvProxy::connectionEvent(Conn *conn){
    st_conn++;
}

void KvProxy::closeEvent(Conn *conn, short events){
    st_conn--;
}

void KvProxy::quitCb(int sig, short events, void *data){ 
    KvProxy *me = (KvProxy*)data;
    if(is_async == true){
        pthread_kill(me->th_async,SIGINT);
    }
    pthread_kill(me->th_check_health,SIGINT);
    timeval tv = {1, 0};
    me->stopRun(&tv);
    exit(EXIT_SUCCESS);
}

void KvProxy::timeOutCb(int id, short events, void *data){
}

std::string KvProxy::ReplyToString(const Reply& rep)
{
	stringstream retstream;

	vector<Reply> reps;
	if (rep.elements().size()>0)
	{
		reps = rep.elements();
	}

	switch (rep.type()) {
		case Reply::ERROR:
			retstream << "-";
			retstream << rep.str();
			retstream << "\r\n";
			break;
		case Reply::STRING:
			retstream << "$";
			retstream << rep.str().size();
			retstream << "\r\n";
			retstream << rep.str();
			retstream << "\r\n";

			break;
		case Reply::STATUS:
			retstream << "+";
			retstream << rep.str();
			retstream << "\r\n";
			break;
		case Reply::INTEGER:
		  	retstream << ":";
		  	retstream << rep.integer();
		  	retstream << "\r\n";
		  	break;
		case Reply::NIL:
			retstream << "$-1\r\n";
			break;
		case Reply::ARRAY:
			retstream << "*";
			retstream << rep.elements().size();
			retstream << "\r\n";


			for (size_t idx=0; idx<reps.size(); ++idx) {
		    	switch (reps[idx].type()) {
					case Reply::ERROR:
						retstream << "-";
						retstream << reps[idx].str();
						retstream << "\r\n";
						break;
					case Reply::STRING:
						retstream << "$";
						retstream << reps[idx].str().size();
						retstream << "\r\n";
						retstream << reps[idx].str();
						retstream << "\r\n";
						break;
					case Reply::STATUS:
						retstream << "+";
						retstream << reps[idx].str();
						retstream << "\r\n";
						break;
					case Reply::INTEGER:
					  	retstream << ":";
					  	retstream << reps[idx].integer();
					  	retstream << "\r\n";
					  	break;
					case Reply::NIL:
						retstream << "$-1\r\n";
						break;
					default:
						retstream << "+OK\r\n";
					    break;
		    	}
			}
		    break;
		default:
			retstream << "+OK\r\n";
			break;
	}

	return retstream.str();
}

std::string KvProxy::VectorToString(const std::vector<std::string>& values)
{
	stringstream retstream;
	retstream << "*" << values.size() << "\r\n";
	for (size_t i=0; i<values.size(); i++)
	{
		if (values[i].size()==0)
		{
			retstream << "$-1\r\n";
		}
		else
		{
			retstream << "$" << values[i].size() << "\r\n";
			retstream << values[i] << "\r\n";
		}
	}

	return retstream.str();
}

void KvProxy::convertStr(std::string &str)
{
	for (size_t i=0; i<str.size(); i++)
	{
		if ((str[i]>=65) && (str[i]<=90))
		{
			str[i] += 32;
		}
	}
}

void daemon(void){
        int pid;
        
        pid = fork();
        if(pid > 0){
            exit(EXIT_SUCCESS);
        }else if(pid < 0){
            exit(EXIT_FAILURE);
        }
        setsid();
        umask(0);
}

void help(){
    cout << "Usage: kvproxy" << endl;
    cout << "-d run as daemon. optional." << endl;
    cout << "-h show help info. optional." << endl;
    cout << "-c specify the path of config. optional." << endl;
    cout << "-v show version info. optional." << endl;
    cout << "more help info: http://www.bo56.com" << endl;
    exit(EXIT_SUCCESS);
}

void show_version(){
    cout << "KvProxy "<< KVPROXY_VERSION << "  (built: " << KVPROXY_BUILT << ") " << endl;
    exit(EXIT_SUCCESS);
}

void initLog(string cwd){
    string log_path;
    string log_level;
    int level;

    log_path = Config::getConfStr("kvproxy","log_path");
    if (log_path.empty()){
        log_path = cwd + "../log/kvproxy.log";
    }
    log_open(log_path.c_str());
    log_level = Config::getConfStr("kvproxy","log_level");
    if (!log_level.empty()){
        level = get_log_level(log_level.c_str());
        set_log_level(level);
    }
}

string get_conf(string section, string key){
    return Config::getConfStr(section, key);
}

int main(int argc ,char **args){    
    string ext_path;
    string ext_name;
    string ext_filename;
    string cwd;
    string conf_path;
    uint32_t thread_count;
    uint32_t port;
    int backlog;
    int ch;
    bool is_daemon = false;
    bool cpu_affinity = false;
    int cpu_num = 0;

    signal(SIGPIPE, SIG_IGN);

    while((ch = getopt(argc, args, "c:dhv")) != -1){
        switch(ch){
            case 'h':
                help();
                break;
            case 'd':
                is_daemon = true;
                break;
            case 'c':
                conf_path = optarg;
                break;
            case 'v':
                show_version();
                break;
            default:
                help();
        }
    }
    
    cwd = get_cwd(args[0]);
    if(conf_path.empty()){
        conf_path = cwd + "../etc/proxy.ini";
    }
    Config::setConfFile(conf_path);
    Config::loadConf();

    if(is_daemon == true){
        daemon();
    }
    
    initLog(cwd);
    cpu_affinity = Config::getConfBool("kvproxy","cpu_affinity");
    thread_count = Config::getConfInt("kvproxy","thread_count");

#ifdef linux
    cpu_num = get_nprocs();
    if(cpu_affinity == true){
        thread_count = cpu_num;
    }else if(thread_count <= 0){
        thread_count = 1;
    }
#endif
    KvProxy server(thread_count, cpu_affinity);
    
    ext_path = Config::getConfStr("kvproxy","ext_path");
    if (ext_path.empty()){
        ext_path = cwd + "../ext";
    }
    if(ext_name.empty()){
        ext_name = Config::getExtName();
    }
    ext_filename = Config::getConfStr(ext_name,"extension");
    
    server.setHosts("hosts", ext_name, thread_count);
    server.setHosts("hosts_backup", ext_name, thread_count);
    server.setHosts("hosts_read", ext_name, thread_count);

    server.addSignalEvent(SIGINT, KvProxy::quitCb);
    timeval tv = {10, 0};
    server.addTimerEvent(KvProxy::timeOutCb, tv, false);
    server.initVar();
    server.createCheckHealthThread();

    port = Config::getConfInt("kvproxy","port");
    if (port == 0){
        port = 55669;
    }
    backlog = Config::getConfInt("kvproxy","backlog");
    if (backlog <= 0){
        backlog = -1;
    }
    server.setPort(port);
    server.startRun(backlog);
    return 0;
}


