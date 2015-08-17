#include "RedisClient.h"

using namespace bfd::redis;

vector<string> ReplyToVec(Reply rep)
{
	vector<string> ret;
	vector<Reply> reps= rep.elements();
	for(size_t i=0; i<reps.size(); i++)
	{
		ret.push_back(reps[i].str());
	}

	return ret;
}

int main(int argc, char* argv[])
{
	if (argc < 3)
	{
		cout << "Example:" << endl;
		cout << "        ./RedistributeTool sentinels newMasterName newAddr newPort" << endl;

		exit(0);
	}

	string sentinels = string(argv[1]);
	string newMasterName = string(argv[2]);
	string newAddr = string(argv[3]);
	int port = atoi(argv[4]);

	RedisClient client(sentinels);


	vector<RedisDB> dbs = client.AddServer(newMasterName, newAddr, port);

	for (size_t dbindex=0; dbindex<dbs.size(); dbindex++)
	{
		vector<string> keys = dbs[dbindex].keys();

		cout << "=======>" << dbindex << "/" <<dbs.size() << endl;
		cout << "=======>keys.size=" << keys.size() << endl;

		for (size_t keyindex=0; keyindex<keys.size(); keyindex++)
		{
			Reply rep = dbs[dbindex].RedisCommand(Command("type")(keys[keyindex]));
			string type = rep.str();

			if (type == "string")
			{
				Reply rep = dbs[dbindex].RedisCommand(Command("get")(keys[keyindex]));
				string value = rep.str();
				client.set(keys[keyindex], value);
			}
			else if (type == "list")
			{
				Reply rep = dbs[dbindex].RedisCommand(Command("lrange")(keys[keyindex])("0")("-1"));

				vector<string> values = ReplyToVec(rep);

				client.lpush(keys[keyindex], values);
			}
			else if (type == "set")
			{
				Reply rep = dbs[dbindex].RedisCommand(Command("smembers")(keys[keyindex]));

				vector<string> members = ReplyToVec(rep);

				client.sadd(keys[keyindex], members);
			}
			else if (type == "zset")
			{
				Reply rep = dbs[dbindex].RedisCommand(Command("zrange")(keys[keyindex])("0")("-1"));

				vector<string> members = ReplyToVec(rep);

				for(size_t memindex=0; memindex<members.size(); memindex++)
				{
					Reply rep = dbs[dbindex].RedisCommand(Command("zscore")(keys[keyindex])(members[memindex]));
					int score = rep.integer();
					client.zadd(keys[keyindex], score, members[memindex]);
				}
			}
			else if (type == "hash")
			{
				Reply rep = dbs[dbindex].RedisCommand(Command("hkeys")(keys[keyindex]));
				vector<string> fields = ReplyToVec(rep);

				rep = dbs[dbindex].RedisCommand(Command("hvals")(keys[keyindex]));
				vector<string> values = ReplyToVec(rep);

				client.hmset(keys[keyindex], fields, values);
			}
//			string type= oldclient.type(keys[keyindex]);
//
//			if (type == "string")
//			{
//				string value = oldclient.get(keys[keyindex]);
//				newclient.set(keys[keyindex], value);
//			}
//			else if (type == "list")
//			{
//				vector<string> values = oldclient.lrange(keys[keyindex], 0, -1);
//				newclient.lpush(keys[keyindex], values);
//			}
//			else if (type == "set")
//			{
//				vector<string> members = oldclient.smembers(keys[keyindex]);
//				newclient.sadd(keys[keyindex], members);
//			}
//			else if (type == "zset")
//			{
//				vector<string> members = oldclient.zrange(keys[keyindex], 0, -1);
//				for(size_t memindex=0; memindex<members.size(); memindex++)
//				{
//					int score = oldclient.zscore(keys[keyindex], members[memindex]);
//					newclient.zadd(keys[keyindex], score, members[memindex]);
//				}
//			}
//			else if (type == "hash")
//			{
//				vector<string> fields = oldclient.hkeys(keys[keyindex]);
//				vector<string> values = oldclient.hvals(keys[keyindex]);
//
//				newclient.hmset(keys[keyindex], fields, values);
//			}
		}
	}

	return 0;
}
