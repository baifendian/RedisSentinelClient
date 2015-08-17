#include "RedisClient.h"

using namespace bfd::redis;

std::string ReplyToString(const Reply& rep)
{
	stringstream retstream;

	vector<Reply> reps;
	if (rep.elements().size()>0)
	{
		reps = rep.elements();
	}

	switch (rep.type()) {
		case Reply::ERROR:
			retstream << rep.str();
			break;
		case Reply::STRING:
			retstream << rep.str();
			break;
		case Reply::STATUS:
			retstream << rep.str();
			break;
		case Reply::INTEGER:
		  	retstream << rep.integer();
		  	break;
		case Reply::NIL:
			retstream << "nil";
			break;
		case Reply::ARRAY:
			retstream << "*";
			retstream << rep.elements().size();
			retstream << endl;


			for (size_t idx=0; idx<reps.size(); ++idx) {
		    	switch (reps[idx].type()) {
					case Reply::ERROR:
						retstream << reps[idx].str();
						retstream << endl;
						break;
					case Reply::STRING:
						retstream << reps[idx].str();
						retstream << endl;
						break;
					case Reply::STATUS:
						retstream << reps[idx].str();
						retstream << endl;
						break;
					case Reply::INTEGER:
					  	retstream << reps[idx].integer();
					  	retstream << endl;
					  	break;
					case Reply::NIL:
						retstream << "nil";
						break;
					default:
						retstream << "+OK";
					    break;
		    	}
			}
		    break;
		default:
			retstream << "OK";
			break;
	}

	return retstream.str();
}

int main(int argc, char* argv[])
{
	if (argc < 3)
	{
		cout << "Example:" << endl;
		cout << "        ./RedisCommandTool sentinels command parameters" << endl;
		exit(0);
	}

	string sentinels = string(argv[1]);
	RedisClient client(sentinels);

	Command comm(argv[2]);

	for (int i=3; i<argc; i++)
	{
		comm(argv[i]);
	}

	Reply rep = client.RedisCommand(comm);

	string ret = ReplyToString(rep);

	cout << ret << endl;

	return 0;
}
