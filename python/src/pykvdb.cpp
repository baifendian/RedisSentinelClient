#include <Python.h>
#include <string>
#include <cstring>
#include "RedisClient.h"

using namespace bfd::redis;
static bfd::redis::RedisClient * redis = NULL;

static char default_sentinel_addr[] = "";
static char default_business[] = "";
static char default_password[] = "";

struct release_py_GIL
{
	PyThreadState *state;
	release_py_GIL()
	{
		state = PyEval_SaveThread();
	}
	~release_py_GIL()
	{
		PyEval_RestoreThread(state);
	}
};

static void delRedisClient(PyObject * ptr)
{
	bfd::redis::RedisClient * redis =
			static_cast<bfd::redis::RedisClient *> (PyCapsule_GetPointer(ptr,
					NULL));
	delete redis;
	return;
}

static PyObject* newRedisClient(PyObject* self, PyObject* args)
{
	char * sentinel_addr = default_sentinel_addr;
	int sentinel_addr_sz = strlen(sentinel_addr);
	char * business = default_business;
	int business_sz = strlen(business);
	char * password = default_password;
	int password_sz = strlen(password);
	if (!PyArg_ParseTuple(args, "|s#s#s#", &sentinel_addr, &sentinel_addr_sz,
			&business, &business_sz, &password, &password_sz))
	{
		Py_RETURN_FALSE;
	}
	{
		release_py_GIL unlocker;
		redis = new bfd::redis::RedisClient(string(sentinel_addr,
				sentinel_addr_sz), string(business, business_sz), string(password, password_sz));
	}
	return PyCapsule_New(redis, NULL, delRedisClient);
}

static PyObject* hkeys(PyObject* self, PyObject* args)
{
	PyObject* tmp;
	char* key = NULL;
	int key_sz = 0;
	if (!PyArg_ParseTuple(args, "Os#", &tmp, &key, &key_sz))
	{
		Py_RETURN_FALSE;
	}
	bfd::redis::RedisClient* redis =
			static_cast<bfd::redis::RedisClient *> (PyCapsule_GetPointer(tmp,
					NULL));
	std::vector<std::string> values;
	Reply reply;
	{
		release_py_GIL unlock;
		reply = redis->RedisCommand(Command("HKEYS")(string(key, key_sz)));
		if (reply.error())
		{
			Py_RETURN_FALSE;
		}
	}
	PyObject* result = PyList_New(0);
	for (int i = 0; i < reply.elements().size(); ++i)
	{
		PyObject *obj_tmp = PyString_FromStringAndSize(
				reply.elements()[i].str().c_str(),
				reply.elements()[i].str().size());
		PyList_Append(result, obj_tmp);
		Py_DECREF(obj_tmp);
	}
	return result;

}
static PyObject* hget(PyObject* self, PyObject* args)
{
	PyObject* tmp;
	char* key = NULL;
	char* field = NULL;
	int key_sz = 0;
	int field_sz = 0;
	if (!PyArg_ParseTuple(args, "Os#s#", &tmp, &key, &key_sz, &field, &field_sz))
	{
		Py_RETURN_FALSE;
	}
	bfd::redis::RedisClient * redis =
			static_cast<bfd::redis::RedisClient *> (PyCapsule_GetPointer(tmp,
					NULL));
	string value;
	{
		release_py_GIL unlocker;
		Reply reply = redis->RedisCommand(Command("HGET")(string(key, key_sz))(
				string(field, field_sz)));
		value = reply.str();
	}
	return PyString_FromStringAndSize(value.c_str(), value.size());

}
static PyObject* hset(PyObject* self, PyObject* args)
{
	PyObject* tmp;
	char* key = NULL;
	char* field = NULL;
	char* val = NULL;
	int key_sz = 0;
	int field_sz = 0;
	int val_sz = 0;
	int expired = 0;
	if (!PyArg_ParseTuple(args, "Os#s#s#i", &tmp, &key, &key_sz, &field,
			&field_sz, &val, &val_sz, &expired))
	{
		Py_RETURN_FALSE;
	}
	bfd::redis::RedisClient * redis =
			static_cast<bfd::redis::RedisClient *> (PyCapsule_GetPointer(tmp,
					NULL));
	{
		release_py_GIL unlocker;

		bool ret = redis->hset(string(key,key_sz), string(field,field_sz), string(val,val_sz));
		if (!ret)
		{
			return PyBool_FromLong(0);;
		}
		ret = redis->expire(string(key,key_sz), expired);
		if (!ret)
		{
			return PyBool_FromLong(0);;
		}
	}
	return PyBool_FromLong(1);
}
static PyObject* sadd(PyObject* self, PyObject* args)
{
	PyObject * tmp;
	int key_sz = 0;
	char* key = NULL;
	char* val = NULL;
	int val_sz = 0;
	if (!PyArg_ParseTuple(args, "Os#s#", &tmp, &key, &key_sz, &val, &val_sz))
	{
		Py_RETURN_FALSE;
	}
	bfd::redis::RedisClient * redis =
			static_cast<bfd::redis::RedisClient *> (PyCapsule_GetPointer(tmp,
					NULL));
	{
		release_py_GIL unlocker;
		Reply reply = redis->RedisCommand(Command("SADD")(string(key, key_sz))(
				string(val, val_sz)));
		if (reply.error() || reply.integer() == 0)
		{
			return PyBool_FromLong(0);
		}
	}
	return PyBool_FromLong(1);

}
static PyObject* zadd(PyObject* self, PyObject* args)
{
	PyObject * tmp;
	char * key = NULL;
	int key_sz = 0;
	int score = 1;
	char * mem = NULL;
	int mem_sz = 0;
	if (!PyArg_ParseTuple(args, "Os#is#", &tmp, &key, &key_sz, &score, &mem,
			&mem_sz))
	{
		Py_RETURN_FALSE;
	}
	bfd::redis::RedisClient * redis =
			static_cast<bfd::redis::RedisClient *> (PyCapsule_GetPointer(tmp,
					NULL));
	bool sz = 0;
	{
		release_py_GIL unlocker;
//		Reply reply = redis->RedisCommand(Command("ZADD")(string(key, key_sz))(
//				score)(string(mem, mem_sz)));
		bool ret = redis->zadd(string(key, key_sz), score, string(mem, mem_sz));
		if (ret)
		{
			return PyLong_FromLong(1);
		}
	}
	return PyLong_FromLong(0);
}
static PyObject* zincrby(PyObject* self, PyObject* args)
{
	PyObject * tmp;
	char * key = NULL;
	int key_sz = 0;
	int score = 1;
	char * mem = NULL;
	int mem_sz = 0;
	if (!PyArg_ParseTuple(args, "Os#is#", &tmp, &key, &key_sz, &score, &mem,
			&mem_sz))
	{
		Py_RETURN_FALSE;
	}
	bfd::redis::RedisClient * redis =
			static_cast<bfd::redis::RedisClient *> (PyCapsule_GetPointer(tmp,
					NULL));
	{
		release_py_GIL unlocker;

		int ret = redis->zincrby(string(key, key_sz), score, string(mem, mem_sz));

		string str = int2string(ret);

		return PyString_FromStringAndSize(str.c_str(),
				str.size());
	}
	return PyBool_FromLong(0);
}

static PyObject* zrange(PyObject* self, PyObject* args)
{
	PyObject *tmp;
	char * key = NULL;
	int key_sz = 0;
	int start = 0;
	int end = -1;
	if (!PyArg_ParseTuple(args, "Os#|ii", &tmp, &key, &key_sz, &start, &end))
	{
		Py_RETURN_FALSE;
	}
	bfd::redis::RedisClient * redis =
			static_cast<bfd::redis::RedisClient *> (PyCapsule_GetPointer(tmp,
					NULL));
	PyObject* result = PyList_New(0);
	std::vector<std::string> values;
	{
		release_py_GIL unlocker;
		values = redis->zrange(string(key, key_sz), start, end);
	}
	for (int i = 0; i < values.size(); ++i)
	{
		PyObject *obj_tmp = PyString_FromStringAndSize(values[i].c_str(),
				values[i].size());
		PyList_Append(result, obj_tmp);
		Py_DECREF(obj_tmp);
	}
	return result;
}

static PyObject* set(PyObject* self, PyObject* args)
{
	PyObject * tmp;
	char * key = NULL;
	int key_sz = 0;
	char * val = NULL;
	int val_sz = 0;
	int expired_time = 0;
	if (!PyArg_ParseTuple(args, "Os#s#", &tmp, &key, &key_sz, &val, &val_sz))
	{
		Py_RETURN_FALSE;
	}
	bfd::redis::RedisClient * redis =
			static_cast<bfd::redis::RedisClient *> (PyCapsule_GetPointer(tmp,
					NULL));
	bool b;
	{
		release_py_GIL unlocker;
		b = redis->set(string(key, key_sz), string(val, val_sz));
	}
	return PyBool_FromLong(b);
}

static PyObject* setex(PyObject* self, PyObject* args)
{
	PyObject * tmp;
	char * key = NULL;
	int key_sz = 0;
	char * val = NULL;
	int val_sz = 0;
	int expired_time = 0;
	if (!PyArg_ParseTuple(args, "Os#s#i", &tmp, &key, &key_sz, &val, &val_sz, &expired_time))
	{
		Py_RETURN_FALSE;
	}
	bfd::redis::RedisClient * redis =
			static_cast<bfd::redis::RedisClient *> (PyCapsule_GetPointer(tmp,
					NULL));
	bool b;
	{
		release_py_GIL unlocker;
		b = redis->setex(string(key, key_sz), string(val, val_sz), expired_time);
	}
	return PyBool_FromLong(b);
}

static PyObject* get(PyObject* self, PyObject* args)
{
	PyObject * tmp;
	char * key = NULL;
	int key_sz = 0;
	if (!PyArg_ParseTuple(args, "Os#", &tmp, &key, &key_sz))
	{
		return NULL;
	}
	bfd::redis::RedisClient * redis =
			static_cast<bfd::redis::RedisClient *> (PyCapsule_GetPointer(tmp,
					NULL));
	string value;
	{
		release_py_GIL unlocker;
		value = redis->get(string(key, key_sz));
	}
	return PyString_FromStringAndSize(value.c_str(), value.size());
}

static PyObject* mget(PyObject* self, PyObject* args)
{
	PyObject * tmp;
	PyObject * pkeys;
	if (!PyArg_ParseTuple(args, "OO", &tmp, &pkeys))
	{
		return NULL;
	}

	int size = PyList_Size(pkeys);

	vector<string> keys;

	for (int i=0; i<size; ++i)
	{
		PyObject* pkey = PyList_GetItem(pkeys, i);

		char *ckey = NULL;
		Py_ssize_t pkeysize = 0;
		int ret = PyString_AsStringAndSize(pkey, &ckey, &pkeysize);
		if (ret < 0)
		{
			return NULL;
		}

		int keysize = pkeysize;

		string key(ckey, keysize);
		keys.push_back(key);
	}

	vector<string> values;
	{
		release_py_GIL unlocker;
		values = redis->mget(keys);
	}

	PyObject* result = PyList_New(0);
	for (size_t i=0; i<values.size(); ++i)
	{
		PyObject *obj_tmp = PyString_FromStringAndSize(
				values[i].c_str(), values[i].size());
		PyList_Append(result, obj_tmp);
		Py_DECREF(obj_tmp);
	}
	return result;
}

static PyObject* mget2(PyObject* self, PyObject* args)
{
	PyObject * tmp;
	PyObject * pkeys;
	if (!PyArg_ParseTuple(args, "OO", &tmp, &pkeys))
	{
		return NULL;
	}

	int size = PyList_Size(pkeys);

	vector<string> keys;

	for (int i=0; i<size; ++i)
	{
		PyObject* pkey = PyList_GetItem(pkeys, i);

		char *ckey = NULL;
		Py_ssize_t pkeysize = 0;
		int ret = PyString_AsStringAndSize(pkey, &ckey, &pkeysize);
		if (ret < 0)
		{
			return NULL;
		}

		int keysize = pkeysize;

		string key(ckey, keysize);
		keys.push_back(key);
	}

	vector<string> values;

	{
		release_py_GIL unlocker;
		values = redis->mget2(keys);
	}

	PyObject* result = PyList_New(0);
	for (size_t i=0; i<values.size(); ++i)
	{
		PyObject *obj_tmp = PyString_FromStringAndSize(
				values[i].c_str(), values[i].size());
		PyList_Append(result, obj_tmp);
		Py_DECREF(obj_tmp);
	}
	return result;
}
static PyObject* lpush(PyObject* self, PyObject* args)
{
	PyObject * tmp;
	char * key = NULL;
	int key_sz = 0;
	char * val = NULL;
	int val_sz = 0;
	int len = -1;
	int expired = -1;
	if (!PyArg_ParseTuple(args, "Os#s#|ii", &tmp, &key, &key_sz, &val, &val_sz,
			&len, &expired))
	{
		/**
		 if (!PyArg_ParseTuple(args, "Os#s#|ii", &tmp, &key, &key_sz, &val, &val_sz, &len, &expired) &&
		 !PyArg_ParseTuple(args, "Os#s#i", &tmp, &key, &key_sz, &val, &val_sz, &len) &&
		 !PyArg_ParseTuple(args, "Os#s#", &tmp, &key, &key_sz, &val, &val_sz)) {
		 **/
		Py_RETURN_FALSE;
	}
	bfd::redis::RedisClient * redis =
			static_cast<bfd::redis::RedisClient *> (PyCapsule_GetPointer(tmp,
					NULL));
	bool b;
	if (len != -1 && expired != -1)
	{
		release_py_GIL unlocker;
//		b
//				= redis->lpush(string(key, key_sz), string(val, val_sz), len,
//						expired);
		b = redis->lpush(string(key, key_sz), string(val, val_sz));
		if (!b) return PyBool_FromLong(b);

		b = redis->ltrim(string(key, key_sz), 0, len);
		if (!b) return PyBool_FromLong(b);

		b = redis->expire(string(key, key_sz), expired);
		if (!b) return PyBool_FromLong(b);
	}
	else if (len != -1 && expired == -1)
	{
		release_py_GIL unlocker;
//		b = redis->lpush(string(key, key_sz), string(val, val_sz), len);

		b = redis->lpush(string(key, key_sz), string(val, val_sz));
		if (!b) return PyBool_FromLong(b);

		b = redis->ltrim(string(key, key_sz), 0, len);
		if (!b) return PyBool_FromLong(b);
	}
	else if (len == -1 && expired == -1)
	{
		release_py_GIL unlocker;
		b = redis->lpush(string(key, key_sz), string(val, val_sz));
	}
	return PyBool_FromLong(b);
}
static PyObject* smembers(PyObject* self, PyObject* args)
{
	PyObject *tmp;
	char* key = NULL;
	int key_sz = 0;
	if (!PyArg_ParseTuple(args, "Os#", &tmp, &key, &key_sz))
	{
		Py_RETURN_FALSE;
	}
	bfd::redis::RedisClient* redis =
			static_cast<bfd::redis::RedisClient *> (PyCapsule_GetPointer(tmp,
					NULL));
	std::vector<std::string> values;
	Reply reply;
	{
		release_py_GIL unlock;
		reply = redis->RedisCommand(Command("SMEMBERS")(string(key, key_sz)));
		if (reply.error())
		{
			Py_RETURN_FALSE;
		}
	}
	PyObject* result = PyList_New(0);
	for (int i = 0; i < reply.elements().size(); ++i)
	{
		PyObject *obj_tmp = PyString_FromStringAndSize(
				reply.elements()[i].str().c_str(),
				reply.elements()[i].str().size());
		PyList_Append(result, obj_tmp);
		Py_DECREF(obj_tmp);
	}
	return result;
}
static PyObject* lrange(PyObject* self, PyObject* args)
{
	PyObject *tmp;
	char * key = NULL;
	int key_sz = 0;
	int start = 0;
	int end = -1;
	if (!PyArg_ParseTuple(args, "Os#|ii", &tmp, &key, &key_sz, &start, &end))
	{
		Py_RETURN_FALSE;
	}
	bfd::redis::RedisClient * redis =
			static_cast<bfd::redis::RedisClient *> (PyCapsule_GetPointer(tmp,
					NULL));
	PyObject* result = PyList_New(0);
	std::vector<std::string> values;
	{
		release_py_GIL unlocker;
		values = redis->lrange(string(key, key_sz), start, end);
	}
	for (int i = 0; i < values.size(); ++i)
	{
		PyObject *obj_tmp = PyString_FromStringAndSize(values[i].c_str(),
				values[i].size());
		PyList_Append(result, obj_tmp);
		Py_DECREF(obj_tmp);
	}
	return result;
}
static PyObject* del(PyObject* self, PyObject* args)
{
	PyObject * tmp;
	char * key = NULL;
	int key_sz = 0;
	if (!PyArg_ParseTuple(args, "Os#", &tmp, &key, &key_sz))
	{
		Py_RETURN_FALSE;
	}
	bfd::redis::RedisClient * redis =
			static_cast<bfd::redis::RedisClient *> (PyCapsule_GetPointer(tmp,
					NULL));
	bool b;
	{
		release_py_GIL unlocker;
		b = redis->del(string(key, key_sz));
	}
	return PyBool_FromLong(b);
}

static PyMethodDef PyBfdRedis_methods[] =
{
{ "newClient", (PyCFunction) ::newRedisClient, METH_VARARGS, NULL },
{ "delClient", (PyCFunction) ::delRedisClient, METH_VARARGS, NULL },
{ "set", (PyCFunction) ::set, METH_VARARGS, NULL },
{ "setex", (PyCFunction) ::setex, METH_VARARGS, NULL },
{ "get", (PyCFunction) ::get, METH_VARARGS, NULL },
{ "mget", (PyCFunction) ::mget, METH_VARARGS, NULL },
{ "mget2", (PyCFunction) ::mget2, METH_VARARGS, NULL },
{ "delete", (PyCFunction) ::del, METH_VARARGS, NULL },
{ "lpush", (PyCFunction) ::lpush, METH_VARARGS, NULL },
{ "lrange", (PyCFunction) ::lrange, METH_VARARGS, NULL },
{ "hget", (PyCFunction) ::hget, METH_VARARGS, NULL },
{ "hset", (PyCFunction) ::hset, METH_VARARGS, NULL },
{ "smembers", (PyCFunction) ::smembers, METH_VARARGS, NULL },
{ "hkeys", (PyCFunction) ::hkeys, METH_VARARGS, NULL },
{ "zadd", (PyCFunction) ::zadd, METH_VARARGS, NULL },
{ "zincrby", (PyCFunction) ::zincrby, METH_VARARGS, NULL },
{ "zrange", (PyCFunction) ::zrange, METH_VARARGS, NULL },
{ "sadd", (PyCFunction) ::sadd, METH_VARARGS, NULL },

{ NULL, NULL, 0, NULL } };

PyMODINIT_FUNC initpykvdb()
{
	Py_InitModule("pykvdb", PyBfdRedis_methods);
}
