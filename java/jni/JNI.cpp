#include "JNI.h"
#include <string>
#include <string.h>
#include <iostream>
#include <map>

jstring JNI::StrToJStr(JNIEnv* env, string& str)
{
	jstring jstr = env->NewStringUTF(str.c_str());

	return jstr;
}

string JNI::JStrToStr(JNIEnv* env, jstring& jstr)
{
	const char* cstr = env->GetStringUTFChars(jstr, NULL);

	std::string str(cstr);

	env->ReleaseStringUTFChars(jstr, cstr);

	return str;
}

vector<string> JNI::JArrayToVec(JNIEnv* env, jobjectArray& jarray)
{
	std::vector<std::string> strs;

	int size=env->GetArrayLength(jarray);

	for (int i=0; i<size; i++)
	{

		jstring jstr = (jstring)env->GetObjectArrayElement(jarray, i);

		string str = JStrToStr(env, jstr);

		strs.push_back(str);
	}

	return strs;
}

jobject JNI::VectorToJList(JNIEnv* env, std::vector<std::string>& values)
{
	jclass class_ArrayList = env->FindClass("java/util/ArrayList");
	jmethodID construct = env->GetMethodID(class_ArrayList, "<init>", "()V");
	jobject obj_List = env->NewObject(class_ArrayList, construct);
	jmethodID list_add = env->GetMethodID(class_ArrayList, "add","(Ljava/lang/Object;)Z");

	for (int i = 0; i < values.size(); i++) {

		jclass strClass = env->FindClass("Ljava/lang/String;");
		jmethodID construction_id = env->GetMethodID(strClass, "<init>", "([BLjava/lang/String;)V");
		jbyteArray bytes = env->NewByteArray(strlen(values[i].c_str()));
		env->SetByteArrayRegion(bytes, 0, strlen(values[i].c_str()), (jbyte*)(values[i].c_str()));
		jstring encoding = env->NewStringUTF("utf-8");
		jobject value = env->NewObject(strClass, construction_id, bytes, encoding);

		env->CallObjectMethod(obj_List, list_add, value);

		env->DeleteLocalRef(value);
		env->DeleteLocalRef(encoding);
		env->DeleteLocalRef(bytes);
		env->DeleteLocalRef(strClass);
	}

	env->DeleteLocalRef(class_ArrayList);

	return obj_List;
}

jobject JNI::VectorToJListByte(JNIEnv* env, std::vector<std::string>& values)
{
	jclass class_ArrayList = env->FindClass("java/util/ArrayList");
	jmethodID construct = env->GetMethodID(class_ArrayList, "<init>", "()V");
	jobject obj_List = env->NewObject(class_ArrayList, construct);
	jmethodID list_add = env->GetMethodID(class_ArrayList, "add","(Ljava/lang/Object;)Z");

	for (int i = 0; i < values.size(); i++) {

		jbyteArray jarrRV =env->NewByteArray(values[i].size());
		jbyte *jby =env->GetByteArrayElements(jarrRV, 0);
		memcpy(jby, values[i].c_str(), values[i].size());

		env->SetByteArrayRegion(jarrRV, 0, values[i].size(), jby);

		env->CallObjectMethod(obj_List, list_add, jarrRV);

	}


	return obj_List;
}

jobject JNI::MapToHashMap(JNIEnv* env, map<std::string, std::string> &kvMap)
{
	jclass class_Hashtable=env->FindClass("java/util/Hashtable");
	jmethodID construct_method=env->GetMethodID( class_Hashtable, "<init>","()V");
	jobject obj_Map =env->NewObject( class_Hashtable, construct_method, "");

	jmethodID add_method= env->GetMethodID(class_Hashtable,"put","(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;");

	map<std::string, std::string>::iterator mapIter = kvMap.begin();
	for (; mapIter != kvMap.end(); mapIter++)
	{
		jstring key = env->NewStringUTF((char*)(mapIter->first).c_str());
		jstring value = env->NewStringUTF((char*)(mapIter->second).c_str());
		env->CallObjectMethod(obj_Map, add_method, key, value);

		env->DeleteLocalRef(key);
		env->DeleteLocalRef(value);
	}

	env->DeleteLocalRef(class_Hashtable);

	return obj_Map;
}
