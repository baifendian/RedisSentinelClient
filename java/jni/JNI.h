/**
 * @brief An easy wrapper of JNI functions.
 *
 * Modification history
 * version          author       date           description
 * 0.1              weimeng      2015.03.20     initial version
 *
 *
 * @author weimeng
 */
#ifndef BFD_JNI_H
#define BFD_JNI_H
#include <jni.h>
#include <string>
#include <vector>
#include <map>

using namespace std;

class JNI {
public:

	static vector<string> JArrayToVec(JNIEnv* env, jobjectArray& jkeys);

	static jstring StrToJStr(JNIEnv* env, string& str);

	static string JStrToStr(JNIEnv* env, jstring& jstr);

	/*
	 * Converts std::vector<std::string> to a java ArrayList Object
	 */
	static jobject VectorToJList(JNIEnv* env, std::vector<std::string>& values);
	/*
	 * Converts std::vector<std::string> to a java ArrayList<byte[]> Object
	 */
	static jobject VectorToJListByte(JNIEnv* env, std::vector<std::string>& values);
	/**
	 * Converts boost::unordered_map<string, string> to a java ArrayList Object
	 */
	static jobject MapToHashMap(JNIEnv* env, map<std::string, std::string> &kvMap);
};

#endif
