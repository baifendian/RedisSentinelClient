package com.redis.sentinel.client;

import java.util.ArrayList;

public class Reply {


	boolean error_;
	public boolean isError_() {
		return error_;
	}
	public void setError_(boolean error_) {
		this.error_ = error_;
	}
	public type_t getType_() {
		return type_;
	}
	public void setType_(type_t type_) {
		this.type_ = type_;
	}
	public String getStr_() {
		return str_;
	}
	public void setStr_(String str_) {
		this.str_ = str_;
	}
	public long getInteger_() {
		return integer_;
	}
	public void setInteger_(long integer_) {
		this.integer_ = integer_;
	}
	public ArrayList<Reply> getElements_() {
		return elements_;
	}
	public void setElements_(ArrayList<Reply> elements_) {
		this.elements_ = elements_;
	}
	public void AddElements(Reply rep) {
		this.elements_.add(rep);
	}
	
	private  type_t type_;
	private  String str_;
	private  long integer_;
	private  ArrayList<Reply> elements_ = null;
	
}
