package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;

import android.R.string;

public class Message implements Serializable{						

	//message packet
	String id;														//	msg Id
	String pred;													// predecessor node
	String succ;													// successor node
	String idHash;													// Id-Hash value

	String key;														// actual key
	String value;													// actual value
	HashMap<String,String> key_value= new HashMap<String, String>();
	String messageType;												// msg type

	String msgFrom;													// msg source	
	String keyHash;													// hash value of the key
	String msgTo;													// msg destination

	//getter and setter methods
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getPred() {
		return pred;
	}
	public void setPred(String pred) {
		this.pred = pred;
	}
	public String getSucc() {
		return succ;
	}
	public void setSucc(String succ) {
		this.succ = succ;
	}
	public String getIdHash() {
		return idHash;
	}
	public void setIdHash(String idHash) {
		this.idHash = idHash;
	}
	public String getMessageType() {
		return messageType;
	}
	public void setMessageType(String messageType) {
		this.messageType = messageType;
	}

	public String getMsgTo() {
		return msgTo;
	}
	public void setMsgTo(String msgTo) {
		this.msgTo = msgTo;
	}

	public String getMsgType() {
		return messageType;
	}
	public void setMsgType(String queryType) {
		this.messageType = queryType;
	}

	public String getMsgFrom() {
		return msgFrom;
	}
	public void setMsgFrom(String msgFrom) {
		this.msgFrom = msgFrom;
	}
	public String getKeyHash() {
		return keyHash;
	}
	public void setKeyHash(String keyHash) {
		this.keyHash = keyHash;
	}


}
