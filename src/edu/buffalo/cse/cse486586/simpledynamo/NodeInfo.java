package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.Serializable;

public class NodeInfo {

	//Node Information

	static String id;					// node Id
	static String pred;					// predecessor node
	static String succ;					// successor node
	static String succ2;				// successor2 node
	static String pred2;				// predecessor2 node
	static String idHash;				// Id-hash value
	static String portNum;				// port number

	//getters and setters 
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

}
