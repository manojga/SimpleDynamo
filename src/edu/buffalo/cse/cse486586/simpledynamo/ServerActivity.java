package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map.Entry;

import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.util.Log;

public class ServerActivity extends AsyncTask<ServerSocket, String, Void>{


	private static final String KEY_FIELD = "key";
	private static final String VALUE_FIELD = "value";

	ContentResolver mContentResolver = SimpleDynamoProvider.context.getContentResolver();
	Uri uri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");

	@Override
	protected Void doInBackground(ServerSocket... sockets) {
		ServerSocket serverSocket = sockets[0];
		// keep receiving input socket messages
		while (true) {
			try {

				ObjectInputStream ois = new ObjectInputStream(serverSocket.accept().getInputStream());
				Message rcvdMsg= (Message)ois.readObject();
				{
					//for insert msg type
					if(rcvdMsg.messageType.equals("insert")){
						SimpleDynamoProvider.doInsert.put(rcvdMsg.key, true);
						ContentValues cv=new ContentValues();
						cv.put(KEY_FIELD,rcvdMsg.key);
						cv.put(VALUE_FIELD, rcvdMsg.value);
						mContentResolver.insert(uri, cv);	
						SimpleDynamoProvider.doInsert.put(rcvdMsg.key, false);
					}

					//for insert replication msg type
					if(rcvdMsg.messageType.equals("insertrep")){
						SimpleDynamoProvider.doInsertRep.put(rcvdMsg.key, true);
						//Log.e("insertrep", rcvdMsg.key+" "+rcvdMsg.value);
						ContentValues cv=new ContentValues();
						cv.put(KEY_FIELD,rcvdMsg.key);
						cv.put(VALUE_FIELD, rcvdMsg.value);
						mContentResolver.insert(uri, cv);	
						SimpleDynamoProvider.doInsertRep.remove(rcvdMsg.key);
					}

					// query to all node msg type
					else if(rcvdMsg.messageType.equals("queryall")){

						Cursor cr = mContentResolver.query(uri, null,"@", null, null);
						if (cr != null)
						{
							cr.moveToFirst();
							while(cr.isAfterLast() == false){
								String returnKey = cr.getString(0);
								String returnValue = cr.getString(1);
								rcvdMsg.key_value.put(returnKey, returnValue);
								cr.moveToNext();
							}
						}
						cr.close();
						rcvdMsg.messageType="queryallresp";
						rcvdMsg.msgTo=String.valueOf((Integer.parseInt(rcvdMsg.msgFrom) * 2));
						rcvdMsg.msgFrom=NodeInfo.id;
						ClientThread ct= new ClientThread(rcvdMsg);
						ct.run();
					}

					// for query all response msg type
					else if(rcvdMsg.messageType.equals("queryallresp")){

						HashMap<String, String> data = new HashMap<String, String>(rcvdMsg.key_value);
						for (Entry<String, String> entry : data.entrySet()) {
							String ky = entry.getKey();
							String val = entry.getValue();
							SimpleDynamoProvider.queryRes.put(ky, val);
						}
						SimpleDynamoProvider.queryAllCount.put(rcvdMsg.msgFrom,true);

					}

					//for simple query msg type
					else if(rcvdMsg.messageType.equals("query"))
					{
						//if(SimpleDynamoProvider.isMyKey(rcvdMsg.key))
						{
							Cursor cr = mContentResolver.query(uri, null,rcvdMsg.key, null, null);
							if (cr != null && cr.getCount()>0)
							{
								cr.moveToFirst();
								//SimpleDynamoProvider.resVl= cr.getString(1);
								rcvdMsg.value = cr.getString(1);
								rcvdMsg.messageType="queryResponse";
								rcvdMsg.msgTo=String.valueOf((Integer.parseInt(rcvdMsg.msgFrom) * 2));
							}
							else{ 
								rcvdMsg.messageType="queryReplica";
								rcvdMsg.msgTo=String.valueOf((Integer.parseInt(rcvdMsg.succ) * 2)); 
								//rcvdMsg.value = null;
								//SimpleDynamoProvider.resVl= null;
							}

							//new ClientActivity().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, rcvdMsg);
							ClientThread ct= new ClientThread(rcvdMsg);
							ct.run();

							cr.close();
						}
					}

					// query replication msg type
					else if(rcvdMsg.messageType.equals("queryReplica")){

						SimpleDynamoProvider.queryReplica.put(rcvdMsg.key, true);
						Cursor cr = mContentResolver.query(uri, null,rcvdMsg.key, null, null);
						if (cr != null && cr.getCount()>0)
						{
							cr.moveToFirst();
							//SimpleDynamoProvider.resVl= cr.getString(1);
							rcvdMsg.value = cr.getString(1);
							rcvdMsg.messageType="queryResponse";
							rcvdMsg.msgTo=String.valueOf((Integer.parseInt(rcvdMsg.msgFrom) * 2));
						}
						else{ 
							//rcvdMsg.value = null;
							//SimpleDynamoProvider.resVl= null;
							rcvdMsg.messageType="queryReplica";
							rcvdMsg.msgTo=String.valueOf((Integer.parseInt(rcvdMsg.pred) * 2)); //succ2
						}

						//new ClientActivity().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, rcvdMsg);
						ClientThread ct= new ClientThread(rcvdMsg);
						ct.run();

						cr.close();
					}

					//for query response msg type
					else if(rcvdMsg.messageType.equals("queryResponse"))
					{
						//						SimpleDynamoProvider.resVl = rcvdMsg.value;
						SimpleDynamoProvider.waitForQueryResponse.put(rcvdMsg.key, false);
						SimpleDynamoProvider.waitForQueryVal.put(rcvdMsg.key, rcvdMsg.value);

					}

					//to delete single key
					else if(rcvdMsg.messageType.equals("delete")||rcvdMsg.messageType.equals("deleterep")){

						//if(SimpleDynamoProvider.isMyKey(rcvdMsg.key)){
						if(rcvdMsg.messageType.equals("deleterep"))
							SimpleDynamoProvider.doDeleteRep=true;
						int cr = mContentResolver.delete(uri, rcvdMsg.key ,null);

						SimpleDynamoProvider.doDeleteRep=false;
					}

					//to delete all keys
					else if(rcvdMsg.messageType.equals("deleteall")){

						if(!rcvdMsg.msgFrom.equals(NodeInfo.id)){
							int cr = mContentResolver.delete(uri, "@",null);
							rcvdMsg.msgTo=String.valueOf((Integer.parseInt(NodeInfo.succ) * 2));
							ClientThread ct= new ClientThread(rcvdMsg);
							ct.run();
						}
					}

					// to synchronize the key-values after failure
					else if(rcvdMsg.messageType.equals("sync")){
						Log.e("sync rcvd","sync req rcvd at srvr frm "+rcvdMsg.id);
						Cursor cr = mContentResolver.query(uri, null,"@", null, null);
						if (cr != null)
						{
							cr.moveToFirst();
							while(cr.isAfterLast() == false){
								String returnKey = cr.getString(0);
								String returnValue = cr.getString(1);
								rcvdMsg.key_value.put(returnKey, returnValue);
								cr.moveToNext();
							}
						}
						cr.close();
						rcvdMsg.messageType="syncresp";
						rcvdMsg.msgTo=String.valueOf((Integer.parseInt(rcvdMsg.id) * 2));
						rcvdMsg.msgFrom=NodeInfo.id;
						ClientThread ct= new ClientThread(rcvdMsg);
						ct.run();
						Log.e("sync resp","sync req sent frm srvr");
					}

					//for sync response meg type
					else if(rcvdMsg.messageType.equals("syncresp"))
					{
						Log.e("sync resp rcvd","sync resp rcvd at clnt frm "+ rcvdMsg.msgFrom);
						HashMap<String, String> data = new HashMap<String, String>(rcvdMsg.key_value);

						//SimpleDynamoProvider.syncRes.put(rcvdMsg.msgFrom, data);
						//SimpleDynamoProvider.waitForSyncRes.put(rcvdMsg.msgFrom, false);
						if(data.size()>0){

							ContentValues cv;
							int i=0;
							for (Entry<String, String> entry : data.entrySet()) {
								String ky = entry.getKey();
								String val = entry.getValue();

								String keyHash=null;
								try {
									keyHash = genHash(ky);
								} catch (NoSuchAlgorithmException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}
								//if(rcvdMsg.msgFrom.equals(NodeInfo.succ))
								{

									String nd=NodeInfo.id;
									String pr1= NodeInfo.pred;
									String pr2 = NodeInfo.pred2;
									if(isKeyInRange(keyHash, nd)||isKeyInRange(keyHash, pr1)||isKeyInRange(keyHash, pr2)){
										cv = new ContentValues();
										cv.put("key" , ky);
										cv.put("value" , val);	
										i++;
										SimpleDynamoProvider.doInsertSyn.put(ky, true);
										mContentResolver.insert(uri, cv);
										SimpleDynamoProvider.doInsertSyn.remove(ky);
									}
								}
							}
							//mContentResolver.bulkInsert(uri, cv);
							//SimpleDynamoProvider.doInsert.put(rcvdMsg.key, false);
							Log.e("sync updated", i+" from "+ rcvdMsg.msgFrom);
						}
					}
				}
			}catch(Exception e){
				//Log.e("server task", e.getMessage());
				e.printStackTrace();
			}
		}
	}

	private Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}


	public boolean isKeyInRange(String keyHash, String node){
		// check which node a key belongs to
		String start=null, end=null; //keyHash=null

		if(SimpleDynamoProvider.allNodesCopy.size()==3){

			if(node.equals("5554")){
				start="208f7f72b198dadd244e61801abe1ec3a4857bc9";
				end="33d6357cfaaf0f72991b0ecd8c56da066613c089";
				int end_key_comp= end.compareTo(keyHash);
				int start_key_comp = start.compareTo(keyHash);
				if(end_key_comp>=0 && start_key_comp<=0)
					return true;
				else 
					return false;
			}

			else if(node.equals("5556")){
				//start="0";
				end="208f7f72b198dadd244e61801abe1ec3a4857bc9";
				String start2= "abf0fd8db03e5ecb199a9b82929e9db79b909643";
				int end_key_comp= end.compareTo(keyHash);
				//int start_key_comp = start.compareTo(keyHash);
				int start2_key_comp= start2.compareTo(keyHash);

				if((end_key_comp>=0) || (start2_key_comp<=0))
					return true;
				else 
					return false;
			}

			else if(node.equals("5558")){
				start="33d6357cfaaf0f72991b0ecd8c56da066613c089";
				end="abf0fd8db03e5ecb199a9b82929e9db79b909643";
				int end_key_comp= end.compareTo(keyHash);
				int start_key_comp = start.compareTo(keyHash);
				if(end_key_comp>=0 && start_key_comp<=0)
					return true;
				else 
					return false;
			}
		}
		else if(SimpleDynamoProvider.allNodesCopy.size()==5){

			if(node.equals("5554")){
				start= SimpleDynamoProvider.allNodesCopy.get("5556").idHash;
				end= SimpleDynamoProvider.allNodesCopy.get("5554").idHash;
				int end_key_comp= end.compareTo(keyHash);
				int start_key_comp = start.compareTo(keyHash);
				if(end_key_comp>=0 && start_key_comp<=0)
					return true;
				else 
					return false;
			}

			else if(node.equals("5556")){
				start=SimpleDynamoProvider.allNodesCopy.get("5562").idHash;
				end=SimpleDynamoProvider.allNodesCopy.get("5556").idHash;
				int end_key_comp= end.compareTo(keyHash);
				int start_key_comp = start.compareTo(keyHash);

				if(end_key_comp>=0 && start_key_comp<=0)
					return true;
				else 
					return false;
			}

			else if(node.equals("5558")){
				start=SimpleDynamoProvider.allNodesCopy.get("5554").idHash;
				end=SimpleDynamoProvider.allNodesCopy.get("5558").idHash;
				int end_key_comp= end.compareTo(keyHash);
				int start_key_comp = start.compareTo(keyHash);
				if(end_key_comp>=0 && start_key_comp<=0)
					return true;
				else 
					return false;
			}

			else if(node.equals("5560")){
				start=SimpleDynamoProvider.allNodesCopy.get("5558").idHash;
				end=SimpleDynamoProvider.allNodesCopy.get("5560").idHash;
				int end_key_comp= end.compareTo(keyHash);
				int start_key_comp = start.compareTo(keyHash);
				if(end_key_comp>=0 && start_key_comp<=0)
					return true;
				else 
					return false;
			}

			else if(node.equals("5562")){
				//start="0";
				end=SimpleDynamoProvider.allNodesCopy.get("5562").idHash;
				String start2= SimpleDynamoProvider.allNodesCopy.get("5560").idHash;
				int end_key_comp= end.compareTo(keyHash);
				//int start_key_comp = start.compareTo(keyHash);
				int start2_key_comp= start2.compareTo(keyHash);

				if((end_key_comp>=0 ) || (start2_key_comp<=0))
					return true;
				else 
					return false;
			}

		}
		return false;

	}
	private String genHash(String input) throws NoSuchAlgorithmException {
		// generate SHA1 hash for a input key
		MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
		byte[] sha1Hash = sha1.digest(input.getBytes());
		Formatter formatter = new Formatter();
		for (byte b : sha1Hash) {
			formatter.format("%02x", b);
		}
		return formatter.toString();
	}
}
