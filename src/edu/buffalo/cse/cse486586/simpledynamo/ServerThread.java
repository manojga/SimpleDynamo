package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.ObjectInputStream;
import java.net.ServerSocket;

import android.content.ContentResolver;
import android.content.ContentValues;
import android.database.Cursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.util.Log;

public class ServerThread implements Runnable {

	private static final String KEY_FIELD = "key";
	private static final String VALUE_FIELD = "value";

	ContentResolver mContentResolver = SimpleDynamoProvider.context.getContentResolver();
	Uri uri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");

	ServerSocket serverSocket;
	public ServerThread(ServerSocket sockets){
		this.serverSocket=sockets;
	}
	@Override
	public void run() {		
		while (true) {
			try {
				// keep receiving input socket messages
				ObjectInputStream ois = new ObjectInputStream(serverSocket.accept().getInputStream());
				Message rcvdMsg= (Message)ois.readObject();

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
				else if(rcvdMsg.messageType.equals("insertrep")){
					SimpleDynamoProvider.doInsertRep.put(rcvdMsg.key, true);
					Log.e("insertrep", rcvdMsg.key+" "+rcvdMsg.value);
					ContentValues cv=new ContentValues();
					cv.put(KEY_FIELD,rcvdMsg.key);
					cv.put(VALUE_FIELD, rcvdMsg.value);
					mContentResolver.insert(uri, cv);	
					SimpleDynamoProvider.doInsertRep.put(rcvdMsg.key, false);
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

					if(rcvdMsg.msgFrom.equals(NodeInfo.id))
					{
						SimpleDynamoProvider.queryRes=rcvdMsg.key_value;
						SimpleDynamoProvider.waitForQueryRes=false;
					}
					else{
						rcvdMsg.msgTo=String.valueOf((Integer.parseInt(NodeInfo.succ) * 2));
						//ClientThread ct= new ClientThread(rcvdMsg);
						//ct.run();
						new Thread(new ClientThread(rcvdMsg)).start();
					}

				}

				//for simple query msg type
				else if(rcvdMsg.messageType.equals("query"))
				{
					{
						Cursor cr = mContentResolver.query(uri, null,rcvdMsg.key, null, null);
						if (cr != null )
						{
							cr.moveToFirst();
							//SimpleDynamoProvider.resVl= cr.getString(1);
							rcvdMsg.value = cr.getString(cr.getColumnIndex("value"));
						}
						else{ 
							rcvdMsg.value = null;
							//SimpleDynamoProvider.resVl= null;
						}
						rcvdMsg.messageType="queryResponse";
						rcvdMsg.msgTo=String.valueOf((Integer.parseInt(rcvdMsg.msgFrom) * 2));
						//new ClientActivity().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, rcvdMsg);
						//ClientThread ct= new ClientThread(rcvdMsg);
						//ct.run();
						new Thread(new ClientThread(rcvdMsg)).start();
						cr.close();
					}
				}

				//for query response msg type
				else if(rcvdMsg.messageType.equals("queryResponse"))
				{

					SimpleDynamoProvider.waitForQueryResponse.put(rcvdMsg.key, false);
					SimpleDynamoProvider.waitForQueryVal.put(rcvdMsg.key, rcvdMsg.value);

				}

				//to delete a key-value pair
				else if(rcvdMsg.messageType.equals("delete")||rcvdMsg.messageType.equals("deleterep")){

					if(rcvdMsg.messageType.equals("deleterep"))
						SimpleDynamoProvider.doDeleteRep=true;
					int cr = mContentResolver.delete(uri, rcvdMsg.key ,null);

					SimpleDynamoProvider.doDeleteRep=false;
				}
				
				//to delete all key value pairs
				else if(rcvdMsg.messageType.equals("deleteall")){

					if(!rcvdMsg.msgFrom.equals(NodeInfo.id)){
						int cr = mContentResolver.delete(uri, "@",null);
						rcvdMsg.msgTo=String.valueOf((Integer.parseInt(NodeInfo.succ) * 2));
						//ClientThread ct= new ClientThread(rcvdMsg);
						//ct.run();
						new Thread(new ClientThread(rcvdMsg)).start();
					}
				}

			}catch(Exception e){
				e.printStackTrace();
			}
		}
	}

	private Uri buildUri(String scheme, String authority) {
		//URI builder
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}
}
