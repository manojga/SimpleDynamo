package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.StreamCorruptedException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import android.R.integer;
import android.content.ContentProvider;
import android.content.ContentUris;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {

	static final int SERVER_PORT = 10000;				// the port on which the server is listening 
	public static final String DATABASE_NAME = "DB3";	// database name
	public static final int DATABASE_VERSION = 1;		// db version

	//variables to enable communication between threads and to store results
	static boolean waitForQueryRes = false;
	static HashMap<String, Boolean> waitForQueryResponse = new HashMap<String, Boolean>();
	static HashMap<String, String> waitForQueryVal = new HashMap<String, String>();
	static HashMap<String, Boolean> doInsert=new HashMap<String, Boolean>();
	static HashMap<String, Boolean> doInsertRep=new HashMap<String, Boolean>();
	static HashMap<String, Boolean> doInsertSyn=new HashMap<String, Boolean>();
	static boolean doDeleteRep=false;
	static Set<String> failedNode = new HashSet<String>(); 
	static HashMap<String, Integer> nodesalive = new HashMap<String, Integer>();
	static HashMap<String, String> keyVal = new HashMap<String, String>();
	static HashMap<String,HashMap<String,String>> syncRes=new HashMap<String, HashMap<String,String>>();
	static HashMap<String,Boolean> waitForSyncRes= new HashMap<String, Boolean>();
	static HashMap<String,Boolean> queryReplica = new HashMap<String, Boolean>();
	static HashMap<String,Boolean> queryAllCount = new HashMap<String, Boolean>();
	static HashMap<String, HashMap<String, String>> keysMissed = new HashMap<String, HashMap<String,String>>();
	static HashMap<String,String> queryRes;
	LinkedHashMap<String, Nodes> allNodes;
	static LinkedHashMap<String, Nodes> allNodesCopy = new LinkedHashMap<String, Nodes>();
	NodeInfo myNode;

	//database create statement
	SQLiteDatabase db;
	static String tableName="table3";
	static final String createTable= "CREATE TABLE "+tableName+"(key text primary key, value text)";

	public static Context context;

	@Override
	public boolean onCreate() {
		// this is first method to be called on loading 
		context = getContext();

		// create a telephony manager to get the port number
		TelephonyManager tel =(TelephonyManager)getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));

		// assign the node details 
		NodeInfo.id=portStr;
		NodeInfo.portNum=myPort;
		NodeInfo.succ=null;
		NodeInfo.pred=null;
		NodeInfo.idHash=null;
		NodeInfo.succ2=null;

		setNode(portStr);

		try {
			//create a new server socket to keep listening for input messages
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			//new Thread(new ServerThread(serverSocket)).start();
			new ServerActivity().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		} catch (IOException e) {
			e.printStackTrace();
			Log.e("server creation error", "Can't create a ServerSocket");
		}
		
		//create a database helper
		DatabaseHelper dbHelper = new DatabaseHelper(context);
		db = dbHelper.getWritableDatabase();
		
		//clear the database on fist load
		db.delete(tableName, null, null);
		fwdMsg(null, null, "sync");
		return (db == null)? false:true;

	}

	private static class DatabaseHelper extends SQLiteOpenHelper {
		DatabaseHelper(Context context){
			super(context, DATABASE_NAME, null, DATABASE_VERSION);
		}
		//use database helper to perform db activities
		@Override
		public void onCreate(SQLiteDatabase db)
		{
			db.execSQL(createTable);
			Log.v(createTable, " -table created");
		}

		@Override
		public void onUpgrade(SQLiteDatabase db, int oldVersion, 
				int newVersion) {
			db.execSQL("DROP TABLE IF EXISTS " +  tableName);
			onCreate(db);
		}
	}

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		//to handle all types of delete key operations 
		if(selection.equals("@")){
			//to delete all keys present in local node
			db.delete(tableName, null, null);
		}
		if(selection.equals("*")){
			//to delete all keys from all nodes
			db.delete(tableName, null, null);
			fwdMsg(selection, null, "deleteall");
		}
		else if(isMyKey(selection) || doDeleteRep/*||(NodeInfo.pred == null && NodeInfo.succ == null)*/)
		{
			//to delete a single key-value pair
			db.delete(tableName, "key = '"+ selection+"'", null);
			if(!doDeleteRep)
				fwdMsg(selection, null, "deleterep");
		}
		else {
			fwdMsg(selection, null, "delete");
		}
		return 0;
	}

	@Override
	public String getType(Uri uri) {

		return null;
	}

	@Override
	public  Uri insert(Uri uri, ContentValues values) {
		// handles all insert operations
		String key = (String) values.get("key");
		String value = (String) values.get("value");

		boolean myKey=isMyKey(key);

		boolean doIns = false;
		if(doInsert.containsKey(key))
		{doIns=doInsert.get(key);}

		boolean doInRep = false;
		if(doInsertRep.containsKey(key))
		{doInRep=doInsertRep.get(key);}

		boolean insSync = false;
		if(doInsertSyn.containsKey(key))
		{insSync=doInsertSyn.get(key);}	

		if(insSync)
			myKey=false;

		if(myKey || doIns ||doInRep ||insSync )
		{
			//insert in avd
			long rowID = db.insertWithOnConflict(tableName, null, values, SQLiteDatabase.CONFLICT_REPLACE);
			Uri _uri = ContentUris.withAppendedId(uri, rowID);
			getContext().getContentResolver().notifyChange(_uri, null);

			if(myKey || doIns){

				keyVal.put(key, value);
				fwdMsg(key, value, "insertrep");
				Log.e("inserted my key", key+" "+value+" at " +NodeInfo.id );
			}
			else
				Log.e("inserted rep key", key+" "+value+" at " +NodeInfo.id );
			return uri;
		}
		else
		{
			//fwd the msg
			fwdMsg(key, value, "insert");
			return uri;	
		}
	}

	@Override
	public  Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs, String sortOrder) {
		//query the value for a specific key
		//		synchronized (locks.get(selection)) {
		Log.v("query request", "rcvd at "+NodeInfo.id +" " +selection);

		boolean isQueryRep = false;
		if(queryReplica.containsKey(selection))
			isQueryRep=queryReplica.get(selection);

		if(selection.equals("@")){
			//query for all local key in this node
			Cursor cr3;
			String[] condition3 = new String[] {selection};
			cr3= db.rawQuery("select * from "+ tableName , null);
			cr3.setNotificationUri(getContext().getContentResolver(), uri);

			return cr3;
		}

		if(selection.equals("*")){
			//query all keys in every node
			queryRes=new HashMap<String, String>();
			queryAllCount=new HashMap<String, Boolean>();

			waitForQueryRes=true;
			//fwd the msg
			fwdMsg(selection, null, "queryall");

			//wait for response only if this is the  requesting avd
			// if(requesting avd is this..)
			while(queryAllCount.size()<(allNodes.size()-2))//(waitForQueryRes)
			{
				;
			}

			Cursor cr= db.rawQuery("select * from "+ tableName , null);

			MatrixCursor mcr =new MatrixCursor(new String[]{"key","value"});
			for (Entry<String, String> entry : queryRes.entrySet()) {
				String ky = entry.getKey();
				String val = entry.getValue();
				String[] res= new String[]{ky, val};
				mcr.addRow(res);
			}

			if (cr != null)
			{
				//store all key values into a matrix 
				cr.moveToFirst();
				while(cr.isAfterLast() == false){
					String returnKey = cr.getString(0);
					String returnValue = cr.getString(1);
					String[] res= new String[]{returnKey, returnValue};
					mcr.addRow(res);
					cr.moveToNext();
				}
			}
			queryAllCount=new HashMap<String, Boolean>();
			queryRes = new HashMap<String, String>();

			return mcr;
		}
		else if(isMyKey(selection) || isQueryRep)
		{
			//create a sql statement to query the database for the specific key
			String[] condition2 = new String[] {selection};
			Cursor cr2= db.rawQuery("select * from "+ tableName +" where key=?", condition2);
			cr2.setNotificationUri(getContext().getContentResolver(), uri);

			cr2.moveToFirst();
			String ss=null;
			if(cr2.getCount()>0)
				ss= cr2.getString(cr2.getColumnIndex("value"));

			Log.v("query response", selection+" "+ ss+ "in node"+ NodeInfo.id);

			return cr2;
		}
		else
		{
			//if key belongs to another node, forward the query request
			waitForQueryResponse.put(selection, true);

			fwdMsg(selection, null, "query");
			long t1 = System.currentTimeMillis();

			while(waitForQueryResponse.get(selection))
			{
				;				//wait for response
			}

			if(waitForQueryVal.containsKey(selection)){
				String[] res={selection ,waitForQueryVal.get(selection)};
				MatrixCursor cr =new MatrixCursor(new String[]{"key","value"});
				cr.addRow(res);
				waitForQueryResponse.remove(selection);
				waitForQueryVal.remove(selection);
				return cr;	
			}
			else return null;
		}
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {

		return 0;
	}

	private String genHash(String input) throws NoSuchAlgorithmException {
		//generate the SHA1 hash
		MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
		byte[] sha1Hash = sha1.digest(input.getBytes());
		Formatter formatter = new Formatter();
		for (byte b : sha1Hash) {
			formatter.format("%02x", b);
		}
		return formatter.toString();
	}

	public boolean isMyKey(String key){
		//method to check if the key belongs in my node
		String keyHash=null, prevHash=null;
		try {
			keyHash = genHash(key);
			prevHash = genHash(NodeInfo.pred);
		} catch (NoSuchAlgorithmException e) {

			e.printStackTrace();
		}

		int me_key_comp= NodeInfo.idHash.compareTo(keyHash);
		int pred_key_comp = prevHash.compareTo(keyHash);

		if(me_key_comp > 0 )
		{
			if(pred_key_comp < 0){
				return true;
			}
			else if(isFirstNode()){
				return true;
			}
		}else if(isFirstNode()&&isAfterLastNode(keyHash)){

			return true;
		}

		return false;
	}

	boolean isFirstNode()
	{
		//method to check if key belongs to the first node
		String pred = null;
		try {
			pred = genHash(NodeInfo.pred);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}

		if(NodeInfo.idHash.compareTo(pred)<0)
			return true;
		else return false;
	}

	boolean isAfterLastNode(String keyHash)
	{
		//method to check if key belongs to the first node  ie after last node as it is circular
		String pred = null;
		try {
			pred = genHash(NodeInfo.pred);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}

		if(keyHash.compareTo(pred)>0)
			return true;
		else return false;
	}

	public boolean isKeyInRange(String key, String node){
		// find the keys range for a node
		String keyHash=null, start=null, end=null;
		try {
			keyHash = genHash(key);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}

		if(allNodes.size()==3){
			//if there are 3 nodes in total, the key distribution is as follows
			if(node.equals("5554")){
				start="208f7f72b198dadd244e61801abe1ec3a4857bc9";
				end="33d6357cfaaf0f72991b0ecd8c56da066613c089";
				int end_key_comp= end.compareTo(keyHash);
				int start_key_comp = start.compareTo(keyHash);
				if(end_key_comp>0 && start_key_comp<0)
					return true;
				else 
					return false;
			}

			else if(node.equals("5556")){
				start="0";
				end="208f7f72b198dadd244e61801abe1ec3a4857bc9";
				String start2= "abf0fd8db03e5ecb199a9b82929e9db79b909643";
				int end_key_comp= end.compareTo(keyHash);
				int start_key_comp = start.compareTo(keyHash);
				int start2_key_comp= start2.compareTo(keyHash);

				if(end_key_comp>0 && start_key_comp<0 && start2_key_comp<0)
					return true;
				else 
					return false;
			}

			else if(node.equals("5558")){
				start="33d6357cfaaf0f72991b0ecd8c56da066613c089";
				end="abf0fd8db03e5ecb199a9b82929e9db79b909643";
				int end_key_comp= end.compareTo(keyHash);
				int start_key_comp = start.compareTo(keyHash);
				if(end_key_comp>0 && start_key_comp<0)
					return true;
				else 
					return false;
			}
		}
		else if(allNodes.size()==5){
			// for a total of 5 nodes, this key distribution is as follows
			if(node.equals("5554")){
				start= allNodes.get("5556").idHash;
				end= allNodes.get("5554").idHash;
				int end_key_comp= end.compareTo(keyHash);
				int start_key_comp = start.compareTo(keyHash);
				if(end_key_comp>0 && start_key_comp<0)
					return true;
				else 
					return false;
			}

			else if(node.equals("5556")){
				start=allNodes.get("5562").idHash;
				end=allNodes.get("5556").idHash;
				int end_key_comp= end.compareTo(keyHash);
				int start_key_comp = start.compareTo(keyHash);

				if(end_key_comp>0 && start_key_comp<0)
					return true;
				else 
					return false;
			}

			else if(node.equals("5558")){
				start=allNodes.get("5554").idHash;
				end=allNodes.get("5558").idHash;
				int end_key_comp= end.compareTo(keyHash);
				int start_key_comp = start.compareTo(keyHash);
				if(end_key_comp>0 && start_key_comp<0)
					return true;
				else 
					return false;
			}

			else if(node.equals("5560")){
				start=allNodes.get("5558").idHash;
				end=allNodes.get("5560").idHash;
				int end_key_comp= end.compareTo(keyHash);
				int start_key_comp = start.compareTo(keyHash);
				if(end_key_comp>0 && start_key_comp<0)
					return true;
				else 
					return false;
			}

			else if(node.equals("5562")){
				end=allNodes.get("5562").idHash;
				String start2= allNodes.get("5560").idHash;
				int end_key_comp= end.compareTo(keyHash);
				int start2_key_comp= start2.compareTo(keyHash);

				if((end_key_comp>0 ) || (start2_key_comp<0))
					return true;
				else 
					return false;
			}

		}
		return false;

	}

	private void setNode(String portStr) {
		
		//method to setup the node details
		allNodes= new LinkedHashMap<String, Nodes>();   	
		
		//setting vary if based on the total number of nodes (mentioned as a comment beside it)
		Nodes avd0 = new Nodes();
		avd0.id="5554";
		avd0.succ="5558";
		avd0.succ2="5560";	//5
		//avd0.succ2="5556";	//3
		avd0.pred="5556";
		//avd0.pred2="5558"; //3
		avd0.pred2="5562"; //5
		Nodes avd1 = new Nodes();
		avd1.id="5556";
		avd1.succ="5554";
		avd1.succ2="5558";	//3  & 5
		avd1.pred="5562";//5
		//avd1.pred="5558";//3
		avd1.pred2="5560"; //5
		//avd1.pred2="5554"; //3
		Nodes avd2 = new Nodes();
		avd2.id="5558";
		avd2.succ="5560";//5 
		//avd2.succ="5556";//3
		avd2.succ2="5562";	//5
		//avd2.succ2="5554";	//3
		avd2.pred="5554";
		avd2.pred2="5556"; //3 & 5
		Nodes avd3 = new Nodes();
		avd3.id="5560";
		avd3.succ="5562";
		avd3.succ2="5556";
		avd3.pred="5558";
		avd3.pred2="5554";
		Nodes avd4 = new Nodes();
		avd4.id="5562";
		avd4.succ="5556";
		avd4.succ2="5554";
		avd4.pred="5560";
		avd4.pred2="5558";

		try{
			avd0.idHash=genHash("5554");
			avd1.idHash=genHash("5556");
			avd2.idHash=genHash("5558");
			avd3.idHash=genHash("5560");
			avd4.idHash=genHash("5562");
		}
		catch (Exception e){
			e.printStackTrace();
		}
		allNodes.put("5562", avd4);
		allNodes.put("5556", avd1);
		allNodes.put("5554", avd0);
		allNodes.put("5558", avd2);
		allNodes.put("5560", avd3);

		allNodesCopy.put("5562", avd4);
		allNodesCopy.put("5556", avd1);
		allNodesCopy.put("5554", avd0);
		allNodesCopy.put("5558", avd2);
		allNodesCopy.put("5560", avd3);

		NodeInfo.succ=allNodes.get(portStr).succ;
		NodeInfo.pred=allNodes.get(portStr).pred;
		NodeInfo.succ2=allNodes.get(portStr).succ2;
		NodeInfo.idHash=allNodes.get(portStr).idHash;
		NodeInfo.pred2=allNodes.get(portStr).pred2;


	}

	private  String getNode(String key) {
		// find the node to which a key belongs 
		String nd=null;
		String hkey=null;
		try{
			hkey = genHash(key);
		}catch (Exception e){ e.printStackTrace();}		

		for(Map.Entry<String, Nodes> n: allNodes.entrySet())
		{
			String nhash=n.getValue().idHash;
			int me_key_comp= nhash.compareTo(hkey);
			if(me_key_comp >= 0 )
			{
				nd= n.getKey();   
				break;
			}
		}
		if(nd==null){
			nd="5562";	// for 5 avds ie the first node
			//nd="5556";	//  for 3 avds ie the first node
		}
		return nd;
	}

	void fwdMsg(String key, String value, String msgTy){
		//method to forward a message to other node
		//based on the message type and destination, mew msg packet is created and sent
		Message fwMsg = new Message();
		fwMsg.id = NodeInfo.id;
		fwMsg.key=key;
		fwMsg.value =  value;
		fwMsg.messageType=msgTy;
		fwMsg.msgFrom=NodeInfo.id;
		fwMsg.succ=NodeInfo.succ;

		if(msgTy.equals("insert")){
			String nd = getNode(key);
			//Log.v("insert fwd", "to " + nd +" " + key+" "+value );
			fwMsg.msgTo= String.valueOf((Integer.parseInt(nd) * 2));
			fwMsg.succ=allNodes.get(nd).succ;	//it is the successor1
			fwMsg.pred = allNodes.get(nd).succ2; //it is the successor2

			//new Thread(new ClientThread(fwMsg)).start();
			ClientThread ct1= new ClientThread(fwMsg);
			ct1.run();
		}

		else if(msgTy.equals("insertrep")){
			//fwMsg.messageType="insertrep";
			Message fwMsg1 = new Message(); 
			fwMsg1.id = NodeInfo.id;
			fwMsg1.key=key;
			fwMsg1.value =  value;
			fwMsg1.messageType=msgTy;
			fwMsg1.msgFrom=NodeInfo.id;
			fwMsg1.messageType="insertrep";
			fwMsg1.msgTo= String.valueOf((Integer.parseInt(NodeInfo.succ) * 2));	
			//new Thread(new ClientThread(fwMsg)).start();
			ClientThread ct2= new ClientThread(fwMsg1);
			ct2.run();

			Message fwMsg2 = new Message(); 
			fwMsg2.id = NodeInfo.id;
			fwMsg2.key=key;
			fwMsg2.value =  value;
			fwMsg2.messageType=msgTy;
			fwMsg2.msgFrom=NodeInfo.id;
			fwMsg2.messageType="insertrep";
			fwMsg2.messageType="insertrep";
			fwMsg2.msgTo= String.valueOf((Integer.parseInt(NodeInfo.succ2) * 2));	
			//new Thread(new ClientThread(fwMsg)).start();
			ClientThread ct3= new ClientThread(fwMsg2);
			ct3.run();
		}
		else if(msgTy.equals("delete")){
			String nd = getNode(key);
			fwMsg.msgTo= String.valueOf((Integer.parseInt(nd) * 2));
			//new Thread(new ClientThread(fwMsg)).start();
			ClientThread ct1= new ClientThread(fwMsg);
			ct1.run();
		}
		else if(msgTy.equals("deleterep")){
			fwMsg.messageType="deleterep";
			fwMsg.msgTo= String.valueOf((Integer.parseInt(NodeInfo.succ) * 2));	
			//new Thread(new ClientThread(fwMsg)).start();
			ClientThread ct2= new ClientThread(fwMsg);
			ct2.run();
			fwMsg.msgTo= String.valueOf((Integer.parseInt(NodeInfo.succ2) * 2));	
			//new Thread(new ClientThread(fwMsg)).start();
			ClientThread ct3= new ClientThread(fwMsg);
			ct3.run();
		}
		else if(msgTy.equals("query")){
			String nd = getNode(key);
			fwMsg.msgTo= String.valueOf((Integer.parseInt(nd) * 2));
			fwMsg.succ=allNodes.get(nd).succ;
			fwMsg.pred=allNodes.get(nd).succ2;	//successor2
			//new Thread(new ClientThread(fwMsg)).start();
			ClientThread ct1= new ClientThread(fwMsg);
			ct1.run();
		}
		else if(msgTy.equals("sync")){
			Message fwMsg1 = new Message();
			fwMsg1.id = NodeInfo.id;
			fwMsg1.messageType=msgTy;
			fwMsg1.msgFrom=NodeInfo.id;
			fwMsg1.msgTo= String.valueOf((Integer.parseInt(NodeInfo.succ) * 2));
			new Thread(new ClientThread(fwMsg1)).start();
			//new ClientActivity().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, fwMsg);

			Message fwMsg2 = new Message();
			fwMsg2.id = NodeInfo.id;
			fwMsg2.messageType=msgTy;
			fwMsg2.msgFrom=NodeInfo.id;
			fwMsg2.msgTo= String.valueOf((Integer.parseInt(NodeInfo.pred) * 2));
			//new ClientActivity().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, fwMsg);
			new Thread(new ClientThread(fwMsg2)).start();

			Message fwMsg3 = new Message();
			fwMsg3.id = NodeInfo.id;
			fwMsg3.messageType=msgTy;
			fwMsg3.msgFrom=NodeInfo.id;
			fwMsg3.msgTo= String.valueOf((Integer.parseInt(NodeInfo.pred2) * 2));
			//new ClientActivity().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, fwMsg);
			new Thread(new ClientThread(fwMsg3)).start();

		}
		else if(fwMsg.messageType.equals("queryall"))
		{

			Message fwMsg1 = new Message();
			fwMsg1.id = NodeInfo.id;
			fwMsg1.messageType=msgTy;
			fwMsg1.msgFrom=NodeInfo.id;
			fwMsg1.msgTo= String.valueOf((Integer.parseInt(NodeInfo.succ) * 2));
			ClientThread ct1= new ClientThread(fwMsg1);
			ct1.run();

			Message fwMsg2 = new Message();
			fwMsg2.id = NodeInfo.id;
			fwMsg2.messageType=msgTy;
			fwMsg2.msgFrom=NodeInfo.id;
			fwMsg2.msgTo= String.valueOf((Integer.parseInt(NodeInfo.pred) * 2));
			ClientThread ct2= new ClientThread(fwMsg2);
			ct2.run();

			Message fwMsg3 = new Message();
			fwMsg3.id = NodeInfo.id;
			fwMsg3.messageType=msgTy;
			fwMsg3.msgFrom=NodeInfo.id;
			fwMsg3.msgTo= String.valueOf((Integer.parseInt(NodeInfo.pred2) * 2));
			ClientThread ct3= new ClientThread(fwMsg3);
			ct3.run();

			Message fwMsg4 = new Message();
			fwMsg4.id = NodeInfo.id;
			fwMsg4.messageType=msgTy;
			fwMsg4.msgFrom=NodeInfo.id;
			fwMsg4.msgTo= String.valueOf((Integer.parseInt(NodeInfo.succ2) * 2));
			ClientThread ct4= new ClientThread(fwMsg4);
			ct4.run();

		}
	}


}

