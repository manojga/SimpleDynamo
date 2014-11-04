package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;

import android.os.AsyncTask;
import android.util.Log;

public class ClientActivity extends AsyncTask<Message, Void, Void>  {

	@Override
	protected Void doInBackground(Message... messageToSend) {

		try{

			String remotePortNum = messageToSend[0].msgTo;																			//extract port number
			Socket socket = new Socket(InetAddress.getByAddress(new byte[] {10, 0, 2, 2}), Integer.parseInt(remotePortNum));		//create socket connection
			ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
			objectOutputStream.writeObject(messageToSend[0]);																		//write msg to output stream
			objectOutputStream.flush();
			objectOutputStream.close();
			socket.close();

		}
		catch (Exception e){
			e.printStackTrace();
			Log.e("AsynClient", "Client error");
		}

		return null;
	}



}