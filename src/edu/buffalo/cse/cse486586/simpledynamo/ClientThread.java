package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.ObjectOutputStream;
import java.io.StreamCorruptedException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;

import android.os.AsyncTask;
import android.util.Log;

public class ClientThread implements Runnable {

	Message messageToSend;

	public ClientThread(Message fwMsg) {
		super();
		this.messageToSend = fwMsg;
	}

	@Override
	public void run() {

		String remotePortNum = messageToSend.msgTo;																				//extract port number
		try{

			Socket socket = new Socket(InetAddress.getByAddress(new byte[] {10, 0, 2, 2}), Integer.parseInt(remotePortNum));	//create socket
			ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
			objectOutputStream.writeObject(messageToSend);																		//write msg to output stream
			objectOutputStream.flush();
			objectOutputStream.close();
			socket.close();
		}
		catch (Exception e){
			if(e instanceof StreamCorruptedException)
			{
				Log.e("client exception", "stream corrupt socket"+remotePortNum);

				if(messageToSend.messageType.equals("insert")){

					Message msg1 = new Message();
					msg1.id=messageToSend.id;
					msg1.key=messageToSend.key;
					msg1.value=messageToSend.value;
					msg1.messageType="insertrep";
					msg1.msgFrom=messageToSend.msgFrom;
					msg1.msgTo=String.valueOf((Integer.parseInt(messageToSend.succ) * 2));
					new ClientActivity().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg1);

					Message msg2 = new Message();
					msg2.id=messageToSend.id;
					msg2.key=messageToSend.key;
					msg2.value=messageToSend.value;
					msg2.messageType="insertrep";
					msg2.msgFrom=messageToSend.msgFrom;
					msg2.msgTo=String.valueOf((Integer.parseInt(messageToSend.pred) * 2));	//succ2
					new ClientActivity().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg2);

				}
				else if(messageToSend.messageType.equals("query")){

					Message msg1 = new Message();
					msg1.id=messageToSend.id;
					msg1.key=messageToSend.key;
					msg1.messageType="queryReplica";
					msg1.msgFrom=messageToSend.msgFrom;
					msg1.msgTo=String.valueOf((Integer.parseInt(messageToSend.succ) * 2));
					new ClientActivity().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg1);
				}
				else if(messageToSend.messageType.equals("queryReplica")){

					Message msg1 = new Message();
					msg1.id=messageToSend.id;
					msg1.key=messageToSend.key;
					msg1.messageType="queryReplica";
					msg1.msgFrom=messageToSend.msgFrom;
					msg1.msgTo=String.valueOf((Integer.parseInt(messageToSend.pred) * 2));	//ie succ2
					new ClientActivity().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg1);
				}
			}
			else
				e.printStackTrace();

		}

	}


}
