package client;

import java.io.IOException;
import java.io.EOFException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.ArrayList;
import java.util.List;

import client.ClientSocketListener.SocketStatus;
import shared.messages.KVMessage;
import shared.messages.KVMessageImpl;
import shared.messages.TextMessage;
import shared.messages.KVMessage.StatusType;
import shared.messages.MessageWrapper;

import shared.Metadata;

import org.apache.log4j.Logger;



public class KVStore implements KVCommInterface {

	private static Logger logger = Logger.getRootLogger();

	private String ServerAddress;
	private int ServerPort;


	private Set<ClientSocketListener> listeners;
	private boolean running;
	
	private Socket clientSocket;
	private ObjectOutputStream output;
 	private ObjectInputStream input;
	// private ObjectOutputStream MetaOut;
	// private ObjectInputStream MetaIn;

	public Metadata metadata;
	
	private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 1024 * BUFFER_SIZE;

	public List<String> subscribedKeys;
	public BlockingQueue<Object> messageQueue;
	public Thread listenForMessagesThread;

	/**
	 * Initialize KVStore with address and port of KVServer
	 * @param address the address of the KVServer
	 * @param port the port of the KVServer
	 */
	public KVStore(String address, int port) {
		ServerAddress = address;
		ServerPort = port;
		subscribedKeys = new ArrayList<>();
		this.messageQueue = new LinkedBlockingQueue<>();
	}

	public void setSubscriberKeys(List<String> newSubscriptions) {
		this.subscribedKeys = new ArrayList<>(newSubscriptions);
	} 

	public int getServerPort(){
		return this.ServerPort; 
	}

	public void setServerAddress(String newServerAddress){
		this.ServerAddress = newServerAddress;
	}

	public void setServerPort(int newServerPort){
		this.ServerPort = newServerPort;
	}

	@Override
	public void connect() throws Exception {
		clientSocket = new Socket(ServerAddress, ServerPort);
		listeners = new HashSet<ClientSocketListener>();
		setRunning(true);
		logger.info("Connection established");

		output = new ObjectOutputStream(clientSocket.getOutputStream());
		input = new ObjectInputStream(clientSocket.getInputStream());
		listenForMessages();
		this.metadata = receiveMetadata();
		logger.info("METADATA RECEIVED");
		System.out.println(this.metadata.toString());
		KVMessageImpl metaDataReceived = new KVMessageImpl(null, null, StatusType.META_ACK);
		sendKVMessage(metaDataReceived);
		// 2) RECEIVE AND STORE METADATA
	}

	public void listenForMessages() {
		System.out.println("listening for messages (objects from kvserver)...");
		listenForMessagesThread = new Thread(() -> {
			try {
                while (!Thread.currentThread().isInterrupted()) {
                    Object receivedObject = receiveObject(); // Method to receive an object from the server

					boolean isSubscription = false;
					if(receivedObject instanceof MessageWrapper) {
						MessageWrapper wrapper = (MessageWrapper) receivedObject;
						if(wrapper.getMessageType() == MessageWrapper.MessageType.KV_MESSAGE){
							KVMessageImpl message = (KVMessageImpl) wrapper.getMessage();
							if(message.getStatus() == StatusType.SUBSCRIPTION_UPDATE){
								isSubscription = true;
								String updatedKey = message.getKey();
								if(subscribedKeys.contains(updatedKey)){
									System.out.println("NOTIFICATION: You are subscribed key " + updatedKey);
									System.out.println("NEW VALUE: " + message.getValue());
								}
							}
						}
					} 
					
					if(isSubscription == false){
						messageQueue.put(receivedObject); // Add the received object to the message queue
					}
              
                }
            } catch (InterruptedException e) {
                // Handle interruption (e.g., when the thread is stopped)
                Thread.currentThread().interrupt();
            } catch (EOFException E) {
                System.out.println("Server died, please reconnect to a new server to continue");
            } catch (IOException i){

			} catch (ClassNotFoundException c){}
		});
		listenForMessagesThread.start();
    }


    public Metadata receiveMetadata() throws IOException {
        try{
            Object receivedMessage = messageQueue.take();
			if (receivedMessage instanceof MessageWrapper) {
				// Handle the message based on its type
				MessageWrapper wrapper = (MessageWrapper) receivedMessage;
				if(wrapper.getMessageType() == MessageWrapper.MessageType.METADATA){
					Metadata metadata = (Metadata) wrapper.getMessage();
					return metadata;
				}
			}
        } catch(Exception C){
            logger.info("ClassNotFoundException");
        }
        return null;
    }


	@Override
	public void disconnect() {
		this.closeConnection();
	}

	public void sendKVMessage(KVMessageImpl msg) throws IOException {
			MessageWrapper wrapper = new MessageWrapper(MessageWrapper.MessageType.KV_MESSAGE, msg);
			output.writeObject(wrapper);
			output.flush();

			logger.info("SEND \t<" 
			+ clientSocket.getInetAddress().getHostAddress() + ":" 
			+ clientSocket.getPort() + ">: '" 
			+"'" + msg.getKey() + " " + msg.getValue()); 
    }

	public void subscribeKey(String key){
    	if (!subscribedKeys.contains(key)) {
        	subscribedKeys.add(key);
    	}
	}

	public void unsubscribeKey(String key){
		subscribedKeys.remove(key);
	}

	public boolean isSubscribed(String key) {
    	return subscribedKeys.contains(key);
	}

	public void printSubscriptions(){
		for(String key : subscribedKeys){
			System.out.println(key);
		}
	}
	@Override
	public KVMessage put(String key, String value) throws Exception {
		logger.info("Putting key: " + key + " Value: " + value);
		KVMessageImpl msg = new KVMessageImpl(key, value, StatusType.PUT);
		sendKVMessage(msg);
		
		KVMessageImpl message = null;
		while (true) {
			Object receivedObject = messageQueue.take(); // Will block until an object is available
			if (receivedObject instanceof MessageWrapper) {
				MessageWrapper wrapper = (MessageWrapper) receivedObject;
				if (wrapper.getMessageType() == MessageWrapper.MessageType.KV_MESSAGE) {
					message = (KVMessageImpl) wrapper.getMessage();
					if (message.getStatus() == StatusType.PUT_SUCCESS || 
						message.getStatus() == StatusType.PUT_UPDATE || 
						message.getStatus() == StatusType.PUT_ERROR ||
						message.getStatus() == StatusType.SERVER_WRITE_LOCK) {
						// If it's the expected PUT response, break out of the loop
						break;
					}
					if (message.getStatus() == StatusType.SERVER_NOT_RESPONSIBLE) {
						System.out.println("need to switch server");
						break; // Or break if you want to stop the operation
					}
					// For other messages, you may want to put them back into the queue or handle differently
				}
			}
		}
		// System.out.println("msg info: " + message.getKey() +" "+message.getValue());
		return message;
	}

	@Override
	public KVMessage get(String key) throws Exception {
		logger.info("Getting value at key: " + key);
		KVMessageImpl msg = new KVMessageImpl(key, "", StatusType.GET);
	
		sendKVMessage(msg);
		
		KVMessageImpl message = null;
		while (true) {
			Object receivedObject = messageQueue.take(); // Will block until an object is available
			if (receivedObject instanceof MessageWrapper) {
				MessageWrapper wrapper = (MessageWrapper) receivedObject;
				if (wrapper.getMessageType() == MessageWrapper.MessageType.KV_MESSAGE) {
					message = (KVMessageImpl) wrapper.getMessage();
					if (message.getStatus() == StatusType.GET_SUCCESS ||
						message.getStatus() == StatusType.GET_ERROR) {
						// If it's the expected GET response, break out of the loop
						break;
					}
					// If it's SERVER_NOT_RESPONSIBLE, handle it accordingly
					if (message.getStatus() == StatusType.SERVER_NOT_RESPONSIBLE) {
						System.out.println("need to switch server");
						break; 
					}
				}
			}
		}
		//System.out.println("msg info: " + message.getKey() +" "+message.getValue());
		return message;
	}

	public KVMessage keyrange() throws Exception{
		logger.info("Getting key range info from server");
		KVMessageImpl msg = new KVMessageImpl(null, null, StatusType.REQUEST_KEYRANGE);

		sendKVMessage(msg);

		
		KVMessageImpl message = null;
		while (true) {
			Object receivedObject = messageQueue.take(); // Will block until an object is available
			if (receivedObject instanceof MessageWrapper) {
				MessageWrapper wrapper = (MessageWrapper) receivedObject;
				if (wrapper.getMessageType() == MessageWrapper.MessageType.KV_MESSAGE) {
					message = (KVMessageImpl) wrapper.getMessage();
					if (message.getStatus() == StatusType.KEYRANGE_SUCCESS) {
						// If it's the expected GET response, break out of the loop
						break;
					}
				}
			}
		}
		return message;
	}

	public KVMessage keyrange_read() throws Exception{
		logger.info("Getting key range read info from server");
		KVMessageImpl msg = new KVMessageImpl(null, null, StatusType.REQUEST_KEYRANGE_READ);
		sendKVMessage(msg);
		
		KVMessageImpl message = null;
		while (true) {
			Object receivedObject = messageQueue.take(); // Will block until an object is available
			if (receivedObject instanceof MessageWrapper) {
				MessageWrapper wrapper = (MessageWrapper) receivedObject;
				if (wrapper.getMessageType() == MessageWrapper.MessageType.KV_MESSAGE) {
					message = (KVMessageImpl) wrapper.getMessage();
					if (message.getStatus() == StatusType.KEYRANGE_READ_SUCCESS) {
						// If it's the expected GET response, break out of the loop
						break;
					}
				}
			}
		}
		return message;
	}

	
	public synchronized void closeConnection() {
		KVMessageImpl clientLeavingMessage = new KVMessageImpl(null, null, StatusType.CLIENT_LEAVING);
		try {
			sendKVMessage(clientLeavingMessage);
		} catch (Exception e) {
			// TODO: handle exception
		}

		logger.info("try to close connection ...");
		
		try {
			tearDownConnection();
			for(ClientSocketListener listener : listeners) {
				listener.handleStatus(SocketStatus.DISCONNECTED);
			}
		} catch (IOException ioe) {
			logger.error("Unable to close connection!");
		}
	}
	
	private void tearDownConnection() throws IOException {
		setRunning(false);
		logger.info("tearing down the connection ...");
		if (clientSocket != null) {
			//input.close();
			//output.close();
			listenForMessagesThread.interrupt();
			clientSocket.close();
			clientSocket = null;
			logger.info("connection closed!");
		}
	}
	
	public boolean isRunning() {
		return running;
	}
	
	public void setRunning(boolean run) {
		running = run;
	}
	
	public void addListener(ClientSocketListener listener){
		listeners.add(listener);
	}
	
	/**
	 * Method sends a TextMessage using this socket.
	 * @param msg the message that is to be sent.
	 * @throws IOException some I/O error regarding the output stream 
	 */
	public void sendMessage(TextMessage msg) throws IOException {
		byte[] msgBytes = msg.getMsgBytes();
		output.write(msgBytes, 0, msgBytes.length);
		output.flush();
		logger.info("Send message:\t '" + msg.getMsg() + "'");
    }
		

	public KVMessageImpl receiveMessage() throws IOException {
        try{
            Object response = input.readObject();
			if (response instanceof MessageWrapper) {
				// Handle the message based on its type
				MessageWrapper wrapper = (MessageWrapper) response;
				if(wrapper.getMessageType() == MessageWrapper.MessageType.KV_MESSAGE){
					KVMessageImpl message = (KVMessageImpl) wrapper.getMessage();
					return message;
				}
			}
        } catch(ClassNotFoundException C){
            logger.info("ClassNotFoundException");
        }
        return null;
    }

	public Object receiveObject() throws IOException, ClassNotFoundException {
		Object object = input.readObject();
		return object;
	}

}
