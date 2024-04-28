package app_kvClient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import javax.naming.ldap.StartTlsRequest;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import client.KVStore;
import ecs.IECSNode;
import client.KVCommInterface;
import logger.LogSetup;
import shared.messages.KVMessage.StatusType;
import shared.messages.KVMessageImpl;
import shared.MD5Hash;
import shared.messages.KVMessage;
import shared.messages.TextMessage;

public class KVClient implements IKVClient {
    

    private static Logger logger = Logger.getRootLogger();
	private static final String PROMPT = "KVClient> ";
	private BufferedReader stdin;
	private KVStore client = null; 
	private boolean stop = false;
	
	private String serverAddress;
	private int serverPort;
    
    @Override
    public void newConnection(String hostname, int port) throws Exception{
        // TODO Auto-generated method stub
		client = new KVStore(hostname, port); 
		client.connect();
    }

    @Override
    public KVStore getStore(){
        // TODO Auto-generated method stub
        return client;
    }
    
	
	public void run() {
		addShutdownHook();
		while(!stop) {
			stdin = new BufferedReader(new InputStreamReader(System.in));
			System.out.print(PROMPT);
			
			try {
				String cmdLine = stdin.readLine();
				this.handleCommand(cmdLine);
			} catch (IOException e) {
				stop = true;
				printError("CLI does not respond - Application terminated ");
			}
		}
	}
	
	private void addShutdownHook(){
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			try{
				if (client != null){
					KVMessageImpl clientLeavingMessage = new KVMessageImpl(null, null, StatusType.CLIENT_LEAVING);
					client.sendKVMessage(clientLeavingMessage);
					client.listenForMessagesThread.interrupt();
				}
			} catch (IOException e){

			}
		}));
	}


	public void handleCommand(String cmdLine) {
		String[] tokens = cmdLine.split("\\s+");

		if(tokens[0].equals("quit")) {	
			stop = true;
			client.disconnect();
			System.out.println(PROMPT + "Application exit!");
		
		} else if (tokens[0].equals("connect")){
			if(tokens.length == 3) {
				try{
					serverAddress = tokens[1];
					serverPort = Integer.parseInt(tokens[2]);
					logger.info("Trying to connect to " + serverAddress + " Port: " + String.valueOf(serverPort));
					List<String> SubscribedKeys = null;
					boolean alreadyConnected = false;
					if(client != null){
						SubscribedKeys = client.subscribedKeys;
						alreadyConnected = true;
					}
					client = new KVStore(serverAddress, serverPort);
					if (alreadyConnected == true){
						client.setSubscriberKeys(SubscribedKeys);
					}
                    client.connect();

				} catch(NumberFormatException nfe) {
					printError("No valid address. Port must be a number!");
					logger.info("Unable to parse argument <port>", nfe);
				} catch (UnknownHostException e) {
					printError("Unknown Host!");
					logger.info("Unknown Host!", e);
				} catch (IOException e) {
					printError("Could not establish connection!");
					logger.warn("Could not establish connection!", e);
				} catch (Exception e){
                    printError("Something else went wrong");
                }
			} else {
				printError("Invalid number of parameters!");
			}
			
		} else  if (tokens[0].equals("send")) {
			if(tokens.length >= 2) {
				if(client != null){
					StringBuilder msg = new StringBuilder();
					for(int i = 1; i < tokens.length; i++) {
						msg.append(tokens[i]);
						if (i != tokens.length -1 ) {
							msg.append(" ");
						}
					}
					sendMessage(msg.toString());	
				} else {
					printError("Not connected!");
				}
			} else {
				printError("No message passed!");
			}
		} else  if (tokens[0].equals("put")) {
			if(tokens.length >= 2) {
				if(client != null){
					StringBuilder value = new StringBuilder();
					for(int i = 2; i < tokens.length; i++) {
						value.append(tokens[i]);
						if (i != tokens.length -1 ) {
							value.append(" ");
						}
					}
					byte[] keyBytes = tokens[1].getBytes();
					String keyString = tokens[1];
					byte[] valueBytes = value.toString().getBytes();
					if(keyBytes.length > 20 || valueBytes.length > 120){
						printError("Data too big!");
					} else {
						try {
							KVMessage recievedMessage = client.put(tokens[1], value.toString());
							
							StatusType returnedStatus = recievedMessage.getStatus();
							if (returnedStatus.equals(StatusType.PUT_ERROR)) {
    							System.out.println("Error occurred.");
							} else if(returnedStatus.equals(StatusType.PUT_SUCCESS)) {
    							System.out.println("the PUT was successful");
							} else if (returnedStatus.equals(StatusType.SERVER_NOT_RESPONSIBLE)){
								System.out.println("connected to wrong server... getting new metadata");
								KVMessageImpl requestForMetadata = new KVMessageImpl(null, null, StatusType.ECS_UPDATE_METADATA);
								client.sendKVMessage(requestForMetadata);
								client.metadata = client.receiveMetadata();
								System.out.println("received metadata... joining new server...");
								// compute which server to join
								client.disconnect();
								String keyHash = MD5Hash.hashString(keyString);
								IECSNode newServerNode = client.metadata.getSuccessor(keyHash);
								client.setServerAddress(newServerNode.getNodeHost());
								client.setServerPort(newServerNode.getNodePort());
								client.connect();
								System.out.println("new connection established... retrying request");

								KVMessage retryPut = client.put(tokens[1], value.toString());
								if(retryPut.getStatus().equals(StatusType.PUT_SUCCESS)){
									System.out.println("the PUT was successful");
								} else {
									System.out.println("error occured");
								}
							} else if (returnedStatus.equals(StatusType.SERVER_WRITE_LOCK)){
								System.out.println("SERVER IS WRITE LOCKED");
							} else if (returnedStatus.equals(StatusType.SERVER_STOPPED)){
								System.out.println("SERVER IS STOPPED");
							}
						} catch (Exception e){
							printError("Put failed");
						}
					}
				} else {
					printError("Not connected!");
				}
			} else {
				printError("No message passed!");
			}
			
		} else if(tokens[0].equals("get")) {
			if(tokens.length >= 2) {
				if(client != null) {
					StringBuilder key = new StringBuilder();
					for(int i = 1; i < tokens.length; i++) {
						key.append(tokens[i]);
						if (i != tokens.length - 1) {
							key.append(" ");
						}
					}
					String keyString = key.toString();
					byte[] keyBytes = keyString.getBytes();  // Get the byte array of the key string
		
					if(keyBytes.length > 20) {
						printError("Key length is over 20 bytes!");
					} else {
						try {
							System.out.println("Getting: " + keyString);
							KVMessage recievedMessage = client.get(keyString);
							
							StatusType returnedStatus = recievedMessage.getStatus();
							if (returnedStatus.equals(StatusType.GET_ERROR)) {
    							System.out.println("Error occurred.");
							} else if(returnedStatus.equals(StatusType.GET_SUCCESS)) {
    							System.out.println("The received value is: " + recievedMessage.getValue());
							} else if(returnedStatus.equals(StatusType.SERVER_NOT_RESPONSIBLE)){
								System.out.println("connected to wrong server... getting new metadata");
								KVMessageImpl requestForMetadata = new KVMessageImpl(null, null, StatusType.ECS_UPDATE_METADATA);
								client.sendKVMessage(requestForMetadata);
								client.metadata = client.receiveMetadata();
								System.out.println("received metadata... joining new server...");
								// compute which server to join
								client.disconnect();
								String keyHash = MD5Hash.hashString(keyString);
								IECSNode newServerNode = client.metadata.getSuccessor(keyHash);
								client.setServerAddress(newServerNode.getNodeHost());
								client.setServerPort(newServerNode.getNodePort());
								client.connect();
								System.out.println("new connection established... retrying request");

								KVMessage retryGet = client.get(keyString);
								if(retryGet.getStatus().equals(StatusType.GET_SUCCESS)){
									System.out.println("The received value is: " + retryGet.getValue());
								} else {
									System.out.println("error occured");
								}
							} else if (returnedStatus.equals(StatusType.SERVER_STOPPED)){
								System.out.println("SERVER IS STOPPED");
							} else {
								System.out.println("wtf happened");
							}
							
							
						} catch (Exception e) {
							printError("get failed: " + e.getMessage());  // Include exception message for more context
						}
					}
				} else {
					printError("Not connected!");
				}
			} else {
				printError("No key provided!");
			}
		} else if (tokens[0].equals("subscribe")) {
			if(tokens.length >= 2) {
				if (client != null) {
					StringBuilder key = new StringBuilder();
					for(int i = 1; i < tokens.length; i++) {
						key.append(tokens[i]);
						if (i != tokens.length - 1) {
							key.append(" ");
						}
					}
					String keyString = key.toString();
					byte[] keyBytes = keyString.getBytes();

					if(keyBytes.length > 20) {
						printError("Key length is over 20 bytes!");
					} else {
						client.subscribeKey(keyString);
						System.out.println("Subscribed to key: " + keyString + "!");
					}
				} else {
					printError("Not connected!");
				}
			} else {
				printError("No key provided!");
			}
		} else if (tokens[0].equals("unsubscribe")) {
			if(tokens.length >= 2) {
				if (client != null) {
					StringBuilder key = new StringBuilder();
					for(int i = 1; i < tokens.length; i++) {
						key.append(tokens[i]);
						if (i != tokens.length - 1) {
							key.append(" ");
						}
					}
					String keyString = key.toString();
					byte[] keyBytes = keyString.getBytes();

					if(keyBytes.length > 20) {
						printError("Key length is over 20 bytes!");
					} else {
						if (client.isSubscribed(keyString) == false) {
							System.out.println("You are not subscribed to key: " + keyString);
						} else {
							client.unsubscribeKey(keyString);
							System.out.println("Unsubscribed from key: " + keyString);
						}
					}
				} else {
					printError("Not connected!");
				}
			} else {
				printError("No key provided!");
			}
		} else if (tokens[0].equals("subscriptions")){
			System.out.println("You're subscribed to: ");
			client.printSubscriptions();
		}
		else if(tokens[0].equals("keyrange")){
			// TO DO
			if(tokens.length == 1){
				try {
							
					KVMessage recievedMessage = client.keyrange();
					StatusType returnedStatus = recievedMessage.getStatus();
					if(returnedStatus.equals(StatusType.KEYRANGE_SUCCESS)){
						System.out.println("KEYRANGE_SUCCESS " + recievedMessage.getValue());
					}
					
				} catch (Exception e) {
					printError("get failed: " + e.getMessage());  // Include exception message for more context
				}
			} else {
				printError("wrong number of arguments!");
			}
		} else if(tokens[0].equals("keyrange_read")) {
			if(tokens.length == 1){
				try {
					KVMessage recievedMessage = client.keyrange_read();
					StatusType returnedStatus = recievedMessage.getStatus();
					if(returnedStatus.equals(StatusType.KEYRANGE_READ_SUCCESS)){
						System.out.println("KEYRANGE_READ_SUCCESS " + recievedMessage.getValue());
					}
				} catch (Exception e) {
					printError("get failed: " + e.getMessage());  // Include exception message for more context
				}
			} else {
				printError("wrong number of arguments!");
			}
		}else if(tokens[0].equals("disconnect")) {
			client.disconnect();
			
		} else if(tokens[0].equals("logLevel")) {
			if(tokens.length == 2) {
				String level = setLevel(tokens[1]);
				if(level.equals(LogSetup.UNKNOWN_LEVEL)) {
					printError("No valid log level!");
				} else {
					System.out.println(PROMPT + 
							"Log level changed to level " + level);
				}
			} else {
				printError("Invalid number of parameters!");
			}
			
		} else if(tokens[0].equals("help")) {
			printHelp();
		} else {
			printError("Unknown command");
			printHelp();
		}
	}

	private void sendMessage(String msg){
		try {
			client.sendMessage(new TextMessage(msg));
		} catch (IOException e) {
			printError("Unable to send message!");
			client.disconnect();
		}
	}


    private String setLevel(String levelString) {
		
		if(levelString.equals(Level.ALL.toString())) {
			logger.setLevel(Level.ALL);
			return Level.ALL.toString();
		} else if(levelString.equals(Level.DEBUG.toString())) {
			logger.setLevel(Level.DEBUG);
			return Level.DEBUG.toString();
		} else if(levelString.equals(Level.INFO.toString())) {
			logger.setLevel(Level.INFO);
			return Level.INFO.toString();
		} else if(levelString.equals(Level.WARN.toString())) {
			logger.setLevel(Level.WARN);
			return Level.WARN.toString();
		} else if(levelString.equals(Level.ERROR.toString())) {
			logger.setLevel(Level.ERROR);
			return Level.ERROR.toString();
		} else if(levelString.equals(Level.FATAL.toString())) {
			logger.setLevel(Level.FATAL);
			return Level.FATAL.toString();
		} else if(levelString.equals(Level.OFF.toString())) {
			logger.setLevel(Level.OFF);
			return Level.OFF.toString();
		} else {
			return LogSetup.UNKNOWN_LEVEL;
		}
	}


    private void printError(String error){
		System.out.println(PROMPT + "Error! " +  error);
	}

    private void printHelp() {
		StringBuilder sb = new StringBuilder();
		sb.append(PROMPT).append("ECHO CLIENT HELP (Usage):\n");
		sb.append(PROMPT);
		sb.append("::::::::::::::::::::::::::::::::");
		sb.append("::::::::::::::::::::::::::::::::\n");
		sb.append(PROMPT).append("connect <host> <port>");
		sb.append("\t establishes a connection to a server\n");
		sb.append(PROMPT).append("send <text message>");
		sb.append("\t\t sends a text message to the server \n");
		sb.append(PROMPT).append("disconnect");
		sb.append("\t\t\t disconnects from the server \n");
		
		sb.append(PROMPT).append("logLevel");
		sb.append("\t\t\t changes the logLevel \n");
		sb.append(PROMPT).append("\t\t\t\t ");
		sb.append("ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF \n");
		
		sb.append(PROMPT).append("quit ");
		sb.append("\t\t\t exits the program");
		System.out.println(sb.toString());
	}


	public static void main(String[] args) {
    	try {
			new LogSetup("logs/client.log", Level.INFO);
			KVClient kvClient = new KVClient();
			kvClient.run();
		} catch (IOException e) {
			System.out.println("Error! Unable to initialize logger!");
			e.printStackTrace();
			System.exit(1);
		}
    }


}
