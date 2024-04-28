package app_kvServer;

import java.net.BindException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.Map;
import java.util.HashSet;
import java.util.Set;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.ObjectInputStream;
import java.math.*;
import java.io.ObjectOutputStream;
import java.security.NoSuchAlgorithmException;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import logger.LogSetup;

import shared.messages.TextMessage;
import shared.messages.KVMessage.StatusType;
import shared.messages.MessageWrapper.MessageType;
import shared.messages.KVMessage;
import shared.messages.KVMessageImpl;
import shared.messages.MessageWrapper;
import app_kvServer.cache.cacheInterface;
import app_kvServer.cache.cacheFIFO;
import app_kvServer.cache.cacheLRU;
import app_kvServer.cache.cacheLFU;
import app_kvServer.storage.DiskStorage;
import app_kvServer.ECSConnection;
import ecs.ECS;
import ecs.IECSNode;
import app_kvServer.ClientConnectionServer;
import shared.MD5Hash;
import shared.Metadata;


public class KVServer implements IKVServer, Runnable{
	/**
	 * Start KV Server at given port
	 * @param port given port for storage server to operate
	 * @param cacheSize specifies how many key-value pairs the server is allowed
	 *           to keep in-memory
	 * @param strategy specifies the cache replacement strategy in case the cache
	 *           is full and there is a GET- or PUT-request on a key that is
	 *           currently not contained in the cache. Options are "FIFO", "LRU",
	 *           and "LFU".
	 */

	private static Logger logger = Logger.getRootLogger();

	public int port;
	public String address;
	private int cacheSize;
	private String strategy; 
	public static String directory = "data";
	private ServerSocket serverSocket;
	public cacheInterface cache;
	private DiskStorage diskStorage;
	public volatile boolean running;
	private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 1024 * BUFFER_SIZE;
	public boolean port_available;

	private String ECSAddress;
	public ECSConnection ecsConnection;
	private ServerSocket dataTransferSocket;
	public int dataTransferPort; 
	private Thread dataTransferListenerThread;
	private int ECSPort;
	private Socket ECSSocket;
	public volatile Metadata metaData;
	public OutputStream toECS;
	public InputStream fromECS;
	public State state;
	public String hashRange[];
	
	public String getResponsibilityRange[];

	public ConcurrentHashMap<Socket, ClientConnectionServer> clientConnections;


	public KVServer(int port, int cacheSize, String strategy, String ECSAddress, int ECSPort) {
		this.port = port;
		this.address = "0.0.0.0";
		this.cacheSize = cacheSize;
		this.ECSAddress = ECSAddress;
		this.ECSPort = ECSPort;
		this.state = State.STOPPED;
		this.clientConnections = new ConcurrentHashMap<>();
		try {
			// Attempt to map the string to an enum value
			CacheStrategy cacheStrategy = CacheStrategy.valueOf(strategy.toUpperCase());
			this.strategy = strategy; // Set the validated strategy
	
			// Initialize the cache based on the strategy
			switch (cacheStrategy) {
				case LRU:
					this.cache = new cacheLRU(cacheSize);
					break;
				case FIFO:
					this.cache = new cacheFIFO(cacheSize);
					break;
				case LFU:
					this.cache = new cacheLFU(cacheSize);
				default:
					this.cache = null; // or some default caching strategy
					break;
			}
		} catch (IllegalArgumentException e) {
			logger.error("Invalid cache strategy provided: " + strategy + ". Defaulting to 'None'", e);
			this.strategy = "None";
			this.cache = null; // Set to default caching strategy (None)
		}
	
		new Thread(this).start();
	}

	public enum State{
		STOPPED,
		WRITE_LOCKED,
		ACTIVE
	}

	public void setState(State state){
		this.state = state;
	}

	public boolean connectToECS() {
		
			System.out.println("Trying to connect to ecs");
			ecsConnection = new ECSConnection(ECSAddress, ECSPort, this);
			System.out.println("Connected to ecs");
			ecsConnection.listenForAnything();
			// ecsConnection.startHeartbeat();
			return true;

	}
	
	public void updateMetadata(Metadata metadata) {
		String serverHash = null;
		try {
			serverHash = MD5Hash.hashString(address + ":" + port);
		} catch (NoSuchAlgorithmException e) {
			// TODO: handle exception
		}
		this.metaData = metadata;
		// Additional logic to handle the updated metadata (e.g., key range changes)
		try{
			this.hashRange = metaData.getHashRing().get(MD5Hash.hashString(address + ":" + port)).getNodeHashRange();
			// need to also update getResponsibilityRange
			if (metaData.hashRing.size() <= 3){
				setGetResponsibilityRange(new String[]{"00000000000000000000000000000000", "ffffffffffffffffffffffffffffffff"});
				dataTransferToTwoSuccessors();
			} else {
				// this.getResponsibilityRange[1] = serverHash;
				IECSNode father = metaData.getPredecessor(serverHash);
				IECSNode grandFather = metaData.getPredecessor(father.getNodeName());
				IECSNode greatGrandFather = metaData.getPredecessor(grandFather.getNodeName());
				setGetResponsibilityRange(new String[]{metaData.incrementHex(greatGrandFather.getNodeName()), serverHash});
				System.out.println("GetResponsibilityRange: " + getResponsibilityRange[0] + " to " + getResponsibilityRange[1]);

				// Now delete everything that is not in the new responsibility range
				Map<String, String> dataToBeKept = getDataForTransfer(getGetResponsibilityRange()[0], getGetResponsibilityRange()[1]);
				Set<String> allKeys = null;
				try {
					allKeys = diskStorage.getAllKeys();
				} catch (IOException e) {
					// TODO: handle exception
				}
				for (String key : allKeys) {
					if (!dataToBeKept.containsKey(key)){
						diskStorage.delete(key);
					}
				}
				// Now send to 2 successors
				dataTransferToTwoSuccessors();
			}
			
		} catch(NoSuchAlgorithmException e){
			logger.info("NoSuchAlgorithmException");
		}
		
	}

	private void initializeDataTransferListener() {
		dataTransferListenerThread = new Thread(() -> {
			try {
				dataTransferSocket = new ServerSocket(0);
				this.dataTransferPort = dataTransferSocket.getLocalPort();
				while (!Thread.currentThread().isInterrupted()) {
					Socket otherServerSocket = dataTransferSocket.accept();
					handleDataTransfer(otherServerSocket);
				}
			} catch (IOException e) {
				// Handle exceptions
			}
		});
		dataTransferListenerThread.start();
	}
	

	private void handleDataTransfer(Socket otherServerSocket) {
		try (ObjectInputStream ois = new ObjectInputStream(otherServerSocket.getInputStream())) {
			// Read the first object, which should be a MessageWrapper for the KVMessage
			Object firstObject = ois.readObject();
			if (firstObject instanceof MessageWrapper) {
				MessageWrapper firstWrapper = (MessageWrapper) firstObject;
				if (firstWrapper.getMessageType() == MessageWrapper.MessageType.KV_MESSAGE) {
					KVMessageImpl message = (KVMessageImpl) firstWrapper.getMessage();
					if (message.getStatus() == StatusType.DELETE_RANGE) {
						// Process the DELETE_RANGE message
						String[] deleteRange = ((String) message.getValue()).split(":");
						Map<String, String> dataToBeDeleted = getDataForTransfer(deleteRange[0], deleteRange[1]);
						for (String key : dataToBeDeleted.keySet()) {
							diskStorage.delete(key);
						}
						System.out.println("Processed DELETE_RANGE message.");
					}
				}
			}
	
			// Read the second object, which should be a MessageWrapper for the data map
			Object secondObject = ois.readObject();
			if (secondObject instanceof MessageWrapper) {
				MessageWrapper secondWrapper = (MessageWrapper) secondObject;
				if (secondWrapper.getMessageType() == MessageWrapper.MessageType.DATA_MAP) {
					Map<String, String> data = (Map<String, String>) secondWrapper.getMessage();
					// Store the received data
					for (Map.Entry<String, String> entry : data.entrySet()) {
						diskStorage.put(entry.getKey(), entry.getValue());
					}
					System.out.println("Received data map and stored it.");
				}
			}
		} catch (IOException | ClassNotFoundException e) {
			// Handle exceptions
			e.printStackTrace();
		}
	}
	
	
	public Map<String, String> getDataForTransfer(String startHash, String endHash) {
		// Retrieve key-value pairs from the disk storage within the specified hash range
		try{
			return diskStorage.getRange(startHash, endHash);
		} catch(IOException E){
			logger.info("IOException");
		}
		return null;
	}

	// This version empties the bin of the sender
	public boolean transferData(String targetServerAddress, int targetServerPort, String startHash, String endHash) {
		try (Socket socket = new Socket(targetServerAddress, targetServerPort);
			 ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream())) {
			// Retrieve the data to be sent
			Map<String, String> data = getDataForTransfer(startHash, endHash);
	
			for (String key : data.keySet()) {
				diskStorage.delete(key);
			} // may not need
			// First send a message with status DELETE_RANGE
			KVMessageImpl deleteRangeMessage = new KVMessageImpl(null, startHash + ":" + endHash, StatusType.DELETE_RANGE);
			MessageWrapper wrapper = new MessageWrapper(MessageWrapper.MessageType.KV_MESSAGE, deleteRangeMessage);
			oos.writeObject(wrapper);
			oos.flush();

			// Then send the data
			wrapper = new MessageWrapper(MessageWrapper.MessageType.DATA_MAP, data);
			oos.writeObject(wrapper);
			oos.flush();
			System.out.println("Sent data map to successor.");
			return true; // Indicate success
		} catch (IOException e) {
			// Handle exceptions
			return false; // Indicate failure
		}
	}

		// This version does not empty the bin of the sender

	public boolean transferDataWithoutEmptying(String targetServerAddress, int targetServerPort, String startHash, String endHash) {
		try (Socket socket = new Socket(targetServerAddress, targetServerPort);
			 ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream())) {
			// Retrieve the data to be sent
			Map<String, String> data = getDataForTransfer(startHash, endHash);

			// First send a message with status DELETE_RANGE
			KVMessageImpl deleteRangeMessage = new KVMessageImpl(null, startHash + ":" + endHash, StatusType.DELETE_RANGE);
			MessageWrapper wrapper = new MessageWrapper(MessageWrapper.MessageType.KV_MESSAGE, deleteRangeMessage);
			oos.writeObject(wrapper);
			oos.flush();

			// Then send the data
			wrapper = new MessageWrapper(MessageWrapper.MessageType.DATA_MAP, data);
			oos.writeObject(wrapper);
			oos.flush();
			System.out.println("Sent data map to successor.");
			return true; // Indicate success
		} catch (IOException e) {
			// Handle exceptions
			return false; // Indicate failure
		}
	}


	public void dataTransferToTwoSuccessors() throws NoSuchAlgorithmException {
		// Hash the server's address and port
		String serverHash = MD5Hash.hashString(address + ":" + port);
		
		// Go through metadata, find 2 successors, and do transferDataWithoutEmptying
		// First successor
		System.out.println(metaData.toString());
		IECSNode successorNode = metaData.getSuccessor(serverHash);
		if (successorNode != null) {
			System.out.println("Found first successor hash: " + successorNode.getNodeName());
			if (transferDataWithoutEmptying(successorNode.getNodeHost(), successorNode.getDataTransferPort(), hashRange[0], hashRange[1])) {
				System.out.println("Sent fresh data to first successor");
			}
		} else {
			return;
		}
		// Second successor
		IECSNode nextSuccessor = metaData.getSuccessor(successorNode.getNodeName());
		if (nextSuccessor != null && !nextSuccessor.getNodeName().equals(serverHash)) {
			System.out.println("Found second successor hash: " + nextSuccessor.getNodeName());
			if (transferDataWithoutEmptying(nextSuccessor.getNodeHost(), nextSuccessor.getDataTransferPort(), hashRange[0], hashRange[1])) {
				System.out.println("Sent fresh data to second successor");
			}
		} else {
			System.out.println("Second Successor not found");
		}
	}
	
	
	private void addShutdownHook(){
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			try {
				if (dataTransferListenerThread != null) {
					dataTransferListenerThread.interrupt();
				}
				if (ecsConnection != null) {
					state = State.STOPPED;
					String kvServerHashInfo = this.address + ":" + this.port;
					KVMessageImpl shutdownMessage = new KVMessageImpl(kvServerHashInfo, null, StatusType.ECS_SHUTDOWN);
					ecsConnection.sendKVMessageToECS(shutdownMessage);
					logger.info("Shutdown message sent to ECS.");
					while(running){
						//wait
					}
					ecsConnection.stopListening();
					serverSocket.close();
					System.out.println("GOODBYE");
				}
			} catch(IOException e){

			}
		}));
	}

	private boolean initializeServer(){
		logger.info("Initialize server ...");
		try{
			if(this.address == "0.0.0.0"){
				serverSocket = new ServerSocket(port);
				this.address = serverSocket.getInetAddress().getHostAddress();
			} else { 

				serverSocket = new ServerSocket(port, 0, InetAddress.getByName(this.address));
				this.address = serverSocket.getInetAddress().getHostAddress();

			} 
			
			logger.info("Server listening on port: " + serverSocket.getLocalPort() + " with address " + this.address);
			return true;
		} catch (IOException ioe) {
			logger.error("Error! Cannot open server socket:");
			if (ioe instanceof BindException){
				//logger.error("Port " + port + " is already bound");
				ioe.printStackTrace();
			}
			
			return false;
		}
	}

	private void initialize_storage(){
		try {
			this.diskStorage = new DiskStorage(address, port);
		} catch (IOException ioe) {
			logger.error("Error creating a new data directory!", ioe);
			throw new RuntimeException("Unable to initialize disk storage", ioe);
		}


	}

	private boolean isRunning(){
		return this.running;
	}

	// Getter for the hash range
	public String[] getHashRange() {
		return hashRange;
	}
	
	// Setter for the hash range
	public void setHashRange(String[] hashRange) {
		this.hashRange = hashRange;
	}

	public String[] getGetResponsibilityRange() {
		return getResponsibilityRange;
	}

	public void setGetResponsibilityRange(String[] getResponsibilityRange){
		this.getResponsibilityRange = getResponsibilityRange;
	}
	
	public void removeClientConnection(Socket clientSocket) {
    	clientConnections.remove(clientSocket);
	}

	@Override
    public void run(){
		try{
			running = initializeServer();
			if(running){
				port_available = true;
				initialize_storage();
				initializeDataTransferListener();
				
				connectToECS();
				addShutdownHook();
				
			}
			String kvServerHashInfo = this.address + ":" + this.port;
			KVMessageImpl joinRequest = new KVMessageImpl(kvServerHashInfo, Integer.toString(this.dataTransferPort), StatusType.ECS_JOIN);
			ecsConnection.sendKVMessageToECS(joinRequest);
			System.out.println("Sent join request with info " + kvServerHashInfo);
		} catch(IOException i){
			logger.info("IOException");
		}

		if(serverSocket != null){
			while(isRunning()){
				try {
					Socket client = serverSocket.accept();
					ClientConnectionServer connection = new ClientConnectionServer(client, this);
					clientConnections.put(client, connection);
					new Thread(connection).start();
					
				} catch (IOException e) {
					logger.error("Error! " + "Unable to establish connection. \n", e);
				}
			}
		}
		System.out.println("Server is not listening for clients");
		logger.info("Server stopped.");
	}
	
	@Override
	public int getPort(){
		return this.port;
	}

	@Override
    public String getHostname(){
		return serverSocket.getInetAddress().getHostName();
	}

	@Override
    public CacheStrategy getCacheStrategy(){
		return CacheStrategy.valueOf(strategy);
	}

	@Override
    public int getCacheSize(){
		return this.cacheSize;
	}

	public cacheInterface getCache(){
		return this.cache;
	}

	@Override
    public boolean inStorage(String key){
		try {
			return diskStorage.get(key) != null;
		} catch (IOException e) {
			logger.error("Error accessing disk storage", e);
			return false;
		}
	}

	@Override
    public boolean inCache(String key){
		return (this.cache != null) && (this.cache.get(key) != null);
	}

	@Override
	public KVMessage getKV(String key)  {
		String value;
		try {
			// Check if the key is in the cache first
			if (this.cache != null) {
				value = this.cache.get(key);
				if (value != null) {
					// Cache hit, return success
					logger.info("CACHE HIT: " + value);
					// this.cache.printCacheContents();
					return new KVMessageImpl(key, value, KVMessage.StatusType.GET_SUCCESS);
				}
				// If value is null, it's not an error, it just means the value is not in the cache
			}
	
			// If the key is not in the cache, check the disk storage
			value = diskStorage.get(key);
			if (value != null) {
				// Disk storage hit, return success
				return new KVMessageImpl(key, value, KVMessage.StatusType.GET_SUCCESS);
			} else {
				// Key does not exist, return error
				return new KVMessageImpl(key, null, KVMessage.StatusType.GET_ERROR);
			}
	
		} catch (Exception e) {
			logger.error("An error occurred during the GET operation", e);
			// Return a GET_ERROR KVMessage with the exception message
			return new KVMessageImpl(key, e.getMessage(), KVMessage.StatusType.GET_ERROR);
		}
	}
	

	@Override
    public KVMessage putKV(String key, String value){
		if (key == null) {
            logger.error("Attempted to write a null key");
            return new KVMessageImpl(null, null, KVMessage.StatusType.PUT_ERROR);
        }
		if (key.length() > 20){
			logger.error("Attempted to write a key that is more than 20 bytes");
			return new KVMessageImpl(null, null, KVMessage.StatusType.PUT_ERROR);
		}
		if (value.length() > 120000){
			logger.error("Attempted to write a value that is more than 120000 bytes");
			return new KVMessageImpl(null, null, KVMessage.StatusType.PUT_ERROR);
		}
		if (this.cache != null) {
			if(value.equals("null")){
				this.cache.delete(key);
			} else { 
				logger.info("PUTTING KV IN CACHE:" + key + " " + value);
				this.cache.put(key, value);
			}
		}
		if(value.equals("null")){
			return diskStorage.delete(key);		
		} else { 
			return diskStorage.put(key, value);
		}
	}

	@Override
    public void clearCache(){
		if (this.cache != null) {
			this.cache.clear();
		}
	}

	@Override
    public void clearStorage(){
		diskStorage.clearStorage();
	}


	@Override
    public void kill(){
		Thread shutdown = new Thread(() -> {
			try {
				if (dataTransferListenerThread != null) {
					dataTransferListenerThread.interrupt();
				}
				if (ecsConnection != null) {
					state = State.STOPPED;
					String kvServerHashInfo = this.address + ":" + this.port;
					KVMessageImpl shutdownMessage = new KVMessageImpl(kvServerHashInfo, null, StatusType.ECS_SHUTDOWN);
					ecsConnection.sendKVMessageToECS(shutdownMessage);
					logger.info("Shutdown message sent to ECS.");
					while(running){
						//wait
					}
					ecsConnection.stopListening();
					serverSocket.close();
					System.out.println("GOODBYE");
				}
			} catch(IOException e){

			}
		});
		shutdown.start();
	}

	@Override
    public void close(){
		try {
			if (serverSocket != null) {
				serverSocket.close();
			}
		} catch (IOException e) {
			logger.error("Error! Unable to close the server socket", e);
		}
	}


	public static void main(String[] args){
		try {
			new LogSetup("logs/server.log", Level.ALL);
				if(args.length == 5){
					int port = Integer.parseInt(args[0]);
					int cacheSize = Integer.parseInt(args[1]);
					String strategy = args[2];
					String ECSAddress = args[3];
					int ECSPort = Integer.parseInt(args[4]);
					KVServer server = new KVServer(port, cacheSize, strategy, ECSAddress, ECSPort);
				} else if(args[0].startsWith("-")){
					int port = 50000; 
					int cacheSize = 10; 
					String strategy = "FIFO"; 
					String address = "localhost"; 
					String ECSAddress = "localhost"; 
					int ECSPort = 40000; 
					String logfile = "";
					
					for(int i=0; i < args.length - 1; i += 2){
						if(args[i].equals("-p")){ // port number
							port = Integer.parseInt(args[i+1]); 
							System.out.println("Recognized port " + port);
						} else if (args[i].equals("-a")){ // address 
							address = args[i+1];
						} else if (args[i].equals("-s")){ // cache strategy
							strategy = args[i+1]; 
						} else if (args[i].equals("-c")){ // cache size
							cacheSize = Integer.parseInt(args[i+1]); 
						} else if (args[i].equals("-d")){ // directory
							directory = args[i+1]; 
							System.out.println("Recognized directory " + directory);
						} else if (args[i].equals("-b")){ // ecs server
							String[] address_port = args[i+1].split(":");
							ECSAddress = address_port[0]; 
							ECSPort = Integer.parseInt(address_port[1]);  
						} else if (args[i].equals("-l")){ // logfile
							
						} else if (args[i].equals("-ll")){ // loglevel 
							if(LogSetup.isValidLevel(args[i+1])){
								logger.setLevel(LogSetup.stringToLevel(args[i+1]));
							}
						}
					}
					KVServer server = new KVServer(port, cacheSize, strategy, ECSAddress, ECSPort);	
					server.address = address; 
				} else{
					System.out.println("Usage: Server <port> <cacheSize> <strategy> <ECSAddress> <ECSPort>");
				}
				//new Thread(server).start();
			
		} catch (IOException e) {
			System.out.println("Error! Unable to initialize logger!");
			e.printStackTrace();
			System.exit(1);
		} catch (NumberFormatException nfe) {
			System.out.println("Error! Invalid numeric argument!");
			System.out.println("Usage: Server <port> <cacheSize> <strategy>");
			System.exit(1);
		} catch (Exception e) {
			System.out.println("Unexpected error: " + e.getMessage());
			e.printStackTrace();
			System.exit(1);
		}
	}
}