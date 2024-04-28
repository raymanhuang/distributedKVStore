package ecs;

import java.io.InputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.io.OutputStream;
import java.io.ObjectInputFilter.Status;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.sql.SQLInvalidAuthorizationSpecException;
import java.time.chrono.IsoChronology;
import java.security.NoSuchAlgorithmException;
import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;

import shared.messages.KVMessage;
import shared.messages.KVMessageImpl;
import shared.messages.TextMessage;
import shared.MD5Hash;
import shared.Metadata;
import shared.messages.MessageWrapper;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import logger.LogSetup;

import shared.messages.KVMessage.StatusType;

public class ServerConnection implements Runnable {

    private static Logger logger = Logger.getRootLogger();
    public Socket kvServerSocket;

    private ObjectOutputStream ObjectOut;
    private ObjectInputStream ObjectIn;
    private boolean isOpen;
    public ECS ecs;
    public int dataTransferPort;
    private Thread heartbeatListenerThread;
    public ServerSocket heartbeatServerSocket;
    public ObjectInputStream heartbeatIn;
    public int heartbeatPort;

	private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 128 * BUFFER_SIZE;

    public ServerConnection(Socket ServerSocket, ECS ecs){
        this.kvServerSocket = ServerSocket;
        this.ecs = ecs;
        this.isOpen = true;
    }

    public void run(){
        try {
            ObjectOut = new ObjectOutputStream(kvServerSocket.getOutputStream());
            ObjectIn = new ObjectInputStream(kvServerSocket.getInputStream());
            listenForHeartbeat();
            logger.info("Connection established with " 
                    + kvServerSocket.getInetAddress().getHostName() 
                    + " on port " + kvServerSocket.getPort());
            
            while(isOpen) {
                try{
                    KVMessage receivedMessage = receiveMessage();
                    KVMessage responseMessage = processRequest(receivedMessage);
                } catch (IOException ioe) {
                    logger.error("Error! Connection lost or unable to read message.", ioe);
                    isOpen = false;
                }
            }
        } catch (IOException e) {
            logger.error("Error! Unable to establish connection streams.", e);
        } finally {
            try{
                closeConnection();
            } catch(IOException i){
                logger.info("IOException");
            }
        }
    }

    

    public void listenForHeartbeat() {
        heartbeatListenerThread = new Thread(() -> {
            String serverInfo = null;
            try {
                heartbeatServerSocket = new ServerSocket(0); // System finds an available port
                heartbeatPort = heartbeatServerSocket.getLocalPort(); // Retrieve the assigned port
                System.out.println("Heartbeat listening on port: " + heartbeatPort);
                KVMessage message = new KVMessageImpl(Integer.toString(heartbeatPort),null,StatusType.HEARTBEAT_PORT);
                MessageWrapper portMessage = new MessageWrapper(MessageWrapper.MessageType.KV_MESSAGE, message);
                
                ObjectOut.writeObject(portMessage);       
                ObjectOut.flush();
                
                Socket heartbeatSocket = heartbeatServerSocket.accept();
                ObjectInputStream heartbeatIn = new ObjectInputStream(heartbeatSocket.getInputStream());

                while (isOpen) {
                    try {
                        heartbeatSocket.setSoTimeout(7000); //maybe 10 seconds timeout (idk if it might take more time from it to send to recieve so i just doubled it)
                        MessageWrapper wrapper = (MessageWrapper) heartbeatIn.readObject();
                        if(wrapper.getMessageType() != MessageWrapper.MessageType.ALIVE) {
                            System.out.println("Heartbeat message was not alive");
                        }else{
                            KVMessageImpl heartbeatMessage= (KVMessageImpl) wrapper.getMessage();
                            
                            System.out.println("Heartbeat was recieved from server: <" + heartbeatMessage.getKey()+":"+ heartbeatMessage.getValue()+">");
                            serverInfo = heartbeatMessage.getKey() + ":"+ heartbeatMessage.getValue();
                        }
                        
                    } catch (SocketTimeoutException E) {
                        String serverHash = null;
                        try {
                           serverHash = MD5Hash.hashString(serverInfo);
                        } catch (NoSuchAlgorithmException n) {
                            // TODO: handle exception
                        }
                        System.out.println("Server with hash " + serverHash + " died");
                        ecs.serverConnections.remove(serverHash);
                        ecs.metaData.removeServer(serverHash);
                        updateAllKVServersMetadata(ecs.metaData);
                        isOpen = false;
                        closeConnection(); //not entirely sure what to do here, prob more steps but i just close the serverconnection rn
                        break;
                    }
                }
                heartbeatIn.close();
                heartbeatSocket.close();
                System.out.println("Server shutdown gracefully... Stopping heartbeat listener");
            } catch (Exception e) {
                System.err.println("Error listening for heartbeat: " + e.getMessage());
            }
        });
        heartbeatListenerThread.start();
    }

    


    public KVMessageImpl receiveMessage() throws IOException {
        try{
            Object response = ObjectIn.readObject();
			if (response instanceof MessageWrapper) {
				// Handle the message based on its type
				MessageWrapper wrapper = (MessageWrapper) response;
				if(wrapper.getMessageType() == MessageWrapper.MessageType.KV_MESSAGE){
					KVMessageImpl message = (KVMessageImpl) wrapper.getMessage();

                    logger.info("RECEIVE \t<" 
                    + kvServerSocket.getInetAddress().getHostAddress() + ":" 
                    + kvServerSocket.getPort() + ">: " 
                    + message.getStatus().name() + " " + message.getKey() + " " + message.getValue());
					return message;
				}
			}
        } catch(ClassNotFoundException C){
            logger.info("ClassNotFoundException");
        }
        return null;
    }


    private KVMessage processRequest(KVMessage msg){
        if(msg != null && msg.getStatus() != null){
            logger.info("Message received of type " + msg.getStatus().name());
        }
        if(msg.getStatus() == StatusType.ECS_JOIN){
            try{
                // compute the KVServers hash ranges, and add it to the metadata
                System.out.println("KVServer with info: " + msg.getKey() + " is joining ECS");
                String toBeHashed = msg.getKey();
                String[] parts = toBeHashed.split(":"); // addressOfNewServer:portOfNewServer
                String newServerHash = null;
                try{
                    newServerHash = MD5Hash.hashString(toBeHashed);
                } catch(NoSuchAlgorithmException n){
                    logger.info("NoSuchAlgorithmException");
                }

                int newKVServerDataTransferPort = Integer.parseInt(msg.getValue());
                IECSNode newServer = null;
                try{
                    newServer = new ECSNode(parts[0], Integer.parseInt(parts[1]), newKVServerDataTransferPort, null);
                } catch(NoSuchAlgorithmException n){
                    logger.info("NoSuchAlgorithmException");
                }        

                IECSNode successor = ecs.metaData.getSuccessor(newServerHash);
                if (successor != null) {
                    ServerConnection successorConnection = ecs.serverConnections.get(successor.getNodeName());
                    if (successorConnection != null) {
                        KVMessageImpl lockMessage = new KVMessageImpl(null, null, StatusType.ECS_LOCK_WRITE);
                        successorConnection.sendKVMessage(lockMessage);
                        ecs.metaData.getSuccessor(newServerHash).setWriteLocked(true);
                        logger.info("Write lock sent to successor: " + successor.getNodeName());
                        String newServerStartHash = ecs.metaData.incrementHex(successor.getNodeName());//this what i changedWOW
                        KVMessageImpl transferDataMessage = new KVMessageImpl(parts[0], // addressOfNewServer
                                                                        Integer.toString(newServer.getDataTransferPort())+":"+newServerStartHash+":"+newServerHash, 
                                                                        StatusType.ECS_TRANSFER_DATA);
                        successorConnection.sendKVMessage(transferDataMessage);
                        ecs.metaData.addServer(newServerHash, newServer);
                        ecs.addServerConnection(newServerHash, this);
                        // KVMessage responseFromSuccessor = successorConnection.receiveMessage();
                        // if (responseFromSuccessor.getStatus().equals(StatusType.DATA_TRANSFER_COMPLETE)){
                        //     System.out.println("New server has received data");
                        // }
                        KVMessageImpl unlockMessage = new KVMessageImpl(null, null, StatusType.ECS_UNLOCK_WRITE);  
                        successorConnection.sendKVMessage(unlockMessage);
                        // responseFromSuccessor = successorConnection.receiveMessage();
                        // if (responseFromSuccessor.getStatus().equals(StatusType.READY_TO_WRITE)){
                        //     System.out.println("Successor is ready to be written to.");
                        // }
                        while(ecs.metaData.getSuccessor(newServerHash).getWriteLocked() == true){
                            //wait
                            System.out.println("waiting for write unlock...");
                            try {
                                Thread.sleep(1000); // Sleep for 1 second
                            } catch (InterruptedException e) {
                                // Handle the interrupted exception
                                Thread.currentThread().interrupt(); // Re-interrupt the thread if it was interrupted during sleep
                            }
                        }
                        logger.info("Write lock released on successor: " + successor.getNodeName());
                        updateAllKVServersMetadata(ecs.metaData);                          
                        logger.info("Metadata update sent to all servers.");
                        KVMessageImpl startNewServerMessage = new KVMessageImpl(null, null, StatusType.ECS_START);
                        this.sendKVMessage(startNewServerMessage);
                    } 
                }
                else{
                    System.out.println("No successor right now");
                    ecs.metaData.addServer(newServerHash, newServer);
                    ecs.addServerConnection(newServerHash, this);
                    updateAllKVServersMetadata(ecs.metaData);
                    KVMessageImpl startNewServerMessage = new KVMessageImpl(null, null, StatusType.ECS_START);
                    this.sendKVMessage(startNewServerMessage);
                }
            } catch(IOException i){
                logger.info("IOException");
            }
        } else if(msg.getStatus() == StatusType.ECS_SHUTDOWN){
            try{
                String toBeHashed = msg.getKey();
                String[] parts = toBeHashed.split(":"); // addressOfServerToBeRemoved:portOfServerToBeRemoved
                String kvServerAddress = parts[0];
                int kvServerPort = Integer.parseInt(parts[1]);
                String serverToBeRemovedHash = null;
                try{
                    serverToBeRemovedHash = MD5Hash.hashString(toBeHashed); 
                } catch(NoSuchAlgorithmException n){
                    logger.info("NoSuchAlgorithmException");
                }
                   
                IECSNode toBeRemoved = ecs.metaData.getServerInfo(serverToBeRemovedHash);
                IECSNode successor = ecs.metaData.getSuccessor(serverToBeRemovedHash);
                ecs.metaData.removeServer(serverToBeRemovedHash);
    
                if(successor != null){
                    System.out.println("THERE IS A SUCCESSOR: TELLING LEAVING NODE TO SEND DATA TO SUCCESSOR");
                    System.out.println("Successor: " + successor.getNodeName());
                    ServerConnection successorConnection = ecs.serverConnections.get(successor.getNodeName());
                    // KVMessageImpl transferDataMessage = new KVMessageImpl(successor.getNodeHost(), 
                    //                                     Integer.toString(successor.getDataTransferPort())+":"+toBeRemoved.getNodeHashRange()[0]+":"+toBeRemoved.getNodeHashRange()[1], 
                    //                                     StatusType.ECS_TRANSFER_DATA);
                    // sendKVMessage(transferDataMessage);
                    // KVMessage responseFromServer = receiveMessage();
                    // if(responseFromServer.getStatus().equals(StatusType.DATA_TRANSFER_COMPLETE)){
                    //     KVMessageImpl goodbyeMessage = new KVMessageImpl(null, null, StatusType.GOODBYE);
                    //     sendKVMessage(goodbyeMessage);
                    // }
                    ecs.serverConnections.remove(serverToBeRemovedHash);
                    ecs.metaData.removeServer(serverToBeRemovedHash);
                    updateAllKVServersMetadata(ecs.metaData);
                    KVMessageImpl goodbyeMessage = new KVMessageImpl(null, null, StatusType.GOODBYE);
                    sendKVMessage(goodbyeMessage);
                    isOpen = false;
                    closeConnection();
                } else {
                    KVMessageImpl goodbyeMessage = new KVMessageImpl(null, null, StatusType.GOODBYE);
                    sendKVMessage(goodbyeMessage);
                    ecs.serverConnections.remove(serverToBeRemovedHash);
                    ecs.metaData.removeServer(serverToBeRemovedHash);
                    updateAllKVServersMetadata(ecs.metaData);
                    isOpen = false;
                    closeConnection();
                }    
            } catch(IOException i){
                logger.info("IOException");
            }
        } else if(msg.getStatus() == StatusType.FAILED){
            return new KVMessageImpl(null, null, StatusType.FAILED);
        } else if(msg.getStatus() == StatusType.READY_TO_WRITE){
            String serverHash = null;
            try {
                serverHash = MD5Hash.hashString(msg.getValue());
            } catch (Exception e) {
                // TODO: handle exception
            }
            if (ecs.metaData.hashRing.containsKey(serverHash)) {
                ecs.metaData.hashRing.get(serverHash).setWriteLocked(false);
            } else {
                System.out.println("server not found");
            }
            
        } else if (msg.getStatus() == StatusType.CLIENT_PUT){
            System.out.println("A client put key: " + msg.getKey());
            // Send a message to all KVServers that there was a put
            KVMessageImpl broadcastPutMessage = new KVMessageImpl(msg.getKey(), msg.getValue(), StatusType.BROADCAST_PUT);
            for (ServerConnection serverConnection : ecs.serverConnections.values()) {
                try{
                    serverConnection.sendKVMessage(broadcastPutMessage);
                } catch (IOException i){

                }
            }
        }
        return null;
    }

    public synchronized void updateAllKVServersMetadata(Metadata metadata) throws IOException{
        for (ServerConnection serverConnection : ecs.serverConnections.values()) {
            System.out.println(metadata.toString());
            String metaString = metadata.toString();
            serverConnection.sendKVMessage(new KVMessageImpl(null, metaString, StatusType.ECS_UPDATE_METADATA));
        }
    }

    public void sendKVMessage(KVMessageImpl msg) throws IOException {
        MessageWrapper wrapper = new MessageWrapper(MessageWrapper.MessageType.KV_MESSAGE, msg);
        ObjectOut.writeObject(wrapper);
        logger.info("SEND \t<" 
			    	+ kvServerSocket.getInetAddress().getHostAddress() + ":" 
			    	+ kvServerSocket.getPort() + ">: '" 
			    	+ "'");
        
    }

    private void closeConnection() throws IOException {
        try {
            if (heartbeatIn != null) {
                heartbeatIn.close();
            }
            if (heartbeatServerSocket != null && !heartbeatServerSocket.isClosed()) {
                heartbeatServerSocket.close();
            }
            if (kvServerSocket != null && !kvServerSocket.isClosed()) {
                kvServerSocket.close();
            }
        } catch (IOException e) {
            logger.error("Error closing connections", e);
        }
    }
    

}
