package app_kvServer;

import shared.Metadata;
import java.io.*;
import java.io.ObjectInputFilter.Status;
import java.net.Socket;
import java.security.NoSuchAlgorithmException;

import app_kvServer.KVServer.State;
import ecs.ECS;
import shared.messages.KVMessage.StatusType;
import shared.messages.KVMessage;
import shared.messages.KVMessageImpl;
import shared.messages.MessageWrapper;

import org.apache.log4j.Logger;


public class ECSConnection{

    public Socket ECSSocket;
    private ObjectOutputStream out;
    private ObjectInputStream in;
    private KVServer kvServer;
    public OutputStream toECS;
	public InputStream fromECS;
    private volatile boolean isRunning = true;
    private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 1024 * BUFFER_SIZE;
    private Thread heartbeatThread;
    private static Logger logger = Logger.getRootLogger();
    public int heartbeatPort;
    
    public ECSConnection(String ecsAddress, int ecsPort, KVServer kvServer) {
        try {
            this.kvServer = kvServer;
            ECSSocket = new Socket(ecsAddress, ecsPort);
            out = new ObjectOutputStream(ECSSocket.getOutputStream());
            in = new ObjectInputStream(ECSSocket.getInputStream());
            System.out.println("ECSConnection created");
        } catch (IOException e) {
            System.err.println("Error in ECSConnection constructor:");
            e.printStackTrace();
        }
    }
    

    public void stopListening() {
        isRunning = false; /// also cause heartbeatThread to end and clean up heartbeatThread resources
        try {
            // Close the socket to force the listening threads to exit their loops
            ECSSocket.close();
        } catch (IOException e) {
            System.err.println("Error closing ECS socket: " + e.getMessage());
        }
    }
    

    public void startHeartbeat() {
        heartbeatThread = new Thread(() -> {
            try {
                Socket heartbeatSocket = new Socket(ECSSocket.getInetAddress(), heartbeatPort); // Assuming ECS  i can listens on the next port for heartbeats
                ObjectOutputStream heartbeatOut = new ObjectOutputStream(heartbeatSocket.getOutputStream());

                while (isRunning) {
                    KVMessage message = new KVMessageImpl(kvServer.getHostname(),Integer.toString(kvServer.getPort()),null);
                    MessageWrapper wrapper = new MessageWrapper(MessageWrapper.MessageType.ALIVE, message);
                    //MessageWrapper wrapper = new MessageWrapper(MessageWrapper.MessageType.ALIVE, null);
                    heartbeatOut.writeObject(wrapper);
                    heartbeatOut.flush();
                    Thread.sleep(5000); // Send a message that its alive ever 5 secs
                }
                heartbeatOut.close();
                heartbeatSocket.close();
            } catch (Exception e) {
                System.err.println("Error in heartbeat mechanism: " + e.getMessage());
                // idk what to put in here think of it later
            }
        });
        heartbeatThread.start();
    }

// Ensure to properly start the heartbeat in your connection setup and handle its termination gracefully

    public void sendKVMessageToECS(KVMessageImpl msg) throws IOException {
        MessageWrapper wrapper = new MessageWrapper(MessageWrapper.MessageType.KV_MESSAGE, msg);
        out.writeObject(wrapper);
        out.flush();
        logger.info("SEND \t<" 
			    	+ ECSSocket.getInetAddress().getHostAddress() + ":" 
			    	+ ECSSocket.getPort() + ">: '" 
			    	+"'");
    }

    public void listenForAnything() {
        new Thread(() -> {
            while (isRunning) {
                try {
                    Object receivedObject = in.readObject();
                    if (receivedObject instanceof MessageWrapper) {
                        MessageWrapper wrapper = (MessageWrapper) receivedObject;
                        if (wrapper.getMessageType() == MessageWrapper.MessageType.KV_MESSAGE) {
                            KVMessageImpl message = (KVMessageImpl) wrapper.getMessage();
                            // Handle KVMessage
                            if (message.getStatus() == StatusType.ECS_LOCK_WRITE) {
                                
                                kvServer.setState(State.WRITE_LOCKED);
                                System.out.println("received writelock");
                            } else if (message.getStatus() == StatusType.ECS_TRANSFER_DATA) {
                                String[] dataParts = message.getValue().split(":");
                                if (dataParts.length == 3) {
                                    String targetServerAddress = message.getKey();
                                    int targetServerPort = Integer.parseInt(dataParts[0]);
                                    String startHash = dataParts[1];
                                    String endHash = dataParts[2];
                                    if (kvServer.transferData(targetServerAddress, targetServerPort, startHash, endHash)) {
                                        sendKVMessageToECS(new KVMessageImpl(null, null, StatusType.DATA_TRANSFER_COMPLETE));
                                    }
                                }
                            } else if (message.getStatus() == StatusType.ECS_UNLOCK_WRITE) {
                                kvServer.setState(State.ACTIVE);
                                System.out.println("write unlocked");
                                sendKVMessageToECS(new KVMessageImpl(null, kvServer.address+":"+kvServer.port, StatusType.READY_TO_WRITE));
                            } else if (message.getStatus() == StatusType.ECS_START) {
                                kvServer.setState(State.ACTIVE);
                                System.out.println("CLEAR TO START");
                            } else if (message.getStatus() == StatusType.GOODBYE) {
                                kvServer.running = false;
                            } else if (message.getStatus() == StatusType.ECS_UPDATE_METADATA) {
                                System.out.println("GETTING METADATA UPDATE FROM ECS");
                                String metaString = message.getValue();
                                Metadata metaObject = null;
                                try{
                                    metaObject = Metadata.fromString(metaString);
                                } catch (NoSuchAlgorithmException n){
                                    logger.info("No such algo exceotion");
                                }
                                kvServer.updateMetadata(metaObject);
                                System.out.println(kvServer.metaData.toString());
                            }else if (message.getStatus() == StatusType.HEARTBEAT_PORT) {
                                heartbeatPort=Integer.parseInt(message.getKey());
                                startHeartbeat();
                            }else if (message.getStatus() == StatusType.BROADCAST_PUT){
                                System.out.println("Some client updated key: " + message.getKey());
                                // Now send a message to all the clients
                                for (ClientConnectionServer connection : kvServer.clientConnections.values()){
                                    connection.sendKVMessage(new KVMessageImpl(message.getKey(), message.getValue() , StatusType.SUBSCRIPTION_UPDATE));
                                }
                            }
                            else {
                                // Handle other types of messages or ignore
                            } 
                        } 
                    }
                } catch (IOException | ClassNotFoundException e) {
                    System.err.println("Error listening for messages from ECS: " + e.getMessage());
                    stopListening();
                    break; // Exit the loop if there's an error (e.g., connection closed)
                }
            }
        }).start();
    }
    


    public void close() throws IOException {
        ECSSocket.close();
    }
}
