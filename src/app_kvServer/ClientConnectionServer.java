package app_kvServer;

import java.io.InputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.security.NoSuchAlgorithmException;
import java.util.Map;

import shared.messages.KVMessage;
import shared.messages.KVMessageImpl;
import shared.messages.TextMessage;
import shared.messages.MessageWrapper;
import shared.MD5Hash;
import shared.Metadata;
import ecs.IECSNode;

import org.apache.log4j.Logger;

import app_kvServer.KVServer.State;
import shared.messages.KVMessage.StatusType;

/**
 * Represents a connection end point for a particular client that is 
 * connected to the server. 
 */

public class ClientConnectionServer implements Runnable{

    private static Logger logger = Logger.getRootLogger();

    private Socket clientSocket;
    private ObjectInputStream input;
    private ObjectOutputStream output;
    private boolean isOpen;
    private KVServer kvServer;
    public static boolean useTextMessages = false;

	private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 128 * BUFFER_SIZE;


    public ClientConnectionServer(Socket clientSocket, KVServer kvServer){
        this.clientSocket = clientSocket;
        this.kvServer = kvServer;
        this.isOpen = true;
    }

    public void run(){
        try {
            output = new ObjectOutputStream(clientSocket.getOutputStream());
            input = new ObjectInputStream(clientSocket.getInputStream());

            logger.info("Connection established with " 
                    + clientSocket.getInetAddress().getHostName() 
                    + " on port " + clientSocket.getPort());
            // 1) SEND METADATA TO THE NEW CLIENT (KVSTORE)
            sendMetadata(kvServer.metaData);
            KVMessage ackFromClient = receiveMessage();
            if(ackFromClient.getStatus() == StatusType.META_ACK){
                logger.info("client received metadata");
            }
            
            while(isOpen) {
                try{
                    KVMessage receivedMessage = receiveMessage();
                    KVMessage responseMessage = processRequest(receivedMessage);
                    if (responseMessage != null && responseMessage.getStatus() != null){
                        sendKVMessage((KVMessageImpl)responseMessage);
                    }
                } catch (IOException ioe) {
                    logger.error("Error! Connection lost or unable to read message.", ioe);
                    isOpen = false;
                }
            }
        } catch (IOException e) {
            logger.error("Error! Unable to establish connection streams.", e);
        } finally {
            closeConnection();
            kvServer.removeClientConnection(clientSocket);
            System.out.println("Client closed");
        }
    }


    public void sendMetadata(Metadata metadata) throws IOException {
        MessageWrapper wrapper = new MessageWrapper(MessageWrapper.MessageType.METADATA, metadata);
        output.writeObject(wrapper);
        output.flush();
        logger.info("Metadata sent to client");
    }

    public void sendKVMessage(KVMessageImpl msg) throws IOException {
        MessageWrapper wrapper = new MessageWrapper(MessageWrapper.MessageType.KV_MESSAGE, msg);
        output.writeObject(wrapper);
        output.flush();
        logger.info("SEND \t<" 
			    	+ clientSocket.getInetAddress().getHostAddress() + ":" 
			    	+ clientSocket.getPort() + ">: '" 
			    	+"'" + msg.getKey() + msg.getValue());
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
        } catch (EOFException e){
        }
        return null;
    }

    private KVMessage processRequest(KVMessage msg){
        if(msg == null){
            return null; 
        }
        if(kvServer.state == State.STOPPED){
            return new KVMessageImpl(null, null, StatusType.SERVER_STOPPED);
        } else {
            if(msg != null && msg.getStatus() != null){
                logger.info("Message received of type " + msg.getStatus().name());
            }
            if(msg.getStatus() == StatusType.PUT){
                // CONSIDER WRITE LOCK
                if(msg.getKey() == null){
                    logger.info("Null key"); 
                    KVMessageImpl response = new KVMessageImpl(null, null, StatusType.PUT_ERROR);
                    return response;
                }
                 if(kvServer.state == State.WRITE_LOCKED){
                    KVMessageImpl response = new KVMessageImpl(null, null, StatusType.SERVER_WRITE_LOCK);
                    return response;
                 }
                 // SEE IF THE KEY IS IN THE RANGE OF THIS SERVER
                 String hashedKey = null;
                 try{
                    hashedKey = MD5Hash.hashString(msg.getKey());
                 } catch(NoSuchAlgorithmException n){
                    logger.info("NoSuchAlgorithmException");
                 }
                 if(!isHashInRange(hashedKey, kvServer.hashRange)){
                     logger.info("KEY OUT OF RANGE");
                     KVMessageImpl response = new KVMessageImpl("SNR", null, StatusType.SERVER_NOT_RESPONSIBLE);
                     return response;
                 }
                logger.info("Putting " + msg.getValue());
                KVMessage putResponse = kvServer.putKV(msg.getKey(), msg.getValue());
                // Before returning, perform data transfer to 2 successors
                try {
                    kvServer.dataTransferToTwoSuccessors();
                } catch (NoSuchAlgorithmException n) {
                    // TODO: handle exception
                }
                //FOR M4: Now send message to ECS
                KVMessageImpl putMessage = new KVMessageImpl(msg.getKey(), msg.getValue(), StatusType.CLIENT_PUT);
                try{
                    kvServer.ecsConnection.sendKVMessageToECS(putMessage);
                } catch (IOException i){

                }
                return putResponse;
            } else if(msg.getStatus() == StatusType.GET){
                //SEE IF THE KEY IS IN THE RANGE OF THIS SERVER
                String hashedKey = null;
                try{
                    hashedKey = MD5Hash.hashString(msg.getKey());
                 } catch(NoSuchAlgorithmException n){
                    logger.info("NoSuchAlgorithmException");
                 }
                if(!isHashInRange(hashedKey, kvServer.getResponsibilityRange)){
                    logger.info("KEY OUT OF RANGE");
                    KVMessageImpl response = new KVMessageImpl(null, null, StatusType.SERVER_NOT_RESPONSIBLE);
                    return response;
                }
                logger.info("Getting " + msg.getValue());
                KVMessage response = kvServer.getKV(msg.getKey());
                logger.info("Got " + response.getValue() + " from key " + response.getKey());
                return response;
            } else if(msg.getStatus() == StatusType.ECS_UPDATE_METADATA){
                try{
                    sendMetadata(kvServer.metaData);
                } catch (IOException i){
                    logger.info("error updating metadata");
                }
                return null;
            } else if(msg.getStatus() == StatusType.REQUEST_KEYRANGE){
                String keyrange_info;
                StringBuilder sb = new StringBuilder();
                for (Map.Entry<String, IECSNode> entry : kvServer.metaData.getHashRing().entrySet()) {
                    IECSNode server = entry.getValue();
                    String[] hashRange = server.getNodeHashRange();
                    String serverInfo = server.getNodeHost() + ":" + server.getNodePort();
                    sb.append(hashRange[0]).append(",").append(hashRange[1]).append(",").append(serverInfo).append("; ");
                }
                keyrange_info = sb.toString();
                KVMessageImpl response = new KVMessageImpl(null, keyrange_info, StatusType.KEYRANGE_SUCCESS);
                return response;
            } else if(msg.getStatus() == StatusType.REQUEST_KEYRANGE_READ){
                String keyrange_read_info;
                StringBuilder sb = new StringBuilder();
                Metadata metadata = kvServer.metaData;
                
                for (Map.Entry<String, IECSNode> entry : metadata.getHashRing().entrySet()) {
                    IECSNode server = entry.getValue();
                    if (metadata.hashRing.size() <= 3){
                        String[] getResponsibilityRange = new String[] {"00000000000000000000000000000000", "ffffffffffffffffffffffffffffffff"};
                        String serverInfo = server.getNodeHost() + ":" + server.getNodePort();
                        sb.append(getResponsibilityRange[0]).append(",").append(getResponsibilityRange[1]).append(",").append(serverInfo).append("; ");
                    } else {
                        String serverHash = server.getNodeName();
                        IECSNode father = metadata.getPredecessor(serverHash);
				        IECSNode grandFather = metadata.getPredecessor(father.getNodeName());
				        IECSNode greatGrandFather = metadata.getPredecessor(grandFather.getNodeName());
                        String serverInfo = server.getNodeHost() + ":" + server.getNodePort();
                        String[] getResponsibilityRange = new String[] {metadata.incrementHex(greatGrandFather.getNodeName()), serverHash};
                        sb.append(getResponsibilityRange[0]).append(",").append(getResponsibilityRange[1]).append(",").append(serverInfo).append("; ");
                    }
                }
                keyrange_read_info = sb.toString();
                KVMessageImpl response = new KVMessageImpl(null, keyrange_read_info, StatusType.KEYRANGE_READ_SUCCESS);
                return response;

            }
             else if(msg.getStatus() == StatusType.FAILED){
                return new KVMessageImpl(null, null, StatusType.FAILED);
            }
            else if(msg.getStatus() == StatusType.CLIENT_LEAVING){
                kvServer.clientConnections.remove(clientSocket);
                int i = 0;
                for(Socket socket : kvServer.clientConnections.keySet()){
                    System.out.println("Client: " + i);
                    i++;
                }
            }
            return null;
        }
        
    }


    public boolean isHashInRange(String hash, String[] hashRange) {
        String startHash = hashRange[0];
        String endHash = hashRange[1];
    
        if (startHash.compareTo(endHash) <= 0) {
            // Normal range, no wrap around
            return hash.compareTo(startHash) >= 0 && hash.compareTo(endHash) <= 0;
        } else {
            // Range wraps around, so we split it into two checks
            return hash.compareTo(startHash) >= 0 || hash.compareTo(endHash) <= 0;
        }
    }   

    private void closeConnection(){ 

    }
    
}

