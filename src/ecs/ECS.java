package ecs;
import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.Map;

import shared.Metadata;
import shared.messages.*;
import shared.MD5Hash;
import shared.ServerInfo;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import logger.LogSetup;

public class ECS implements Runnable{
    public volatile Metadata metaData;
    private ServerSocket ECSSocket;
    private int ECSport;
    private String ECSAddress;
    private boolean isRunning;
    public volatile Map<String, ServerConnection> serverConnections;
    public int heartbeatPort;

    private static Logger logger = Logger.getRootLogger();

    public ECS(String ECSAdrress, int ECSport){
        this.ECSAddress = ECSAddress;
        this.ECSport = ECSport;
        this.metaData = new Metadata();
        this.serverConnections = new HashMap<>();
    }

    public void addServerConnection(String serverName, ServerConnection connection) {
        serverConnections.put(serverName, connection);
    }


    private boolean isRunning(){
		return this.isRunning;
	}

    private boolean initializeECSServer(){
		logger.info("Initialize ECSserver ...");
		try{
			ECSSocket = new ServerSocket(ECSport);
			logger.info("ECSServer listening on port: " + ECSSocket.getLocalPort() + " address: " + ECSSocket.getInetAddress().getHostAddress());
			return true;
		} catch (IOException ioe) {
			logger.error("Error! Cannot open server socket:");
			if (ioe instanceof BindException){
				logger.error("Port " + ECSport + " is already bound");
			}
			return false;
		}
	}

    @Override
    public void run() {
		isRunning = initializeECSServer();

		if(ECSSocket != null){
			while(isRunning()){
				try {
                    System.out.println("waiting for kvserver...");
					Socket kvServer = ECSSocket.accept();
					ServerConnection connection = new ServerConnection(kvServer, this);
                    new Thread(connection).start();
                    System.out.println("server added");

					// logger.info("Connected to " 
	                // 		+ kvServer.getInetAddress().getHostName() 
	                // 		+  " on port " + kvServer.getPort());
				
				} catch (IOException e) {
					logger.error("Error! " + "Unable to establish connection. \n", e);
				}
			}
		}
		logger.info("ECS Server stopped.");
	}


    public boolean startECSServer() {
        try {
            new Thread(this).start();
            System.out.println("ECS server started on port " + ECSport);
            return true;
        } catch (Exception e) {
            System.err.println("Error starting ECS server: " + e.getMessage());
            return false;
        }
    }
    public void disconnect(){
        try{
        ECSSocket.close();
        }
        catch(IOException e){
        
        }
        isRunning=false;

    }
    
}
