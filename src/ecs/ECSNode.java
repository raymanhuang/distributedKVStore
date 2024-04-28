package ecs;

import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.io.IOException;
import java.io.Serializable;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import logger.LogSetup;

import shared.messages.TextMessage;
import shared.MD5Hash;
import shared.messages.KVMessage;
import shared.messages.KVMessageImpl;
import shared.MD5Hash;

import app_kvServer.cache.cacheInterface;
import app_kvServer.cache.cacheFIFO;
import app_kvServer.cache.cacheLRU;
import app_kvServer.cache.cacheLFU;
import app_kvServer.storage.DiskStorage;
import app_kvServer.ClientConnectionServer;


public class ECSNode implements IECSNode, Serializable {
    private String nodeName; 
    private String nodeHost;
    private int nodePort;
    public int dataTransferPort;
    private String[] nodeHashRange;
    public String[] nodeGetResponsibilityRange;
    public boolean writeLocked;

    private static Logger logger = Logger.getRootLogger();


    public ECSNode(String nodeHost, int nodePort, int dataTransferPort, String[] nodeHashRange) throws NoSuchAlgorithmException{
        this.nodeHost = nodeHost;
        this.nodePort = nodePort;
        try{
            this.nodeName = MD5Hash.hashString(nodeHost + ":" + nodePort);
        } catch(NoSuchAlgorithmException n){
            logger.info("NoSuchAlgorithmException");
        }
        this.dataTransferPort = dataTransferPort;
        this.nodeHashRange = nodeHashRange;
        this.writeLocked = false;
    }

    public boolean getWriteLocked(){
        return writeLocked;
    }

    public void setWriteLocked(boolean writeLocked){
        this.writeLocked = writeLocked;
    }

    @Override
    public int getDataTransferPort(){
        return this.dataTransferPort;
    }

    @Override
    public String getNodeName() {
        return nodeName;
    }

    @Override
    public String getNodeHost() {
        return nodeHost;
    }

    @Override
    public int getNodePort() {
        return nodePort;
    }

    @Override
    public String[] getNodeHashRange() {
        return nodeHashRange;
    }
    
    public void setNodeHashRange(String[] hashRange){
        this.nodeHashRange = hashRange;
    }
    
}
