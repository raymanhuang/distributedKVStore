package app_kvServer.storage;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.HashSet;
import java.util.Set;
import java.util.Random;
import java.nio.file.*;
import java.security.NoSuchAlgorithmException;

import shared.messages.KVMessage;
import shared.messages.KVMessageImpl;
import shared.MD5Hash;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class DiskStorage {

    private static Logger logger = Logger.getRootLogger();
    private final Path StorageDir;

    public DiskStorage(String directoryName) throws IOException {
        try{
            StorageDir = Paths.get(directoryName).toAbsolutePath();
            if(!Files.exists(StorageDir)){
                Files.createDirectories(StorageDir);
            }
        } catch (IOException e) {
            logger.error("Error creating storage directory: " + directoryName, e);
            throw new RuntimeException("Unable to create storage directory", e);
        }
    }

    public DiskStorage(String address, int port) throws IOException {
        String directoryName = "data-" + address + "-" + port;
        StorageDir = Paths.get(directoryName).toAbsolutePath();
        if (!Files.exists(StorageDir)) {
            Files.createDirectories(StorageDir);
            logger.info("Storage directory created at: " + StorageDir);
        }
    }

    public synchronized String get(String key) throws IOException {
        Path filePath = StorageDir.resolve(key);
        try {
            if(Files.exists(filePath)){
                return new String(Files.readAllBytes(filePath));
            }
        } catch (IOException e) {
            logger.error("Error reading key: " + key, e);
        }
        return null;
    }

    public synchronized KVMessage put(String key, String value) {
    
        Path filePath = StorageDir.resolve(key);
        KVMessage response;
        try {
            // Determine if this is a new entry or an update
            KVMessage.StatusType statusType = Files.exists(filePath) ? KVMessage.StatusType.PUT_UPDATE : KVMessage.StatusType.PUT_SUCCESS;
            
            // Perform the write operation
            Files.write(filePath, value.getBytes());
            
            // Create a response message with the determined status
            response = new KVMessageImpl(key, value, statusType);
            
            logger.info("PUT operation successful for key: " + key);
        } catch (IOException e) {
            // Log the error and create an error response message
            logger.error("Error writing key: " + key, e);
            response = new KVMessageImpl(key, value, KVMessage.StatusType.PUT_ERROR);
        }
        
        return response;
    }
    

    public synchronized KVMessage delete(String key) {
        Path filePath = StorageDir.resolve(key);
        KVMessage response;
        try {
            boolean fileExisted = Files.deleteIfExists(filePath);
            if (fileExisted) {
                // The file was successfully deleted
                response = new KVMessageImpl(key, "null", KVMessage.StatusType.DELETE_SUCCESS);
            } else {
                // The file did not exist
                response = new KVMessageImpl(key, "null", KVMessage.StatusType.DELETE_ERROR);
            }
        } catch (IOException e) {
            // An IOException occurred while attempting to delete the file
            logger.error("Error deleting key: " + key, e);
            response = new KVMessageImpl(key, "null", KVMessage.StatusType.DELETE_ERROR);
        }
        return response;
    }

    public synchronized Map<String, String> getRange(String startHash, String endHash) throws IOException {
        Map<String, String> rangeData = new HashMap<>();
    
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(StorageDir)) {
            for (Path filePath : stream) {
                String key = filePath.getFileName().toString();
                String keyHash = MD5Hash.hashString(key);
    
                if (isInRange(keyHash, startHash, endHash)) {
                    String value = new String(Files.readAllBytes(filePath));
                    rangeData.put(key, value);
                }
            }
        } catch (IOException | NoSuchAlgorithmException e) {
            logger.error("Error retrieving range data", e);
            throw new IOException("Error retrieving range data", e);
        }
    
        return rangeData;
    }

    public Set<String> getAllKeys() throws IOException {
        Set<String> keys = new HashSet<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(StorageDir)) {
            for (Path entry : stream) {
                keys.add(entry.getFileName().toString());
            }
        }
        return keys;
    }
    
    private boolean isInRange(String keyHash, String startHash, String endHash) {
        if (startHash.compareTo(endHash) <= 0) {
            // Normal range
            return keyHash.compareTo(startHash) >= 0 && keyHash.compareTo(endHash) <= 0;
        } else {
            // Wrap-around range
            return keyHash.compareTo(startHash) >= 0 || keyHash.compareTo(endHash) <= 0;
        }
    }
    
    

    public synchronized void clearStorage() {
        try {
            Files.walk(StorageDir)
                .filter(Files::isRegularFile)
                .forEach(path -> {
                    try {
                        Files.delete(path);
                    } catch (IOException e) {
                        logger.error("Error deleting file: " + path, e);
                    }
                });
        } catch (IOException e) {
            logger.error("Error clearing storage", e);
        }
    }
    

}