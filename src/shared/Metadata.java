package shared;

import java.io.Serializable;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.security.NoSuchAlgorithmException;
import java.math.*;

import ecs.IECSNode;
import ecs.ECSNode;

public class Metadata implements Serializable {

    public NavigableMap<String, IECSNode> hashRing;

    public Metadata() {
        this.hashRing = new TreeMap<>();
    }

    public IECSNode getSuccessor(String serverHash) {
        // Find the entry in the hash ring with the smallest hash greater than the given server hash
        Map.Entry<String, IECSNode> successorEntry = hashRing.higherEntry(serverHash);
        if (successorEntry == null) {
            // If there is no higher entry, the successor is the first entry in the hash ring (wrap around)
            successorEntry = hashRing.firstEntry();
            if (successorEntry == null) {
                return null;
            }
            if (successorEntry.getValue().getNodeName().equals(serverHash)) {
                successorEntry = null;
            }
        }
        return (successorEntry != null) ? successorEntry.getValue() : null;
    }

    public IECSNode getPredecessor(String serverHash) {
        // Find the entry in the hash ring with the largest hash smaller than the given server hash
        Map.Entry<String, IECSNode> predecessorEntry = hashRing.lowerEntry(serverHash);
        if(predecessorEntry == null) {
            // If there is no lower entry, the predecessor is the last entry in the hash ring (wrap)
            predecessorEntry = hashRing.lastEntry();
            if (predecessorEntry == null) {
                return null;
            }
            if (predecessorEntry.getValue().getNodeName().equals(serverHash)) {
                predecessorEntry = null;
            }
        }
        return (predecessorEntry != null) ? predecessorEntry.getValue() : null;
    }
    

    public synchronized void addServer(String serverHash, IECSNode serverInfo) {
        // Determine the predecessor and successor of the new server
        Map.Entry<String, IECSNode> lowerEntry = hashRing.lowerEntry(serverHash);
        Map.Entry<String, IECSNode> higherEntry = hashRing.higherEntry(serverHash);
    
        String startHash;
        String endHash = serverHash; // The new server's hash is the end of its range
    
        if (lowerEntry != null) {
            // The start of the new server's range is the predecessor's end hash + 1
            startHash = incrementHex(lowerEntry.getKey());
        } else {
            // The new server is the first in the ring, wrap around to the last server's end hash
            if (!hashRing.isEmpty()) {
                startHash = incrementHex(hashRing.lastEntry().getKey());
            } else {
                // If the ring is empty, the server covers the whole hash space
                startHash = "00000000000000000000000000000000";
                endHash = "ffffffffffffffffffffffffffffffff";
                serverInfo.setNodeHashRange(new String[]{startHash, endHash});
                hashRing.put(serverHash, serverInfo);
                return;
            }
        }
    
        // Set the new server's hash range
        serverInfo.setNodeHashRange(new String[]{startHash, endHash});
    
        // Add the new server to the hash ring
        hashRing.put(serverHash, serverInfo);
    
        // Update the successor's startHash if there is one
        if (higherEntry != null) {
            String[] successorHashRange = higherEntry.getValue().getNodeHashRange();
            successorHashRange[0] = incrementHex(serverHash); // The new server's hash is the start of the successor's range
            successorHashRange[1] = higherEntry.getKey();
            higherEntry.getValue().setNodeHashRange(successorHashRange);
        } else {
            // If there is no successor, then this is the last server in the ring
            // Update the first server's start hash to be just after the new server's hash
            if (!hashRing.isEmpty()) {
                Map.Entry<String, IECSNode> firstEntry = hashRing.firstEntry();
                String[] firstHashRange = firstEntry.getValue().getNodeHashRange();
                firstHashRange[0] = incrementHex(serverHash);
                firstHashRange[1] = firstEntry.getKey();
                firstEntry.getValue().setNodeHashRange(firstHashRange);
            }
        }
    }
    
    
    public String incrementHex(String hexStr) {
        if (hexStr.length() != 32 || !hexStr.matches("[0-9a-fA-F]+")) {
            throw new IllegalArgumentException("Input must be a 32-character hexadecimal string.");
        }
    
        // Convert the hex string to a BigInteger, add one, and convert back to a hex string
        BigInteger hexNum = new BigInteger(hexStr, 16);
        BigInteger incremented = hexNum.add(BigInteger.ONE);
    
        // If the result is longer than 32 characters, it means it wrapped around
        String incrementedHex = incremented.toString(16);
        if (incrementedHex.length() > 32) {
            return "00000000000000000000000000000001";
        }
    
        // Pad the result with leading zeros to make it 32 characters long
        return String.format("%032x", incremented);
    }

    public String decrementHex(String hexStr) {
        if (hexStr.length() != 32 || !hexStr.matches("[0-9a-fA-F]+")) {
            throw new IllegalArgumentException("Input must be a 32-character hexadecimal string.");
        }
    
        // Convert the hex string to a BigInteger, subtract one, and convert back to a hex string
        BigInteger hexNum = new BigInteger(hexStr, 16);
        BigInteger decremented = hexNum.subtract(BigInteger.ONE);
    
        // If the result is negative, it means it wrapped around
        if (decremented.signum() == -1) {
            return "ffffffffffffffffffffffffffffffff";
        }
    
        // Pad the result with leading zeros to make it 32 characters long
        return String.format("%032x", decremented);
    }
    
    

    public synchronized void removeServer(String serverHash) {
        // Get the server info of the server to be removed
        IECSNode removedServerInfo = hashRing.get(serverHash);
    
        if (removedServerInfo != null) {
            // Find the predecessor and successor of the server to be removed
            Map.Entry<String, IECSNode> lowerEntry = hashRing.lowerEntry(serverHash);
            Map.Entry<String, IECSNode> higherEntry = hashRing.higherEntry(serverHash);
    
            if (lowerEntry != null && higherEntry != null) {
                // There are both predecessor and successor
                String[] predecessorHashRange = lowerEntry.getValue().getNodeHashRange();
                String[] successorHashRange = higherEntry.getValue().getNodeHashRange();
    
                // Extend the successor's range to include the removed server's range
                successorHashRange[0] = predecessorHashRange[1];
                higherEntry.getValue().setNodeHashRange(successorHashRange);
            } else if (lowerEntry == null && higherEntry != null && hashRing.higherEntry(higherEntry.getKey()) != null) {
                // The removed server is the first server in the ring and has a grandson
                String[] successorHashRange = higherEntry.getValue().getNodeHashRange();
                // Extend the successor's range to cover from the last server to itself
                successorHashRange[0] = incrementHex(hashRing.lastEntry().getKey());
                successorHashRange[1] = higherEntry.getKey();
                higherEntry.getValue().setNodeHashRange(successorHashRange);
            } else if (lowerEntry == null && higherEntry != null && hashRing.higherEntry(higherEntry.getKey()) == null){
                // The removed server is first server in the ring and does not have a grandson, only a son
                // the remaining server is the last server
                String[] successorHashRange = higherEntry.getValue().getNodeHashRange();
                // Extend the successor's range to cover from the last server to itself
                successorHashRange[0] = "00000000000000000000000000000000";
                successorHashRange[1] = "ffffffffffffffffffffffffffffffff";
                higherEntry.getValue().setNodeHashRange(successorHashRange);
            } else if (lowerEntry != null && higherEntry == null && hashRing.lowerEntry(lowerEntry.getKey()) != null) {
                // The removed server is the last server in the ring and has a grandparent
                String[] predecessorHashRange = lowerEntry.getValue().getNodeHashRange();
                predecessorHashRange[0] = lowerEntry.getKey();
                predecessorHashRange[1] = decrementHex(hashRing.firstEntry().getKey());
                lowerEntry.getValue().setNodeHashRange(predecessorHashRange);
            } else if (lowerEntry != null && higherEntry == null && hashRing.lowerEntry(lowerEntry.getKey()) == null) {
                // The removed server is the last server in the ring and does not have a grandparent, only a dad
                String[] predecessorHashRange = lowerEntry.getValue().getNodeHashRange();
                // Extend the successor's range to cover from the last server to itself
                predecessorHashRange[0] = "00000000000000000000000000000000";
                predecessorHashRange[1] = "ffffffffffffffffffffffffffffffff";
                lowerEntry.getValue().setNodeHashRange(predecessorHashRange);
            }
            else {
                // The removed server is the only server in the ring, no need to update any ranges
            }
    
            // Remove the server from the hash ring
            hashRing.remove(serverHash);
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

    public IECSNode getServerInfo(String keyHash) {
        Map.Entry<String, IECSNode> entry = hashRing.ceilingEntry(keyHash);
        if (entry == null) {
            // Wrap around the hash ring
            entry = hashRing.firstEntry();
        }
        return entry.getValue();
    }

    public NavigableMap<String, IECSNode> getHashRing() {
        return new TreeMap<>(hashRing); // Return a copy
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Metadata:\n");
        for (Map.Entry<String, IECSNode> entry : hashRing.entrySet()) {
            String hash = entry.getKey();
            IECSNode node = entry.getValue();
            sb.append("Hash: ").append(hash)
              .append(", Server: ").append(node.getNodeHost())
              .append(":").append(node.getNodePort())
              .append(", DataTransferPort: ").append(node.getDataTransferPort())
              .append(", Range: [").append(node.getNodeHashRange()[0])
              .append(", ").append(node.getNodeHashRange()[1])
              .append("]\n");
        }
        return sb.toString();
    }
    

    public static Metadata fromString(String metadataString) throws NoSuchAlgorithmException {
        Metadata metadata = new Metadata();
        String[] lines = metadataString.split("\n");
    
        for (String line : lines) {
            if (line.startsWith("Hash: ")) {
                String[] parts = line.split(", ");
                String hash = parts[0].substring("Hash: ".length());
                
                String serverPart = parts[1].substring("Server: ".length());
                String[] serverInfo = serverPart.split(":");
                String nodeHost = serverInfo[0];
                int nodePort = Integer.parseInt(serverInfo[1]);
    
                int dataTransferPort = Integer.parseInt(parts[2].substring("DataTransferPort: ".length()));
    
                String rangePart = parts[3];
                String[] rangeInfo = rangePart.substring("Range: [".length(), rangePart.length() - 1).split(", ");
                String[] nodeHashRange = new String[]{rangeInfo[0], rangeInfo[0]};
    
                IECSNode node = new ECSNode(nodeHost, nodePort, dataTransferPort, nodeHashRange);
                metadata.addServer(hash, node);
            }
        }
    
        return metadata;
    }
    
    


}
