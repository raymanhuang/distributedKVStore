package shared;

import java.io.Serializable;

public class ServerInfo implements Serializable {
    private String address;
    private final int port;
    private String startHash;
    private String endHash;
    private String[] hashRange;

    public ServerInfo(String address, int port, String startHash, String endHash) {
        this.address = address;
        this.port = port;
        this.startHash = startHash;
        this.endHash = endHash;
        this.hashRange[0] = startHash;
        this.hashRange[1] = endHash;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String newAddress) {
        this.address = newAddress;
    }

    public int getPort() {
        return port;
    }

    public String getStartHash() {
        return startHash;
    }

    public void setStartHash(String newStartHash) {
        this.startHash = newStartHash;
    }

    public String getEndHash() {
        return endHash;
    }

    public void setEndHash(String newEndHash) {
        this.endHash = newEndHash;
    }

    public void setHashRange(String[] hashRange){
        this.hashRange = hashRange;
    }

    public String[] getHashRange(){
        return this.hashRange;
    }
}
