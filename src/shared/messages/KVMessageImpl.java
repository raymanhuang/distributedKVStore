package shared.messages;

import java.util.Arrays;

public class KVMessageImpl implements KVMessage {

    private static final char LINE_FEED = 0x0A;
	private static final char RETURN = 0x0D;

    private String msg;
	private byte[] msgBytes;

    private String key;
    private String value;
    private StatusType status;

    public KVMessageImpl(String key, String value, StatusType status) {
        this.key = key;
        this.value = value;
        this.status = status;
    }

    public KVMessageImpl(byte[] bytes) { //for sending just a text message 
		this.msgBytes = addCtrChars(bytes);
		this.msg = new String(msgBytes).trim();
	}

    /**
	 * @return the key that is associated with this message, 
	 * 		null if not key is associated.
	 */
	public String getKey(){
        return this.key;
    }
	
	/**
	 * @return the value that is associated with this message, 
	 * 		null if not value is associated.
	 */
	public String getValue(){
        return this.value;
    }
	
	/**
	 * @return a status string that is used to identify request types, 
	 * response types and error types associated to the message.
	 */
	public StatusType getStatus(){
        return this.status;
    }

    public String getMessage(){
        return this.msg;
    }

    private byte[] addCtrChars(byte[] bytes) {
		byte[] ctrBytes = new byte[]{LINE_FEED, RETURN};
		byte[] tmp = new byte[bytes.length + ctrBytes.length];
		
		System.arraycopy(bytes, 0, tmp, 0, bytes.length);
		System.arraycopy(ctrBytes, 0, tmp, bytes.length, ctrBytes.length);
		
		return tmp;		
	}

    public byte[] KVtoByteArray(){
 
		byte statusByte = this.getStatus().getByteval();
        byte[] valBytes = {};
        byte[] keyBytes = {};
        if(this.getValue() != null){
            valBytes = this.getValue().getBytes(); 
        }
        if(this.getKey() != null){
            keyBytes = this.getKey().getBytes(); 
        }

		byte[] tmp = new byte[1 + keyBytes.length + 1 + valBytes.length];
        
        tmp[0] = statusByte; // statusByte then the key, then a return as a delimiter then the value
        System.arraycopy(keyBytes, 0, tmp, 1, keyBytes.length); 
        tmp[keyBytes.length +1] = 0x0F; 
        System.arraycopy(valBytes, 0, tmp, keyBytes.length+2, valBytes.length);
		
		return addCtrChars(tmp);		
	}

    public static KVMessageImpl ByteArrayToKV(byte[] bytes){

        byte statusByte = bytes[0]; 
        StatusType status = StatusType.fromByte(statusByte);

        // Find the position of the control byte
        int controlByteIndex = -1;
        for (int i = 1; i < bytes.length; i++) {
            if (bytes[i] == 0x0F) {
                controlByteIndex = i;
                break;
            }
        }

         // Extract the key bytes
        byte[] keyBytes = Arrays.copyOfRange(bytes, 1, controlByteIndex);
        String key = new String(keyBytes);
        
        // Extract the value bytes
        byte[] valueBytes = Arrays.copyOfRange(bytes, controlByteIndex + 1, bytes.length);
        String value = new String(valueBytes);
        
        KVMessageImpl message = new KVMessageImpl(key, value, status);
        return message; 
    }
}

