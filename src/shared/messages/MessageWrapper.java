package shared.messages;
import java.io.Serializable;

public class MessageWrapper implements Serializable {
    
    public enum MessageType {
        KV_MESSAGE,
        METADATA,
        DATA_MAP,
        ALIVE,
        HEARTBEAT_PORT
    }

    private MessageType messageType;
    private Object message;

    public MessageWrapper(MessageType messageType, Object message) {
        this.messageType = messageType;
        this.message = message;
    }

    public MessageType getMessageType() {
        return messageType;
    }

    public Object getMessage() {
        return message;
    }
}
