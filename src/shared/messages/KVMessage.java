package shared.messages;

import java.io.Serializable;

public interface KVMessage extends Serializable {
	
	public enum StatusType {
		GET((byte) 0x01), 			/* Get - request */
		GET_ERROR((byte) 0x02), 		/* requested tuple (i.e. value) not found */
		GET_SUCCESS((byte) 0x03), 	/* requested tuple (i.e. value) found */
		PUT((byte) 0x04), 			/* Put - request */
		PUT_SUCCESS((byte) 0x05), 	/* Put - request successful, tuple inserted */
		PUT_UPDATE((byte) 0x06), 	/* Put - request successful, i.e. value updated */
		PUT_ERROR((byte) 0x07), 		/* Put - request not successful */
		DELETE_SUCCESS((byte) 0x08), /* Delete - request successful */
		DELETE_ERROR((byte) 0x09), 	/* Delete - request successful */
		FAILED((byte) 0x10),

		SERVER_STOPPED((byte) 0x11),
		SERVER_WRITE_LOCK((byte) 0x12),
		SERVER_NOT_RESPONSIBLE((byte) 0x13),
		
		ECS_START((byte) 0x14),
		ECS_STOP((byte) 0x15),
		ECS_SHUTDOWN((byte) 0x16),
		ECS_UPDATE_METADATA((byte) 0x17),
		ECS_MOVE_DATA((byte) 0x18),
		ECS_LOCK_WRITE((byte) 0x19),
		ECS_UNLOCK_WRITE((byte) 0x20),
		ECS_JOIN((byte) 0x21),
		ECS_LEAVE((byte) 0x22),
		ECS_TRANSFER_DATA((byte) 0x23),
		DATA_TRANSFER_COMPLETE((byte) 0x24),
		SERVER_WRITE_UNLOCK((byte) 0x25),
		GOODBYE((byte) 0x26),
		META_ACK((byte) 0x27),
		REQUEST_KEYRANGE((byte) 0x28),
		KEYRANGE_SUCCESS((byte) 0x29),
		
		DELETE_RANGE((byte) 0x30),
		HEARTBEAT_PORT ((byte) 0x32),
		REQUEST_KEYRANGE_READ((byte) 0x33),
		KEYRANGE_READ_SUCCESS((byte) 0x34),
		READY_TO_WRITE((byte) 0x31),
		SUBSCRIBE((byte) 0x35),
		UNSUBSCRIBE((byte) 0x36),
		CLIENT_PUT((byte) 0x37),
		BROADCAST_PUT((byte) 0x38),
		CLIENT_LEAVING((byte) 0x39),
		SUBSCRIPTION_UPDATE((byte) 0x40)
;

		private byte byteval; 

		StatusType(byte byteval){
			this.byteval = byteval; 
		}

		public byte getByteval(){
			return this.byteval;
		}

		public static StatusType fromByte(byte value) {
			for (StatusType status : StatusType.values()) {
				if (status.getByteval() == value) {
					return status;
				}
			}
			throw new IllegalArgumentException("No enum constant with byte value: " + value);
		}
	}

	/**
	 * @return the key that is associated with this message, 
	 * 		null if not key is associated.
	 */
	public String getKey();
	
	/**
	 * @return the value that is associated with this message, 
	 * 		null if not value is associated.
	 */
	public String getValue();
	
	/**
	 * @return a status string that is used to identify request types, 
	 * response types and error types associated to the message.
	 */
	public StatusType getStatus();
	
}


