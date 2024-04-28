package testing;

import org.junit.Test;

import app_kvServer.KVServer;
import junit.framework.TestCase;

import client.KVStore;
import ecs.ECS;
import junit.framework.TestCase;
import shared.messages.KVMessage;
import shared.messages.KVMessage.StatusType;

import java.beans.Transient;
import java.io.IOException;;

public class AdditionalTest extends TestCase {

	private static KVStore kvClient;
	private static KVStore kvClient2;
	private static KVServer server; 
	private static ECS ecs; 
	
	@Test
    public void testStart() {
        System.out.println("STARTING ECS SERVER");
        ecs = new ECS("localhost", 40000);
        ecs.startECSServer();

		kvClient = new KVStore("localhost", 42000); 

		try {
            server = new KVServer(42000, 10, "FIFO", "localhost", 40000);
		    Thread.sleep(1000); // Give time for server to start
			kvClient.connect();
			Thread.sleep(2000);
		} catch (Exception e) {
            e.printStackTrace();
		}

		
	}
	
	
	@Test
    public void testPutDuplicateKey() {
        String key = "duplicateKey";
        String value1 = "value1";
        String value2 = "value2";
        KVMessage response1 = null;
        KVMessage response2 = null;
        Exception ex1 = null;
        Exception ex2 = null;

        try {
            response1 = kvClient.put(key, value1);
            response2 = kvClient.put(key, value2);
        } catch (Exception e) {
			e.printStackTrace();
            ex1 = e;
        }
        assertTrue(ex1 == null);
		assertTrue(response1.getStatus() == StatusType.PUT_SUCCESS);
		assertTrue(response2.getStatus() == StatusType.PUT_UPDATE);
    }


    @Test
    public void testPutNullKey() {
        String key = null;
        String value = "value";
        KVMessage response = null;
        Exception ex = null;

        try {
            response = kvClient.put(key, value);
        } catch (Exception e) {
            ex = e;
        }
		assertTrue(response.getStatus() == StatusType.PUT_ERROR);
    }

	@Test
	public void testKeyLengthLimit(){
		String perfectKey = "iamtwentycharacterss";
		String largeKey = "iamgreaterthantwentycharacters";
		KVMessage response1 = null;
		KVMessage response2 = null;
		Exception ex = null;
		try {
            response1 = kvClient.put(perfectKey, "test");
			response2 = kvClient.put(largeKey, "test");
        } catch (Exception e) {
            ex = e;
        }
		assertTrue(response1.getStatus() == StatusType.PUT_SUCCESS);
		assertTrue(response2.getStatus() == StatusType.PUT_ERROR);
	}

	@Test 
	public void testFIFOCacheEviction(){
		// for the tester cache size is 10, put in 11, check if oldest key/value is still out the cache
		// and newest KV is still in the cache
		Exception ex = null;
		server.clearCache();
		try{
			for(int i = 0; i < 15; i++){
				kvClient.put("key"+i, "foo");
			}
		} catch (Exception e){
			ex = e;
		}
		assertTrue("foo".equals(server.cache.get("key14")));
		assertTrue(server.cache.get("key0") == null);
	}

	@Test
	public void testDeleteNonexistentKey() {
        String key = "nonexistentKey";
        KVMessage response = null;
        Exception ex = null;

        try {
            response = kvClient.put(key, "null");
        } catch (Exception e) {
            ex = e;
        }
        assertTrue(response.getStatus() == StatusType.DELETE_ERROR);
    }

	@Test 
	public void testPersistentDataBase(){
		KVMessage response = null;
		Exception ex = null;
		String key = "amIHere";
		String value = "yesIAm";

		try{
			response = kvClient.put(key, value);
		} catch (Exception e){
			ex = e;
		}
		assertTrue(response.getStatus() == StatusType.PUT_SUCCESS);

		try { 
			kvClient.disconnect();
			server.kill();
			Thread.sleep(6000); 
		} catch (Exception e){
			e.printStackTrace();
		}

		try {
			kvClient = new KVStore("localhost", 42000);
			server = new KVServer(42000, 10, "FIFO", "localhost", 40000);  
			Thread.sleep(1000);
		} catch(Exception e){
			e.printStackTrace();
		}

		try {
			kvClient.connect();
		} catch (Exception e) {
			ex = e;
		}

		try {
			response = kvClient.get(key);
		} catch (Exception e) {
			ex = e;
		}
		assertTrue(value.equals(response.getValue()));
	}

	@Test 
	public void testMultipleClients(){
		Exception ex;
		kvClient2 = new KVStore("localhost", 42000);
		try {
			kvClient2.connect();
		} catch (Exception e) {
			ex = e;
		}
		KVMessage clientOneResponse = null;
		KVMessage clientTwoResponse = null;
		try{
			clientOneResponse = kvClient.put("foo", "bar");
		} catch (Exception e){
			ex = e;
		}
		assertTrue(clientOneResponse.getStatus() == StatusType.PUT_SUCCESS);
		try{
			clientTwoResponse = kvClient2.get("foo");
		} catch (Exception e){
			ex = e;
		}
		assertTrue(clientTwoResponse.getValue().equals("bar"));
		kvClient2.disconnect();
	}

	@Test 
	public void testGetDeletedKey(){
		String key = "deleteMe";
		String value = "foo";
		
		KVMessage response = null;
		Exception ex = null;

		try {
			kvClient.put(key, value);
			response = kvClient.put(key, "null");
			
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(response.getStatus() == StatusType.DELETE_SUCCESS);

		try {
			response = kvClient.get(key);
		} catch (Exception e) {
			ex = e;
		}
		assertTrue(response.getStatus() == StatusType.GET_ERROR);
	}

	@Test 
	public void testClearStorage(){
		String key = "insert";
		String value = "me";

		KVMessage response = null;
		Exception ex = null;

		try{
			response = kvClient.put(key, value);
		} catch (Exception e) {
			ex = e;
		}
		assertTrue(response.getStatus() == StatusType.PUT_SUCCESS);

		try{
			response = kvClient.get(key);
		} catch (Exception e) {
			ex = e;
		}
		assertTrue(response.getStatus() == StatusType.GET_SUCCESS);

		server.clearCache();
		server.clearStorage();

		try{
			response = kvClient.get(key);
		} catch (Exception e) {
			ex = e;
		}
		assertTrue(response.getStatus() == StatusType.GET_ERROR);
	}

	public void testClearCache(){
		String key = "insert";
		String value = "me";

		KVMessage response = null;
		Exception ex = null;

		try{
			response = kvClient.put(key, value);
		} catch (Exception e) {
			ex = e;
		}
		assertTrue(response.getStatus() == StatusType.PUT_SUCCESS);
		assertTrue(server.cache.get(key).equals(value));

		server.cache.clear();

		assertTrue(server.cache.get(key) == null);
	}

	@Test
	public void testEnd() {
		if(kvClient != null){
        kvClient.disconnect();
		} 
		if(kvClient2 != null){
		kvClient2.disconnect();
		}

		
        server.clearStorage(); 
		server.clearCache();
		server.kill(); 
        try{
            Thread.sleep(1000);
        } catch (Exception e) {}
            
        if (ecs != null) {
            ecs.disconnect(); // Assuming this method correctly stops the ECS
        }
    }
}
