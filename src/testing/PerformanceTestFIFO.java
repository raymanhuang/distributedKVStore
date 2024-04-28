package testing;

import java.beans.Transient;

import org.junit.Test;

import app_kvServer.KVServer;
import logger.LogSetup;
import org.apache.log4j.Level;
import client.KVStore;
import junit.framework.TestCase;
import shared.messages.KVMessage;
import shared.messages.KVMessage.StatusType;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.IOException;


public class PerformanceTestFIFO extends TestCase{

	private KVStore kvClient;
    private KVServer server;

	private void writePerformanceResult(String testName, long duration) {
        try (FileWriter fw = new FileWriter("performanceResults.txt", true);
             BufferedWriter bw = new BufferedWriter(fw);
             PrintWriter out = new PrintWriter(bw)) {
            out.println(testName + " duration: " + duration + " ms");
        } catch (IOException e) {
            // Exception handling for file writing
            e.printStackTrace();
        }
    }
	
	public void setUp() {
		try {
            new LogSetup("logs/testing/PerformanceTestFIFO.log", Level.ERROR);
            server = new KVServer(50000, 1000, "FIFO", "localhost", 50001); 
            server.clearStorage();
            new Thread(server).start();
        } catch (IOException e) {
            e.printStackTrace();
        }
		kvClient = new KVStore("localhost", 50000);
		try {
			kvClient.connect();
		} catch (Exception e) {
		}
	}

	public void tearDown() {
		kvClient.disconnect();
		server.clearStorage();
		server.kill();
	}

	@Test
	public void testEightyPutsTwentyGets(){
		int totalOperations = 1000;
		int puts = (int)(totalOperations * 0.8);
		int gets = totalOperations - puts;

		long startTime = System.nanoTime();

		try{
			for (int i = 0; i < puts; i++){
				KVMessage response = kvClient.put("key"+i, "value"+i);
				assertTrue(response.getStatus() == StatusType.PUT_SUCCESS);
			}
		} catch(Exception e){
			fail("Unexpected exception during put/get: " + e.getMessage());
		}

		try{
			for (int i = 0; i < gets; i++){
				KVMessage response = kvClient.get("key"+i);
				assertTrue(response.getStatus() == StatusType.GET_SUCCESS);
			}
		} catch(Exception e){
			fail("Unexpected exception during put/get: " + e.getMessage());
		}

		long endTime = System.nanoTime();
		long duration = (endTime - startTime) / 1000000;
		writePerformanceResult("FIFOEightyPutsTwentyGets", duration);
	}

	@Test 
	public void testFiftyPutsFiftyGets(){
		int totalOperations = 1000;
		int puts = (int)(totalOperations * 0.5);
		int gets = totalOperations - puts;

		long startTime = System.nanoTime();

		try{
			for (int i = 0; i < puts; i++){
				KVMessage response = kvClient.put("key"+i, "value"+i);
				assertTrue(response.getStatus() == StatusType.PUT_SUCCESS);
			}
		} catch(Exception e){
			fail("Unexpected exception during put/get: " + e.getMessage());
		}

		try{
			for (int i = 0; i < gets; i++){
				KVMessage response = kvClient.get("key"+i);
				assertTrue(response.getStatus() == StatusType.GET_SUCCESS);
			}
		} catch(Exception e){
			fail("Unexpected exception during put/get: " + e.getMessage());
		}

		long endTime = System.nanoTime();
		long duration = (endTime - startTime) / 1000000;
		writePerformanceResult("FIFOFiftyPutsFiftyGets", duration);
	}

	

	@Test 
	public void testTwentyPutsEightyGets(){
		int totalOperations = 1000;
		int puts = (int)(totalOperations * 0.2);
		int gets = totalOperations - puts;

		long startTime = System.nanoTime();

		try{
			for (int i = 0; i < puts; i++){
				KVMessage response = kvClient.put("key"+i, "value"+i);
				assertTrue(response.getStatus() == StatusType.PUT_SUCCESS);
			}
		} catch(Exception e){
			fail("Unexpected exception during put/get: " + e.getMessage());
		}

		try{
			for (int i = 0; i < gets; i++){
				KVMessage response = kvClient.get("key"+(i/4));
				assertTrue(response.getStatus() == StatusType.GET_SUCCESS);
			}
		} catch(Exception e){
			fail("Unexpected exception during put/get: " + e.getMessage());
		}

		long endTime = System.nanoTime();
		long duration = (endTime - startTime) / 1000000;
		writePerformanceResult("FIFOTwentyPutsEightyGets", duration);
	}


}



