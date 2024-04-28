package testing;

import org.junit.Test;

import app_kvClient.KVClient;

import java.io.File;
import java.io.ObjectOutputStream;
import java.net.Socket;

import app_kvServer.KVServer;
import junit.framework.TestCase;

import client.KVStore;
import ecs.ECS;
import ecs.IECSNode;
import ecs.ServerConnection;
import shared.MD5Hash;
import shared.messages.KVMessage;
import shared.messages.KVMessage.StatusType;

import java.beans.Transient;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Vector;

public class AdditionalTestsM3 extends TestCase {
    private static Vector<KVStore> clientset = new Vector<>();
    private static ECS ecs;
    private static Vector<KVServer> serverset = new Vector<>();
    private static KVClient cmd; 


    @Test
    public void test1Start() {

        ecs = new ECS("localhost", 40000);
        ecs.startECSServer();

		try {
            KVServer server1 = new KVServer(42000, 10, "FIFO", "localhost", 40000);
		    Thread.sleep(1000); // Give time for server to start
            serverset.addElement(server1);
            server1.clearCache();
            server1.clearStorage();

            KVServer server2 = new KVServer(44000, 10, "FIFO", "localhost", 40000);
		    Thread.sleep(1000); // Give time for server to start
            serverset.addElement(server2);
            server2.clearCache();
            server2.clearStorage();

            KVServer server3 = new KVServer(46000, 10, "FIFO", "localhost", 40000);
		    Thread.sleep(1000); // Give time for server to start
            serverset.addElement(server3);
            server3.clearCache();
            server3.clearStorage();

		} catch (Exception e) {
            e.printStackTrace();
		}
	}

    @Test
    public void test2Replication(){ 

        KVMessage response1 = null;
        KVMessage response2 = null;

        try { 
            KVStore Client = new KVStore("localhost", 42000); 
            clientset.addElement(Client);

			Client.connect();
			Thread.sleep(200);

            response1 = Client.put("test", "value"); 


            Thread.sleep(100);
            KVStore Client2 = new KVStore("localhost", 44000); 
            clientset.addElement(Client2);

			Client2.connect();

            response2 = Client2.get("test");
			

           
        } catch (Exception e){
            e.printStackTrace();
        }

        assertTrue(response1.getStatus() == StatusType.PUT_SUCCESS);
        assertTrue(response2.getStatus() == StatusType.GET_SUCCESS);

        String directoryPath = "data-0.0.0.0-44000/"; // Data from 42000 should be replicaed in 44000
        String fileName = "test";
        File file = new File(directoryPath, fileName);

        assertTrue(file.exists());

        directoryPath = "data-0.0.0.0-46000/"; 
        file = new File(directoryPath, fileName); 

        assertTrue(file.exists());


    }

    @Test
    public void test3ResponsibilityRange(){ 
        KVServer server1 = serverset.get(0); 
        String server1Hash = null; 
        try { 
         server1Hash = MD5Hash.hashString(server1.address + ":" + server1.port);
        } catch(Exception e){} 

        String responsibility[] = server1.getResponsibilityRange; 

        assertTrue(responsibility[responsibility.length-1] == "ffffffffffffffffffffffffffffffff");
    }

    @Test
    public void test4MultipleReplication(){

        KVMessage response1 = null; 
        KVMessage response2 = null; 

        try { 
            KVStore Client3 = new KVStore("localhost", 44000); 
            clientset.addElement(Client3);
			Client3.connect();
			Thread.sleep(200);

            response1 = Client3.put("hello", "value"); 

            KVStore Client1 = clientset.get(1); 

            response2 = Client1.get("hello");

            System.out.println(response1.getStatus().toString());

            Thread.sleep(200);


        } catch (Exception e){
            e.printStackTrace();
        }

        assertTrue(response1.getStatus() == StatusType.SERVER_NOT_RESPONSIBLE);


    }

    @Test
    public void test5HeartbeatMessages(){ 
        KVServer server1 = serverset.get(0); 
        
        // checking port is not null
        assertTrue(server1.ecsConnection.heartbeatPort != 0); 

        // waiting for 7 seconds and checking that the server is still active (would have timed out)
        try{
            Thread.sleep(7000); 
        } catch (Exception e){}

        assertTrue(server1.running);
        
    }

    @Test
    public void test6HeartbeatFailureDetection(){ 

        int ring_size_before = ecs.metaData.hashRing.size();

        try { 
            KVServer server1 = serverset.get(0); 
            server1.kill();
            Thread.sleep(8000);
        } catch(Exception e){
            e.printStackTrace();
        }

        int ring_size_after = ecs.metaData.hashRing.size();
        
        assertTrue(ring_size_before - ring_size_after == 1);

    }

    @Test 
    public void test7DataPersistenceAfterShutdown(){ 

        String directoryPath = "data-0.0.0.0-42000/";
        String fileName = "test";
        File file = new File(directoryPath, fileName);

        assertTrue(file.exists());
    }

    @Test
    public void test8ReplicationPersistenceAfterShutdown(){
        String directoryPath = "data-0.0.0.0-44000/";
        String fileName = "test";
        File file = new File(directoryPath, fileName);

        assertTrue(file.exists());
    }

    @Test
    public void test9MetadataUpdate(){ 
        // tests that server2 has the new metadata after sever1 was shutdown (ecs sent it the new metadata)
        KVServer server2 = serverset.get(1); 

        assertTrue(server2.metaData.hashRing.size() == 2); // otherwise this would still be 3
    }

    @Test 
    public void test10ClientReconnection(){ 
        cmd = new KVClient(); 

        try {

            System.out.println("CUSTOM CLIENT COMMANDS: ");
            cmd.handleCommand("connect localhost 44000");
            Thread.sleep(100);
            
            cmd.handleCommand("put hello");
            cmd.handleCommand("get test");
        } catch (Exception e) {
            e.printStackTrace();
        } 

        assertTrue(cmd.getStore().getServerPort() == 46000); // this means the client has switched servers to get the key

    }

    @Test
    public void test11KeyrangeRead(){ 
        KVStore Client2 = clientset.get(1); 
        KVMessage response = null; 
        try {

            response = Client2.keyrange_read(); 

        } catch (Exception e){
            e.printStackTrace();
        }

        assertTrue(response.getStatus() == StatusType.KEYRANGE_READ_SUCCESS);

    }

    @Test
	public void test12End() {
        clientset.forEach(client -> client.disconnect());
        for(KVServer server : serverset){
            if(server.running){
                server.clearStorage(); 
		        server.clearCache();
		        server.kill(); 
                try{
                    Thread.sleep(1000);
                } catch (Exception e) {}
            }
        }
        if (ecs != null) {
            ecs.disconnect(); // Assuming this method correctly stops the ECS
        }
    }
    
}
