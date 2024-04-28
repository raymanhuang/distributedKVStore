package testing;

import org.junit.Test;

import app_kvClient.KVClient;

import java.io.File;

import app_kvServer.KVServer;
import junit.framework.TestCase;

import client.KVStore;
import ecs.ECS;
import ecs.IECSNode;
import shared.messages.KVMessage;
import shared.messages.KVMessage.StatusType;

import java.beans.Transient;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Vector;

public class AdditionalTestsM2 extends TestCase {
    private static Vector<KVStore> clientset = new Vector<>();
    private static ECS ecs;
    private static Vector<KVServer> serverset = new Vector<>();
	
    @Test
    public void testStart() {
/*         // clean up old files if any
        String directoryPath = "data-0.0.0.0-44000/";
        deleteFilesRecursively(new File(directoryPath));

        directoryPath = "data-0.0.0.0-42000/";
        deleteFilesRecursively(new File(directoryPath)); */
        
        System.out.println("STARTING ECS SERVER");
        ecs = new ECS("localhost", 40000);
        ecs.startECSServer();

		try {
            KVServer server = new KVServer(42000, 10, "FIFO", "localhost", 40000);
		    Thread.sleep(1000); // Give time for server to start
            serverset.addElement(server);
		} catch (Exception e) {
            e.printStackTrace();
		}
	}



    @Test
    public void testOneServerHashRange() {
        String[] keyrange = {"0", "1"};
        try { 
            KVStore client = new KVStore("localhost", 42000);
            
            client.connect();

            clientset.addElement(client);

            keyrange =  client.keyrange().getValue().split(",");

           
        } catch (Exception e){
            e.printStackTrace();
        }

        assertTrue(keyrange[0].equals("00000000000000000000000000000000"));
        assertTrue(keyrange[1].equals("ffffffffffffffffffffffffffffffff"));
    }


    
    @Test
    public void testTwoServersHash(){
        // add another server
        try {
            KVServer server = new KVServer(44000, 10, "FIFO", "localhost", 40000);
		    Thread.sleep(1000); // Give time for server to start
            serverset.addElement(server); 


            String[] range = server.getHashRange(); 

            assertTrue(range[0].equals("0a69ed1a2dc1a6f37349f8d12e2aaad6"));
            assertTrue(range[1].equals("2b2b7bae3ca6a8ef2ccf6c68fb12dfc5"));
		} catch (Exception e) {
            e.printStackTrace();
		}
    }


    @Test
    public void testServerNotResponsible(){ 

        String status = null; 
        try {
            KVStore client = clientset.get(0); 

            //client.disconnect();
            client.put("hello", "hello"); // this key should go to the server at port 42000 

            // so going to connect to the other server and try and get it 
            KVStore client2 = new KVStore("localhost", 44000); 
            client2.connect(); 
            clientset.addElement(client2);

            KVMessage response = client2.get("hello");
            status = response.getStatus().toString(); 
            client2.disconnect();
        }
        catch (Exception e){
            e.printStackTrace();
        }

        assertTrue(status.equals("SERVER_NOT_RESPONSIBLE"));
        
    }

    @Test
    public void testRetry(){ 
        KVClient cmd = new KVClient(); 

        try {
            System.out.println("CUSTOM CLIENT COMMANDS: ");
            cmd.handleCommand("connect localhost 44000");
            Thread.sleep(100);
            
            cmd.handleCommand("get hello");
        } catch (Exception e) {} 

        assertTrue(cmd.getStore().getServerPort() == 42000); // this means the client has switched servers to get the key
    }

    @Test
    public void testDataTransfer(){
        // going to shut down the 42000 server and see if the hello key is moved to the 44000 server 
        clientset.get(0).disconnect();

        // Checking that it exists in 42000 first 
        String directoryPath = "data-0.0.0.0-42000/";
        String fileName = "hello";
        File file = new File(directoryPath, fileName);

        assertTrue(file.exists());

        //shutting down the server 
        try {
        System.out.println("SERVER SHUTDOWN INCOMING");
        serverset.get(0).kill();
        Thread.sleep(5000);
        /*while(serverset.get(0).running){
            Thread.sleep(1000);
            System.out.println("STATUS: " + serverset.get(0).state.toString());
        }*/

        } catch (Exception e){

        }

        //check if it was transferred to 44000 
        directoryPath = "data-0.0.0.0-44000/";
        file = new File(directoryPath, fileName);
        assertTrue(file.exists());

    }

    @Test 
    public void testDataPersistence(){
        // shutting down the remaining server and seeing if the hello key is still available when it is started again 
        String get_status = ""; 
        try { 
            serverset.get(1).kill();
            Thread.sleep(10000);
            
            System.out.println("REBOOTING SERVER");
            KVServer server = new KVServer(44000, 10, "FIFO", "localhost", 40000);
            serverset.addElement(server);
            Thread.sleep(1000);

            get_status = server.getKV("hello").getStatus().toString();
        } catch (Exception e){
            e.printStackTrace();
        }

        assertTrue(get_status.equals("GET_SUCCESS"));
        
    }

    @Test 
    public void testStoppedStatus(){
        String state = ""; 
        KVServer server = new KVServer(46000, 10, "FIFO", "localhost", 40000);
        serverset.addElement(server);
        state = serverset.get(3).state.toString();
        assertTrue(state.equals("STOPPED"));
    }
    @Test
    public void testWriteLock(){
        String state = ""; 
        try { 
            int count = 0; 
            while(count < 250){ // keep trying every 5ms cause not sure how long it will take to receive WRITE_LOCK message
                state = serverset.get(2).state.toString();
                if(state.toString() == "WRITE_LOCKED") count = 300; // break out of loop 
                Thread.sleep(5);
                count++; 
            }
            
        } catch (Exception e){
            e.printStackTrace();
        }
        assertTrue(state.equals("WRITE_LOCKED"));
    }

    @Test 
    public void testMetadata(){ 
        boolean contains44000 = false; 
        boolean contains46000 = false; 

        try {
            Thread.sleep(2000);
            KVServer server = serverset.get(3); 
            NavigableMap<String, IECSNode> hashRing = server.metaData.getHashRing();
            for(Map.Entry<String, IECSNode> entry : hashRing.entrySet()){
                int nodePort = entry.getValue().getNodePort();
                if(nodePort == 44000) contains44000 = true; 
                if(nodePort == 46000) contains46000 = true; 
            }
        } catch (Exception e){} 

        assertTrue(contains44000 && contains46000); 

    }

    @Test
    public void testECSDown(){
        ecs.disconnect();
        String get_status = ""; 

        KVServer server = serverset.get(2);
        get_status = server.getKV("hello").getStatus().toString();

        assertTrue(get_status.equals("GET_SUCCESS"));


    }

    @Test
	public void testEnd() {
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


    public static void deleteFilesRecursively(File file) {
        if (file.isDirectory()) {
            File[] files = file.listFiles();
            if (files != null) {
                for (File subFile : files) {
                    deleteFilesRecursively(subFile);
                }
            }
        }
        if (!file.delete()) {
            System.err.println("Failed to delete file: " + file.getAbsolutePath());
        } else {
            System.out.println("Deleted file: " + file.getAbsolutePath());
        }
    }

}