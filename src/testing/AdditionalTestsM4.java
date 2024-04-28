package testing;

import junit.framework.TestCase;
import org.junit.Test;

import client.KVStore;
import ecs.ECS;
import java.util.Vector;
import app_kvClient.KVClient;
import app_kvServer.KVServer;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.File;
import java.io.BufferedReader;
import java.io.FileReader;

public class AdditionalTestsM4 extends TestCase {
    private static Vector<KVStore> clientset = new Vector<>();
    private static ECS ecs;
    private static Vector<KVServer> serverset = new Vector<>();
    private static KVClient client1; 
    private static KVClient client2; 

    private ByteArrayOutputStream outContent;
    private FileOutputStream fileOut; 


    @Test
    public void test1Start() {

        outContent = new ByteArrayOutputStream();

        // Redirecting standard out to new stream 
        System.setOut(new PrintStream(outContent));


        // Delete old log file
        File file = new File("m4-log.txt");
        
        if (file.exists()) {
            file.delete();
        }

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

        try {
            Thread.sleep(50);
            fileOut = new FileOutputStream("m4-log.txt");
            writeToLog();
            fileOut.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

	}

    @Test
    public void test2Subscribe(){ 
        try {

        outContent = new ByteArrayOutputStream();
        System.setOut(new PrintStream(outContent));

        client1 = new KVClient(); 

        client1.handleCommand("connect localhost 44000");
        client1.handleCommand("put hello value");

        client1.handleCommand("subscribe hello");


        fileOut = new FileOutputStream("m4-log.txt", false);
        writeToLog();
        fileOut.close();
        
        assertTrue(searchStringInLog("the PUT was successful"));
        assertTrue(searchStringInLog("Subscribed to key: hello!"));
        
        } catch (Exception e) {}
    }

    @Test
    public void test3NotificationOnUpdate(){
        try{ 
        
        outContent = new ByteArrayOutputStream();
        System.setOut(new PrintStream(outContent));


        client2 = new KVClient(); 

        client2.handleCommand("connect localhost 46000");
        client2.handleCommand("put hello newvalue");

        Thread.sleep(30);

        fileOut = new FileOutputStream("m4-log.txt", false);
        writeToLog();
        fileOut.close();


        assertTrue(searchStringInLog("NOTIFICATION: You are subscribed key hello")); 
        assertTrue(searchStringInLog("Some client updated key: hello"));
        assertTrue(searchStringInLog("NEW VALUE: newvalue"));

        } catch (Exception e){}
    }

    @Test 
    public void test4NotificationOnDelete(){ 
        try { 
        outContent = new ByteArrayOutputStream();
        System.setOut(new PrintStream(outContent));

        client2.handleCommand("put hello"); // delete 

        Thread.sleep(30);
        fileOut = new FileOutputStream("m4-log.txt", false);
        writeToLog();
        fileOut.close();


        assertTrue(searchStringInLog("NOTIFICATION: You are subscribed key hello"));
        assertTrue(searchStringInLog("NEW VALUE: "));

        } catch (Exception e){} 
    }

    @Test 
    public void test5SubscribeBeforeCreated(){ 
        try{ 
        outContent = new ByteArrayOutputStream();
        System.setOut(new PrintStream(outContent));

        client2.handleCommand("subscribe newkey");

        client1.handleCommand("put newkey apple");

        Thread.sleep(30);
        fileOut = new FileOutputStream("m4-log.txt", false);
        writeToLog();
        fileOut.close();

        assertTrue(searchStringInLog("Subscribed to key: newkey!")); // client 2 subscribed successfully

        assertTrue(searchStringInLog("NOTIFICATION: You are subscribed key newkey")); // client 2 received notification
        assertTrue(searchStringInLog("NEW VALUE: apple"));

        } catch (Exception e){}
    }

    @Test 
    public void test6KeyLongerThan20Chars(){
        try { 
        
        outContent = new ByteArrayOutputStream();
        System.setOut(new PrintStream(outContent));

        client1.handleCommand("subscribe verylongkeythatislongerthan20characters");


        Thread.sleep(30);
        fileOut = new FileOutputStream("m4-log.txt", false);
        writeToLog();
        fileOut.close();


        assertTrue(searchStringInLog("Error! Key length is over 20 bytes!"));

        } catch (Exception e) {} 
    }

    @Test 
    public void test7Unsubscribe(){
        try {
        
        outContent = new ByteArrayOutputStream();
        System.setOut(new PrintStream(outContent));

        client2.handleCommand("unsubscribe newkey");

        client1.handleCommand("put newkey orange");

        Thread.sleep(30);
        fileOut = new FileOutputStream("m4-log.txt", false);
        writeToLog();
        fileOut.close();

        assertTrue(searchStringInLog("Unsubscribed from key: newkey"));
        assertFalse(searchStringInLog("NOTIFICATION: You are subscribed key newkey"));

        } catch (Exception e) {}
    }

    @Test 
    public void test8UnsubscribeNonExistentKey(){ 
        try {
        
        outContent = new ByteArrayOutputStream();
        System.setOut(new PrintStream(outContent));


        client2.handleCommand("unsubscribe randomkey");


        Thread.sleep(30);
        fileOut = new FileOutputStream("m4-log.txt", false);
        writeToLog();
        fileOut.close();


        assertTrue(searchStringInLog("You are not subscribed to key: randomkey"));
        

        } catch (Exception e){}
    }

    @Test
    public void test9MultipleSubscriptions(){ 
        try {
        outContent = new ByteArrayOutputStream();
        System.setOut(new PrintStream(outContent));

        client2.handleCommand("subscribe key1");
        client2.handleCommand("subscribe key2");
        client2.handleCommand("subscribe key3");

        client1.handleCommand("put key1 val");
        client1.handleCommand("put key2 val");
        client1.handleCommand("put key3 val");

        Thread.sleep(30);
        fileOut = new FileOutputStream("m4-log.txt", false);
        writeToLog();
        fileOut.close();

        assertTrue(searchStringInLog("NOTIFICATION: You are subscribed key key1"));
        assertTrue(searchStringInLog("NOTIFICATION: You are subscribed key key2"));
        assertTrue(searchStringInLog("NOTIFICATION: You are subscribed key key3"));

        } catch (Exception e){}
    }

    @Test
    public void test10NotificationForSelfPut(){
        try{ 
        outContent = new ByteArrayOutputStream();
        System.setOut(new PrintStream(outContent));

        client2.handleCommand("put key1 new");
        
        Thread.sleep(30);
        fileOut = new FileOutputStream("m4-log.txt", false);
        writeToLog();
        fileOut.close();


        assertTrue(searchStringInLog("NOTIFICATION: You are subscribed key key1"));
        assertTrue(searchStringInLog("NEW VALUE: new"));
        } catch (Exception e){}
    }

    @Test 
    public void test11SubscriptionsList(){
        try { 
        outContent = new ByteArrayOutputStream();
        System.setOut(new PrintStream(outContent));

        client2.handleCommand("subscriptions");
        
        Thread.sleep(30);
        fileOut = new FileOutputStream("m4-log.txt", false);
        writeToLog();
        fileOut.close();

        assertTrue(searchStringInLog("key1"));
        assertTrue(searchStringInLog("key2"));
        assertTrue(searchStringInLog("key3"));

        } catch (Exception e){}
    }

    
    @Test
	public void test12End() {
        
        System.setOut(System.out);

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
            ecs.disconnect(); 
        }

        try {
        fileOut.close();
        } catch (Exception e){} 
    }

    public boolean writeToLog(){ 
        try { 
            outContent.writeTo(fileOut); 
            return true; 
        } catch (Exception e){
            System.setOut(System.out);
            e.printStackTrace();
            return false; 
        }
    }


    public static boolean searchStringInLog(String searchString) throws IOException {
        try (BufferedReader reader = new BufferedReader(new FileReader("m4-log.txt"))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.contains(searchString)) {
                    return true;
                }
            }
        }
        return false;
    }
}
