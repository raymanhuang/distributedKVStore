
package testing;
import client.KVStore;
import ecs.ECS;
import ecs.ServerConnection;
import app_kvServer.KVServer;
import junit.framework.TestCase;
import org.junit.Before;
import org.junit.Test;
import shared.messages.KVMessage;
import shared.messages.KVMessage.StatusType;
import shared.messages.KVMessageImpl;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Wow5Test extends TestCase {

    private KVStore client;
    private ECS ecs;
    private Map<Integer,KVServer> serverset;
    private int clientPort;
    private Map<String, String> emailData;

    @Before
    public void setUp() throws Exception {
        ecs = new ECS("localhost", 54000);
        ecs.startECSServer();
        int numberOfServers = 5;
        serverset = new HashMap<>(); // Initialize serverset
        int count = 0;
        int i = 0;
        while(count < numberOfServers) {
            KVServer server = new KVServer(45000 + i, 10, "LRU", "localhost", 54000);
            Thread.sleep(5000); // Give time for servers to start
            if (server.port_available == false) { 
                i++;
                continue;
            }
            serverset.put(count, server);
            if (count == 0) {
                clientPort = 45000 + i; // Save the port of the first successful server
            }
            count++;
            i++;
        }
    
        Thread.sleep(5000); // Give time for servers to start

        client = new KVStore("localhost", clientPort);
        client.connect();
    
        String datasetPath = "maildir"; // Ensure this is the correct path
        emailData = processEnronDataset(datasetPath);
    }
    private Map<String, String> processEnronDataset(String datasetPath) {
        Map<String, String> emailData = new HashMap<>();
        Path path = Paths.get(datasetPath);
        try {
            Files.walk(path)
                    .filter(Files::isRegularFile)
                    .forEach(filePath -> {
                        String content = extractContent(filePath);
                        String messageID = extractMessageID(content);
                        if (messageID != null && !messageID.isEmpty()) {
                            String croppedMessageID = messageID.length() > 20 ? messageID.substring(0, 20) : messageID; // Crop message ID if longer than 20 bytes
                            String contentSnippet = content.substring(Math.min(content.length(), messageID.length()), Math.min(content.length(), messageID.length() + 15)); // Get next 15 characters of content
                            emailData.put(croppedMessageID, contentSnippet); // Modified to put croppedMessageID and contentSnippet
                        }
                    });
        } catch (IOException e) {
            e.printStackTrace();
        }
        return emailData;
    }

    private String extractContent(Path filePath) {
        try {
            return new String(Files.readAllBytes(filePath));
        } catch (IOException e) {
            e.printStackTrace();
            return "";
        }
    }

    private String extractMessageID(String content) {
        Pattern pattern = Pattern.compile("Message-ID: <(.+?)>");
        Matcher matcher = pattern.matcher(content);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return null;
    }

    @Test
    public void testSomething() {
        int operations = 50; // Number of PUT and GET operations
        Map<String, Long> putLatencies = new HashMap<>();
        Map<String, Long> getLatencies = new HashMap<>();

        // Perform PUT operations and measure latency
        long startPutTime = System.currentTimeMillis();
        emailData.keySet().stream().limit(operations).forEach(key -> {
            String value = emailData.get(key);
            long startTime = System.nanoTime();
            try {
                client.put(key, value);
                long endTime = System.nanoTime();
                putLatencies.put(key, (endTime - startTime) / 1000000); // Convert to milliseconds
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        long endPutTime = System.currentTimeMillis();

        // Calculate PUT throughput
        long totalPutTime = endPutTime - startPutTime; // Total time for PUT operations in milliseconds
        double putThroughput = (double) operations / (totalPutTime / 1000.0); // Operations per second

        // Perform GET operations and measure latency
        long startGetTime = System.currentTimeMillis();
        emailData.keySet().stream().limit(operations).forEach(key -> {
            long startTime = System.nanoTime();
            try {
                client.get(key);
                long endTime = System.nanoTime();
                getLatencies.put(key, (endTime - startTime) / 1000000); // Convert to milliseconds
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        long endGetTime = System.currentTimeMillis();

        // Calculate GET throughput
        long totalGetTime = endGetTime - startGetTime; // Total time for GET operations in milliseconds
        double getThroughput = (double) operations / (totalGetTime / 1000.0); // Operations per second

        // Logging results for visibility
        System.out.println("PUT Latency: " + totalPutTime + "ms, Throughput: " + putThroughput + " ops/sec");
        System.out.println("GET Latency: " + totalGetTime + "ms, Throughput: " + getThroughput + " ops/sec");
    }

 
    public void tearDown() { // Correct annotation and method name
        if (client != null) {
            client.disconnect();
        }
        for (KVServer server : serverset.values()) {
            server.kill(); // Assuming this method correctly stops the server
        }
        if (ecs != null) {
            ecs.disconnect(); // Assuming this method correctly stops the ECS
        }
    }
}