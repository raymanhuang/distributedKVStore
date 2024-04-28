package testing;

import java.io.IOException;

import org.apache.log4j.Level;

import app_kvServer.KVServer;
import ecs.ECS;
import junit.framework.Test;
import junit.framework.TestSuite;
import logger.LogSetup;



public class AllTests {

	static {
		try {
			new LogSetup("logs/testing/test.log", Level.ERROR);
			ECS ecs = new ECS("localhost", 45000);
        	ecs.startECSServer();
			KVServer server = new KVServer(50000, 10, "FIFO", "localhost", 45000);
			Thread.sleep(1000);
			
			server.clearStorage();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e){
			e.printStackTrace();
		}
	}
	
	
	public static Test suite() {
		TestSuite clientSuite = new TestSuite("Basic Storage ServerTest-Suite");
		clientSuite.addTestSuite(ConnectionTest.class);
		clientSuite.addTestSuite(InteractionTest.class);
		return clientSuite;
	}
	
}