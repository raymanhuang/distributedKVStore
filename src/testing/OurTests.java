package testing;

import java.io.IOException;

import org.apache.log4j.Level;

import app_kvServer.KVServer;
import junit.framework.Test;
import junit.framework.TestSuite;
import logger.LogSetup;


public class OurTests {

    public static Test suite() {
		TestSuite clientSuite = new TestSuite("Basic Storage ServerOurTests-Suite");
		clientSuite.addTestSuite(AdditionalTest.class);  
		return clientSuite;
	}

}
