package testing;

import java.io.IOException;

import org.apache.log4j.Level;

import app_kvServer.KVServer;
import junit.framework.Test;
import junit.framework.TestSuite;
import logger.LogSetup;


public class M2FunctionalityTest {

    public static Test suite() {
		TestSuite clientSuite = new TestSuite("Basic Storage ServerM2Tests-Suite");
		clientSuite.addTestSuite(AdditionalTestsM2.class);  
		return clientSuite;
	}

}