package testing;

import java.io.IOException;

import org.apache.log4j.Level;

import app_kvServer.KVServer;
import junit.framework.Test;
import junit.framework.TestSuite;
import logger.LogSetup;

public class PerformanceTests {

    public static Test suite() {
		TestSuite clientSuite = new TestSuite("Basic Storage ServerPerformanceTest-Suite");
		clientSuite.addTestSuite(Wowtest.class);
		clientSuite.addTestSuite(Wow2Test.class);
		clientSuite.addTestSuite(Wow3Test.class);
		clientSuite.addTestSuite(Wow4Test.class);
		//clientSuite.addTestSuite(Wow5Test.class);
		clientSuite.addTestSuite(Wow6Test.class);
		clientSuite.addTestSuite(Wow7Test.class);
		clientSuite.addTestSuite(Wow8Test.class);
		
		return clientSuite;
	}

}