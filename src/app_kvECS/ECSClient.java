package app_kvECS;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.UnknownHostException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import shared.*;
import ecs.*;
import logger.LogSetup;

import java.util.Map;
import java.util.Collection;

public class ECSClient implements IECSClient {

    private static Logger logger = Logger.getRootLogger();
    private static final String PROMPT = "ECS> ";
    private BufferedReader stdin;
    private boolean stop = false;
    private ECS ecs = null;

    public ECSClient(String ecsAddress, int ecsPort) {
        this.ecs = new ECS(ecsAddress, ecsPort);
    }

    @Override
    public boolean start() {
        // TODO
        return ecs.startECSServer();
    }


    private void handleCommand(String cmdLine) {
		String[] tokens = cmdLine.split("\\s+");

		if(tokens[0].equals("start")) {	
            if(this.start() == false){
                logger.info("Unable to start!");
            } else {
                logger.info("Start Success!");
            }
		}  
	}

    public void run() {
		while(!stop) {
			stdin = new BufferedReader(new InputStreamReader(System.in));
			System.out.print(PROMPT);
			
			try {
				String cmdLine = stdin.readLine();
				this.handleCommand(cmdLine);
			} catch (IOException e) {
				stop = true;
				logger.info("ECSClient does not respond - Application terminated ");
			}
		}
	}

    public static void main(String[] args) {
        
        if (args.length == 2) {
            String ecsAddress = args[0];
            int ecsPort = Integer.parseInt(args[1]);
    
            try {
                new LogSetup("logs/ecs.log", Level.INFO);
                ECSClient ecsClient = new ECSClient(ecsAddress, ecsPort);
                ecsClient.run();
            } catch (IOException e) {
                System.out.println("Error! Unable to initialize logger!");
                e.printStackTrace();
                System.exit(1);
            }
        } else if (args[0].startsWith("-")){

            String ecsAddress = "localhost"; 
			int ecsPort = 50001; 

            for(int i=0; i < args.length - 1; i += 2){
                if(args[i].equals("-p")){ // port number
                    ecsPort = Integer.parseInt(args[i+1]); 
                } else if (args[i].equals("-a")){ // address 
                    ecsAddress = args[i+1];
                } else if (args[i].equals("-ll")){ // loglevel 
                    if(LogSetup.isValidLevel(args[i+1])){
                        logger.setLevel(LogSetup.stringToLevel(args[i+1]));
                    }
                }
            }

            try {
                new LogSetup("logs/ecs.log", Level.ALL);
                ECSClient ecsClient = new ECSClient(ecsAddress, ecsPort);
                ecsClient.run();
            } catch (IOException e) {
                System.out.println("Error! Unable to initialize logger!");
                e.printStackTrace();
                System.exit(1);
            }
        } else {
            System.out.println("Usage: ECSClient <ECS address> <ECS port>");
            System.exit(1);
        }   
        
    }
}
