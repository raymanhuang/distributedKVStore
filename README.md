# distributedKVStore
Course project for Distributed Systems at UofT

# Milestone 4 README

## Project Overview
This document provides instructions for setting up and interacting with the distributed key-value store system developed for Milestone 4, including the newly implemented notification mechanism that allows clients to subscribe to key mutations.

## Getting Started
To get started, you'll need to launch the ECS (External Configuration Service), servers, and clients in the following order:

### Starting the ECS

The ECS Should be started first as servers must connect to it inorder to function properly
'''
java -jar m4-ecs.jar localhost 6000
'''

where localhost x represents the server adress and port number

once the ecs has started and requests user input, enter :start


### Starting a Server
To start a server instance, run the following command. Ensure each server is started with a unique port number.
'''
java -jar m4-server.jar 3000 10 FIFO localhost 6000
'''
parameters: 3000 10 FIFO localhost 6000 represent the servers port number, cache size, Caching Statedgy and the ECS adress and port number



### Starting a Client
'''
java -jar m4-client.jar
'''
after staring the client connect to a server by entering: connect <server adress> <serverport>




## Client Commands
Once a client is connected, you can use the following commands to interact with the key-value store and utilize the notification mechanism.

### Subscribe to a Key
To subscribe to updates (mutations) on a specific key:
```
subscribe <key>
```

### Unsubscribe from a Key
To stop receiving updates on a specific key:
```
unsubscribe <key>
```

### View Subscriptions
To print the list of keys you are currently subscribed to:
```
subscriptions
```

These commands enable real-time interaction with the distributed key-value store, allowing clients to stay informed about important data mutations.

## Conclusion
For further assistance, please refer to the detailed project documentation or contact Any member from our group (group number 21)

