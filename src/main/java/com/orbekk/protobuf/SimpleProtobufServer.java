/**
 * Copyright 2012 Kjetil Ørbekk <kjetil.orbekk@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orbekk.protobuf;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.protobuf.Service;

public class SimpleProtobufServer extends Thread {
    private static int DEFAULT_NUM_HANDLERS = 20;
    private static int DEFAULT_CONCURRENT_REQUESTS = 1;

    private final static Logger logger = Logger.getLogger(
            SimpleProtobufServer.class.getName());
    private final ServerSocket serverSocket;
    private final ExecutorService incomingHandlerPool;
    private final ExecutorService outgoingHandlerPool =
            Executors.newCachedThreadPool();
    private final ExecutorService requestHandlerPool;
    private final ServiceHolder services = new ServiceHolder();
    private final Set<ConnectionHandler> activeConnections =
            Collections.synchronizedSet(new HashSet<ConnectionHandler>());
    
    public static SimpleProtobufServer create(int port) {
        return create(port, DEFAULT_NUM_HANDLERS, DEFAULT_CONCURRENT_REQUESTS);
    }
    
    public static SimpleProtobufServer create(int port, int maxNumHandlers,
            int maxConcurrentRequests) {
        try {
            InetSocketAddress address = new InetSocketAddress(port);
            ServerSocket serverSocket = new ServerSocket();
            serverSocket.setReuseAddress(true);
            serverSocket.bind(address);
            ExecutorService incomingHandlerPool =
                    Executors.newFixedThreadPool(maxNumHandlers);
            ExecutorService requestHandlerPool =
                    Executors.newFixedThreadPool(maxConcurrentRequests);
            return new SimpleProtobufServer(serverSocket, incomingHandlerPool,
                    requestHandlerPool);
        } catch (IOException e) {
            logger.log(Level.WARNING, "Could not create server. ", e);
            return null;
        }
    }

    public SimpleProtobufServer(ServerSocket serverSocket, 
            ExecutorService incomingHandlerPool,
            ExecutorService requestHandlerPool) {
        this.serverSocket = serverSocket;
        this.incomingHandlerPool = incomingHandlerPool;
        this.requestHandlerPool = requestHandlerPool;
    }
    
    public int getPort() {
        return serverSocket.getLocalPort();
    }

    public void registerService(Service service) {
        services.registerService(service);
    }
    
    public void removeService(Service service) {
        services.removeService(service);
    }
    
    public void removeService(String fullServiceName) {
        services.removeService(fullServiceName);
    }

    private synchronized void handleConnection(Socket connection) {
        if (serverSocket.isClosed()) {
            return;
        }
        
        final ConnectionHandler handler = ConnectionHandler.create(
                connection, requestHandlerPool, services);
        activeConnections.add(handler);
        final Runnable realIncomingHandler = handler.createIncomingHandler();
        
        class HelperHandler implements Runnable {
            @Override public void run() {
                activeConnections.add(handler);
                try {
                    realIncomingHandler.run();
                } finally {
                    activeConnections.remove(handler);
                }
            }
        }
        
        incomingHandlerPool.execute(new HelperHandler());
        outgoingHandlerPool.execute(handler.createOutgoingHandler());
    }
    
    @Override public synchronized void interrupt() {
        super.interrupt();
        for (ConnectionHandler handler : activeConnections) {
            handler.closeConnection();
        }

        try {
            serverSocket.close();
        } catch (IOException e) {
            logger.log(Level.WARNING, "Error closing socket.", e);
        }
    }

    public void run() {
        logger.info("Running server on port " + serverSocket.getLocalPort());
        while (!serverSocket.isClosed()) {
            try {
                Socket connection = serverSocket.accept();
                handleConnection(connection);
            } catch (IOException e) {
                logger.log(Level.WARNING, "Could not establish connection. ",
                        e);
            }
        }
        logger.info("Server exits.");
    }
}
