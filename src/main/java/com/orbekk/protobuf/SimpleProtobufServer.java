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
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.HashSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.Service;

public class SimpleProtobufServer extends Thread {
    private volatile boolean isStopped = false;
    private final static Logger logger = Logger.getLogger(
            SimpleProtobufServer.class.getName());
    private final ServerSocket serverSocket;
    private final Set<Socket> activeClientSockets = 
            Collections.synchronizedSet(new HashSet<Socket>());
    private final Map<String, Service> registeredServices =
            Collections.synchronizedMap(new HashMap<String, Service>());
    private final ExecutorService pool;

    public static SimpleProtobufServer create(int port, int maxNumHandlers) {
        try {
            InetSocketAddress address = new InetSocketAddress(port);
            ServerSocket serverSocket = new ServerSocket();
            serverSocket.setReuseAddress(true);
            serverSocket.bind(address);
            ExecutorService pool =
                    Executors.newFixedThreadPool(maxNumHandlers);
            return new SimpleProtobufServer(serverSocket, pool);
        } catch (IOException e) {
            logger.log(Level.WARNING, "Could not create server. ", e);
            return null;
        }
    }

    public SimpleProtobufServer(ServerSocket serverSocket, ExecutorService pool) {
        this.serverSocket = serverSocket;
        this.pool = pool;
    }
    
    public int getPort() {
        return serverSocket.getLocalPort();
    }

    public void registerService(Service service) {
        String serviceName = service.getDescriptorForType().getFullName();
        if (registeredServices.containsKey(serviceName)) {
            logger.warning("Already registered service with this name.");
        }
        logger.info("Registering service: " + serviceName);
        registeredServices.put(serviceName, service);
    }

    public void handleRequest(Data.Request request, OutputStream out)
            throws IOException {
        final Service service = registeredServices.get(request.getFullServiceName());
        Rpc rpc = new Rpc();
        final Data.Response.Builder response = Data.Response.newBuilder();
        response.setRequestId(request.getRequestId());
        if (service == null) {
            response.setError(Data.Response.RpcError.UNKNOWN_SERVICE);
            response.build().writeDelimitedTo(out);
            return;
        }
        final Descriptors.MethodDescriptor method = service.getDescriptorForType()
                .findMethodByName(request.getMethodName());
        if (method == null) {
            response.setError(Data.Response.RpcError.UNKNOWN_METHOD);
            response.build().writeDelimitedTo(out);
            return;
        }
        RpcCallback<Message> doneCallback = new RpcCallback<Message>() {
            @Override public void run(Message responseMessage) {
                if (responseMessage == null) {
                    responseMessage = service
                            .getResponsePrototype(method)
                            .toBuilder().build();
                }
                response.setResponseProto(responseMessage.toByteString());
            }
        };
        Message requestMessage = service.getRequestPrototype(method)
                .toBuilder()
                .mergeFrom(request.getRequestProto())
                .build();
        service.callMethod(method, rpc, requestMessage, doneCallback);
        rpc.writeTo(response);
        response.build().writeDelimitedTo(out);
    }

    private void handleConnection(final Socket connection) {
        if (isStopped) {
            return;
        }
        Runnable handler = new Runnable() {
            @Override public void run() {
                logger.info("Handling client connection " + connection);
                activeClientSockets.add(connection);
                try {
                    while (true) {
                        Data.Request r1 = Data.Request.parseDelimitedFrom(
                            connection.getInputStream());
                        if (r1 == null) {
                            try {
                                connection.close();
                            } catch (IOException e) {
                                // Connection is closed.
                            }
                        }
                        handleRequest(r1, connection.getOutputStream());
                    }
                } catch (IOException e) {
                    logger.info("Closed connection: " + connection);
                } finally {
                    try {
                        connection.close();
                    } catch (IOException e) {
                    }
                    activeClientSockets.remove(connection);
                }
            }
        };
        pool.execute(handler);
    }
    
    @Override public void interrupt() {
        super.interrupt();
        isStopped = true;
        for (Socket socket : activeClientSockets) {
            try {
                socket.close();
            } catch (IOException e) {
                logger.log(Level.WARNING, "Error closing socket.", e);
            }
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
