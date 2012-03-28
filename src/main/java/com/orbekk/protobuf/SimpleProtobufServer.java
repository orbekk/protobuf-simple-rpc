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

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.Service;

public class SimpleProtobufServer extends Thread {
    private static Logger logger = Logger.getLogger(
            SimpleProtobufServer.class.getName());
    private ServerSocket serverSocket;
    private Set<Socket> activeClientSockets = 
            Collections.synchronizedSet(new HashSet<Socket>());
    private Map<String, Service> registeredServices =
            Collections.synchronizedMap(
                    new HashMap<String, Service>());

    public static SimpleProtobufServer create(int port) {
        try {
            InetSocketAddress address = new InetSocketAddress(port);
            ServerSocket serverSocket = new ServerSocket();
            serverSocket.setReuseAddress(true);
            serverSocket.bind(address);
            return new SimpleProtobufServer(serverSocket);
        } catch (IOException e) {
            logger.log(Level.WARNING, "Could not create server. ", e);
            return null;
        }
    }

    public SimpleProtobufServer(ServerSocket serverSocket) {
        this.serverSocket = serverSocket;
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
        new Thread(new Runnable() {
            @Override public void run() {
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
        }).start();
    }
    
    @Override public void interrupt() {
        super.interrupt();
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
