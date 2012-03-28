package com.orbekk.protobuf;

import java.util.logging.Level;
import java.util.logging.Logger;
import java.io.OutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Scanner;
import com.google.protobuf.Message;
import com.google.protobuf.Service;
import com.google.protobuf.RpcController;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.Descriptors;
import java.util.Map;
import java.util.HashMap;

public class SimpleProtobufServer extends Thread {
    private static Logger logger = Logger.getLogger(
            SimpleProtobufServer.class.getName());
    private ServerSocket serverSocket;
    private Map<String, Service> registeredServices =
            new HashMap<String, Service>();

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

    public synchronized void registerService(Service service) {
        String serviceName = service.getDescriptorForType().getFullName();
        if (registeredServices.containsKey(serviceName)) {
            logger.warning("Already registered service with this name.");
        }
        logger.info("Registering service: " + serviceName);
        registeredServices.put(serviceName, service);
    }

    public void handleRequest(Rpc.Request request, OutputStream out)
            throws IOException {
        Service service = registeredServices.get(request.getFullServiceName());
        final Rpc.Response.Builder response = Rpc.Response.newBuilder();
        response.setRequestId(request.getRequestId());
        if (service == null) {
            response.setError(Rpc.Response.Error.UNKNOWN_SERVICE);
            response.build().writeDelimitedTo(out);
            return;
        }
        Descriptors.MethodDescriptor method = service.getDescriptorForType()
                .findMethodByName(request.getMethodName());
        if (method == null) {
            response.setError(Rpc.Response.Error.UNKNOWN_METHOD);
            response.build().writeDelimitedTo(out);
            return;
        }
        RpcCallback<Message> doneCallback = new RpcCallback<Message>() {
            @Override public void run(Message responseMessage) {
                response.setResponseProto(responseMessage.toByteString());
            }
        };
        Message requestMessage = service.getRequestPrototype(method)
                .toBuilder()
                .mergeFrom(request.getRequestProto())
                .build();
        service.callMethod(method, null,  requestMessage, doneCallback);
        response.build().writeDelimitedTo(out);
    }

    private void handleConnection(final Socket connection) {
        new Thread(new Runnable() {
            @Override public void run() {
                try {
                    while (true) {
                        Rpc.Request r1 = Rpc.Request.parseDelimitedFrom(
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
                }
            }
        }).start();
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
    }

    public static void main(String[] args) {
        SimpleProtobufServer server = SimpleProtobufServer.create(10000);
        Test.TestService testService = new Test.TestService() {
            @Override public void run(RpcController controller,
                    Test.TestRequest request,
                    RpcCallback<Test.TestResponse> done) {
                System.out.println("Hello from TestService!");
                done.run(Test.TestResponse.newBuilder()
                        .setId("Hello from server.")
                        .build());
            }
        };
        server.registerService(testService);
        server.start();
        try {
            server.join();
        } catch (InterruptedException e) {
            System.out.println("Stopped.");
        }
    }
}
