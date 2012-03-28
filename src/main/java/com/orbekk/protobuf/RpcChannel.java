package com.orbekk.protobuf;

import java.io.Closeable;
import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.protobuf.Descriptors;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;

public class RpcChannel extends Thread implements
        com.google.protobuf.RpcChannel {
    static final Logger logger =
            Logger.getLogger(RpcChannel.class.getName());
    private String host;
    private int port;
    private volatile Socket socket = null;
    private AtomicLong nextId = new AtomicLong(0);
    private Map<Long, RpcChannel.OngoingRequest> rpcs =
            Collections.synchronizedMap(
                    new HashMap<Long, RpcChannel.OngoingRequest>());
    private BlockingQueue<Socket> sockets = new LinkedBlockingQueue<Socket>();

    private static class OngoingRequest implements Closeable {
        long id;
        Rpc rpc;
        RpcCallback<Message> done;
        Message responsePrototype;
        Map<Long, RpcChannel.OngoingRequest> rpcs;
        
        public OngoingRequest(long id, Rpc rpc,
                RpcCallback<Message> done, Message responsePrototype,
                Map<Long, RpcChannel.OngoingRequest> rpcs) {
            this.id = id;
            this.rpc = rpc;
            this.done = done;
            this.responsePrototype = responsePrototype;
            this.rpcs = rpcs;
        }

        @Override
        public void close() throws IOException {
            throw new AssertionError("Not implemented");
        }
    }
    
    public static RpcChannel create(String host, int port) {
        RpcChannel channel = new RpcChannel(host, port);
        channel.start();
        return channel;
    }
    
    private RpcChannel(String host, int port) {
        this.host = host;
        this.port = port;
    }

    private Socket getSocket() {
        if (socket == null || socket.isClosed()) {
            try {
                logger.info("Creating new socket to " + host + ":" + port);
                socket = new Socket(host, port);
                sockets.add(socket);
            } catch (UnknownHostException e) {
                return null;
            } catch (IOException e) {
                logger.log(Level.WARNING,
                        "Could not establish connection.", e);
                return null;
            }
        }
        return socket;
    }
    
    private Data.Request createRequest(Descriptors.MethodDescriptor method,
            RpcController controller,
            Message requestMessage,
            Message responsePrototype,
            RpcCallback<Message> done) {
        long id = nextId.incrementAndGet();
        Rpc rpc = (Rpc)controller;
        OngoingRequest ongoingRequest = new OngoingRequest(id, rpc,
                done, responsePrototype, rpcs);
        rpcs.put(id, ongoingRequest);
        
        Data.Request request = Data.Request.newBuilder()
                .setRequestId(id)
                .setFullServiceName(method.getService().getFullName())
                .setMethodName(method.getName())
                .setRequestProto(requestMessage.toByteString())
                .build();
        
        return request;
    }
    
    private void finishRequest(Data.Response response) {
        OngoingRequest ongoingRequest = rpcs.remove(response.getRequestId());
        if (ongoingRequest != null) {
            try {
                Message responsePb = ongoingRequest.responsePrototype.toBuilder()
                        .mergeFrom(response.getResponseProto()).build();
                ongoingRequest.rpc.readFrom(response);
                ongoingRequest.done.run(responsePb);
            } catch (InvalidProtocolBufferException e) {
                throw new AssertionError("Should fail here.");
            }
        }
    }

    @Override public void callMethod(
            Descriptors.MethodDescriptor method,
            RpcController controller,
            Message requestMessage,
            Message responsePrototype,
            RpcCallback<Message> done) {
        try {
            Data.Request request = createRequest(method, controller,
                    requestMessage, responsePrototype, done);
            Socket socket = getSocket();
            request.writeDelimitedTo(socket.getOutputStream());
        } catch (IOException e) {
            throw new AssertionError("Should return error.");
        }
    }
    
    private void handleResponses(Socket socket) {
        try {
            logger.info("Handling responses to socket " + socket);
            while (!socket.isClosed()) {
                Data.Response response;
                response = Data.Response.parseDelimitedFrom(
                        socket.getInputStream());
                finishRequest(response);
            }
        } catch (IOException e) {
            if (!rpcs.isEmpty()) {
                logger.log(Level.WARNING, "IO Error. Canceling " +
                        rpcs.size() + " requests.", e);
                cancelAllRpcs();
            }
        } finally {
            if (socket != null && !socket.isClosed()) {
                try {
                    socket.close();
                } catch (IOException e) {
                    // Socket is closed.
                }
            }
        }
    }
    
    private void cancelAllRpcs() {
        synchronized (rpcs) {
            for (OngoingRequest request : rpcs.values()) {
                request.rpc.setFailed("connection closed");
                request.done.run(null);
            }
            rpcs.clear();
        }       
    }
    
    public void run() {
        while (!Thread.interrupted()) {
            try {
                Socket socket = sockets.take();
                handleResponses(socket);
            } catch (InterruptedException e) {
                // Interrupts handled by outer loop
            }
        }
    }

    public void close() {
        if (socket != null) {
            try {
                socket.close();
            } catch (IOException e) {
                logger.info("Error closing socket.");
            }
        }
    }
}