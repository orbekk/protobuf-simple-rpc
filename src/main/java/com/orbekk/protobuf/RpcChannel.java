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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;

public class RpcChannel implements com.google.protobuf.RpcChannel {
    public static int NUM_CONCURRENT_REQUESTS = 5;
    private static final Logger logger =
            Logger.getLogger(RpcChannel.class.getName());
    private final String host;
    private final int port;
    private final AtomicLong nextId = new AtomicLong(0);
    private final Timer timer;
    private final ExecutorService responseHandlerPool =
            Executors.newSingleThreadExecutor();
    private final BlockingQueue<Data.Request> requestQueue =
            new ArrayBlockingQueue<Data.Request>(NUM_CONCURRENT_REQUESTS);
    private volatile Socket socket = null;
    private final ConcurrentHashMap<Long, RequestMetadata> ongoingRequests =
            new ConcurrentHashMap<Long, RequestMetadata>();
    private volatile OutgoingHandler outgoingHandler = null;
    private volatile IncomingHandler incomingHandler = null;
    
    class RequestMetadata {
        public final long id;
        public final Rpc rpc;
        public final RpcCallback<Message> done;
        public final Message responsePrototype;
        
        public RequestMetadata(long id, Rpc rpc, RpcCallback<Message> done,
                Message responsePrototype) {
            this.id = id;
            this.rpc = rpc;
            this.done = done;
            this.responsePrototype = responsePrototype;
        }
    }
    
    class ResponseHandler implements Runnable {
        private final Data.Response response;
        
        public ResponseHandler(Data.Response response) {
            this.response = response;
        }
        
        @Override public void run() {
            handleResponse(response);
        }
    }
    
    class CancelRequestTask extends TimerTask {
        public final long id;
        
        public CancelRequestTask(long id) {
            this.id = id;
        }

        @Override
        public void run() {
            RequestMetadata request = ongoingRequests.remove(id);
            if (request != null) {
                cancelRequest(request, "timeout");
            }
        }
    }
    
    class OutgoingHandler extends Thread {
        private final Socket socket;
        private final BlockingQueue<Data.Request> requests;

        public OutgoingHandler(Socket socket,
                BlockingQueue<Data.Request> requests) {
            super("OutgoingHandler");
            this.socket = socket;
            this.requests = requests;
        }
        
        @Override public void run() {
            try {
                BufferedOutputStream outputStream = new BufferedOutputStream(
                        socket.getOutputStream());
                LinkedList<Data.Request> buffer =
                        new LinkedList<Data.Request>();
                for (;;) {
                    buffer.clear();
                    buffer.add(requests.take());
                    requests.drainTo(buffer);
                    for (Data.Request request : buffer) {
                        request.writeDelimitedTo(outputStream);
                    }
                    outputStream.flush();
                }
            } catch (InterruptedException e) {
                tryCloseSocket(socket);
                return;
            } catch (IOException e) {
                tryCloseSocket(socket);
                return;
            }
        }

        @Override public void interrupt() {
            super.interrupt();
            tryCloseSocket(socket);
        }
    }
    
    class IncomingHandler extends Thread {
        private Socket socket;
        private ExecutorService responseHandlerPool;
        
        public IncomingHandler(Socket socket,
                ExecutorService responseHandlerPool) {
            super("IncomingHandler");
            this.socket = socket;
            this.responseHandlerPool = responseHandlerPool;
        }
        
        @Override public void run() {
            try {
                BufferedInputStream inputStream = new BufferedInputStream(
                        socket.getInputStream());
                for (;;) {
                    Data.Response response = Data.Response
                            .parseDelimitedFrom(inputStream);
                    if (response == null) {
                        throw new IOException("Could not read response.");
                    }
                    responseHandlerPool.execute(new ResponseHandler(response));
                }
            } catch (IOException e) {
                responseHandlerPool.shutdown();
                tryCloseSocket(socket);
                return;
            }
        }
        
        @Override public void interrupt() {
            super.interrupt();
            tryCloseSocket(socket);
        }
    }
    
    public static RpcChannel createOrNull(String host, int port) {
        try {
            return create(host, port);
        } catch (UnknownHostException e) {
            logger.log(Level.WARNING, "Unable to create RPC channel.", e);
            return null;
        } catch (IOException e) {
            logger.log(Level.WARNING, "Unable to create RPC channel.", e);
            return null;
        }
    }

    public static RpcChannel create(String host, int port)
            throws UnknownHostException, IOException {
        Timer timer = new Timer();
        RpcChannel channel = new RpcChannel(timer, host, port);
        channel.start();
        return channel;
    }
    
    RpcChannel(Timer timer, String host, int port) {
        this.timer = timer;
        this.host = host;
        this.port = port;
    }
    
    public void start() throws UnknownHostException, IOException {
        if (outgoingHandler != null) {
            throw new IllegalStateException("start() called twice.");
        }
        socket = new Socket(host, port);
        outgoingHandler = new OutgoingHandler(socket, requestQueue);
        incomingHandler = new IncomingHandler(socket, responseHandlerPool);

        outgoingHandler.start();
        incomingHandler.start();
    }
    
    public void close() {
        tryCloseSocket(socket);
        outgoingHandler.interrupt();
        incomingHandler.interrupt();
        timer.cancel();
        cancelAllRequests("channel closed.");
    }

    private void tryCloseSocket(Socket socket) {
        try {
            cancelAllRequests("channel closed");
            socket.close();
        } catch (IOException e1) {
            logger.log(Level.WARNING,
                    "Unable to close socket " + socket,
                    e1);
        }
    }

    private void addTimeoutHandler(RequestMetadata request) {
        long timeout = request.rpc.getTimeout();
        if (timeout > 0) {
            timer.schedule(new CancelRequestTask(request.id), timeout);
        }
    }
    
    @Override
    public void callMethod(MethodDescriptor method,
            RpcController rpc, Message requestMessage,
            Message responsePrototype,
            RpcCallback<Message> done) {
        long id = nextId.incrementAndGet();
        Rpc rpc_ = (Rpc) rpc;
        RequestMetadata request_ = new RequestMetadata(id, rpc_, done,
                responsePrototype);
        addTimeoutHandler(request_);
        ongoingRequests.put(id, request_);
        
        if (logger.isLoggable(Level.FINER)) {
            logger.finer(String.format("O(%d) => %s(%s)",
                    id, method.getFullName(), requestMessage));
        }
        
        Data.Request requestData = Data.Request.newBuilder()
                .setRequestId(id)
                .setFullServiceName(method.getService().getFullName())
                .setMethodName(method.getName())
                .setRequestProto(requestMessage.toByteString())
                .build();
        
        try {
            requestQueue.put(requestData);
        } catch (InterruptedException e) {
            cancelRequest(request_, "channel closed");
        }
    }
    
    private void cancelAllRequests(String reason) {
        for (RequestMetadata request : ongoingRequests.values()) {
            cancelRequest(request, reason);
        }
    }
    
    private void cancelRequest(RequestMetadata request, String reason) {
        request.rpc.setFailed(reason);
        request.rpc.cancel();
        request.done.run(null);
        request.rpc.complete();
    }
    
    private void handleResponse(Data.Response response) {
        RequestMetadata request =
                ongoingRequests.remove(response.getRequestId());
        if (request == null) {
            logger.info("Unknown request. Possible timeout? " + response);
            return;
        }
        try {
            Message responsePb = null;
            if (response.hasResponseProto()) {
                responsePb = request.responsePrototype.toBuilder()
                        .mergeFrom(response.getResponseProto()).build();
            }
            if (logger.isLoggable(Level.FINER)) {
                logger.finer(String.format("O(%d) <= %s",
                        response.getRequestId(),
                        responsePb));
            }
            request.rpc.readFrom(response);
            if (responsePb == null && request.rpc.isOk()) {
                logger.warning("Invalid response from server: " + response);
                request.rpc.setFailed("invalid response from server.");
            }
            request.done.run(responsePb);
            request.rpc.complete();
        } catch (InvalidProtocolBufferException e) {
            cancelRequest(request, "invalid response from server");
        }
    }
}

