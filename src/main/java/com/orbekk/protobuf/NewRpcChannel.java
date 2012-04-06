package com.orbekk.protobuf;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Map;
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

public class NewRpcChannel implements com.google.protobuf.RpcChannel {
    public static int NUM_CONCURRENT_REQUESTS = 50;
    private static final Logger logger =
            Logger.getLogger(RpcChannel.class.getName());
    private final String host;
    private final int port;
    private final AtomicLong nextId = new AtomicLong(0);
    private final ExecutorService responseHandlerPool =
            Executors.newSingleThreadExecutor();
    private final BlockingQueue<Data.Request> requestQueue =
            new ArrayBlockingQueue(NUM_CONCURRENT_REQUESTS);
    private volatile Socket socket = null;
    private final ConcurrentHashMap<Long, RequestMetadata> ongoingRequests =
            new ConcurrentHashMap<Long, RequestMetadata>();
    
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
    
    class OutgoingHandler implements Runnable {
        private final Socket socket;
        private final BlockingQueue<Data.Request> requests;

        public OutgoingHandler(Socket socket,
                BlockingQueue<Data.Request> requests) {
            this.socket = socket;
            this.requests = requests;
        }
        
        @Override public void run() {
            for (;;) {
                try {
                    Data.Request request = requests.take();
                    request.writeDelimitedTo(socket.getOutputStream());
                } catch (InterruptedException e) {
                    tryCloseSocket(socket);
                    return;
                } catch (IOException e) {
                    tryCloseSocket(socket);
                    return;
                }
            }
        }


    }
    
    class IncomingHandler implements Runnable {
        private Socket socket;
        private ExecutorService responseHandlerPool;
        
        public IncomingHandler(Socket socket,
                ExecutorService responseHandlerPool) {
            this.socket = socket;
            this.responseHandlerPool = responseHandlerPool;
        }
        
        @Override public void run() {
            for (;;) {
                try {
                    Data.Response response = Data.Response
                            .parseDelimitedFrom(socket.getInputStream());
                    responseHandlerPool.execute(new ResponseHandler(response));
                } catch (IOException e) {
                    responseHandlerPool.shutdown();
                    tryCloseSocket(socket);
                    return;
                }
            }
        }
    }
    
    public static NewRpcChannel create(String host, int port)
            throws UnknownHostException, IOException {
        NewRpcChannel channel = new NewRpcChannel(host, port);
        channel.start();
        return channel;
    }
    
    NewRpcChannel(String host, int port) {
        this.host = host;
        this.port = port;
    }
    
    public void start() throws UnknownHostException, IOException {
        socket = new Socket(host, port);
        OutgoingHandler outgoing = new OutgoingHandler(socket,
                requestQueue);
        IncomingHandler incoming = new IncomingHandler(socket,
                responseHandlerPool);
        
        new Thread(outgoing, "RequestSender: " + host + ":" + port).start();
        new Thread(incoming, "RequestReceiver: " + host + ":" + port).start();
    }

    private void tryCloseSocket(Socket socket) {
        try {
            socket.close();
        } catch (IOException e1) {
            logger.log(Level.WARNING,
                    "Unable to close socket " + socket,
                    e1);
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
        ongoingRequests.put(id, request_);
        
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
    
    private void cancelRequest(RequestMetadata request, String reason) {
        throw new IllegalStateException("Not implemented");
    }
    
    private void handleResponse(Data.Response response) {
        RequestMetadata request =
                ongoingRequests.remove(response.getRequestId());
        if (request == null) {
            logger.info("Unknown request. Possible timeout?" + response);
            return;
        }
        try {
            Message responsePb = request.responsePrototype.toBuilder()
                    .mergeFrom(response.getResponseProto()).build();
            request.rpc.readFrom(response);
            request.done.run(responsePb);
        } catch (InvalidProtocolBufferException e) {
            cancelRequest(request, "invalid response from server");
        }
    }
}

