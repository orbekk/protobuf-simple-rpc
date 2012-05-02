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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.protobuf.Descriptors;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.Service;

public class RequestDispatcher extends Thread {
    private static final Logger logger = Logger.getLogger(RequestDispatcher.class.getName());
    public static int DEFAULT_QUEUE_SIZE = 5;
    private volatile boolean isStopped = false;
    private final BlockingQueue<Data.Response> output;
    private final ServiceHolder services;
    
    /** A pool that can be shared among all dispatchers. */
    private final ExecutorService pool;

    private static class RequestHandler implements Runnable {
        private final Data.Request request;
        private final Data.Response.Builder response =
                Data.Response.newBuilder();
        private final BlockingQueue<Data.Response> output;
        private final ServiceHolder services;
        private final Rpc rpc = new Rpc();
        
        private final RpcCallback<Message> callback =
                new RpcCallback<Message>() {
            @Override public void run(Message responseMessage) {
                if (responseMessage == null && rpc.isOk()) {
                    throw new IllegalStateException(
                            "responseMessage is null, but rpc is ok:  " +
                            rpc);
                }
                if (responseMessage != null) {
                    response.setResponseProto(responseMessage.toByteString());
                }
                if (logger.isLoggable(Level.FINER)) {
                    logger.finer(String.format("I(%d): %s <= ",
                            request.getRequestId(), responseMessage));
                }
                rpc.writeTo(response);
                try {
                    output.put(response.build());
                } catch (InterruptedException e) {
                    // Terminate callback.
                    return;
                }
            }
        };
        
        public RequestHandler(Data.Request request,
                BlockingQueue<Data.Response> output,
                ServiceHolder services) {
            this.request = request;
            this.output = output;
            this.services = services;
        }
        
        public void internalRun() throws InterruptedException {
            response.setRequestId(request.getRequestId());
            Service service = services.get(request.getFullServiceName());
            if (service == null) {
                response.setError(Data.Response.RpcError.UNKNOWN_SERVICE);
                output.put(response.build());
                return;
            }
            
            Descriptors.MethodDescriptor method =
                    service.getDescriptorForType()
                            .findMethodByName(request.getMethodName());
            if (method == null) {
                response.setError(Data.Response.RpcError.UNKNOWN_METHOD);
                output.put(response.build());
                return;
            }
            
            Message requestMessage = null;
            try {
                requestMessage = service.getRequestPrototype(method)
                        .toBuilder().mergeFrom(request.getRequestProto()).build();
            } catch (InvalidProtocolBufferException e) {
                response.setError(Data.Response.RpcError.INVALID_PROTOBUF);
                output.put(response.build());
                return;
            }
            
            if (logger.isLoggable(Level.FINER)) {
                logger.fine(String.format("I(%d) => %s(%s)",
                        request.getRequestId(),
                        method.getFullName(),
                        requestMessage));
            }
            service.callMethod(method, rpc, requestMessage, callback);
        }
        
        @Override public void run() {
            try {
                internalRun();
            } catch (InterruptedException e) {
                // Terminate request.
                return;
            }
        }
    }
    
    public RequestDispatcher(ExecutorService pool,
            BlockingQueue<Data.Response> output,
            ServiceHolder services) {
        this.pool = pool;
        this.output = output;
        this.services = services;
    }

    public void handleRequest(Data.Request request) throws InterruptedException {
        RequestHandler handler = new RequestHandler(request, output, services);
        pool.execute(handler);
    }
}
