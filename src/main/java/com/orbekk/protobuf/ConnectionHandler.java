package com.orbekk.protobuf;

import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;

public class ConnectionHandler {
    private final Socket connection;
    private final BlockingQueue<Data.Response> dispatcherOutput;
    private final RequestDispatcher dispatcher;

    private class IncomingHandler implements Runnable {
        @Override public void run() {
            dispatcher.start();
            while (!connection.isClosed()) {
                try {
                    Data.Request r = Data.Request.parseDelimitedFrom(
                            connection.getInputStream());
                    if (r == null) {
                        tryCloseConnection();
                    } else {
                        try {
                            dispatcher.handleRequest(r);
                        } catch (InterruptedException e) {
                            tryCloseConnection();
                            return;
                        }
                    }
                } catch (IOException e) {
                    tryCloseConnection();
                }
            }
            dispatcher.interrupt();
        }
    }

    private class OutgoingHandler implements Runnable {
        @Override public void run() {
            while (!connection.isClosed()) {
                try {
                    Data.Response response = dispatcherOutput.take();
                    try {
                        response.writeDelimitedTo(connection.getOutputStream());
                    } catch (IOException e) {
                        tryCloseConnection();
                    }
                } catch (InterruptedException e) {
                    tryCloseConnection();
                }
            }
            dispatcher.interrupt();
        }
    }
    
    public static ConnectionHandler create(Socket connection,
            ExecutorService requestPool, ServiceHolder services) {
        BlockingQueue<Data.Response> dispatcherOutput =
                new ArrayBlockingQueue(RequestDispatcher.DEFAULT_QUEUE_SIZE);
        RequestDispatcher dispatcher = new RequestDispatcher(
                requestPool, dispatcherOutput, services);
        return new ConnectionHandler(connection, dispatcherOutput,
                dispatcher);
    }
    
    ConnectionHandler(Socket connection,
            BlockingQueue<Data.Response> dispatcherOutput,
            RequestDispatcher dispatcher) {
        this.connection = connection;
        this.dispatcherOutput = dispatcherOutput;
        this.dispatcher = dispatcher;
    }

    public void closeConnection() {
        tryCloseConnection();
    }
    
    private void tryCloseConnection() {
        try {
            connection.close();
        } catch (IOException e) {
            // Assume connection is closed.
        }
    }
    
    public Runnable createIncomingHandler() {
        return new IncomingHandler();
    }
    
    public Runnable createOutgoingHandler() {
        return new OutgoingHandler();
    }
}
