package com.orbekk.protobuf;

import java.util.logging.Level;
import java.util.logging.Logger;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Scanner;

public class SimpleProtobufServer extends Thread {
    private static Logger logger = Logger.getLogger(
            SimpleProtobufServer.class.getName());
    ServerSocket serverSocket;

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

    private void handleConnection(final Socket connection) {
        new Thread(new Runnable() {
            @Override public void run() {
                try {
                    Rpc.Request r1 = Rpc.Request.parseDelimitedFrom(
                        connection.getInputStream());
                    Rpc.Request r2 = Rpc.Request.parseDelimitedFrom(
                        connection.getInputStream());
                    System.out.println(r1);
                    System.out.println(r2);
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
        SimpleProtobufServer.create(10000).start();
    }
}
