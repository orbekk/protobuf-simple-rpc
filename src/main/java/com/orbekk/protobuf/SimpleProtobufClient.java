package com.orbekk.protobuf;

import java.net.Socket;
import java.net.UnknownHostException;
import java.io.IOException;

public class SimpleProtobufClient {
    public void run() {
        try {
            Socket socket = new Socket("localhost", 10000);
            Rpc.Request r1 = Rpc.Request.newBuilder()
                .setFullServiceName("Service1")
                .build();
            Rpc.Request r2 = Rpc.Request.newBuilder()
                .setFullServiceName("Service2")
                .build();
            r1.writeDelimitedTo(socket.getOutputStream());
            r2.writeDelimitedTo(socket.getOutputStream());
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        new SimpleProtobufClient().run();
    }
}
