package com.orbekk.example;

import java.util.concurrent.CountDownLatch;

import com.google.protobuf.RpcCallback;
import com.orbekk.example.Example.FortuneReply;
import com.orbekk.protobuf.Rpc;
import com.orbekk.protobuf.RpcChannel;

public class ExampleClient {
    public void runClient(int port) {
        RpcChannel channel = null;
        try {
            channel = RpcChannel.create("localhost", port);
            Example.FortuneService service =
                    Example.FortuneService.newStub(channel);
            printFortune(service);
        } finally {
            if (channel != null) {
                channel.close();
            }
        }
        
    }

    public void printFortune(Example.FortuneService service) {
        Rpc rpc = new Rpc();  // Represents a single rpc call.
        Example.Empty request = Example.Empty.newBuilder().build();
        
        // An RPC call is asynchronous. A CountDownLatch is a nice way to wait
        // for the callback to finish.
        final CountDownLatch doneSignal = new CountDownLatch(1);
        service.getFortune(rpc, request, new RpcCallback<Example.FortuneReply>() {
            @Override public void run(FortuneReply reply) {
                System.out.println(reply.getFortune());
                doneSignal.countDown();
            }
        });
        
        try {
            doneSignal.await();
        } catch (InterruptedException e) {
            System.out.println("Interrupted while waiting for fortune. :(");
        }
    }
    public static void main(String[] args) {
        if (args.length == 0) {
            System.err.println("Usage: ExampleClient <port>");
            System.exit(1);
        }
        new ExampleClient().runClient(Integer.valueOf(args[0]));
    }
}
