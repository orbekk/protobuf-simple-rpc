package com.orbekk.protobuf;

import java.util.concurrent.CountDownLatch;
import java.util.logging.Logger;

import com.google.protobuf.RpcCallback;

public class SimpleProtobufClient {
    static final Logger logger =
            Logger.getLogger(SimpleProtobufClient.class.getName());

    public void run() {
        RpcChannel channel = RpcChannel.create("localhost", 10000);
        Test.TestService test = Test.TestService.newStub(channel);
        Test.TestRequest request = Test.TestRequest.newBuilder()
                .setId("Hello!")
                .build();
        int count = 10;
        final CountDownLatch stop = new CountDownLatch(count);
        for (int i = 0; i < count; i++) {
            logger.info("Sending request.");
            test.run(null, request, new RpcCallback<Test.TestResponse>() {
                @Override public void run(Test.TestResponse response) {
                    System.out.println("Response from server: " + response);
                    stop.countDown();
                }
            });
        }
        try {
            stop.await();
        } catch (InterruptedException e) {
            // Stop waiting.
        }
    }

    public static void main(String[] args) {
        new SimpleProtobufClient().run();
    }
}
