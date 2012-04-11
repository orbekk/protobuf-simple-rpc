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
package com.orbekk.example;

import java.util.concurrent.CountDownLatch;

import com.google.protobuf.RpcCallback;
import com.orbekk.example.Example.FortuneReply;
import com.orbekk.protobuf.NewRpcChannel;
import com.orbekk.protobuf.Rpc;

public class ExampleClient {
    public void runClient(int port) {
        NewRpcChannel channel = null;
        try {
            channel = NewRpcChannel.createOrNull("localhost", port);
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
