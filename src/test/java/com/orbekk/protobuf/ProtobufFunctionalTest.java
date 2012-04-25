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

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Before;
import org.junit.Ignore;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.orbekk.protobuf.Test.Type1;
import com.orbekk.protobuf.Test.Type2;

public class ProtobufFunctionalTest {
    CountDownLatch returnC = new CountDownLatch(1);
    SimpleProtobufServer server = SimpleProtobufServer.create(0, 50, 1);
    int serverport = server.getPort();
    RpcChannel channel;
    TestService directService = new TestService();
    Test.Service service;
    
    @Before public void setUp() throws Exception {
        channel = RpcChannel.create("localhost", serverport);
        service = Test.Service.newStub(channel);
        server.start();
        server.registerService(directService);
    }
    
    public class TestService extends Test.Service {
        @Override
        public void testA(RpcController controller, Type1 request,
                RpcCallback<Type2> done) {
            Type2 response = Type2.newBuilder()
                    .setMessage("TestA")
                    .build();
            done.run(response);
        }

        @Override
        public void testB(RpcController controller, Type1 request,
                RpcCallback<Type2> done) {
            controller.setFailed("error");
            done.run(null);
        }

        @Override
        public void testC(RpcController controller, Type1 request,
                RpcCallback<Type2> done) {
            try {
                returnC.await();
            } catch (InterruptedException e) {
            }
            Type2 response = Type2.newBuilder()
                    .setResult(request.getA() + request.getB())
                    .setMessage("TestC result")
                    .build();
            done.run(response);
        }
    }
    
    @org.junit.Test public void testNewRpcChannel() throws Exception {
        RpcChannel channel = RpcChannel.create("localhost", serverport);
        Test.Service service = Test.Service.newStub(channel);
        Test.Type1 request = Test.Type1.newBuilder().build();
        int count = 10000;
        final CountDownLatch stop = new CountDownLatch(count);
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            final Rpc rpc = new Rpc();
            service.testA(rpc, request, new RpcCallback<Type2>() {
                @Override public void run(Type2 result) {
                    stop.countDown();
                }
            });
        }
        stop.await();
        long elapsedTime = System.currentTimeMillis() - startTime;
        System.out.println("Elapsed time for " + count + " requests: " +
                elapsedTime + ". " + count/elapsedTime*1000 + "r/s");
    }
    
    @org.junit.Test public void testDoneSignal() throws Exception {
        RpcChannel channel = RpcChannel.create("localhost", serverport);
        Test.Service service = Test.Service.newStub(channel);
        Test.Type1 request = Test.Type1.newBuilder().build();

        final AtomicBoolean callbackFinished = new AtomicBoolean(false);
        Rpc rpc = new Rpc();
        service.testA(rpc, request, new RpcCallback<Type2>() {
            @Override public void run(Type2 result) {
                callbackFinished.set(true);
            }
        });
        rpc.await();
        assertThat(callbackFinished.get(), is(true));
    }
    
    @org.junit.Test public void respondsNormally() throws Exception {
        Test.Type1 request = Test.Type1.newBuilder().build();
        int count = 10;
        final CountDownLatch stop = new CountDownLatch(count);
        for (int i = 0; i < count; i++) {
            final Rpc rpc = new Rpc();
            service.testA(rpc, request, new RpcCallback<Type2>() {
                @Override public void run(Type2 result) {
                    assertThat(result.getMessage(), is("TestA"));
                    assertThat(rpc.isOk(), is(true));
                    stop.countDown();
                }
            });
        }
        stop.await();
    }
    
    @org.junit.Test public void testError() throws Exception {
        Test.Type1 request = Test.Type1.newBuilder().build();
        final CountDownLatch stop = new CountDownLatch(1);
        final Rpc rpc = new Rpc();
        service.testB(rpc, request, new RpcCallback<Type2>() {
            @Override public void run(Type2 result) {
                assertThat(rpc.isOk(), is(false));
                assertThat(rpc.failed(), is(true));
                assertThat(rpc.errorText(), is("error"));
                stop.countDown();
            }
        });
        stop.await();
    }
    
    @org.junit.Test public void failsWhenServerDies() throws Exception {
        Test.Type1 request = Test.Type1.newBuilder().build();
        final CountDownLatch stop = new CountDownLatch(1);
        final Rpc rpc = new Rpc();
        service.testC(rpc, request, new RpcCallback<Type2>() {
            @Override public void run(Type2 result) {
                assertThat(rpc.failed(), is(true));
                assertThat(rpc.errorText(), is("channel closed"));
                stop.countDown();
            }
        });
        server.interrupt();
        stop.await();
    }
    
    @org.junit.Test public void testTimout() throws Exception {
        Test.Type1 request = Test.Type1.newBuilder().build();
        final CountDownLatch stop = new CountDownLatch(1);

        final Rpc rpc = new Rpc();
        rpc.setTimeout(1);
        service.testC(rpc, request, new RpcCallback<Type2>() {
            @Override public void run(Type2 result) {
                stop.countDown();
            }
        });
        rpc.await();
        assertThat(rpc.failed(), is(true));
        assertThat(rpc.errorText(), is("timeout"));
        returnC.countDown();
        stop.await();
    }
}
