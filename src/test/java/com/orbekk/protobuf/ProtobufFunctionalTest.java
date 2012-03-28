package com.orbekk.protobuf;

import java.util.concurrent.CountDownLatch;

import org.junit.Before;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.orbekk.protobuf.Test.Type1;
import com.orbekk.protobuf.Test.Type2;

public class ProtobufFunctionalTest {
    CountDownLatch returnC = new CountDownLatch(1);
    SimpleProtobufServer server = SimpleProtobufServer.create(0);
    int serverport = server.getPort();
    RpcChannel channel = RpcChannel.create("localhost", serverport);
    TestService directService = new TestService();
    Test.Service service = Test.Service.newStub(channel);
    
    @Before public void setUp() {
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
                assertThat(rpc.errorText(), is("connection closed"));
                stop.countDown();
            }
        });
        server.interrupt();
        stop.await();
    }
}
