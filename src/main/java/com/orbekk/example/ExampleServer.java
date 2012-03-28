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

import java.util.Random;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.orbekk.example.Example.Empty;
import com.orbekk.example.Example.FortuneReply;
import com.orbekk.protobuf.SimpleProtobufServer;

public class ExampleServer {
    public final static String fortunes[] = new String[] {
      "  The difference between the right word and the almost right word is " +
      "the difference between lightning and the lightning bug.  -- Mark Twain",
      "You will lose your present job and have to become a door to door " +
      "mayonnaise salesman.",
      "You worry too much about your job.  Stop it.  You are not paid enough " +
      "to worry."
    };
    
    public class FortuneService extends Example.FortuneService {
        Random random = new Random();
        
        @Override
        public void getFortune(RpcController controller, Empty request,
                RpcCallback<FortuneReply> done) {
            String fortune = fortunes[random.nextInt(fortunes.length)];
            FortuneReply.Builder reply =
                    FortuneReply.newBuilder().setFortune(fortune);
            done.run(reply.build());
        }
    }
    
    public void runServer(int port) {
        SimpleProtobufServer server = SimpleProtobufServer.create(port);
        server.registerService(new FortuneService());
        server.start();
        System.out.println("Running server on port " + server.getPort());
        try {
            server.join();
        } catch (InterruptedException e) {
        }
    }
    
    public static void main(String[] args) {
        int port = 0;
        if (args.length > 0) {
            port = Integer.valueOf(args[0]);
        }
        new ExampleServer().runServer(port);
    }
}
