# protobuf-simple-rpc

A simple socket based RPC transport layer for protocol buffers. Implements an
asynchronous RpcChannel. Look at the
[example](https://github.com/orbekk/protobuf-simple-rpc/tree/master/src/main/java/com/orbekk/example)
for how to implement a simple service.

## Background and target applications

During another project I needed fast RPCs for Android phones. In the beginning
I used [jsonrpc4j](http://code.google.com/p/jsonrpc4j/) and
[Jetty](http://jetty.codehaus.org/jetty/) which are a wonderful pieces of
software, but unfortunately can be really slow.

If you need a lightweight RPC mechanism you may find this useful.

## Asynchronous RPC

Because I want to keep this library small (and also because I'm lazy) I only
support asynchronous RPCs. You can simulate synchronous calls by waiting for
each RPC to complete.
