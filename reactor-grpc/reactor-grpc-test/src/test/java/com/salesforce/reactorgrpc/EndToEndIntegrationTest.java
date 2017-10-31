/*
 *  Copyright (c) 2017, salesforce.com, inc.
 *  All rights reserved.
 *  Licensed under the BSD 3-Clause license.
 *  For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.reactorgrpc;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.io.IOException;
import java.time.Duration;

@SuppressWarnings("Duplicates")
public class EndToEndIntegrationTest {
    private static Server server;
    private static ManagedChannel channel;

    @BeforeClass
    public static void setupServer() throws Exception {
        GreeterGrpc.GreeterImplBase svc = new RxGreeterGrpc.GreeterImplBase() {

            @Override
            public Mono<HelloResponse> sayHello(Mono<HelloRequest> rxRequest) {
                return rxRequest.map(protoRequest -> greet("Hello", protoRequest));
            }

            @Override
            public Flux<HelloResponse> sayHelloRespStream(Mono<HelloRequest> rxRequest) {
                return rxRequest.flatMapMany(protoRequest -> Flux.just(
                        greet("Hello", protoRequest),
                        greet("Hi", protoRequest),
                        greet("Greetings", protoRequest)));
            }

            @Override
            public Mono<HelloResponse> sayHelloReqStream(Flux<HelloRequest> rxRequest) {
                return rxRequest
                        .map(HelloRequest::getName)
                        .collectList()
                        .map(names -> greet("Hello", String.join(" and ", names)));
            }

            @Override
            public Flux<HelloResponse> sayHelloBothStream(Flux<HelloRequest> rxRequest) {
                return rxRequest
                        .map(HelloRequest::getName)
                        .buffer(2)
                        .map(names -> greet("Hello", String.join(" and ", names)));
            }

            private HelloResponse greet(String greeting, HelloRequest request) {
                return greet(greeting, request.getName());
            }

            private HelloResponse greet(String greeting, String name) {
                return HelloResponse.newBuilder().setMessage(greeting + " " + name).build();
            }
        };

        server = ServerBuilder.forPort(0).addService(svc).build().start();
        channel = ManagedChannelBuilder.forAddress("localhost", server.getPort()).usePlaintext(true).build();
    }

    @Before
    public void init() {
        StepVerifier.setDefaultTimeout(Duration.ofSeconds(3));
    }

    @AfterClass
    public static void stopServer() throws InterruptedException {
        server.shutdown();
        server.awaitTermination();
        channel.shutdown();

        server = null;
        channel = null;
    }

    @Test
    public void oneToOne() throws IOException {
        RxGreeterGrpc.RxGreeterStub stub = RxGreeterGrpc.newRxStub(channel);
        Mono<HelloRequest> req = Mono.just(HelloRequest.newBuilder().setName("rxjava").build());
        Mono<HelloResponse> resp = stub.sayHello(req);

        StepVerifier.create(resp.map(HelloResponse::getMessage))
                .expectNext("Hello rxjava")
                .verifyComplete();
    }

    @Test
    public void oneToMany() throws IOException {
        RxGreeterGrpc.RxGreeterStub stub = RxGreeterGrpc.newRxStub(channel);
        Mono<HelloRequest> req = Mono.just(HelloRequest.newBuilder().setName("rxjava").build());
        Flux<HelloResponse> resp = stub.sayHelloRespStream(req);

        StepVerifier.create(resp.map(HelloResponse::getMessage))
                .expectNext("Hello rxjava", "Hi rxjava", "Greetings rxjava")
                .verifyComplete();
    }

    @Test
    public void manyToOne() throws Exception {
        RxGreeterGrpc.RxGreeterStub stub = RxGreeterGrpc.newRxStub(channel);
        Flux<HelloRequest> req = Flux.just(
                HelloRequest.newBuilder().setName("a").build(),
                HelloRequest.newBuilder().setName("b").build(),
                HelloRequest.newBuilder().setName("c").build());

        Mono<HelloResponse> resp = stub.sayHelloReqStream(req);

        StepVerifier.create(resp.map(HelloResponse::getMessage))
                .expectNext("Hello a and b and c")
                .verifyComplete();
    }

    @Test
    public void manyToMany() throws Exception {
        RxGreeterGrpc.RxGreeterStub stub = RxGreeterGrpc.newRxStub(channel);
        Flux<HelloRequest> req = Flux.just(
                HelloRequest.newBuilder().setName("a").build(),
                HelloRequest.newBuilder().setName("b").build(),
                HelloRequest.newBuilder().setName("c").build(),
                HelloRequest.newBuilder().setName("d").build(),
                HelloRequest.newBuilder().setName("e").build());

        Flux<HelloResponse> resp = stub.sayHelloBothStream(req);

        StepVerifier.create(resp.map(HelloResponse::getMessage))
                .expectNext("Hello a and b", "Hello c and d", "Hello e")
                .verifyComplete();
    }
}
