/*
 *  Copyright (c) 2017, salesforce.com, inc.
 *  All rights reserved.
 *  Licensed under the BSD 3-Clause license.
 *  For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.reactorgrpc;

import io.grpc.*;
import io.grpc.stub.StreamObserver;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("Duplicates")
public class ReactiveClientStandardServerInteropTest {
    private static Server server;
    private static ManagedChannel channel;

    @BeforeClass
    public static void setupServer() throws Exception {
        GreeterGrpc.GreeterImplBase svc = new GreeterGrpc.GreeterImplBase() {

            @Override
            public void sayHello(HelloRequest request, StreamObserver<HelloResponse> responseObserver) {
                responseObserver.onNext(HelloResponse.newBuilder().setMessage("Hello " + request.getName()).build());
                responseObserver.onCompleted();
            }

            @Override
            public void sayHelloRespStream(HelloRequest request, StreamObserver<HelloResponse> responseObserver) {
                responseObserver.onNext(HelloResponse.newBuilder().setMessage("Hello " + request.getName()).build());
                responseObserver.onNext(HelloResponse.newBuilder().setMessage("Hi " + request.getName()).build());
                responseObserver.onNext(HelloResponse.newBuilder().setMessage("Greetings " + request.getName()).build());
                responseObserver.onCompleted();
            }

            @Override
            public StreamObserver<HelloRequest> sayHelloReqStream(StreamObserver<HelloResponse> responseObserver) {
                return new StreamObserver<HelloRequest>() {
                    List<String> names = new ArrayList<>();

                    @Override
                    public void onNext(HelloRequest request) {
                        names.add(request.getName());
                    }

                    @Override
                    public void onError(Throwable t) {
                        responseObserver.onError(t);
                    }

                    @Override
                    public void onCompleted() {
                        String message = "Hello " + String.join(" and ", names);
                        responseObserver.onNext(HelloResponse.newBuilder().setMessage(message).build());
                        responseObserver.onCompleted();
                    }
                };
            }

            @Override
            public StreamObserver<HelloRequest> sayHelloBothStream(StreamObserver<HelloResponse> responseObserver) {
                return new StreamObserver<HelloRequest>() {
                    List<String> names = new ArrayList<>();

                    @Override
                    public void onNext(HelloRequest request) {
                        names.add(request.getName());
                    }

                    @Override
                    public void onError(Throwable t) {
                        responseObserver.onError(t);
                    }

                    @Override
                    public void onCompleted() {
                        // Will fail for odd number of names, but that's not what is being tested, so ¯\_(ツ)_/¯
                        for (int i = 0; i < names.size(); i += 2) {
                            String message = "Hello " + names.get(i) + " and " + names.get(i+1);
                            responseObserver.onNext(HelloResponse.newBuilder().setMessage(message).build());
                        }
                        responseObserver.onCompleted();
                    }
                };
            }
        };

        server = ServerBuilder.forPort(0).addService(svc).build().start();
        channel = ManagedChannelBuilder.forAddress("localhost", server.getPort()).usePlaintext(true).build();
    }

    @Before
    public void init() {
        StepVerifier.setDefaultTimeout(Duration.ofSeconds(1));
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
    public void oneToOne() {
        RxGreeterGrpc.RxGreeterStub stub = RxGreeterGrpc.newRxStub(channel);
        Mono<String> rxRequest = Mono.just("World");
        Mono<String> rxResponse = stub.sayHello(rxRequest.map(this::toRequest)).map(this::fromResponse);

        StepVerifier.create(rxResponse)
                .expectNext("Hello World")
                .verifyComplete();
    }

    @Test
    public void oneToMany() {
        RxGreeterGrpc.RxGreeterStub stub = RxGreeterGrpc.newRxStub(channel);
        Mono<String> rxRequest = Mono.just("World");
        Flux<String> rxResponse = stub.sayHelloRespStream(rxRequest.map(this::toRequest)).map(this::fromResponse);

        StepVerifier.create(rxResponse)
                .expectNext("Hello World", "Hi World", "Greetings World")
                .verifyComplete();
    }

    @Test
    public void manyToOne() {
        RxGreeterGrpc.RxGreeterStub stub = RxGreeterGrpc.newRxStub(channel);
        Flux<String> rxRequest = Flux.just("A", "B", "C");
        Mono<String> rxResponse = stub.sayHelloReqStream(rxRequest.map(this::toRequest)).map(this::fromResponse);

        StepVerifier.create(rxResponse)
                .expectNext("Hello A and B and C")
                .verifyComplete();
    }

    @Test
    public void manyToMany() {
        RxGreeterGrpc.RxGreeterStub stub = RxGreeterGrpc.newRxStub(channel);
        Flux<String> rxRequest = Flux.just("A", "B", "C", "D");
        Flux<String> rxResponse = stub.sayHelloBothStream(rxRequest.map(this::toRequest)).map(this::fromResponse);

        StepVerifier.create(rxResponse)
                .expectNext("Hello A and B", "Hello C and D")
                .verifyComplete();
    }

    private HelloRequest toRequest(String name) {
        return HelloRequest.newBuilder().setName(name).build();
    }

    private String fromResponse(HelloResponse response) {
        return response.getMessage();
    }
}
