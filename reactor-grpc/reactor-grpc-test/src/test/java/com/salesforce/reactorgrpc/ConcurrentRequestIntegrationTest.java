/*
 *  Copyright (c) 2017, salesforce.com, inc.
 *  All rights reserved.
 *  Licensed under the BSD 3-Clause license.
 *  For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.reactorgrpc;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("Duplicates")
public class ConcurrentRequestIntegrationTest {
    private static Server server;
    private static ManagedChannel channel;

    @BeforeClass
    public static void setupServer() throws Exception {
        GreeterGrpc.GreeterImplBase svc = new RxGreeterGrpc.GreeterImplBase() {

            @Override
            public Mono<HelloResponse> sayHello(Mono<HelloRequest> rxRequest) {
                return rxRequest
                        .doOnSuccess(System.out::println)
                        .map(protoRequest -> greet("Hello", protoRequest));
            }

            @Override
            public Flux<HelloResponse> sayHelloRespStream(Mono<HelloRequest> rxRequest) {
                return rxRequest
                        .doOnSuccess(System.out::println)
                        .flatMapMany(protoRequest -> Flux.just(
                            greet("Hello", protoRequest),
                            greet("Hi", protoRequest),
                            greet("Greetings", protoRequest)));
            }

            @Override
            public Mono<HelloResponse> sayHelloReqStream(Flux<HelloRequest> rxRequest) {
                return rxRequest
                        .doOnNext(System.out::println)
                        .map(HelloRequest::getName)
                        .collectList()
                        .map(names -> greet("Hello", String.join(" and ", names)));
            }

            @Override
            public Flux<HelloResponse> sayHelloBothStream(Flux<HelloRequest> rxRequest) {
                return rxRequest
                        .doOnNext(System.out::println)
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

    @AfterClass
    public static void stopServer() throws InterruptedException {
        server.shutdown();
        server.awaitTermination();
        channel.shutdown();

        server = null;
        channel = null;
    }

    @Test
    public void fourKindsOfRequestAtOnce() throws Exception {
        StepVerifier.setDefaultTimeout(Duration.ofSeconds(3));

        RxGreeterGrpc.RxGreeterStub stub = RxGreeterGrpc.newRxStub(channel);

        // == MAKE REQUESTS ==
        // One to One
        Mono<HelloRequest> req1 = Mono.just(HelloRequest.newBuilder().setName("rxjava").build());
        Mono<HelloResponse> resp1 = stub.sayHello(req1);

        // One to Many
        Mono<HelloRequest> req2 = Mono.just(HelloRequest.newBuilder().setName("rxjava").build());
        Flux<HelloResponse> resp2 = stub.sayHelloRespStream(req2);

        // Many to One
        Flux<HelloRequest> req3 = Flux.just(
                HelloRequest.newBuilder().setName("a").build(),
                HelloRequest.newBuilder().setName("b").build(),
                HelloRequest.newBuilder().setName("c").build());

        Mono<HelloResponse> resp3 = stub.sayHelloReqStream(req3);

        // Many to Many
        Flux<HelloRequest> req4 = Flux.just(
                HelloRequest.newBuilder().setName("a").build(),
                HelloRequest.newBuilder().setName("b").build(),
                HelloRequest.newBuilder().setName("c").build(),
                HelloRequest.newBuilder().setName("d").build(),
                HelloRequest.newBuilder().setName("e").build());

        Flux<HelloResponse> resp4 = stub.sayHelloBothStream(req4);

        // == VERIFY RESPONSES ==
        ListeningExecutorService executorService = MoreExecutors.listeningDecorator(Executors.newCachedThreadPool());

        // Run all four verifications in parallel
        try {
            // One to One
            ListenableFuture<Boolean> oneToOne = executorService.submit(() -> {
                StepVerifier.create(resp1.map(HelloResponse::getMessage))
                        .expectNext("Hello rxjava")
                        .verifyComplete();
                return true;
            });

            // One to Many
            ListenableFuture<Boolean> oneToMany = executorService.submit(() -> {
                StepVerifier.create(resp2.map(HelloResponse::getMessage))
                        .expectNext("Hello rxjava", "Hi rxjava", "Greetings rxjava")
                        .verifyComplete();
                return true;
            });

            // Many to One
            ListenableFuture<Boolean> manyToOne = executorService.submit(() -> {
                StepVerifier.create(resp3.map(HelloResponse::getMessage))
                        .expectNext("Hello a and b and c")
                        .verifyComplete();
                return true;
            });

            // Many to Many
            ListenableFuture<Boolean> manyToMany = executorService.submit(() -> {
                StepVerifier.create(resp4.map(HelloResponse::getMessage))
                        .expectNext("Hello a and b", "Hello c and d", "Hello e")
                        .verifyComplete();
                return true;
            });

            ListenableFuture<List<Boolean>> allFutures = Futures.allAsList(Lists.newArrayList(oneToOne, oneToMany, manyToOne, manyToMany));
            // Block for response
            List<Boolean> results = allFutures.get(3, TimeUnit.SECONDS);
            assertThat(results).containsExactly(true, true, true, true);

        } finally {
            executorService.shutdown();
        }
    }
}
