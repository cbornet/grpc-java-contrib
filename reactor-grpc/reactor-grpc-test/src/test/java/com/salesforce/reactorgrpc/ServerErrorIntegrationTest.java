/*
 *  Copyright (c) 2017, salesforce.com, inc.
 *  All rights reserved.
 *  Licensed under the BSD 3-Clause license.
 *  For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.reactorgrpc;

import io.grpc.*;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;

@SuppressWarnings("ALL")
public class ServerErrorIntegrationTest {
    private static Server server;
    private static ManagedChannel channel;

    @BeforeClass
    public static void setupServer() throws Exception {
        GreeterGrpc.GreeterImplBase svc = new RxGreeterGrpc.GreeterImplBase() {
            @Override
            public Mono<HelloResponse> sayHello(Mono<HelloRequest> rxRequest) {
                return Mono.error(new StatusRuntimeException(Status.INTERNAL));
            }

            @Override
            public Flux<HelloResponse> sayHelloRespStream(Mono<HelloRequest> rxRequest) {
                return Flux.error(new StatusRuntimeException(Status.INTERNAL));
            }

            @Override
            public Mono<HelloResponse> sayHelloReqStream(Flux<HelloRequest> rxRequest) {
                return Mono.error(new StatusRuntimeException(Status.INTERNAL));
            }

            @Override
            public Flux<HelloResponse> sayHelloBothStream(Flux<HelloRequest> rxRequest) {
                return Flux.error(new StatusRuntimeException(Status.INTERNAL));
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
    public static void stopServer() {
        server.shutdown();
        channel.shutdown();

        server = null;
        channel = null;
    }

    @Test
    public void oneToOne() {
        RxGreeterGrpc.RxGreeterStub stub = RxGreeterGrpc.newRxStub(channel);
        Mono<HelloResponse> resp = stub.sayHello(Mono.just(HelloRequest.getDefaultInstance()));

        StepVerifier.create(resp)
                .verifyErrorMatches(t -> t instanceof StatusRuntimeException && ((StatusRuntimeException)t).getStatus() == Status.INTERNAL);
    }

    @Test
    public void oneToMany() {
        RxGreeterGrpc.RxGreeterStub stub = RxGreeterGrpc.newRxStub(channel);
        Flux<HelloResponse> resp = stub.sayHelloRespStream(Mono.just(HelloRequest.getDefaultInstance()));
        Flux<HelloResponse> test = resp
                .doOnNext(msg -> System.out.println(msg))
                .doOnError(throwable -> System.out.println(throwable.getMessage()))
                .doOnComplete(() -> System.out.println("Completed"))
                .doOnCancel(() -> System.out.println("Client canceled"));

        StepVerifier.create(test)
                .verifyErrorMatches(t -> t instanceof StatusRuntimeException && ((StatusRuntimeException)t).getStatus() == Status.INTERNAL);
    }

    @Test
    public void manyToOne() {
        RxGreeterGrpc.RxGreeterStub stub = RxGreeterGrpc.newRxStub(channel);
        Mono<HelloResponse> resp = stub.sayHelloReqStream(Flux.just(HelloRequest.getDefaultInstance()));
        StepVerifier.create(resp)
                .verifyErrorMatches(t -> t instanceof StatusRuntimeException && ((StatusRuntimeException)t).getStatus() == Status.INTERNAL);
    }

    @Test
    public void manyToMany() {
        RxGreeterGrpc.RxGreeterStub stub = RxGreeterGrpc.newRxStub(channel);
        Flux<HelloResponse> resp = stub.sayHelloBothStream(Flux.just(HelloRequest.getDefaultInstance()));
        StepVerifier.create(resp)
                .verifyErrorMatches(t -> t instanceof StatusRuntimeException && ((StatusRuntimeException)t).getStatus() == Status.INTERNAL);
    }

}
