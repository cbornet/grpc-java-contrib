/*
 *  Copyright (c) 2017, salesforce.com, inc.
 *  All rights reserved.
 *  Licensed under the BSD 3-Clause license.
 *  For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.reactorgrpc;

import com.google.protobuf.Empty;
import com.salesforce.servicelibs.NumberProto;
import com.salesforce.servicelibs.RxNumbersGrpc;
import io.grpc.*;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("ALL")
public class CancellationPropagationIntegrationTest {
    private static Server server;
    private static ManagedChannel channel;
    private static TestService svc = new TestService();

    private static class TestService extends RxNumbersGrpc.NumbersImplBase {
        private AtomicInteger lastNumberProduced = new AtomicInteger(Integer.MIN_VALUE);
        private AtomicBoolean wasCanceled = new AtomicBoolean(false);
        private AtomicBoolean explicitCancel = new AtomicBoolean(false);

        public void reset() {
            lastNumberProduced.set(Integer.MIN_VALUE);
            wasCanceled.set(false);
            explicitCancel.set(false);
        }

        public int getLastNumberProduced() {
            return lastNumberProduced.get();
        }

        public boolean wasCanceled() {
            return wasCanceled.get();
        }

        public void setExplicitCancel(boolean explicitCancel) {
            this.explicitCancel.set(explicitCancel);
        }

        @Override
        public Flux<NumberProto.Number> responsePressure(Mono<Empty> request) {
            // Produce a very long sequence
            return Flux.fromIterable(new Sequence(10000))
                    .doOnNext(i -> lastNumberProduced.set(i))
                    .map(CancellationPropagationIntegrationTest::protoNum)
                    .doOnCancel(() -> {
                        wasCanceled.set(true);
                        System.out.println("Server canceled");
                    });
        }

        @Override
        public Mono<NumberProto.Number> requestPressure(Flux<NumberProto.Number> request) {
            if (explicitCancel.get()) {
                // Process a very long sequence
                Disposable subscription = request.subscribe(n -> System.out.println("S: " + n.getNumber(0)));
                return Mono
                        .just(protoNum(-1))
                        .delayElement(Duration.ofMillis(250))
                        // Explicitly cancel by disposing the subscription
                        .doOnSuccess(x -> subscription.dispose());
            } else {
                // Process some of a very long sequence and cancel implicitly with a take(10)
                return request.map(req -> req.getNumber(0))
                        .doOnNext(System.out::println)
                        .take(10)
                        .last(-1)
                        .map(CancellationPropagationIntegrationTest::protoNum);
            }
        }

        @Override
        public Flux<NumberProto.Number> twoWayPressure(Flux<NumberProto.Number> request) {
            return requestPressure(request).flux();
        }
    }

    @BeforeClass
    public static void setupServer() throws Exception {
        server = ServerBuilder.forPort(0).addService(svc).build().start();
        channel = ManagedChannelBuilder.forAddress("localhost", server.getPort()).usePlaintext(true).build();
    }

    @Before
    public void init() {
        StepVerifier.setDefaultTimeout(Duration.ofSeconds(3));
        svc.reset();
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
    public void clientCanCancelServerStreamExplicitly() throws InterruptedException {
        AtomicInteger lastNumberConsumed = new AtomicInteger(Integer.MAX_VALUE);
        RxNumbersGrpc.RxNumbersStub stub = RxNumbersGrpc.newRxStub(channel);
        Flux<NumberProto.Number> test = stub
                .responsePressure(Mono.just(Empty.getDefaultInstance()))
                .doOnNext(number -> {lastNumberConsumed.set(number.getNumber(0)); System.out.println("C: " + number.getNumber(0));})
                .doOnError(throwable -> System.out.println(throwable.getMessage()))
                .doOnComplete(() -> System.out.println("Completed"))
                .doOnCancel(() -> System.out.println("Client canceled"));

        Disposable subscription = test.publish().connect();

        Thread.sleep(1000);
        subscription.dispose();
        Thread.sleep(1000);

        // Cancellation may or may not deliver the last generated message due to delays in the gRPC processing thread
        assertThat(Math.abs(lastNumberConsumed.get() - svc.getLastNumberProduced())).isLessThanOrEqualTo(3);
        assertThat(svc.wasCanceled()).isTrue();
    }

    @Test
    public void clientCanCancelServerStreamImplicitly() throws InterruptedException {
        RxNumbersGrpc.RxNumbersStub stub = RxNumbersGrpc.newRxStub(channel);
        Flux<NumberProto.Number> test = stub
                .responsePressure(Mono.just(Empty.getDefaultInstance()))
                .doOnNext(number -> System.out.println(number.getNumber(0)))
                .doOnError(throwable -> System.out.println(throwable.getMessage()))
                .doOnComplete(() -> System.out.println("Completed"))
                .doOnCancel(() -> System.out.println("Client canceled"))
                .take(10);

        Disposable subscription = test.publish().connect();

        Thread.sleep(1000);

        assertThat(svc.wasCanceled()).isTrue();
    }

    @Test
    public void serverCanCancelClientStreamImplicitly() {
        RxNumbersGrpc.RxNumbersStub stub = RxNumbersGrpc.newRxStub(channel);

        svc.setExplicitCancel(false);

        AtomicBoolean requestWasCanceled = new AtomicBoolean(false);
        AtomicBoolean requestDidProduce = new AtomicBoolean(false);

        Flux<NumberProto.Number> request = Flux.fromIterable(new Sequence(10000))
                .map(CancellationPropagationIntegrationTest::protoNum)
                .doOnNext(x -> {
                    requestDidProduce.set(true);
                    System.out.println("Produced: " + x.getNumber(0));
                })
                .doOnCancel(() -> {
                    requestWasCanceled.set(true);
                    System.out.println("Client canceled");
                });

        Mono<NumberProto.Number> observer = stub
                .requestPressure(request)
                .doOnSuccess(number -> System.out.println(number.getNumber(0)))
                .doOnError(throwable -> System.out.println(throwable.getMessage()));

        StepVerifier.create(observer)
                .verifyError(StatusRuntimeException.class);

        assertThat(requestWasCanceled.get()).isTrue();
        assertThat(requestDidProduce.get()).isTrue();
    }

    @Test
    public void serverCanCancelClientStreamExplicitly() {
        RxNumbersGrpc.RxNumbersStub stub = RxNumbersGrpc.newRxStub(channel);

        svc.setExplicitCancel(true);

        AtomicBoolean requestWasCanceled = new AtomicBoolean(false);
        AtomicBoolean requestDidProduce = new AtomicBoolean(false);

        Flux<NumberProto.Number> request = Flux.fromIterable(new Sequence(10000))
                .map(CancellationPropagationIntegrationTest::protoNum)
                .doOnNext(n -> {
                    requestDidProduce.set(true);
                    System.out.println("P: " + n.getNumber(0));
                })
                .doOnCancel(() -> {
                    requestWasCanceled.set(true);
                    System.out.println("Client canceled");
                });

        Mono<NumberProto.Number> observer = stub
                .requestPressure(request)
                .doOnSuccess(number -> System.out.println(number.getNumber(0)))
                .doOnError(throwable -> System.out.println(throwable.getMessage()));

        StepVerifier.create(observer)
                .verifyError(StatusRuntimeException.class);
        assertThat(requestWasCanceled.get()).isTrue();
        assertThat(requestDidProduce.get()).isTrue();
    }

    @Test
    public void serverCanCancelClientStreamImplicitlyBidi() {
        RxNumbersGrpc.RxNumbersStub stub = RxNumbersGrpc.newRxStub(channel);

        svc.setExplicitCancel(false);

        AtomicBoolean requestWasCanceled = new AtomicBoolean(false);
        AtomicBoolean requestDidProduce = new AtomicBoolean(false);

        Flux<NumberProto.Number> request = Flux.fromIterable(new Sequence(10000))
                .map(CancellationPropagationIntegrationTest::protoNum)
                .doOnNext(x -> {
                    requestDidProduce.set(true);
                    System.out.println("Produced: " + x.getNumber(0));
                })
                .doOnCancel(() -> {
                    requestWasCanceled.set(true);
                    System.out.println("Client canceled");
                });

        Flux<NumberProto.Number> observer = stub
                .twoWayPressure(request)
                .doOnNext(number -> System.out.println(number.getNumber(0)))
                .doOnError(throwable -> System.out.println(throwable.getMessage()));

        StepVerifier.create(observer)
                .verifyError(StatusRuntimeException.class);
        assertThat(requestWasCanceled.get()).isTrue();
        assertThat(requestDidProduce.get()).isTrue();
    }

    @Test
    public void serverCanCancelClientStreamExplicitlyBidi() {
        RxNumbersGrpc.RxNumbersStub stub = RxNumbersGrpc.newRxStub(channel);

        svc.setExplicitCancel(true);

        AtomicBoolean requestWasCanceled = new AtomicBoolean(false);
        AtomicBoolean requestDidProduce = new AtomicBoolean(false);

        Flux<NumberProto.Number> request = Flux.fromIterable(new Sequence(10000))
                .map(CancellationPropagationIntegrationTest::protoNum)
                .doOnNext(n -> {
                    requestDidProduce.set(true);
                    System.out.println("P: " + n.getNumber(0));
                })
                .doOnCancel(() -> {
                    requestWasCanceled.set(true);
                    System.out.println("Client canceled");
                });

        Flux<NumberProto.Number> observer = stub
                .twoWayPressure(request)
                .doOnNext(number -> System.out.println(number.getNumber(0)))
                .doOnError(throwable -> System.out.println(throwable.getMessage()));

        StepVerifier.create(observer)
                .verifyError(StatusRuntimeException.class);
        assertThat(requestWasCanceled.get()).isTrue();
        assertThat(requestDidProduce.get()).isTrue();
    }

    private static NumberProto.Number protoNum(int i) {
        Integer[] ints = {i};
        return NumberProto.Number.newBuilder().addAllNumber(Arrays.asList(ints)).build();
    }
}
