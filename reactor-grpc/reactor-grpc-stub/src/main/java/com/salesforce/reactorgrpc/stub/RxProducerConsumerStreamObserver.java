/*
 *  Copyright (c) 2017, salesforce.com, inc.
 *  All rights reserved.
 *  Licensed under the BSD 3-Clause license.
 *  For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.reactorgrpc.stub;

import com.google.common.base.Preconditions;
import io.grpc.stub.ClientCallStreamObserver;
import reactor.core.publisher.Flux;

/**
 * RxProducerConsumerStreamObserver configures client-side manual flow control for when the client is both producing
 * and consuming streams of messages.
 *
 * @param <TRequest>
 * @param <TResponse>
 */
public class RxProducerConsumerStreamObserver<TRequest, TResponse> extends RxConsumerStreamObserver<TRequest, TResponse> {
    private Flux<TRequest> rxProducer;
    private RxStreamBackpressureOnReadyHandler<TRequest> onReadyHandler;

    public RxProducerConsumerStreamObserver(Flux<TRequest> rxProducer) {
        this.rxProducer = rxProducer;
    }

    @Override
    public void beforeStart(ClientCallStreamObserver<TRequest> requestStream) {
        super.beforeStart(Preconditions.checkNotNull(requestStream));
        onReadyHandler = new RxStreamBackpressureOnReadyHandler<>(requestStream);
    }

    public void rxSubscribe() {
        rxProducer.subscribe(onReadyHandler);
    }

    public void cancel() {
        onReadyHandler.cancel();
    }
}
