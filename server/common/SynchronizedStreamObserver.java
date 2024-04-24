/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.server.common;

import io.grpc.stub.StreamObserver;

public class SynchronizedStreamObserver<T> implements StreamObserver<T> {

    private final StreamObserver<T> streamObserver;

    public SynchronizedStreamObserver(StreamObserver<T> stream) {
        streamObserver = stream;
    }

    public static <T> StreamObserver<T> of(StreamObserver<T> streamObserver) {
        return new SynchronizedStreamObserver<>(streamObserver);
    }

    @Override
    public synchronized void onNext(T value) {
        streamObserver.onNext(value);
    }

    @Override
    public synchronized void onCompleted() {
        streamObserver.onCompleted();
    }

    @Override
    public synchronized void onError(Throwable t) {
        streamObserver.onError(t);
    }
}
