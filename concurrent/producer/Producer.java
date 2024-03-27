/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.concurrent.producer;

import javax.annotation.concurrent.ThreadSafe;
import java.util.concurrent.Executor;

@ThreadSafe
public interface Producer<T> {

    void produce(Queue<T> queue, int request, Executor executor);

    void recycle();

    @ThreadSafe
    interface Queue<U> {

        void put(U item);

        void done();

        void done(Throwable e);
    }
}
