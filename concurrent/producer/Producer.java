/*
 * Copyright (C) 2022 Vaticle
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
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
