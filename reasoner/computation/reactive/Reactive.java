/*
 * Copyright (C) 2021 Vaticle
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
 *
 */

package com.vaticle.typedb.core.reasoner.computation.reactive;

import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.reasoner.computation.actor.Processor;

import java.util.Set;
import java.util.function.Function;

public interface Reactive {

    String groupName();

    interface Provider<R> extends Reactive {

        void pull(Receiver<R> receiver, Set<Processor.Monitor.Reference> monitors);  // Should be idempotent if already pulling

        void propagateMonitors(Receiver<R> receiver, Set<Processor.Monitor.Reference> monitors);

        interface Publisher<T> extends Provider<T> {

            void publishTo(Receiver.Subscriber<T> subscriber);

            Stream<T,T> findFirst();

            <R> Stream<T, R> map(Function<T, R> function);

            <R> Stream<T,R> flatMapOrRetry(Function<T, FunctionalIterator<R>> function);

            Stream<T, T> buffer();

            Stream<T, T> deduplicate();

        }

    }

    interface Receiver<R> extends Reactive {

        void receive(Provider<R> provider, R packet);

        interface Subscriber<T> extends Receiver<T> {

            void subscribeTo(Provider<T> publisher);

        }

    }

    interface Stream<INPUT, OUTPUT> extends Receiver.Subscriber<INPUT>, Provider.Publisher<OUTPUT> {

    }

}
