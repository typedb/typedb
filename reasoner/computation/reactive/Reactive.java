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
import com.vaticle.typedb.core.concurrent.actor.Actor;
import com.vaticle.typedb.core.reasoner.computation.actor.Processor;

import java.util.function.Function;

public interface Reactive {

    Identifier identifier();

    interface Identifier extends Reactive {  // TODO: Don't extend Reactive if possible

        String toString();

        Actor.Driver<? extends Processor<?, ?, ?, ?>> processor();

        interface Input<PACKET> extends Identifier {

            @Override
            Actor.Driver<? extends Processor<PACKET, ?, ?, ?>> processor();

        }

        interface Output<PACKET> extends Identifier {

            @Override
            Actor.Driver<? extends Processor<?, PACKET, ?, ?>> processor();

        }
    }

    interface Provider<PACKET> extends Reactive {

        void pull(Identifier.Input<PACKET> receiverId);

    }

    interface Publisher<T> extends Reactive {

        void pull(Subscriber<T> subscriber);

        void registerSubscriber(Subscriber<T> subscriber);

        Stream<T,T> findFirst();

        <R> Stream<T, R> map(Function<T, R> function);

        <R> Stream<T,R> flatMapOrRetry(Function<T, FunctionalIterator<R>> function);

        Stream<T, T> buffer();

        Stream<T, T> deduplicate();

    }

    interface Receiver<R> extends Reactive {

        void receive(Identifier.Output<R> providerId, R packet);

    }

    interface Subscriber<R> extends Reactive {

        void receive(Publisher<R> publisher, R packet);

        void registerPublisher(Publisher<R> publisher);

        interface Finishable<T> extends Reactive.Subscriber<T> {

            void finished();

        }
    }

    interface Stream<INPUT, OUTPUT> extends Subscriber<INPUT>, Publisher<OUTPUT> {

    }

}
