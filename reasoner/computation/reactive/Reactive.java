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

    Identifier<?, ?> identifier();

    interface Identifier<P_IN, P_OUT> {

        String toString();

        // TODO: Weird to have a processor inside an Identifier, if anything we would expect to see a processor ID
        //  here, or use some kind of compound ID of Reactive + Processor where we need it
        Actor.Driver<? extends Processor<P_IN, P_OUT, ?, ?>> processor();

    }

    interface Provider<RECEIVER> extends Reactive {

        void registerReceiver(RECEIVER receiver);

        void pull(RECEIVER receiver);

        interface Publisher<PACKET> extends Provider<Receiver.Subscriber<PACKET>> {

            @Override
            void pull(Receiver.Subscriber<PACKET> subscriber);

            @Override
            void registerReceiver(Receiver.Subscriber<PACKET> subscriber);

            <MAPPED> Stream<PACKET, MAPPED> map(Function<PACKET, MAPPED> function);

            <MAPPED> Stream<PACKET, MAPPED> flatMap(Function<PACKET, FunctionalIterator<MAPPED>> function);

            Stream<PACKET, PACKET> buffer();

            Stream<PACKET, PACKET> deduplicate();

        }
    }

    interface Receiver<PROVIDER, PACKET> extends Reactive {

        void registerProvider(PROVIDER provider);

        void receive(PROVIDER provider, PACKET packet);

        interface Subscriber<PACKET> extends Receiver<Provider.Publisher<PACKET>, PACKET> {

            @Override
            void receive(Provider.Publisher<PACKET> publisher, PACKET packet);

            @Override
            void registerProvider(Provider.Publisher<PACKET> publisher);

            interface Finishable<PACKET> extends Reactive.Receiver.Subscriber<PACKET> {

                void finished();

            }
        }
    }

    interface Stream<INPUT, OUTPUT> extends Receiver.Subscriber<INPUT>, Provider.Publisher<OUTPUT> {

    }

}
