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

//    Processor<?, ?, ?, ?> processor();  // TODO: It's weird to be able to access your receiver/provider's processor, but this is needed for monitoring?

    Identifier<?, ?> identifier();

    interface Identifier<P_IN, P_OUT> {

        String toString();

        // TODO: Weird to have a processor inside an Identifier, if anything we would expect to see a processor ID
        //  here, or use some kind of compound ID of Reactive + Processor where we need it
        Actor.Driver<? extends Processor<P_IN, P_OUT, ?, ?>> processor();

    }

    interface Publisher<PACKET> extends Reactive {

        void pull(Subscriber<PACKET> subscriber);

        void registerReceiver(Subscriber<PACKET> subscriber);

        <MAPPED> Stream<PACKET, MAPPED> map(Function<PACKET, MAPPED> function);

        <MAPPED> Stream<PACKET, MAPPED> flatMap(Function<PACKET, FunctionalIterator<MAPPED>> function);

        Stream<PACKET, PACKET> buffer();

        Stream<PACKET, PACKET> deduplicate();

    }

    interface Subscriber<PACKET> extends Reactive {

        void receive(Publisher<PACKET> publisher, PACKET packet);

        void registerProvider(Publisher<PACKET> publisher);

        interface Finishable<PACKET> extends Reactive.Subscriber<PACKET> {

            void finished();

        }
    }

    interface Stream<INPUT, OUTPUT> extends Subscriber<INPUT>, Publisher<OUTPUT> {

    }
}
