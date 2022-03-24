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

    }

    interface Provider<PACKET> extends Reactive {

        void pull(Receiver.Input<PACKET> receiverId);

        @Override
        Output<PACKET> identifier();

        interface Output<PACKET> extends Identifier {

            @Override
            Actor.Driver<? extends Processor<?, PACKET, ?, ?>> processor();

        }
    }

    interface Publisher<PACKET> extends Reactive {

        void pull(Subscriber<PACKET> subscriber);

        void registerSubscriber(Subscriber<PACKET> subscriber);

        Stream<PACKET, PACKET> findFirst();

        <MAPPED> Stream<PACKET, MAPPED> map(Function<PACKET, MAPPED> function);

        <MAPPED> Stream<PACKET, MAPPED> flatMapOrRetry(Function<PACKET, FunctionalIterator<MAPPED>> function);

        Stream<PACKET, PACKET> buffer();

        Stream<PACKET, PACKET> deduplicate();

    }

    interface Receiver<PACKET> extends Reactive {

        void receive(Provider.Output<PACKET> providerId, PACKET packet);

        @Override
        Input<PACKET> identifier();

        interface Input<PACKET> extends Identifier {

            @Override
            Actor.Driver<? extends Processor<PACKET, ?, ?, ?>> processor();

        }
    }

    interface Subscriber<PACKET> extends Reactive {

        void receive(Publisher<PACKET> publisher, PACKET packet);

        void registerPublisher(Publisher<PACKET> publisher);

        interface Finishable<PACKET> extends Reactive.Subscriber<PACKET> {

            void finished();

        }
    }

    interface Stream<INPUT, OUTPUT> extends Subscriber<INPUT>, Publisher<OUTPUT> {

    }

}
