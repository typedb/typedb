/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.reasoner.processor.reactive;

import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.reasoner.processor.AbstractProcessor;

import java.util.function.Function;

public interface Reactive {

    AbstractProcessor<?, ?, ?, ?> processor();

    Identifier identifier();

    interface Identifier {

        String toString();

    }

    interface Publisher<PACKET> extends Reactive {

        void pull(Subscriber<PACKET> subscriber);

        void registerSubscriber(Subscriber<PACKET> subscriber);

        <MAPPED> Stream<PACKET, MAPPED> map(Function<PACKET, MAPPED> function);

        <MAPPED> Stream<PACKET, MAPPED> flatMap(Function<PACKET, FunctionalIterator<MAPPED>> function);

        Stream<PACKET, PACKET> buffer();

        Stream<PACKET, PACKET> distinct();

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
