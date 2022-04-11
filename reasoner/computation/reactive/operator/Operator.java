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

package com.vaticle.typedb.core.reasoner.computation.reactive.operator;

import com.vaticle.typedb.common.collection.Either;
import com.vaticle.typedb.core.reasoner.computation.reactive.Reactive.Publisher;
import com.vaticle.typedb.core.reasoner.computation.reactive.Reactive.Subscriber;

import java.util.Set;

public interface Operator {

    interface Source<OUTPUT> {

        boolean isExhausted(Subscriber<OUTPUT> subscriber);

        OUTPUT next(Subscriber<OUTPUT> subscriber);
    }

    interface Accepter<INPUT> extends Operator {

        void accept(Publisher<INPUT> publisher, INPUT packet);

    }

    interface Transformer<INPUT, OUTPUT> {

        Set<Publisher<INPUT>> initialise();

        Either<Publisher<INPUT>, Set<OUTPUT>> accept(Publisher<INPUT> publisher, INPUT packet);

    }

    interface Sink<INPUT> extends Accepter<INPUT> {
        // TODO: Add methods to usefully retrieve items from the sink
    }

    interface Pool<INPUT, OUTPUT> {

        boolean accept(Publisher<INPUT> publisher, INPUT packet);

        boolean hasNext(Subscriber<OUTPUT> subscriber);

        OUTPUT next(Subscriber<OUTPUT> subscriber);

    }

    interface Bridge<PACKET> extends Accepter<PACKET>, Source<PACKET> {
        // TODO: To be used for negation
    }

}
