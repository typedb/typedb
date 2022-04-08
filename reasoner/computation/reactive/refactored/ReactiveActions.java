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

package com.vaticle.typedb.core.reasoner.computation.reactive.refactored;

import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.reasoner.computation.reactive.Reactive;
import com.vaticle.typedb.core.reasoner.computation.reactive.refactored.operator.Operator;

import java.util.function.Function;

public interface ReactiveActions {

    interface PublisherActions<OUTPUT> extends ReactiveActions {

        void processEffects(Operator.Effects effects);

        void subscriberReceive(Reactive.Subscriber<OUTPUT> subscriber, OUTPUT packet);

        void tracePull(Reactive.Subscriber<OUTPUT> subscriber);

        <MAPPED> Reactive.Stream<OUTPUT, MAPPED> map(Reactive.Publisher<OUTPUT> publisher,
                                                     Function<OUTPUT, MAPPED> function);

        <MAPPED> Reactive.Stream<OUTPUT, MAPPED> flatMap(Reactive.Publisher<OUTPUT> publisher,
                                                         Function<OUTPUT, FunctionalIterator<MAPPED>> function);

        Reactive.Stream<OUTPUT, OUTPUT> buffer(Reactive.Publisher<OUTPUT> publisher);

        Reactive.Stream<OUTPUT,OUTPUT> distinct(Reactive.Publisher<OUTPUT> publisher);

    }

    interface SubscriberActions<INPUT> extends ReactiveActions {

        void registerPath(Reactive.Publisher<INPUT> publisher);

        void traceReceive(Reactive.Publisher<INPUT> publisher, INPUT packet);

        void rePullPublisher(Reactive.Publisher<INPUT> publisher);

    }

}
