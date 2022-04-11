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

import java.util.HashSet;
import java.util.Set;

public interface Operator {

    interface Source<OUTPUT> {

        boolean isExhausted(Subscriber<OUTPUT> subscriber);

        Supplied<OUTPUT> next(Subscriber<OUTPUT> subscriber);
    }

    interface Accepter<INPUT> extends Operator {

        Effects accept(Publisher<INPUT> publisher, INPUT packet);

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

        Supplied<OUTPUT> next(Subscriber<OUTPUT> subscriber);

    }

    interface Bridge<PACKET> extends Accepter<PACKET>, Source<PACKET> {

    }

    abstract class Effects {
        // TODO: We should be able to do without these classes by reporting the change in number of answers the
        //  reactives sees from before and after applying the operator. There are a couple of edge-cases to this that
        //  make it hard, notably fanOut.

        int answersCreated;
        int answersConsumed;

        private Effects(int answersCreated, int answersConsumed) {
            this.answersCreated = answersCreated;
            this.answersConsumed = answersConsumed;
        }

        public void addAnswerCreated() {
            answersCreated += 1;
        }

        public int answersCreated() {
            return answersCreated;
        }

        public int answersConsumed() {
            return answersConsumed;
        }

    }

    class Supplied<OUTPUT> extends Effects {

        private OUTPUT output;

        private Supplied(int answersCreated, int answersConsumed) {
            super(answersCreated, answersConsumed);
        }

        public static <PACKET> Supplied<PACKET> create() {
            return new Supplied<>(0, 0);
        }

        public void setOutput(OUTPUT output) {
            this.output = output;
        }

        public OUTPUT output() {
            assert output != null;
            return output;
        }
    }

}
