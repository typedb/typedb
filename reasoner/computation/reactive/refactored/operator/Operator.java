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

package com.vaticle.typedb.core.reasoner.computation.reactive.refactored.operator;

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

        Effects<INPUT> accept(Publisher<INPUT> publisher, INPUT packet);

    }

    interface Transformer<INPUT, OUTPUT> extends Accepter<INPUT> {

        @Override
        Transformed<OUTPUT, INPUT> accept(Publisher<INPUT> publisher, INPUT packet);

    }

    interface Sink<INPUT> extends Accepter<INPUT> {
        // TODO: Add methods to usefully retrieve items from the sink
    }

    interface Pool<INPUT, OUTPUT> extends Accepter<INPUT> {

        boolean hasNext(Subscriber<OUTPUT> subscriber);

        Supplied<OUTPUT> next(Subscriber<OUTPUT> subscriber);

    }

    class Effects<PACKET> {
        // TODO: We should be able to do without these classes by reporting the change in number of answers the
        //  reactives sees from before and after applying the operator. There are a couple of edge-cases to this that
        //  make it hard, notably fanOut.

        private final Set<Publisher<PACKET>> newPublishers;
        int answersCreated;
        int answersConsumed;

        private Effects(int answersCreated, int answersConsumed) {
            this.answersCreated = answersCreated;
            this.answersConsumed = answersConsumed;
            this.newPublishers = new HashSet<>();
        }

        public static <PACKET> Effects<PACKET> createEffects() {  // TODO: Name clash with child class if called create()
            return new Effects<>(0, 0);
        }

        public void addAnswerCreated() {
            answersCreated += 1;
        }

        public void addAnswerConsumed() {
            answersConsumed += 1;
        }

        public int answersCreated() {
            return answersCreated;
        }

        public int answersConsumed() {
            return answersConsumed;
        }

        public void addNewPublisher(Publisher<PACKET> newPublisher) {
            newPublishers.add(newPublisher);
        }

        public Set<Publisher<PACKET>> newPublishers() {
            return newPublishers;
        }

    }

    class Transformed<OUTPUT, INPUT> extends Effects<INPUT> {

        private final Set<OUTPUT> outputs;

        private Transformed(Set<OUTPUT> outputs, int answersCreated, int answersConsumed) {
            super(answersCreated, answersConsumed);
            this.outputs = outputs;
        }

        public static <OUTPUT, INPUT> Transformed<OUTPUT, INPUT> create(Set<OUTPUT> outputs) {
            return new Transformed<>(outputs, 0, 0);
        }

        public static <OUTPUT, INPUT> Transformed<OUTPUT, INPUT> create() {
            return new Transformed<>(new HashSet<>(), 0, 0);
        }

        public void addOutput(OUTPUT output) {
            outputs.add(output);
        }

        public Set<OUTPUT> outputs() {
            return outputs;
        }

    }

    class Supplied<OUTPUT> extends Effects<OUTPUT> {

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
