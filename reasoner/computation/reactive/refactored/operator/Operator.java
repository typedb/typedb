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

import java.util.HashSet;
import java.util.Set;

public interface Operator<INPUT, OUTPUT, PROVIDER, RECEIVER> {

    interface Withdrawable<INPUT, OUTPUT, PROVIDER, RECEIVER> extends Operator<INPUT, OUTPUT, PROVIDER, RECEIVER> {

        boolean hasNext(RECEIVER receiver);

        Supplied<OUTPUT, PROVIDER> next(RECEIVER receiver);

    }

    interface Source<OUTPUT, RECEIVER> extends Withdrawable<Void, OUTPUT, Void, RECEIVER> {
        // TODO: We can argue that Source shouldn't extend Withdrawable and have hasNext() because it has more of a
        //  isExhausted(), where asking isExhausted will always return true after it has been true once, whereas
        //  hasNext() could switch back
    }

    interface Accepter<INPUT, OUTPUT, PROVIDER, RECEIVER> extends Operator<INPUT, OUTPUT, PROVIDER, RECEIVER> {

        Effects<PROVIDER> accept(PROVIDER provider, INPUT packet);

    }

    interface Transformer<INPUT, OUTPUT, PROVIDER, RECEIVER> extends Accepter<INPUT, OUTPUT, PROVIDER, RECEIVER> {

        @Override
        Transformed<OUTPUT, PROVIDER> accept(PROVIDER provider, INPUT packet);

    }

    interface Sink<INPUT, OUTPUT, PROVIDER> extends Accepter<INPUT, OUTPUT, PROVIDER, Void> {
        // TODO: Add methods to usefully retrieve items from the sink
    }

    interface Pool<INPUT, OUTPUT, PROVIDER, RECEIVER> extends Withdrawable<INPUT, OUTPUT, PROVIDER, RECEIVER>,
                                                              Accepter<INPUT, OUTPUT, PROVIDER, RECEIVER> {

    }

    class Effects<PROVIDER> {

        private final Set<PROVIDER> newProviders;
        int answersCreated;
        int answersConsumed;
        private boolean sourceFinished;

        private Effects(int answersCreated, int answersConsumed) {
            this.answersCreated = answersCreated;
            this.answersConsumed = answersConsumed;
            this.newProviders = new HashSet<>();
            this.sourceFinished = false;
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

        public void addNewProvider(PROVIDER newProvider) {
            newProviders.add(newProvider);
        }

        public Set<PROVIDER> newProviders() {
            return newProviders;
        }

        public void addSourceFinished() {  // TODO: This is specific to sources only, and there should be a better, type-safe, way to do this
            sourceFinished = true;
        }

        public boolean sourceFinished() {
            return sourceFinished;
        }
    }

    class Transformed<OUTPUT, PROVIDER> extends Effects<PROVIDER> {

        private final Set<OUTPUT> outputs;

        private Transformed(Set<OUTPUT> outputs, int answersCreated, int answersConsumed) {
            super(answersCreated, answersConsumed);
            this.outputs = outputs;
        }

        public static <OUTPUT, PROVIDER> Transformed<OUTPUT, PROVIDER> create(Set<OUTPUT> outputs) {
            return new Transformed<>(outputs, 0, 0);
        }

        public static <OUTPUT, PROVIDER> Transformed<OUTPUT, PROVIDER> create() {
            return new Transformed<>(new HashSet<>(), 0, 0);
        }

        public void addOutput(OUTPUT output) {
            outputs.add(output);
        }

        public Set<OUTPUT> outputs() {
            return outputs;
        }

    }

    class Supplied<OUTPUT, PROVIDER> extends Effects<PROVIDER> {

        private OUTPUT output;

        private Supplied(int answersCreated, int answersConsumed) {
            super(answersCreated, answersConsumed);
        }

        public static <PACKET> Supplied<PACKET, Void> create() {
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
