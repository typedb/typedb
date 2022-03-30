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

import java.util.HashSet;
import java.util.Set;

public interface Operator<INPUT, OUTPUT, PROVIDER> {

    Outcome<OUTPUT, PROVIDER> operate(PROVIDER provider, INPUT packet);

    class Outcome<OUTPUT, PROVIDER> {

        private final Set<OUTPUT> outputs;
        private final Set<PROVIDER> newProviders;
        int answersCreated;
        int answersConsumed;
        private boolean sourceFinished;

        private Outcome(Set<OUTPUT> outputs, int answersCreated, int answersConsumed) {
            this.outputs = outputs;
            this.answersCreated = answersCreated;
            this.answersConsumed = answersConsumed;
            this.newProviders = new HashSet<>();
            this.sourceFinished = false;
        }

        public static <OUTPUT, PROVIDER> Outcome<OUTPUT, PROVIDER> create(Set<OUTPUT> outputs) {
            return new Outcome<>(outputs, 0, 0);
        }

        public static <OUTPUT, PROVIDER> Outcome<OUTPUT, PROVIDER> create() {
            return new Outcome<>(new HashSet<>(), 0, 0);
        }

        public void addOutput(OUTPUT output) {
            outputs.add(output);
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

        public Set<OUTPUT> outputs() {
            return outputs;
        }

        public void addNewProvider(PROVIDER newProvider) {
            newProviders.add(newProvider);
        }

        public Set<PROVIDER> newProviders() {
            return newProviders;
        }

        public void addSourceFinished() {
            sourceFinished = true;
        }

        public boolean sourceFinished() {
            return sourceFinished;
        }
    }

}
