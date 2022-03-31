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

import com.vaticle.typedb.core.common.exception.TypeDBException;

import java.util.HashSet;
import java.util.Set;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_CAST;

public interface Operator<INPUT, OUTPUT, PROVIDER, RECEIVER> {

    boolean isSource();

    Source<OUTPUT, RECEIVER> asSource();

    boolean isTransformer();

    Transformer<INPUT, OUTPUT, PROVIDER, ?> asTransformer();

    boolean isSink();

    Sink<INPUT, OUTPUT, PROVIDER> asSink();

    boolean isWithdrawable();

    Withdrawable<INPUT, OUTPUT, PROVIDER, RECEIVER> asWithdrawable();

    boolean isAccepter();

    Accepter<INPUT, OUTPUT, PROVIDER, RECEIVER> asAccepter();

    interface Withdrawable<INPUT, OUTPUT, PROVIDER, RECEIVER> extends Operator<INPUT, OUTPUT, PROVIDER, RECEIVER> {

        boolean hasNext(RECEIVER receiver);

        OUTPUT next(RECEIVER receiver);

    }

    interface Source<OUTPUT, RECEIVER> extends Withdrawable<Void, OUTPUT, Void, RECEIVER> {

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

    abstract class OperatorImpl<INPUT, OUTPUT, PROVIDER, RECEIVER> implements Operator<INPUT, OUTPUT, PROVIDER, RECEIVER> {

        @Override
        public boolean isSource() {
            return false;
        }

        @Override
        public Source<OUTPUT, RECEIVER> asSource() {
            throw TypeDBException.of(ILLEGAL_CAST);
        }

        @Override
        public boolean isTransformer() {
            return false;
        }

        @Override
        public Transformer<INPUT, OUTPUT, PROVIDER, ?> asTransformer() {
            throw TypeDBException.of(ILLEGAL_CAST);
        }

        @Override
        public boolean isWithdrawable() {
            return false;
        }

        @Override
        public Withdrawable<INPUT, OUTPUT, PROVIDER, RECEIVER> asWithdrawable() {
            throw TypeDBException.of(ILLEGAL_CAST);
        }

        @Override
        public boolean isAccepter() {
            return false;
        }

        @Override
        public Accepter<INPUT, OUTPUT, PROVIDER, RECEIVER> asAccepter() {
            throw TypeDBException.of(ILLEGAL_CAST);
        }

        @Override
        public boolean isSink() {
            return false;
        }

        @Override
        public Sink<INPUT, OUTPUT, PROVIDER> asSink() {
            throw TypeDBException.of(ILLEGAL_CAST);
        }
    }

    abstract class SourceImpl<OUTPUT, RECEIVER> extends OperatorImpl<Void, OUTPUT, Void, RECEIVER> implements Source<OUTPUT, RECEIVER> {

        @Override
        public boolean isSource() {
            return true;
        }

        @Override
        public Source<OUTPUT, RECEIVER> asSource() {
            return this;
        }

        @Override
        public boolean isWithdrawable() {
            return true;
        }

        @Override
        public Withdrawable<Void, OUTPUT, Void, RECEIVER> asWithdrawable() {
            return this;
        }
    }

    abstract class TransformerImpl<INPUT, OUTPUT, PROVIDER, RECEIVER> extends OperatorImpl<INPUT, OUTPUT, PROVIDER, RECEIVER> implements Transformer<INPUT, OUTPUT, PROVIDER, RECEIVER> {

        @Override
        public boolean isTransformer() {
            return true;
        }

        @Override
        public Transformer<INPUT, OUTPUT, PROVIDER, RECEIVER> asTransformer() {
            return this;
        }

        @Override
        public boolean isAccepter() {
            return true;
        }


        @Override
        public Accepter<INPUT, OUTPUT, PROVIDER, RECEIVER> asAccepter() {
            return super.asAccepter();
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

}
