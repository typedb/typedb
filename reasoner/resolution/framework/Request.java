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

package com.vaticle.typedb.core.reasoner.resolution.framework;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.concurrent.actor.Actor;
import com.vaticle.typedb.core.reasoner.resolution.answer.AnswerState;
import com.vaticle.typedb.core.reasoner.resolution.framework.ResolutionTracer.Trace;

import javax.annotation.Nullable;
import java.util.Objects;
import java.util.Set;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;

public interface Request {

    Visit visit();

    class Visit implements Request {

        protected final Actor.Driver<? extends Resolver<?>> sender;
        protected final Actor.Driver<? extends Resolver<?>> receiver;
        protected final AnswerState.Partial<?> partialAnswer;
        protected final int planIndex;

        private Visit(@Nullable Actor.Driver<? extends Resolver<?>> sender, Actor.Driver<? extends Resolver<?>> receiver,
                        AnswerState.Partial<?> partialAnswer, int planIndex, Trace trace) {
            this.sender = sender;
            this.receiver = receiver;
            this.partialAnswer = partialAnswer;
            this.planIndex = planIndex;
        }

        public Actor.Driver<? extends Resolver<?>> receiver() {
            return receiver;
        }

        public Actor.Driver<? extends Resolver<?>> sender() {
            return sender;
        }

        public AnswerState.Partial<?> partialAnswer() {
            return partialAnswer;
        }

        public Factory factory() {
            return Factory.create(sender(), receiver(), partialAnswer(), planIndex());
        }

        @Override
        public Visit visit() {
            return this;
        }

        public int planIndex() {
            return planIndex;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Visit visit = (Visit) o;
            return Objects.equals(sender, visit.sender) &&
                    Objects.equals(receiver, visit.receiver) &&
                    Objects.equals(partialAnswer, visit.partialAnswer());
        }

        @Override
        public int hashCode() {
            return Objects.hash(this.sender, this.receiver, this.partialAnswer);
        }

        @Override
        public String toString() {
            return "Visit{" +
                    "sender=" + sender +
                    ", receiver=" + receiver +
                    ", partial=" + partialAnswer +
                    '}';
        }

        public boolean isToSubsumed() {
            return false;
        }

        public ToSubsumed asToSubsumed() {
            throw TypeDBException.of(ILLEGAL_STATE);
        }

        public boolean isToSubsumer() {
            return false;
        }

        public ToSubsumer asToSubsumer() {
            throw TypeDBException.of(ILLEGAL_STATE);
        }

        public static class ToSubsumed extends Visit {

            private final Actor.Driver<? extends Resolver<?>> subsumer;

            private ToSubsumed(@Nullable Actor.Driver<? extends Resolver<?>> sender,
                               Actor.Driver<? extends Resolver<?>> receiver,
                               @Nullable Actor.Driver<? extends Resolver<?>> subsumer,
                               AnswerState.Partial<?> partialAnswer, int planIndex, Trace trace) {
                super(sender, receiver, partialAnswer, planIndex, trace);
                this.subsumer = subsumer;
            }

            public Actor.Driver<? extends Resolver<?>> subsumer() {
                return subsumer;
            }

            @Override
            public boolean isToSubsumed() {
                return true;
            }

            @Override
            public ToSubsumed asToSubsumed() {
                return this;
            }

        }

        public static class ToSubsumer extends Visit {

            private final ToSubsumed toSubsumed;

            private ToSubsumer(@Nullable Actor.Driver<? extends Resolver<?>> sender,
                               Actor.Driver<? extends Resolver<?>> receiver,
                               ToSubsumed toSubsumed, AnswerState.Partial<?> partialAnswer, int planIndex, Trace trace) {
                super(sender, receiver, partialAnswer, planIndex, trace);
                this.toSubsumed = toSubsumed;
            }

            public ToSubsumed toSubsumed() {
                return toSubsumed;
            }

            @Override
            public boolean isToSubsumer() {
                return true;
            }

            @Override
            public ToSubsumer asToSubsumer() {
                return this;
            }

        }
    }

    class Revisit implements Request {

        private final Visit visit;
        private final Set<Response.Cycle.Origin> cycles;

        protected Revisit(Visit visit, Set<Response.Cycle.Origin> cycles) {
            this.visit = visit;
            this.cycles = cycles;
        }

        public static Revisit create(Visit visit, Set<Response.Cycle.Origin> cycles) {
            return new Revisit(visit, cycles);
        }

        @Override
        public Visit visit() {
            return visit;
        }

        public Set<Response.Cycle.Origin> cycles() {
            return cycles;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Revisit revisit = (Revisit) o;
            return visit.equals(revisit.visit) &&
                    cycles.equals(revisit.cycles);
        }

        @Override
        public int hashCode() {
            return Objects.hash(visit, cycles);
        }

        @Override
        public String toString() {
            return "Revisit{" +
                    "visit=" + visit +
                    ", cycles=" + cycles +
                    '}';
        }
    }

    class Factory {
        protected final Actor.Driver<? extends Resolver<?>> sender;
        protected final Actor.Driver<? extends Resolver<?>> receiver;
        protected final AnswerState.Partial<?> partialAnswer;
        protected final int planIndex;

        private final int hash;

        protected Factory(Actor.Driver<? extends Resolver<?>> sender, Actor.Driver<? extends Resolver<?>> receiver,
                          AnswerState.Partial<?> partialAnswer, int planIndex) {
            this.sender = sender;
            this.receiver = receiver;
            this.partialAnswer = partialAnswer;
            this.planIndex = planIndex;
            this.hash = Objects.hash(this.sender, this.receiver, this.partialAnswer);
        }

        public static Factory create(Actor.Driver<? extends Resolver<?>> sender, Actor.Driver<? extends Resolver<?>> receiver,
                                     AnswerState.Partial<?> partialAnswer, int planIndex) {
            return new Factory(sender, receiver, partialAnswer, planIndex);
        }

        public static Factory create(Actor.Driver<? extends Resolver<?>> sender, Actor.Driver<? extends Resolver<?>> receiver,
                                     AnswerState.Partial<?> partialAnswer) {
            return new Factory(sender, receiver, partialAnswer, -1);
        }

        public static Factory create(Actor.Driver<? extends Resolver<?>> receiver, AnswerState.Partial<?> partialAnswer) {
            return new Factory(null, receiver, partialAnswer, -1);
        }

        public static Factory of(Visit request) {
            return Factory.create(request.sender(), request.receiver(), request.partialAnswer(), request.planIndex());
        }

        public Visit createVisit(Trace trace) {
            return new Visit(sender, receiver, partialAnswer, planIndex, trace);
        }

        public Revisit createRevisit(Trace trace, Set<Response.Cycle.Origin> cycles) {
            return new Revisit(createVisit(trace), cycles);
        }

        public int planIndex() {
            return planIndex;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Factory factory = (Factory) o;
            return Objects.equals(sender, factory.sender) &&
                    Objects.equals(receiver, factory.receiver) &&
                    Objects.equals(partialAnswer, factory.partialAnswer);
        }

        @Override
        public int hashCode() {
            return hash;
        }

        @Override
        public String toString() {
            return "Factory{" +
                    "sender=" + sender +
                    ", receiver=" + receiver +
                    ", partial=" + partialAnswer +
                    '}';
        }
    }
}
