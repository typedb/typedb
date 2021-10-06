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

import com.vaticle.typedb.core.concurrent.actor.Actor;
import com.vaticle.typedb.core.reasoner.resolution.answer.AnswerState;
import com.vaticle.typedb.core.reasoner.resolution.framework.ResolutionTracer.Trace;
import com.vaticle.typedb.core.reasoner.resolution.framework.Response.Blocked.Cycle;

import javax.annotation.Nullable;
import java.util.Objects;
import java.util.Set;

public interface Request {

    Visit visit();

    Trace trace();

    class Visit implements Request {

        private final Factory factory;
        private final Trace trace;
        protected final Actor.Driver<? extends Resolver<?>> sender;
        protected final Actor.Driver<? extends Resolver<?>> receiver;
        protected final AnswerState.Partial<?> partialAnswer;
        protected final int planIndex;

        private Visit(Factory factory, @Nullable Actor.Driver<? extends Resolver<?>> sender,
                      Actor.Driver<? extends Resolver<?>> receiver, AnswerState.Partial<?> partialAnswer, int planIndex,
                      Trace trace) {
            this.factory = factory;
            this.sender = sender;
            this.receiver = receiver;
            this.partialAnswer = partialAnswer;
            this.planIndex = planIndex;
            this.trace = trace;
        }

        public Factory factory() {
            return factory;
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

        @Override
        public Visit visit() {
            return this;
        }

        @Override
        public Trace trace() {
            return trace;
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
                    Objects.equals(partialAnswer, visit.partialAnswer) &&
                    Objects.equals(trace, visit.trace);
        }

        @Override
        public int hashCode() {
            return Objects.hash(this.sender, this.receiver, this.partialAnswer, this.trace);
        }

        @Override
        public String toString() {
            return "Visit{" +
                    "sender=" + sender +
                    ", receiver=" + receiver +
                    ", partial=" + partialAnswer +
                    ", trace=" + trace +
                    '}';
        }

    }

    class Revisit implements Request {

        private final Visit visit;
        private final Set<Cycle> cycles;

        protected Revisit(Visit visit, Set<Cycle> cycles) {
            this.visit = visit;
            this.cycles = cycles;
        }

        public static Revisit create(Visit visit, Set<Cycle> cycles) {
            return new Revisit(visit, cycles);
        }

        @Override
        public Visit visit() {
            return visit;
        }

        @Override
        public Trace trace() {
            return visit().trace;
        }

        public Set<Cycle> cycles() {
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

        private final Actor.Driver<? extends Resolver<?>> sender;
        private final Actor.Driver<? extends Resolver<?>> receiver;
        private final AnswerState.Partial<?> partialAnswer;
        private final int planIndex;
        private final int hash;

        protected Factory(@Nullable Actor.Driver<? extends Resolver<?>> sender, Actor.Driver<? extends Resolver<?>> receiver,
                          AnswerState.Partial<?> partialAnswer, int planIndex) {
            this.sender = sender;
            this.receiver = receiver;
            this.partialAnswer = partialAnswer;
            this.planIndex = planIndex;
            this.hash = Objects.hash(this.sender, this.receiver, this.partialAnswer, this.planIndex);
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
            return new Visit(this, sender, receiver, partialAnswer, planIndex, trace);
        }

        public Revisit createRevisit(Trace trace, Set<Cycle> cycles) {
            return new Revisit(createVisit(trace), cycles);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Factory factory = (Factory) o;
            return planIndex == factory.planIndex &&
                    Objects.equals(sender, factory.sender) &&
                    receiver.equals(factory.receiver) &&
                    partialAnswer.equals(factory.partialAnswer);
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
                    ", partialAnswer=" + partialAnswer +
                    ", planIndex=" + planIndex +
                    '}';
        }
    }
}
