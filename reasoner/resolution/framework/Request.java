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

import javax.annotation.Nullable;
import java.util.Objects;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.reasoner.resolution.framework.ResolutionTracer.TraceId.downstreamId;

public interface Request {

    class Visit implements Request {

        protected final Actor.Driver<? extends Resolver<?>> sender;
        protected final Actor.Driver<? extends Resolver<?>> receiver;
        protected final AnswerState.Partial<?> partialAnswer;
        protected final int planIndex;
        private final ResolutionTracer.TraceId traceId;
        private final int hash;

        protected Visit(@Nullable Actor.Driver<? extends Resolver<?>> sender, Actor.Driver<? extends Resolver<?>> receiver,
                        ResolutionTracer.TraceId traceId, AnswerState.Partial<?> partialAnswer, int planIndex) {
            this.traceId = traceId;
            this.sender = sender;
            this.receiver = receiver;
            this.partialAnswer = partialAnswer;
            this.planIndex = planIndex;
            this.hash = Objects.hash(this.sender, this.receiver, this.partialAnswer);
        }

        public static Visit create(Actor.Driver<? extends Resolver<?>> sender, Actor.Driver<? extends Resolver<?>> receiver, ResolutionTracer.TraceId traceId, AnswerState.Partial<?> partialAnswer, int planIndex) {
            return new Visit(sender, receiver, traceId, partialAnswer, planIndex);
        }

        public static Visit create(Actor.Driver<? extends Resolver<?>> sender, Actor.Driver<? extends Resolver<?>> receiver, AnswerState.Partial<?> partialAnswer, int planIndex) {
            return new Visit(sender, receiver, downstreamId(), partialAnswer, planIndex);
        }

        public static Visit create(Actor.Driver<? extends Resolver<?>> sender, Actor.Driver<? extends Resolver<?>> receiver, AnswerState.Partial<?> partialAnswer) {
            return new Visit(sender, receiver, downstreamId(), partialAnswer, -1);
        }

        public static Visit create(Actor.Driver<? extends Resolver<?>> sender, Actor.Driver<? extends Resolver<?>> receiver, ResolutionTracer.TraceId traceId, AnswerState.Partial<?> partialAnswer) {
            return new Visit(sender, receiver, traceId, partialAnswer, -1);
        }

        public static Visit create(Actor.Driver<? extends Resolver<?>> receiver, ResolutionTracer.TraceId traceId, AnswerState.Partial<?> partialAnswer) {
            return new Visit(null, receiver, traceId, partialAnswer, -1);
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

        public int planIndex() {
            return planIndex;
        }

        public ResolutionTracer.TraceId traceId() {
            return traceId;
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
            return hash;
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
                               @Nullable Actor.Driver<? extends Resolver<?>> subsumer, ResolutionTracer.TraceId traceId,
                               AnswerState.Partial<?> partialAnswer, int planIndex) {
                super(sender, receiver, traceId, partialAnswer, planIndex);
                this.subsumer = subsumer;
            }

            public static Visit create(Actor.Driver<? extends Resolver<?>> sender,
                                       Actor.Driver<? extends Resolver<?>> receiver,
                                       Actor.Driver<? extends Resolver<?>> subsumer, ResolutionTracer.TraceId traceId,
                                       AnswerState.Partial<?> partialAnswer) {
                return new ToSubsumed(sender, receiver, subsumer, traceId, partialAnswer, -1);
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
                               ToSubsumed toSubsumed, AnswerState.Partial<?> partialAnswer, int planIndex) {
                super(sender, receiver, toSubsumed.traceId(), partialAnswer, planIndex);
                this.toSubsumed = toSubsumed;
            }

            public static ToSubsumer create(@Nullable Actor.Driver<? extends Resolver<?>> sender, Actor.Driver<?
                    extends Resolver<?>> receiver, ToSubsumed toSubsumed, AnswerState.Partial<?> partialAnswer) {
                return new ToSubsumer(sender, receiver, toSubsumed, partialAnswer, -1);
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
        private final Response.Cycle.Origin cycle;
        private final int hash;

        protected Revisit(Visit visit, Response.Cycle.Origin cycle) {
            this.visit = visit;
            this.cycle = cycle;
            this.hash = Objects.hash(super.hashCode(), cycle);
        }

        public static Revisit create(Visit visit, Response.Cycle.Origin cycle) {
            return new Revisit(visit, cycle);
        }

        public Visit visit() {
            return visit;
        }

        public Response.Cycle.Origin cycle() {
            return cycle;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;
            Revisit revisit = (Revisit) o;
            return cycle.equals(revisit.cycle);
        }

        @Override
        public int hashCode() {
            return hash;
        }

        @Override
        public String toString() {
            return "Revisit{" +
                    "visit=" + visit +
                    ", cycle=" + cycle +
                    '}';
        }
    }
}
