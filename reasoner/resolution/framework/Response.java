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
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concurrent.actor.Actor;
import com.vaticle.typedb.core.logic.resolvable.Concludable;
import com.vaticle.typedb.core.reasoner.resolution.answer.AnswerState;
import com.vaticle.typedb.core.reasoner.resolution.answer.AnswerState.Partial;
import com.vaticle.typedb.core.reasoner.resolution.framework.ResolutionTracer.Trace;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;

public abstract class Response {

    private final Request.Visit sourceRequest;
    private final Trace trace;

    private Response(Request.Visit sourceRequest, @Nullable Trace trace) {
        this.sourceRequest = sourceRequest;
        this.trace = trace;
    }

    public Request sourceRequest() {
        return sourceRequest;
    }

    public Trace trace() {
        return trace;
    }

    Actor.Driver<? extends Resolver<?>> receiver() {
        return sourceRequest.sender();
    }

    Actor.Driver<? extends Resolver<?>> sender() {
        return sourceRequest.receiver();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Response response = (Response) o;
        return sourceRequest.equals(response.sourceRequest) &&
                Objects.equals(trace, response.trace);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sourceRequest, trace);
    }

    @Override
    public String toString() {
        return "Response{" +
                "sourceRequest=" + sourceRequest +
                ", trace=" + trace +
                '}';
    }

    public static class Answer extends Response {
        private final Partial<?> answer;
        private final int hash;

        private Answer(Request.Visit sourceRequest, Partial<?> answer, @Nullable Trace trace) {
            super(sourceRequest, trace);
            this.answer = answer;
            this.hash = Objects.hash(super.hashCode(), answer);
        }

        public static Answer create(Request.Visit sourceRequest, Partial<?> answer, @Nullable Trace trace) {
            return new Answer(sourceRequest, answer, trace);
        }

        public Partial<?> answer() {
            return answer;
        }

        public int planIndex() {
            return sourceRequest().visit().planIndex();
        }

        @Override
        public String toString() {
            return "Answer{" +
                    "sourceRequest=" + sourceRequest() +
                    ", answer=" + answer +
                    ", trace=" + trace() +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;
            Answer that = (Answer) o;
            return answer.equals(that.answer);
        }

        @Override
        public int hashCode() {
            return hash;
        }

    }

    public static class Fail extends Response {

        public Fail(Request.Visit sourceRequest, @Nullable Trace trace) {
            super(sourceRequest, trace);
        }

        @Override
        public String toString() {
            return "Fail{" +
                    "sourceRequest=" + sourceRequest() +
                    ", trace=" + trace() +
                    '}';
        }
    }

    public static class Blocked extends Response {

        protected Map<Cycle, Integer> cycles;
        private final int hash;

        public Blocked(Request.Visit sourceRequest, Map<Cycle, Integer> cycles, @Nullable Trace trace) {
            super(sourceRequest, trace);
            this.cycles = cycles;
            this.hash = Objects.hash(super.hashCode(), cycles);
        }

        public Map<Cycle, Integer> cycles() {
            return cycles;
        }

        @Override
        public String toString() {
            return "Blocked{" +
                    "sourceRequest=" + sourceRequest() +
                    ", cycles=" + cycles +
                    ", trace=" + trace() +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;
            Blocked blocked = (Blocked) o;
            return cycles.equals(blocked.cycles);
        }

        @Override
        public int hashCode() {
            return hash;
        }

        public static class Cycle {

            private final Partial.Concludable<?> origin;

            public static Map<Cycle, Integer> create(Partial.Concludable<?> initial, Concludable concludable,
                                                     ConceptMap conceptMap, int numAnswersSeen) {
                AnswerState.Partial<?> ans = initial;
                while (ans.parent().isPartial()) {
                    ans = ans.parent().asPartial();
                    if (ans.isConcludable()) {
                        if (ans.asConcludable().concludable().alphaEquals(concludable).first().isPresent()
                                && ans.conceptMap().equals(conceptMap)) {
                            Cycle cycle = new Cycle(ans.asConcludable());
                            Map<Cycle, Integer> cyclesMap = new HashMap<>();
                            cyclesMap.put(cycle, numAnswersSeen);
                            return cyclesMap;
                        }
                    }
                }
                throw TypeDBException.of(ILLEGAL_STATE);
            }

            public Cycle(Partial.Concludable<?> origin) {
                this.origin = origin;
            }

            public Partial.Concludable<?> origin() {
                return origin;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                Cycle cycle = (Cycle) o;
                return origin.equals(cycle.origin);
            }

            @Override
            public int hashCode() {
                return Objects.hash(origin);
            }

            @Override
            public String toString() {
                return "Cycle{" +
                        "origin=" + origin +
                        '}';
            }
        }
    }
}
