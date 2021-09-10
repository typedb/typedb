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
import com.vaticle.typedb.core.reasoner.resolution.answer.AnswerState.Partial;
import com.vaticle.typedb.core.reasoner.resolution.framework.ResolutionTracer.Trace;

import java.util.Objects;
import java.util.Set;

import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typedb.common.util.Objects.className;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Pattern.INVALID_CASTING;

public interface Response {

    Request.Factory sourceRequest();

    boolean isAnswer();

    boolean isFail();

    Trace trace();

    default Answer asAnswer() {
        throw TypeDBException.of(INVALID_CASTING, className(this.getClass()), className(Answer.class));
    }

    default Fail asFail() {
        throw TypeDBException.of(INVALID_CASTING, className(this.getClass()), className(Fail.class));
    }

    default Actor.Driver<? extends Resolver<?>> receiver() {
        return sourceRequest().sender();
    }

    default Actor.Driver<? extends Resolver<?>> sender() {
        return sourceRequest().receiver();
    }

    class Answer implements Response {
        private final Request.Factory sourceRequest;
        private final Partial<?> answer;
        private final Trace trace;

        private Answer(Request.Factory sourceRequest, Partial<?> answer, Trace trace) {
            this.sourceRequest = sourceRequest;
            this.answer = answer;
            this.trace = trace;
        }

        public static Answer create(Request.Factory sourceRequest, Partial<?> answer, Trace trace) {
            return new Answer(sourceRequest, answer, trace);
        }

        @Override
        public Request.Factory sourceRequest() {
            return sourceRequest;
        }

        public Partial<?> answer() {
            return answer;
        }

        @Override
        public Trace trace() {
            return trace;
        }

        public int planIndex() {
            return sourceRequest.planIndex();
        }

        @Override
        public boolean isAnswer() {
            return true;
        }

        @Override
        public boolean isFail() {
            return false;
        }

        @Override
        public Answer asAnswer() {
            return this;
        }

        @Override
        public String toString() {
            return "\nAnswer{" +
                    "\nsourceRequest=" + sourceRequest +
                    ",\nanswer=" + answer +
                    "\n}\n";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final Answer that = (Answer) o;
            return Objects.equals(sourceRequest, that.sourceRequest) &&
                    Objects.equals(answer, that.answer);
        }

        @Override
        public int hashCode() {
            return Objects.hash(sourceRequest, answer);
        }
    }

    class Fail implements Response {
        private final Request.Factory sourceRequest;
        private final Trace trace;

        public Fail(Request.Factory sourceRequest, Trace trace) {
            this.sourceRequest = sourceRequest;
            this.trace = trace;
        }

        @Override
        public Request.Factory sourceRequest() {
            return sourceRequest;
        }

        @Override
        public Trace trace() {
            return trace;
        }

        @Override
        public boolean isAnswer() {
            return false;
        }

        @Override
        public boolean isFail() {
            return true;
        }

        @Override
        public Fail asFail() {
            return this;
        }


        @Override
        public String toString() {
            return "Exhausted{" +
                    "sourceRequest=" + sourceRequest +
                    '}';
        }
    }

    class Cycle implements Response {

        private final Request.Factory sourceRequest;
        private final Trace trace;
        protected Set<Origin> origins;

        public Cycle(Request.Factory sourceRequest, Set<Origin> origins, Trace trace) {
            this.sourceRequest = sourceRequest;
            this.origins = origins;
            this.trace = trace;
        }

        private Cycle(Request.Factory sourceRequest, Trace trace) {
            this.sourceRequest = sourceRequest;
            this.trace = trace;
        }

        @Override
        public Request.Factory sourceRequest() {
            return sourceRequest;
        }

        @Override
        public Trace trace() {
            return trace;
        }

        @Override
        public boolean isAnswer() {
            return false;
        }

        @Override
        public boolean isFail() {
            return false;
        }

        public Set<Origin> origins() {
            return origins;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Cycle cycle = (Cycle) o;
            return sourceRequest.equals(cycle.sourceRequest) &&
                    origins.equals(cycle.origins);
        }

        @Override
        public int hashCode() {
            return Objects.hash(sourceRequest, origins);
        }

        public static class Origin extends Cycle {

            private final int numAnswersSeen;

            public Origin(Request.Factory sourceRequest, int numAnswersSeen, Trace trace) {
                super(sourceRequest, trace);
                this.origins = set();
                this.numAnswersSeen = numAnswersSeen;
            }

            public int numAnswersSeen() {
                return numAnswersSeen;
            }

            @Override
            public Set<Origin> origins() {
                return set(this);
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                if (!super.equals(o)) return false;
                Origin origin = (Origin) o;
                return numAnswersSeen == origin.numAnswersSeen;
            }

            @Override
            public int hashCode() {
                return Objects.hash(super.hashCode(), numAnswersSeen);
            }

            @Override
            public String toString() {
                return "Origin{" +
                        "sourceRequest=" + sourceRequest().toString() +
                        ", numAnswersSeen=" + numAnswersSeen +
                        '}';
            }
        }
    }
}
