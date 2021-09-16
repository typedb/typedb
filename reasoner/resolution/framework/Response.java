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

import static com.vaticle.typedb.common.util.Objects.className;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Pattern.INVALID_CASTING;

public interface Response {

    Request.Template sourceRequest();

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
        private final Request.Template sourceRequest;
        private final Partial<?> answer;
        private final Trace trace;

        private Answer(Request.Template sourceRequest, Partial<?> answer, Trace trace) {
            this.sourceRequest = sourceRequest;
            this.answer = answer;
            this.trace = trace;
        }

        public static Answer create(Request.Template sourceRequest, Partial<?> answer, Trace trace) {
            return new Answer(sourceRequest, answer, trace);
        }

        @Override
        public Request.Template sourceRequest() {
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
        private final Request.Template sourceRequest;
        private final Trace trace;

        public Fail(Request.Template sourceRequest, Trace trace) {
            this.sourceRequest = sourceRequest;
            this.trace = trace;
        }

        @Override
        public Request.Template sourceRequest() {
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

    class Blocked implements Response {

        private final Request.Template sourceRequest;
        private final Trace trace;
        protected Set<Origin> origins;

        public Blocked(Request.Template sourceRequest, Set<Origin> origins, Trace trace) {
            this.sourceRequest = sourceRequest;
            this.origins = origins;
            this.trace = trace;
        }

        @Override
        public Request.Template sourceRequest() {
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
            Blocked blocked = (Blocked) o;
            return sourceRequest.equals(blocked.sourceRequest) &&
                    origins.equals(blocked.origins);
        }

        @Override
        public int hashCode() {
            return Objects.hash(sourceRequest, origins);
        }

        public static class Origin {

            private final int numAnswersSeen;
            private final Actor.Driver<? extends Resolver<?>> resolver;

            public Origin(Actor.Driver<? extends Resolver<?>> resolver, int numAnswersSeen) {
                this.resolver = resolver;
                this.numAnswersSeen = numAnswersSeen;
            }

            public Actor.Driver<? extends Resolver<?>> resolver() {
                return resolver;
            }

            public int numAnswersSeen() {
                return numAnswersSeen;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                Origin origin = (Origin) o;
                return numAnswersSeen == origin.numAnswersSeen &&
                        resolver.equals(origin.resolver);
            }

            @Override
            public int hashCode() {
                return Objects.hash(numAnswersSeen, resolver);
            }

            @Override
            public String toString() {
                return "Origin{" +
                        "resolver=" + resolver.toString() +
                        ", numAnswersSeen=" + numAnswersSeen +
                        '}';
            }
        }
    }
}
