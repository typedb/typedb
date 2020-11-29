/*
 * Copyright (C) 2020 Grakn Labs
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

package grakn.core.reasoner.resolution.framework;

import java.util.List;

public interface Response {
    Request sourceRequest();

    boolean isAnswer();

    boolean isExhausted();

    boolean isRootResponse();

    default Answer asAnswer() {
        throw new ClassCastException("Cannot cast " + this.getClass().getSimpleName() + " to " + Answer.class.getSimpleName());
    }

    default Exhausted asExhausted() {
        throw new ClassCastException("Cannot cast " + this.getClass().getSimpleName() + " to " + Exhausted.class.getSimpleName());
    }

    class Answer implements Response {
        private final Request sourceRequest;
        private final grakn.core.reasoner.resolution.framework.Answer answer;
        private final List<Object> unifiers;

        public Answer(Request sourceRequest,
                      grakn.core.reasoner.resolution.framework.Answer answer,
                      List<Object> unifiers) {
            this.sourceRequest = sourceRequest;
            this.answer = answer;
            this.unifiers = unifiers;
        }

        @Override
        public Request sourceRequest() {
            return sourceRequest;
        }

        public grakn.core.reasoner.resolution.framework.Answer answer() {
            return answer;
        }

        public List<Object> unifiers() {
            return unifiers;
        }

        @Override
        public boolean isAnswer() { return true; }

        @Override
        public boolean isExhausted() { return false; }

        @Override
        public boolean isRootResponse() { return false; }

        @Override
        public Answer asAnswer() {
            return this;
        }

        @Override
        public String toString() {
            return "\nAnswer{" +
                    "\nsourceRequest=" + sourceRequest +
                    ",\nanswer=" + answer +
                    ",\nunifiers=" + unifiers +
                    "\n}\n";
        }
    }

    class Exhausted implements Response {
        private final Request sourceRequest;

        public Exhausted(Request sourceRequest) {
            this.sourceRequest = sourceRequest;
        }

        @Override
        public Request sourceRequest() {
            return sourceRequest;
        }

        @Override
        public boolean isAnswer() { return false; }

        @Override
        public boolean isExhausted() { return true; }

        @Override
        public boolean isRootResponse() { return false; }

        @Override
        public Exhausted asExhausted() {
            return this;
        }


        @Override
        public String toString() {
            return "Exhausted{" +
                    "sourceRequest=" + sourceRequest +
                    '}';
        }
    }

    class RootResponse implements Response {

        private final Request sourceRequest;

        public RootResponse(Request sourceRequest) {
            this.sourceRequest = sourceRequest;
        }

        @Override
        public Request sourceRequest() { return sourceRequest; }

        @Override
        public boolean isAnswer() { return false; }

        @Override
        public boolean isExhausted() { return false; }

        @Override
        public boolean isRootResponse() { return true; }
    }

}
