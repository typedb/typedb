/*
 * Copyright (C) 2021 Grakn Labs
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

import grakn.core.common.exception.GraknException;

import static grakn.common.util.Objects.className;
import static grakn.core.common.exception.ErrorMessage.Pattern.INVALID_CASTING;

public interface Response {
    Request sourceRequest();

    boolean isAnswer();

    boolean isExhausted();

    default Answer asAnswer() {
        throw GraknException.of(INVALID_CASTING, className(this.getClass()), className(Answer.class));
    }

    default Exhausted asExhausted() {
        throw GraknException.of(INVALID_CASTING, className(this.getClass()), className(Exhausted.class));
    }

    class Answer implements Response {
        private final Request sourceRequest;
        private final ResolutionAnswer answer;
        private final int planIndex;

        public Answer(Request sourceRequest,
                      ResolutionAnswer answer) {
            this.sourceRequest = sourceRequest;
            this.answer = answer;
            this.planIndex = sourceRequest.planIndex();
        }

        @Override
        public Request sourceRequest() {
            return sourceRequest;
        }

        public ResolutionAnswer answer() {
            return answer;
        }

        public int planIndex() {
            return planIndex;
        }

        @Override
        public boolean isAnswer() {
            return true;
        }

        @Override
        public boolean isExhausted() {
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
        public boolean isAnswer() {
            return false;
        }

        @Override
        public boolean isExhausted() {
            return true;
        }

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
}
