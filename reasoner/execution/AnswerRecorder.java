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

package grakn.core.reasoner.execution;

import grakn.common.concurrent.actor.Actor;
import grakn.core.reasoner.execution.framework.Answer;
import grakn.core.reasoner.execution.framework.ExecutionActor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class AnswerRecorder extends Actor.State<AnswerRecorder> {
    private static final Logger LOG = LoggerFactory.getLogger(AnswerRecorder.class);

    Map<Actor<? extends ExecutionActor<?>>, Integer> actorIndices;
    Map<AnswerIndex, Answer> answers;

    public AnswerRecorder(final Actor<AnswerRecorder> self) {
        super(self);
        answers = new HashMap<>();
        actorIndices = new HashMap<>();
    }

    @Override
    protected void exception(final Exception e) {
        LOG.error("Actor exception", e);
    }

    public void record(Answer answer) {
        merge(answer);
    }

    /**
     * Recursively merge derivation tree nodes into the existing derivation nodes that are recorded in the
     * answer index. Always keep the pre-existing derivation node, and merge the new ones into the existing node.
     */
    private Answer merge(Answer newAnswer) {
        Answer.Derivation newDerivation = newAnswer.derivation();
        Map<Actor<? extends ExecutionActor<?>>, Answer> subAnswers = newDerivation.answers();

        Map<Actor<? extends ExecutionActor<?>>, Answer> mergedSubAnswers = new HashMap<>();
        for (Actor<? extends ExecutionActor<?>> key : subAnswers.keySet()) {
            Answer subAnswer = subAnswers.get(key);
            Answer mergedSubAnswer = merge(subAnswer);
            mergedSubAnswers.put(key, mergedSubAnswer);
        }
        newDerivation.replace(mergedSubAnswers);

        int actorIndex = actorIndices.computeIfAbsent(newAnswer.producer(), key -> actorIndices.size());
        LOG.debug("actor index for " + newAnswer.producer() + ": " + actorIndex);
        AnswerIndex newAnswerIndex = new AnswerIndex(actorIndex, newAnswer.conceptMap());
        if (answers.containsKey(newAnswerIndex)) {
            Answer existingAnswer = answers.get(newAnswerIndex);
            Answer.Derivation existingDerivation = existingAnswer.derivation();
            existingDerivation.update(newDerivation.answers());
            return existingAnswer;
        } else {
            answers.put(newAnswerIndex, newAnswer);
            return newAnswer;
        }
    }

    static class AnswerIndex {
        private final int actorIndex;
        private final List<Long> conceptMap;

        public AnswerIndex(int actorIndex, final List<Long> conceptMap) {
            this.actorIndex = actorIndex;
            this.conceptMap = conceptMap;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            AnswerIndex that = (AnswerIndex) o;
            return actorIndex == that.actorIndex &&
                    Objects.equals(conceptMap, that.conceptMap);
        }

        @Override
        public int hashCode() {
            return Objects.hash(actorIndex, conceptMap);
        }

        @Override
        public String toString() {
            return "AnswerIndex{" +
                    "actorIndex=" + actorIndex +
                    ", conceptMap=" + conceptMap +
                    '}';
        }
    }
}
