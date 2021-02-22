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

package grakn.core.reasoner.resolution;

import grakn.core.common.exception.GraknException;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concurrent.actor.Actor;
import grakn.core.reasoner.resolution.answer.AnswerState;
import grakn.core.reasoner.resolution.answer.AnswerState.Derivation;
import grakn.core.reasoner.resolution.answer.AnswerState.Partial;
import grakn.core.reasoner.resolution.framework.Resolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static grakn.core.common.exception.ErrorMessage.Internal.UNIMPLEMENTED;

public class ResolutionRecorder extends Actor.State<ResolutionRecorder> {
    private static final Logger LOG = LoggerFactory.getLogger(ResolutionRecorder.class);

    private final Map<Actor<? extends Resolver<?>>, Integer> actorIndices;
    private final Map<AnswerIndex, Partial<?>> answers;

    public ResolutionRecorder(Actor<ResolutionRecorder> self) {
        super(self);
        answers = new HashMap<>();
        actorIndices = new HashMap<>();
    }

    @Override
    protected void exception(Throwable e) {
        LOG.error("Actor exception", e);
    }

    public void record(AnswerState answer) {
        throw GraknException.of(UNIMPLEMENTED);
//        merge(answer);
    }

    /**
     * Recursively merge derivation tree nodes into the existing derivation nodes that are recorded in the
     * answer index. Always keep the pre-existing derivation node, and merge the new ones into the existing node.
     * @return
     * @param newAnswer
     */
    private Partial<?> merge(Partial<?> newAnswer) {
        Derivation newDerivation = newAnswer.derivation();
        Map<Actor<? extends Resolver<?>>, Partial<?>> subAnswers = newDerivation.answers();

        Map<Actor<? extends Resolver<?>>, Partial<?>> mergedSubAnswers = new HashMap<>();
        for (Actor<? extends Resolver<?>> key : subAnswers.keySet()) {
            Partial<?> subAnswer = subAnswers.get(key);
            Partial<?> mergedSubAnswer = merge(subAnswer);
            mergedSubAnswers.put(key, mergedSubAnswer);
        }
        newDerivation.replace(mergedSubAnswers);

        int actorIndex = actorIndices.computeIfAbsent(newAnswer.resolvedBy(), key -> actorIndices.size());
        LOG.debug("actor index for " + newAnswer.resolvedBy() + ": " + actorIndex);
        AnswerIndex newAnswerIndex = new AnswerIndex(actorIndex, newAnswer.conceptMap());
        if (answers.containsKey(newAnswerIndex)) {
            Partial<?> existingAnswer = answers.get(newAnswerIndex);
            Derivation existingDerivation = existingAnswer.derivation();
            existingDerivation.update(newDerivation.answers());
            return existingAnswer;
        } else {
            answers.put(newAnswerIndex, newAnswer);
            return newAnswer;
        }
    }

    static class AnswerIndex {
        private final int actorIndex;
        private final ConceptMap conceptMap;

        public AnswerIndex(int actorIndex, ConceptMap conceptMap) {
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
