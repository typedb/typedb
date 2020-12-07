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

package grakn.core.reasoner.resolution;

import grakn.common.concurrent.actor.Actor;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.reasoner.resolution.framework.ResolutionAnswer;
import grakn.core.reasoner.resolution.framework.Resolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class ResolutionRecorder extends Actor.State<ResolutionRecorder> {
    private static final Logger LOG = LoggerFactory.getLogger(ResolutionRecorder.class);

    private final Map<Actor<? extends Resolver<?>>, Integer> actorIndices;
    private final Map<AnswerIndex, ResolutionAnswer> answers;

    public ResolutionRecorder(final Actor<ResolutionRecorder> self) {
        super(self);
        answers = new HashMap<>();
        actorIndices = new HashMap<>();
    }

    @Override
    protected void exception(final Exception e) {
        LOG.error("Actor exception", e);
    }

    public void record(ResolutionAnswer answer) {
        merge(answer);
    }

    /**
     * Recursively merge derivation tree nodes into the existing derivation nodes that are recorded in the
     * answer index. Always keep the pre-existing derivation node, and merge the new ones into the existing node.
     */
    private ResolutionAnswer merge(ResolutionAnswer newAnswer) {
        ResolutionAnswer.Derivation newDerivation = newAnswer.derivation();
        Map<Actor<? extends Resolver<?>>, ResolutionAnswer> subAnswers = newDerivation.answers();

        Map<Actor<? extends Resolver<?>>, ResolutionAnswer> mergedSubAnswers = new HashMap<>();
        for (Actor<? extends Resolver<?>> key : subAnswers.keySet()) {
            ResolutionAnswer subAnswer = subAnswers.get(key);
            ResolutionAnswer mergedSubAnswer = merge(subAnswer);
            mergedSubAnswers.put(key, mergedSubAnswer);
        }
        newDerivation.replace(mergedSubAnswers);

        int actorIndex = actorIndices.computeIfAbsent(newAnswer.producer(), key -> actorIndices.size());
        LOG.debug("actor index for " + newAnswer.producer() + ": " + actorIndex);
        AnswerIndex newAnswerIndex = new AnswerIndex(actorIndex, newAnswer.conceptMap());
        if (answers.containsKey(newAnswerIndex)) {
            ResolutionAnswer existingAnswer = answers.get(newAnswerIndex);
            ResolutionAnswer.Derivation existingDerivation = existingAnswer.derivation();
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

        public AnswerIndex(int actorIndex, final ConceptMap conceptMap) {
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
