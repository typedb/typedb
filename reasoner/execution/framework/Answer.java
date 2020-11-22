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

package grakn.core.reasoner.execution.framework;

import grakn.common.concurrent.actor.Actor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static grakn.common.collection.Collections.map;

public class Answer {
    private final List<Long> conceptMap;
    private final Derivation derivation;
    private final Actor<? extends ExecutionActor<?>> producer;
    private final String patternAnswered;

    public Answer(List<Long> conceptMap,
                  String patternAnswered,
                  Derivation derivation,
                  Actor<? extends ExecutionActor<?>> producer) {
        this.conceptMap = conceptMap;
        this.patternAnswered = patternAnswered;
        this.derivation = derivation;
        this.producer = producer;
    }

    public List<Long> conceptMap() {
        return conceptMap;
    }

    public Derivation derivation() {
        return derivation;
    }

    public boolean isInferred() {
        return !derivation.equals(Derivation.EMPTY);
    }

    public Actor<? extends ExecutionActor<?>> producer() {
        return producer;
    }

    @Override
    public String toString() {
        return "Answer{" +
                "conceptMap=" + conceptMap +
                ", executionRecord=" + derivation +
                ", patternAnswered='" + patternAnswered + '\'' +
                ", producer=" + producer +
                '}';
    }

    public static class Derivation {
        public static final Derivation EMPTY = new Derivation(map());

        private Map<Actor<? extends ExecutionActor<?>>, Answer> answers;

        public Derivation(Map<Actor<? extends ExecutionActor<?>>, Answer> answers) {
            this.answers = map(answers);
        }

        public Derivation withAnswer(Actor<? extends ExecutionActor<?>> producer, Answer answer) {
            Map<Actor<? extends ExecutionActor<?>>, Answer> copiedResolution = new HashMap<>(answers);
            copiedResolution.put(producer, answer);
            return new Derivation(copiedResolution);
        }

        public void update(Map<Actor<? extends ExecutionActor<?>>, Answer> newResolutions) {
            assert answers.keySet().stream().noneMatch(key -> answers.containsKey(key)) : "Cannot overwrite any derivations during an update";
            Map<Actor<? extends ExecutionActor<?>>, Answer> copiedResolutinos = new HashMap<>(answers);
            copiedResolutinos.putAll(newResolutions);
            this.answers = copiedResolutinos;
        }

        public void replace(Map<Actor<? extends ExecutionActor<?>>, Answer> newResolutions) {
            this.answers = map(newResolutions);
        }

        public Map<Actor<? extends ExecutionActor<?>>, Answer> answers() {
            return this.answers;
        }

        @Override
        public String toString() {
            return "Derivation{" + "answers=" + answers + '}';
        }
    }
}
