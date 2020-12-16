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
 */

package grakn.core.reasoner.resolution;

import grakn.core.common.exception.GraknException;
import grakn.core.concept.Concept;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.pattern.Conjunction;
import grakn.core.pattern.variable.Variable;
import graql.lang.pattern.variable.Reference;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;

/**
 * Compute mock iterators of concept maps. The ConceptMaps will contains MockConcept
 *
 * TODO this will get removed as we integrate the execution model fully into the main codebase
 */
public class MockTransaction {
    private final long computeLength;

    public MockTransaction(long computeLength) {
        this.computeLength = computeLength;
    }

    // real conjunctions
    public Iterator<ConceptMap> query(Conjunction conjunction, ConceptMap partialConceptMap) {
        Map<String, Long> variableSeeds = new HashMap<>();
        for (Variable variable : conjunction.variables()) {
            if (variable.reference().isName()) {
                String name = variable.identifier().reference().asName().toString().substring(1);
                Long seed = new Random(variable.hashCode()).nextLong();
                variableSeeds.put(name, seed);
            }
        }
        return iterator(variableSeeds, partialConceptMap);
    }

    private Iterator<ConceptMap> iterator(Map<String, Long> variableSeeds, ConceptMap partialConceptMap) {
        return new Iterator<ConceptMap>() {
            int counter = 0;

            @Override
            public boolean hasNext() {
                return counter < computeLength;
            }

            @Override
            public ConceptMap next() {
                Map<Reference.Name, Concept> concepts = new HashMap<>(partialConceptMap.concepts());

                variableSeeds.forEach((var, seed) -> {
                    MockConcept mockConcept = new MockConcept(seed + counter);
                    concepts.put(Reference.named(var), mockConcept);
                });

                ConceptMap conceptMap = new ConceptMap(concepts);
                counter++;
                return conceptMap;
            }
        };
    }


    public static class MockConcept implements Concept {
        private final long id;

        public MockConcept(long id) {
            this.id = id;
        }

        @Override
        public String toString() {
            return "MockConcept{" +
                    "id='" + id + '\'' +
                    '}';
        }

        @Override
        public boolean isDeleted() {
            return false;
        }

        @Override
        public void delete() {
            // noop
        }

        @Override
        public GraknException exception(GraknException exception) {
            return exception;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final MockConcept that = (MockConcept) o;
            return id == that.id;
        }

        @Override
        public int hashCode() {
            return Objects.hash(id);
        }
    }
}
