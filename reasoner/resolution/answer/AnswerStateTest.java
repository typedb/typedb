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
 */

package grakn.core.reasoner.resolution.answer;

import grakn.core.common.exception.GraknException;
import grakn.core.concept.Concept;
import grakn.core.concept.ConceptImpl;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.reasoner.resolution.answer.AnswerState.Partial;
import grakn.core.reasoner.resolution.answer.AnswerState.Partial.Mapped;
import grakn.core.reasoner.resolution.answer.AnswerState.Top;
import grakn.core.traversal.common.Identifier;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static grakn.common.collection.Collections.set;
import static grakn.core.reasoner.resolution.answer.AnswerState.Partial.Identity.identity;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;

public class AnswerStateTest {

    @Test
    public void test_initial_empty_mapped_to_downstream_and_back() {
        Map<Identifier.Variable.Retrievable, Identifier.Variable.Retrievable> mapping = new HashMap<>();
        mapping.put(Identifier.Variable.name("a"), Identifier.Variable.name("x"));
        mapping.put(Identifier.Variable.name("b"), Identifier.Variable.name("y"));
        Set<Identifier.Variable.Name> filter = set(Identifier.Variable.name("a"), Identifier.Variable.name("b"));
        Mapped mapped = Top.initial(filter, false, null).toDownstream().mapToDownstream(Mapping.of(mapping), null);
        assertTrue(mapped.conceptMap().concepts().isEmpty());

        Map<Identifier.Variable.Retrievable, Concept> concepts = new HashMap<>();
        concepts.put(Identifier.Variable.name("x"), new MockConcept(0));
        concepts.put(Identifier.Variable.name("y"), new MockConcept(1));
        Partial<?> partial = mapped.aggregateToUpstream(new ConceptMap(concepts));
        Map<Identifier.Variable.Retrievable, Concept> expected = new HashMap<>();
        expected.put(Identifier.Variable.name("a"), new MockConcept(0));
        expected.put(Identifier.Variable.name("b"), new MockConcept(1));
        assertEquals(new ConceptMap(expected), partial.conceptMap());
    }

    @Test
    public void test_initial_partially_mapped_to_downstream_and_back() {
        Map<Identifier.Variable.Retrievable, Identifier.Variable.Retrievable>  mapping = new HashMap<>();
        mapping.put(Identifier.Variable.name("a"), Identifier.Variable.name("x"));
        mapping.put(Identifier.Variable.name("b"), Identifier.Variable.name("y"));
        Map<Identifier.Variable.Retrievable, Concept> concepts = new HashMap<>();
        concepts.put(Identifier.Variable.name("a"), new MockConcept(0));
        Set<Identifier.Variable.Name> filter = set(Identifier.Variable.name("a"), Identifier.Variable.name("b"));
        Top top = Top.initial(filter, false, null);
        Mapped mapped = identity(new ConceptMap(concepts), top,  null, null, false)
                .mapToDownstream(Mapping.of(mapping), null);

        Map<Identifier.Variable.Retrievable, Concept> expectedMapped = new HashMap<>();
        expectedMapped.put(Identifier.Variable.name("x"), new MockConcept(0));
        assertEquals(new ConceptMap(expectedMapped), mapped.conceptMap());

        Map<Identifier.Variable.Retrievable, Concept> downstreamConcepts = new HashMap<>();
        downstreamConcepts.put(Identifier.Variable.name("x"), new MockConcept(0));
        downstreamConcepts.put(Identifier.Variable.name("y"), new MockConcept(1));
        Partial<?> partial = mapped.aggregateToUpstream(new ConceptMap(downstreamConcepts));

        Map<Identifier.Variable.Retrievable, Concept> expectedWithInitial = new HashMap<>();
        expectedWithInitial.put(Identifier.Variable.name("a"), new MockConcept(0));
        expectedWithInitial.put(Identifier.Variable.name("b"), new MockConcept(1));
        assertEquals(new ConceptMap(expectedWithInitial), partial.conceptMap());
    }

    @Test
    public void test_initial_with_unmapped_elements() {
        Map<Identifier.Variable.Retrievable, Identifier.Variable.Retrievable>  mapping = new HashMap<>();
        mapping.put(Identifier.Variable.name("a"), Identifier.Variable.name("x"));
        mapping.put(Identifier.Variable.name("b"), Identifier.Variable.name("y"));
        Map<Identifier.Variable.Retrievable, Concept> concepts = new HashMap<>();
        concepts.put(Identifier.Variable.name("a"), new MockConcept(0));
        concepts.put(Identifier.Variable.name("c"), new MockConcept(2));
        Set<Identifier.Variable.Name> filter = set(Identifier.Variable.name("a"), Identifier.Variable.name("b"));
        Top top = Top.initial(filter, false, null);
        Mapped mapped = identity(new ConceptMap(concepts), top, null, null, false)
                .mapToDownstream(Mapping.of(mapping), null);

        Map<Identifier.Variable.Retrievable, Concept> expectedMapped = new HashMap<>();
        expectedMapped.put(Identifier.Variable.name("x"), new MockConcept(0));
        assertEquals(new ConceptMap(expectedMapped), mapped.conceptMap());

        Map<Identifier.Variable.Retrievable, Concept> downstreamConcepts = new HashMap<>();
        downstreamConcepts.put(Identifier.Variable.name("x"), new MockConcept(0));
        downstreamConcepts.put(Identifier.Variable.name("y"), new MockConcept(1));
        Partial<?> partial = mapped.aggregateToUpstream(new ConceptMap(downstreamConcepts));

        Map<Identifier.Variable.Retrievable, Concept> expectedWithInitial = new HashMap<>();
        expectedWithInitial.put(Identifier.Variable.name("a"), new MockConcept(0));
        expectedWithInitial.put(Identifier.Variable.name("b"), new MockConcept(1));
        expectedWithInitial.put(Identifier.Variable.name("c"), new MockConcept(2));
        assertEquals(new ConceptMap(expectedWithInitial), partial.conceptMap());
    }

    public static class MockConcept extends ConceptImpl implements Concept {
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
