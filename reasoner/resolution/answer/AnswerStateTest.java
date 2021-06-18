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
 */

package com.vaticle.typedb.core.reasoner.resolution.answer;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.concept.Concept;
import com.vaticle.typedb.core.concept.ConceptImpl;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.reasoner.resolution.answer.AnswerState.Partial;
import com.vaticle.typedb.core.reasoner.resolution.answer.AnswerState.Partial.Concludable;
import com.vaticle.typedb.core.reasoner.resolution.answer.AnswerStateImpl.TopImpl.MatchImpl.InitialImpl;
import com.vaticle.typedb.core.traversal.common.Identifier;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static com.vaticle.typedb.common.collection.Collections.set;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AnswerStateTest {

    @Test
    public void test_root_empty_mapped_to_downstream_and_back() {
        Map<Identifier.Variable.Retrievable, Identifier.Variable.Retrievable> mapping = new HashMap<>();
        mapping.put(Identifier.Variable.name("a"), Identifier.Variable.name("x"));
        mapping.put(Identifier.Variable.name("b"), Identifier.Variable.name("y"));
        Set<Identifier.Variable.Retrievable> filter = set(Identifier.Variable.name("a"), Identifier.Variable.name("b"));
        Concludable.Match<?> mapped = InitialImpl.create(filter, new ConceptMap(), null, false).toDownstream().toDownstream(Mapping.of(mapping), null);
        assertTrue(mapped.conceptMap().concepts().isEmpty());

        Map<Identifier.Variable.Retrievable, Concept> concepts = new HashMap<>();
        concepts.put(Identifier.Variable.name("x"), new MockConcept(0));
        concepts.put(Identifier.Variable.name("y"), new MockConcept(1));
        Partial.Compound<?, ?> partial = mapped.toUpstreamLookup(new ConceptMap(concepts), false);
        Map<Identifier.Variable.Retrievable, Concept> expected = new HashMap<>();
        expected.put(Identifier.Variable.name("a"), new MockConcept(0));
        expected.put(Identifier.Variable.name("b"), new MockConcept(1));
        assertEquals(new ConceptMap(expected), partial.conceptMap());
    }

    @Test
    public void test_root_partially_mapped_to_downstream_and_back() {
        Map<Identifier.Variable.Retrievable, Identifier.Variable.Retrievable> mapping = new HashMap<>();
        mapping.put(Identifier.Variable.name("a"), Identifier.Variable.name("x"));
        mapping.put(Identifier.Variable.name("b"), Identifier.Variable.name("y"));
        Map<Identifier.Variable.Retrievable, Concept> concepts = new HashMap<>();
        concepts.put(Identifier.Variable.name("a"), new MockConcept(0));
        Set<Identifier.Variable.Retrievable> filter = set(Identifier.Variable.name("a"), Identifier.Variable.name("b"));
        Concludable.Match<?> mapped = InitialImpl.create(filter, new ConceptMap(), null, false).toDownstream()
                .with(new ConceptMap(concepts))
                .toDownstream(Mapping.of(mapping), null);

        Map<Identifier.Variable.Retrievable, Concept> expectedMapped = new HashMap<>();
        expectedMapped.put(Identifier.Variable.name("x"), new MockConcept(0));
        assertEquals(new ConceptMap(expectedMapped), mapped.conceptMap());

        Map<Identifier.Variable.Retrievable, Concept> downstreamConcepts = new HashMap<>();
        downstreamConcepts.put(Identifier.Variable.name("x"), new MockConcept(0));
        downstreamConcepts.put(Identifier.Variable.name("y"), new MockConcept(1));
        Partial.Compound<?, ?> partial = mapped.toUpstreamLookup(new ConceptMap(downstreamConcepts), false);

        Map<Identifier.Variable.Retrievable, Concept> expectedWithInitial = new HashMap<>();
        expectedWithInitial.put(Identifier.Variable.name("a"), new MockConcept(0));
        expectedWithInitial.put(Identifier.Variable.name("b"), new MockConcept(1));
        assertEquals(new ConceptMap(expectedWithInitial), partial.conceptMap());
    }

    @Test
    public void test_root_with_unmapped_elements() {
        Map<Identifier.Variable.Retrievable, Identifier.Variable.Retrievable> mapping = new HashMap<>();
        mapping.put(Identifier.Variable.name("a"), Identifier.Variable.name("x"));
        mapping.put(Identifier.Variable.name("b"), Identifier.Variable.name("y"));
        Map<Identifier.Variable.Retrievable, Concept> concepts = new HashMap<>();
        concepts.put(Identifier.Variable.name("a"), new MockConcept(0));
        concepts.put(Identifier.Variable.name("c"), new MockConcept(2));
        Set<Identifier.Variable.Retrievable> filter = set(Identifier.Variable.name("a"), Identifier.Variable.name("b"));
        Concludable.Match<?> mapped = InitialImpl.create(filter, new ConceptMap(), null, false).toDownstream()
                .with(new ConceptMap(concepts))
                .toDownstream(Mapping.of(mapping), null);

        Map<Identifier.Variable.Retrievable, Concept> expectedMapped = new HashMap<>();
        expectedMapped.put(Identifier.Variable.name("x"), new MockConcept(0));
        assertEquals(new ConceptMap(expectedMapped), mapped.conceptMap());

        Map<Identifier.Variable.Retrievable, Concept> downstreamConcepts = new HashMap<>();
        downstreamConcepts.put(Identifier.Variable.name("x"), new MockConcept(0));
        downstreamConcepts.put(Identifier.Variable.name("y"), new MockConcept(1));
        Partial.Compound<?, ?> partial = mapped.toUpstreamLookup(new ConceptMap(downstreamConcepts), false);

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
        public TypeDBException exception(TypeDBException exception) {
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
