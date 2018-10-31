/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
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

package ai.grakn.graql.internal.query;

import ai.grakn.concept.Concept;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.Var;
import ai.grakn.graql.internal.query.answer.ConceptMapImpl;
import com.google.common.collect.ImmutableMap;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static ai.grakn.graql.Graql.var;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;


public class ConceptMapTest {

    private final Var varInAnswer = var("x");
    private final Concept conceptInAnswer = mock(Concept.class);

    private final ConceptMapImpl answer = new ConceptMapImpl(ImmutableMap.of(varInAnswer, conceptInAnswer));

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Test
    public void whenGettingAConceptThatIsInTheAnswer_ReturnTheConcept() {
        assertEquals(conceptInAnswer, answer.get(varInAnswer));
    }

    @Test
    public void whenGettingAConceptThatIsNotInTheAnswer_Throw() {
        Var varNotInAnswer = var("y");

        exception.expect(GraqlQueryException.class);
        exception.expectMessage(GraqlQueryException.varNotInQuery(varNotInAnswer).getMessage());

        answer.get(varNotInAnswer);
    }
}