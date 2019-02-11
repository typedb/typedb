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

package grakn.core.graql.query;

import com.google.common.collect.ImmutableMap;
import grakn.core.graql.answer.ConceptMap;
import grakn.core.graql.concept.Concept;
import grakn.core.graql.query.statement.Variable;
import graql.lang.exception.GraqlException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;


public class ConceptMapTest {

    private final Variable varInAnswer = new Variable("x");
    private final Concept conceptInAnswer = mock(Concept.class);

    private final ConceptMap answer = new ConceptMap(ImmutableMap.of(varInAnswer, conceptInAnswer));

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Test
    public void whenGettingAConceptThatIsInTheAnswer_ReturnTheConcept() {
        assertEquals(conceptInAnswer, answer.get(varInAnswer));
    }

    @Test
    public void whenGettingAConceptThatIsNotInTheAnswer_Throw() {
        Variable varNotInAnswer = new Variable("y");

        exception.expect(GraqlException.class);
        exception.expectMessage(GraqlException.varNotInQuery(varNotInAnswer.toString()).getMessage());

        answer.get(varNotInAnswer);
    }
}