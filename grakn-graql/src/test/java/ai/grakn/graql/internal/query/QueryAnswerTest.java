/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.graql.internal.query;

import ai.grakn.concept.Concept;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.Var;
import com.google.common.collect.ImmutableMap;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static ai.grakn.graql.Graql.var;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

/**
 * @author Felix Chapman
 */
public class QueryAnswerTest {

    private final Var varInAnswer = var("x");
    private final Concept conceptInAnswer = mock(Concept.class);

    private final QueryAnswer answer = new QueryAnswer(ImmutableMap.of(varInAnswer, conceptInAnswer));

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