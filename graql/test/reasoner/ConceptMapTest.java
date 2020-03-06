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

package grakn.core.graql.reasoner;

import com.google.common.collect.ImmutableMap;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.graql.reasoner.utils.AnswerUtil;
import grakn.core.kb.concept.api.Concept;
import grakn.core.kb.concept.api.GraknConceptException;
import graql.lang.statement.Variable;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;


public class ConceptMapTest {

    private final Variable someVar = new Variable("x");
    private final Concept someConcept = mock(Concept.class);
    private final ConceptMap answer = new ConceptMap(ImmutableMap.of(someVar, someConcept));

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Test
    public void whenGettingAConceptThatIsInTheAnswer_ReturnTheConcept() {
        assertEquals(someConcept, answer.get(someVar));
    }

    @Test
    public void whenGettingAConceptThatIsNotInTheAnswer_Throw() {
        Variable varNotInAnswer = new Variable("y");

        exception.expect(GraknConceptException.class);
        exception.expectMessage(GraknConceptException.variableDoesNotExist(varNotInAnswer.toString()).getMessage());

        answer.get(varNotInAnswer);
    }

    @Test
    public void whenJoiningIncompatibleAnswers_emptyAnswerIsReturned() {
        Concept anotherConcept = mock(Concept.class);
        ConceptMap anotherAnswer = new ConceptMap(ImmutableMap.of(someVar, anotherConcept));
        ConceptMap joinedAnswer = AnswerUtil.joinAnswers(answer, anotherAnswer);
        assertTrue(joinedAnswer.isEmpty());
    }

}