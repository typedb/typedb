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
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.engine.printer;

import ai.grakn.engine.controller.response.Answer;
import ai.grakn.engine.controller.response.Concept;
import ai.grakn.engine.controller.response.ConceptBuilder;
import ai.grakn.graql.answer.ConceptMap;
import ai.grakn.graql.answer.Value;
import ai.grakn.graql.internal.query.answer.ConceptMapImpl;
import ai.grakn.test.kbs.MovieKB;
import ai.grakn.test.rule.SampleKBContext;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static ai.grakn.graql.Graql.count;
import static ai.grakn.graql.Graql.var;
import static org.junit.Assert.assertEquals;

public class JacksonPrinterTest {
    private static ObjectMapper mapper = new ObjectMapper();
    private static JacksonPrinter printer = JacksonPrinter.create();

    @ClassRule
    public static final SampleKBContext rule = MovieKB.context();

    @Test
    public void whenGraqlQueryResultsInConcept_EnsureConceptWrapperIsReturned() throws JsonProcessingException {
        ai.grakn.concept.Concept concept = rule.tx().graql().match(var("x").isa("person")).get("x")
                .stream().map(ans -> ans.get("x")).iterator().next();
        Concept conceptWrapper = ConceptBuilder.build(concept);
        assertWrappersMatch(conceptWrapper, concept);
    }

    @Test
    public void whenGraqlQueryResultsInConcepts_EnsureConceptsAreWrappedAndReturned() throws JsonProcessingException {
        Set<ai.grakn.concept.Concept> concepts = rule.tx().graql().
                match(var("x").isa("person")).get("x")
                .stream().map(ans -> ans.get("x")).collect(Collectors.toSet());
        List<Concept> conceptsWrapper = concepts.stream().map(ConceptBuilder::<Concept>build).collect(Collectors.toList());
        assertWrappersMatch(conceptsWrapper, concepts);
    }

    @Test
    public void whenGraqlQueryReturnsConceptMap_EnsureConceptsInAnswerAreWrappedAndReturned() throws JsonProcessingException {
        ConceptMap answer = rule.tx().graql().match(var("x").isa("title").val("Godfather")).iterator().next();
        Answer answerWrapper = Answer.create(answer);
        assertWrappersMatch(answerWrapper, answer);
    }

    @Test
    public void whenGraqlQueryReturnsConceptMaps_EnsureAnswersArwWrappedAndReturned() throws JsonProcessingException {
        Set<ConceptMap> answers = rule.tx().graql().
                match(var("x").isa("title").val("Godfather")).stream().collect(Collectors.toSet());
        Set<Answer> answersWrapper = answers.stream().map(Answer::create).collect(Collectors.toSet());
        assertWrappersMatch(answersWrapper, answers);
    }

    @Test
    public void whenGraqlQueryReturnsValue_EnsureNativeBooleanRepresentationIsReturned() throws JsonProcessingException {
        Value count = rule.tx().graql().match(var("x").isa("person")).aggregate(count()).execute();
        assertWrappersMatch(count.number(), count);
    }

    @Test
    public void whenPrintingMap_MapGraqlStringOverKeyAndValues() {
        ConceptMapImpl ans = new ConceptMapImpl();

        Map<?, ?> map = ImmutableMap.of(ans, ImmutableList.of(ans));

        Map<?, ?> expected = ImmutableMap.of(Answer.create(ans), ImmutableList.of(Answer.create(ans)));

        assertEquals(expected, printer.map(map));
    }

    private static void assertWrappersMatch(Object expected, Object toPrint) throws JsonProcessingException {
        String response = mapper.writeValueAsString(expected);
        assertEquals(response, printer.toString(toPrint));
    }
}
