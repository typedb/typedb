/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.engine.printer;

import ai.grakn.engine.controller.response.Concept;
import ai.grakn.engine.controller.response.ConceptBuilder;
import ai.grakn.test.kbs.MovieKB;
import ai.grakn.test.rule.SampleKBContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;

import static ai.grakn.graql.Graql.var;
import static org.junit.Assert.assertEquals;

public class JacksonPrinterTest {
    private static ObjectMapper mapper = new ObjectMapper();
    private static JacksonPrinter printer = new JacksonPrinter();

    @ClassRule
    public static final SampleKBContext rule = MovieKB.context();

    @Test
    public void whenGraqlQueryResultsInConcept_EnsureConceptWrpperIsReturned() throws IOException {
        ai.grakn.concept.Concept concept = rule.tx().graql().match(var("x").isa("person")).get("x").iterator().next();
        Concept conceptWrapper = ConceptBuilder.build(concept);
        String response = mapper.writeValueAsString(conceptWrapper);
        assertEquals(response, printer.graqlString(concept));
    }
}
