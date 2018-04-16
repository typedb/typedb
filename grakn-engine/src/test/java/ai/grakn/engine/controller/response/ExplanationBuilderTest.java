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
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.engine.controller.response;

/*-
 * #%L
 * grakn-engine
 * %%
 * Copyright (C) 2016 - 2018 Grakn Labs Ltd
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */

import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Label;
import ai.grakn.graql.GetQuery;
import ai.grakn.graql.internal.query.QueryAnswer;
import ai.grakn.test.kbs.GenealogyKB;
import ai.grakn.test.rule.SampleKBContext;
import ai.grakn.util.Schema;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ExplanationBuilderTest {

    @ClassRule
    public static SampleKBContext genealogyKB = GenealogyKB.context();

    //NOTE: This test ix expected to be slower than average.
    @Test
    public void whenExplainInferred_returnsLinkedExplanation() {
        Label person = Label.of("person");
        Label siblings = Label.of("siblings");
        Label parentship = Label.of("parentship");

        String mainQuery = "match ($x, $y) isa cousins; limit 15; get;";
        genealogyKB.tx().graql().infer(true).parser().<GetQuery>parseQuery(mainQuery)
                .forEach(answer -> {
            String cousin1 = answer.get("x").getId().getValue();
            String cousin2 = answer.get("y").getId().getValue();

            String specificQuery = "match " +
                    "$x id " + cousin1 + ";" +
                    "$y id " + cousin2 + ";" +
                    "(cousin: $x, cousin: $y) isa cousins; limit 1;get;";

            GetQuery query = genealogyKB.tx().graql().infer(true).parse(specificQuery);
            ai.grakn.graql.admin.Answer specificAnswer = query.execute().stream().findFirst().orElse(new QueryAnswer());

            Set<ConceptId> originalEntityIds = specificAnswer.getExplanation().getAnswers().stream()
                    .flatMap(ans -> ans.concepts().stream())
                    .map(ai.grakn.concept.Concept::getId)
                    .collect(Collectors.toSet());

            List<Answer> explanation = ExplanationBuilder.buildExplanation(specificAnswer);

            Set<ConceptId> entityIds = explanation.stream()
                    .flatMap(exp -> exp.conceptMap().values().stream())
                    .filter(c -> c.baseType().equals("ENTITY"))
                    .map(Concept::id)
                    .collect(Collectors.toSet());

            //ensure we deal with the same entities
            assertEquals(originalEntityIds, entityIds);

            assertEquals(3, explanation.size());
            explanation.forEach(explanationAnswer -> {
                explanationAnswer.conceptMap().values().forEach(concept ->{
                    Schema.BaseType baseType = Schema.BaseType.valueOf(concept.baseType());
                    Label typeLabel = ((Thing) concept).type().label();
                    switch(baseType){
                        case ENTITY:
                            assertEquals(person, typeLabel);
                            break;
                        case RELATIONSHIP:
                            assertTrue(typeLabel.equals(siblings) || typeLabel.equals(parentship));
                            break;
                    }
                });
            });
        });
    }
}
