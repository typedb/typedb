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

package ai.grakn.engine.controller.response;

import ai.grakn.concept.Label;
import ai.grakn.engine.printer.JacksonPrinter;
import ai.grakn.graql.GetQuery;
import ai.grakn.graql.Printer;
import ai.grakn.graql.internal.query.QueryAnswer;
import ai.grakn.test.kbs.GenealogyKB;
import ai.grakn.test.rule.SampleKBContext;
import ai.grakn.util.Schema;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ExplanationBuilderTest {

    @ClassRule
    public static SampleKBContext genealogyKB = GenealogyKB.context();

    @Test
    public void whenExplainInferred_returnsLinkedExplanation() {
        Label person = Label.of("person");
        Label siblings = Label.of("siblings");
        Label parentship = Label.of("parentship");

        Printer printer = new JacksonPrinter();
        String mainQuery = "match ($x, $y) isa cousins; limit 15; get;";
        List<ai.grakn.graql.admin.Answer> answers = genealogyKB.tx().graql().infer(true).parser().<GetQuery>parseQuery(mainQuery).execute();
        answers.forEach(answer -> {
            String cousin1 = answer.get("x").getId().getValue();
            String cousin2 = answer.get("y").getId().getValue();

            String specificQuery = "match " +
                    "$x id " + cousin1 + ";" +
                    "$y id " + cousin2 + ";" +
                    "(cousin: $x, cousin: $y) isa cousins; limit 1;get;";

            GetQuery query = genealogyKB.tx().graql().infer(true).parse(specificQuery);
            ai.grakn.graql.admin.Answer specificAnswer = query.execute().stream().findFirst().orElse(new QueryAnswer());

            List<Answer> explanation = ExplanationBuilder.buildExplanation(specificAnswer.getExplanation().getAnswers(), printer);

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
