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

package ai.grakn.graph.internal;

import ai.grakn.Grakn;
import ai.grakn.GraknGraph;
import ai.grakn.GraknSession;
import ai.grakn.GraknTxType;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Rule;
import ai.grakn.concept.RuleType;
import ai.grakn.exception.GraphOperationException;
import ai.grakn.exception.InvalidGraphException;
import ai.grakn.graql.Pattern;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static ai.grakn.util.ErrorMessage.NULL_VALUE;
import static ai.grakn.util.Schema.VertexProperty.RULE_WHEN;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertEquals;

//NOTE: This test is inside the graql module due to the inability to have graql constructs inside the graph module
public class RuleTest {
    @org.junit.Rule
    public final ExpectedException expectedException = ExpectedException.none();

    private Pattern when;
    private Pattern then;
    private GraknGraph graknGraph;
    private GraknSession session;

    @Before
    public void setupRules(){
        session = Grakn.session(Grakn.IN_MEMORY, "absd");
        graknGraph = Grakn.session(Grakn.IN_MEMORY, "absd").open(GraknTxType.WRITE);
        when = graknGraph.graql().parsePattern("$x isa entity-type");
        then = graknGraph.graql().parsePattern("$x isa entity-type");
    }

    @After
    public void closeSession() throws Exception {
        graknGraph.close();
        session.close();
    }

    @Test
    public void whenCreatingRulesWithNullValues_Throw() throws Exception {
        RuleType conceptType = graknGraph.putRuleType("A Thing");
        Rule rule = conceptType.putRule(when, then);
        assertEquals(when, rule.getWhen());
        assertEquals(then, rule.getThen());

        expectedException.expect(GraphOperationException.class);
        expectedException.expectMessage(NULL_VALUE.getMessage(RULE_WHEN));

        conceptType.putRule(null, null);
    }

    @Test
    public void whenCreatingRulesWithNonExistentEntityType_Throw() throws InvalidGraphException {
        graknGraph.putEntityType("My-Type");

        when = graknGraph.graql().parsePattern("$x isa Your-Type");
        then = graknGraph.graql().parsePattern("$x isa My-Type");
        Rule rule = graknGraph.admin().getMetaRuleInference().putRule(when, then);

        expectedException.expect(InvalidGraphException.class);
        expectedException.expectMessage(
                ErrorMessage.VALIDATION_RULE_MISSING_ELEMENTS.getMessage(Schema.VertexProperty.RULE_WHEN.name(), rule.getId(), rule.type().getLabel(), "Your-Type"));

        graknGraph.commit();
    }

    @Test
    public void whenAddingRuleWithDisjunctionInTheHead_Throw() throws InvalidGraphException{
        graknGraph.putRelationType("relation1");
        when = graknGraph.graql().parsePattern("(role: $x) or (role: $x, role: $y)");
        then = graknGraph.graql().parsePattern("(role: $x, role: $y) isa relation1");

        Rule rule = graknGraph.admin().getMetaRuleInference().putRule(when, then);

        expectedException.expect(InvalidGraphException.class);
        expectedException.expectMessage(
                ErrorMessage.VALIDATION_RULE_DISJUNCTION_IN_BODY.getMessage(rule.getId(), rule.type().getLabel()));

        graknGraph.commit();
    }

    @Test
    public void whenCreatingRules_EnsureHypothesisAndConclusionTypesAreFilledOnCommit() throws InvalidGraphException{
        EntityType t1 = graknGraph.putEntityType("type1");
        EntityType t2 = graknGraph.putEntityType("type2");

        when = graknGraph.graql().parsePattern("$x isa type1");
        then = graknGraph.graql().parsePattern("$x isa type2");

        Rule rule = graknGraph.admin().getMetaRuleInference().putRule(when, then);
        assertThat(rule.getHypothesisTypes(), empty());
        assertThat(rule.getConclusionTypes(), empty());

        graknGraph.commit();

        assertThat(rule.getHypothesisTypes(), containsInAnyOrder(t1));
        assertThat(rule.getConclusionTypes(), containsInAnyOrder(t2));
    }

    @Test
    public void whenAddingDuplicateRulesOfTheSameTypeWithTheSamePattern_ReturnTheSameRule(){
        graknGraph.putEntityType("type1");
        when = graknGraph.graql().parsePattern("$x isa type1");
        then = graknGraph.graql().parsePattern("$x isa type1");

        Rule rule1 = graknGraph.admin().getMetaRuleInference().putRule(when, then);
        Rule rule2 = graknGraph.admin().getMetaRuleInference().putRule(when, then);

        assertEquals(rule1, rule2);
    }

    @Ignore //This is ignored because we currently have no way to determine if patterns with different variables name are equivalent
    @Test
    public void whenAddingDuplicateRulesOfTheSameTypeWithDifferentPatternVariables_ReturnTheSameRule(){
        graknGraph.putEntityType("type1");
        when = graknGraph.graql().parsePattern("$x isa type1");
        then = graknGraph.graql().parsePattern("$y isa type1");

        Rule rule1 = graknGraph.admin().getMetaRuleInference().putRule(when, then);
        Rule rule2 = graknGraph.admin().getMetaRuleInference().putRule(when, then);

        assertEquals(rule1, rule2);
    }
}