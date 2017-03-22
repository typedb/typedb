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

import ai.grakn.concept.EntityType;
import ai.grakn.concept.Rule;
import ai.grakn.concept.RuleType;
import ai.grakn.exception.GraknValidationException;
import ai.grakn.exception.InvalidConceptValueException;
import ai.grakn.graql.Pattern;
import ai.grakn.util.ErrorMessage;
import org.junit.Before;
import org.junit.Test;

import static ai.grakn.util.ErrorMessage.NULL_VALUE;
import static ai.grakn.util.Schema.ConceptProperty.RULE_LHS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class RuleTest extends GraphTestBase{
    private Pattern lhs;
    private Pattern rhs;

    @Before
    public void setupRules(){
        lhs = graknGraph.graql().parsePattern("$x isa entity-type");
        rhs = graknGraph.graql().parsePattern("$x isa entity-type");
    }

    @Test
    public void testType() {
        RuleType conceptType = graknGraph.putRuleType("A Thing");
        Rule rule = conceptType.addRule(lhs, rhs);
        assertNotNull(rule.type());
        assertEquals(conceptType, rule.type());
    }

    @Test
    public void testRuleValues() throws Exception {
        RuleType conceptType = graknGraph.putRuleType("A Thing");
        Rule rule = conceptType.addRule(lhs, rhs);
        assertEquals(lhs, rule.getLHS());
        assertEquals(rhs, rule.getRHS());

        expectedException.expect(InvalidConceptValueException.class);
        expectedException.expectMessage(NULL_VALUE.getMessage(RULE_LHS));

        conceptType.addRule(null, null);
    }

    @Test
    public void addRuleWithNonExistentEntityType() throws GraknValidationException {
        graknGraph.putEntityType("My-Type");

        lhs = graknGraph.graql().parsePattern("$x isa Your-Type");
        rhs = graknGraph.graql().parsePattern("$x isa My-Type");
        Rule rule = graknGraph.admin().getMetaRuleInference().addRule(lhs, rhs);

        expectedException.expect(GraknValidationException.class);
        expectedException.expectMessage(
                ErrorMessage.VALIDATION_RULE_MISSING_ELEMENTS.getMessage("LHS", rule.getId(), rule.type().getName(), "Your-Type"));

        graknGraph.commit();
    }

    @Test
    public void addRuleWithNonExistentRoleType() throws GraknValidationException {
        graknGraph.putEntityType("My-Type");

        lhs = graknGraph.graql().parsePattern("$x isa My-Type");
        rhs = graknGraph.graql().parsePattern("$x has-role Your-Type");
        Rule rule = graknGraph.admin().getMetaRuleInference().addRule(lhs, rhs);

        expectedException.expect(GraknValidationException.class);
        expectedException.expectMessage(
                ErrorMessage.VALIDATION_RULE_MISSING_ELEMENTS.getMessage("RHS", rule.getId(), rule.type().getName(), "Your-Type"));

        graknGraph.commit();
    }

    @Test
    public void checkRuleSetsAreFilledUponCommit() throws GraknValidationException{
        EntityType t1 = graknGraph.putEntityType("type1");
        EntityType t2 = graknGraph.putEntityType("type2");

        lhs = graknGraph.graql().parsePattern("$x isa type1");
        rhs = graknGraph.graql().parsePattern("$x isa type2");

        Rule rule = graknGraph.admin().getMetaRuleInference().addRule(lhs, rhs);
        assertTrue("Hypothesis is not empty before commit", rule.getHypothesisTypes().isEmpty());
        assertTrue("Conclusion is not empty before commit", rule.getConclusionTypes().isEmpty());

        graknGraph.commit();

        assertThat(rule.getHypothesisTypes(), containsInAnyOrder(t1));
        assertThat(rule.getConclusionTypes(), containsInAnyOrder(t2));
    }
}