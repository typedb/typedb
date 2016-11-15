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

import ai.grakn.concept.Rule;
import ai.grakn.concept.RuleType;
import ai.grakn.concept.Type;
import ai.grakn.exception.InvalidConceptValueException;
import ai.grakn.graql.Pattern;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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
        expectedException.expectMessage(allOf(
                containsString(ErrorMessage.NULL_VALUE.getMessage(Schema.ConceptProperty.RULE_LHS))
        ));

        conceptType.addRule(null, null);
    }

    @Test
    public void testExpectation() throws Exception {
        RuleType conceptType = graknGraph.putRuleType("A Thing");
        Rule rule = conceptType.addRule(lhs, rhs);
        assertFalse(rule.getExpectation());
        rule.setExpectation(true);
        assertTrue(rule.getExpectation());
    }

    @Test
    public void testMaterialise() throws Exception {
        RuleType conceptType = graknGraph.putRuleType("A Thing");
        Rule rule = conceptType.addRule(lhs, rhs);
        assertFalse(rule.isMaterialise());
        rule.setMaterialise(true);
        assertTrue(rule.isMaterialise());
    }

    @Test
    public void testAddHypothesis() throws Exception {
        RuleType conceptType = graknGraph.putRuleType("A Thing");
        Rule rule = conceptType.addRule(lhs, rhs);
        Vertex ruleVertex = graknGraph.getTinkerPopGraph().traversal().V(((RuleImpl) rule).getBaseIdentifier()).next();
        Type type1 = graknGraph.putEntityType("A Concept Type 1");
        Type type2 = graknGraph.putEntityType("A Concept Type 2");
        assertFalse(ruleVertex.edges(Direction.BOTH, Schema.EdgeLabel.HYPOTHESIS.getLabel()).hasNext());
        rule.addHypothesis(type1).addHypothesis(type2);
        assertTrue(ruleVertex.edges(Direction.BOTH, Schema.EdgeLabel.HYPOTHESIS.getLabel()).hasNext());
    }

    @Test
    public void testAddConclusion() throws Exception {
        RuleType conceptType = graknGraph.putRuleType("A Thing");
        Rule rule = conceptType.addRule(lhs, rhs);
        Vertex ruleVertex = graknGraph.getTinkerPopGraph().traversal().V(((RuleImpl) rule).getBaseIdentifier()).next();
        Type type1 = graknGraph.putEntityType("A Concept Type 1");
        Type type2 = graknGraph.putEntityType("A Concept Type 2");
        assertFalse(ruleVertex.edges(Direction.BOTH, Schema.EdgeLabel.CONCLUSION.getLabel()).hasNext());
        rule.addConclusion(type1).addConclusion(type2);
        assertTrue(ruleVertex.edges(Direction.BOTH, Schema.EdgeLabel.CONCLUSION.getLabel()).hasNext());
    }

    @Test
    public void testHypothesisTypes(){
        RuleType ruleType = graknGraph.putRuleType("A Rule Type");
        Rule rule = ruleType.addRule(lhs, rhs);
        assertEquals(0, rule.getHypothesisTypes().size());

        Type ct1 = graknGraph.putEntityType("A Concept Type 1");
        Type ct2 = graknGraph.putEntityType("A Concept Type 2");
        rule.addHypothesis(ct1).addHypothesis(ct2);
        assertEquals(2, rule.getHypothesisTypes().size());
        assertTrue(rule.getHypothesisTypes().contains(ct1));
        assertTrue(rule.getHypothesisTypes().contains(ct2));
    }

    @Test
    public void testConclusionTypes(){
        RuleType ruleType = graknGraph.putRuleType("A Rule Type");
        Rule rule = ruleType.addRule(lhs, rhs);
        assertEquals(0, rule.getConclusionTypes().size());

        Type ct1 = graknGraph.putEntityType("A Concept Type 1");
        Type ct2 = graknGraph.putEntityType("A Concept Type 2");
        rule.addConclusion(ct1).addConclusion(ct2);
        assertEquals(2, rule.getConclusionTypes().size());
        assertTrue(rule.getConclusionTypes().contains(ct1));
        assertTrue(rule.getConclusionTypes().contains(ct2));
    }

}