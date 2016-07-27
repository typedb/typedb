/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.core.implementation;

import io.mindmaps.core.model.Rule;
import io.mindmaps.core.model.RuleType;
import io.mindmaps.core.model.Type;
import io.mindmaps.factory.MindmapsTestGraphFactory;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class RuleTest {

    private MindmapsTransactionImpl mindmapsGraph;

    @Before
    public void buildGraph() {
        mindmapsGraph = (MindmapsTransactionImpl) MindmapsTestGraphFactory.newEmptyGraph().newTransaction();
        mindmapsGraph.initialiseMetaConcepts();
    }

    @Test
    public void testType() {
        RuleType conceptType = mindmapsGraph.putRuleType("A Thing");
        Rule rule = mindmapsGraph.putRule("A rule", conceptType);
        assertNotNull(rule.type());
        assertEquals(conceptType, rule.type());
    }

    @Test
    public void testLHS() throws Exception {
        RuleType conceptType = mindmapsGraph.putRuleType("A Thing");
        Rule rule = mindmapsGraph.putRule("A rule", conceptType);
        assertNull(rule.getLHS());
        rule.setLHS("LHS");
        assertEquals("LHS", rule.getLHS());
    }

    @Test
    public void testRHS() throws Exception {
        RuleType conceptType = mindmapsGraph.putRuleType("A Thing");
        Rule rule = mindmapsGraph.putRule("A rule", conceptType);
        assertNull(rule.getRHS());
        rule.setRHS("RHS");
        assertEquals("RHS", rule.getRHS());
    }

    @Test
    public void testExpectation() throws Exception {
        RuleType conceptType = mindmapsGraph.putRuleType("A Thing");
        Rule rule = mindmapsGraph.putRule("A rule", conceptType);
        assertFalse(rule.getExpectation());
        rule.setExpectation(true);
        assertTrue(rule.getExpectation());
    }

    @Test
    public void testMaterialise() throws Exception {
        RuleType conceptType = mindmapsGraph.putRuleType("A Thing");
        Rule rule = mindmapsGraph.putRule("A rule", conceptType);
        assertFalse(rule.isMaterialise());
        rule.setMaterialise(true);
        assertTrue(rule.isMaterialise());
    }

    @Test
    public void testAddHypothesis() throws Exception {
        RuleType conceptType = mindmapsGraph.putRuleType("A Thing");
        Rule rule = mindmapsGraph.putRule("A Rule", conceptType);
        Vertex ruleVertex = mindmapsGraph.getTinkerPopGraph().traversal().V(((RuleImpl) rule).getBaseIdentifier()).next();
        Type type1 = mindmapsGraph.putEntityType("A Concept Type 1");
        Type type2 = mindmapsGraph.putEntityType("A Concept Type 2");
        assertFalse(ruleVertex.edges(Direction.BOTH, DataType.EdgeLabel.HYPOTHESIS.getLabel()).hasNext());
        rule.addHypothesis(type1).addHypothesis(type2);
        assertTrue(ruleVertex.edges(Direction.BOTH, DataType.EdgeLabel.HYPOTHESIS.getLabel()).hasNext());
    }

    @Test
    public void testAddConclusion() throws Exception {
        RuleType conceptType = mindmapsGraph.putRuleType("A Thing");
        Rule rule = mindmapsGraph.putRule("A Rule", conceptType);
        Vertex ruleVertex = mindmapsGraph.getTinkerPopGraph().traversal().V(((RuleImpl) rule).getBaseIdentifier()).next();
        Type type1 = mindmapsGraph.putEntityType("A Concept Type 1");
        Type type2 = mindmapsGraph.putEntityType("A Concept Type 2");
        assertFalse(ruleVertex.edges(Direction.BOTH, DataType.EdgeLabel.CONCLUSION.getLabel()).hasNext());
        rule.addConclusion(type1).addConclusion(type2);
        assertTrue(ruleVertex.edges(Direction.BOTH, DataType.EdgeLabel.CONCLUSION.getLabel()).hasNext());
    }

    @Test
    public void testHypothesisTypes(){
        RuleType ruleType = mindmapsGraph.putRuleType("A Rule Type");
        Rule rule = mindmapsGraph.putRule("A Rule", ruleType);
        assertEquals(0, rule.getHypothesisTypes().size());

        Type ct1 = mindmapsGraph.putEntityType("A Concept Type 1");
        Type ct2 = mindmapsGraph.putEntityType("A Concept Type 2");
        rule.addHypothesis(ct1).addHypothesis(ct2);
        assertEquals(2, rule.getHypothesisTypes().size());
        assertTrue(rule.getHypothesisTypes().contains(ct1));
        assertTrue(rule.getHypothesisTypes().contains(ct2));
    }

    @Test
    public void testConclusionTypes(){
        RuleType ruleType = mindmapsGraph.putRuleType("A Rule Type");
        Rule rule = mindmapsGraph.putRule("A Rule", ruleType);
        assertEquals(0, rule.getConclusionTypes().size());

        Type ct1 = mindmapsGraph.putEntityType("A Concept Type 1");
        Type ct2 = mindmapsGraph.putEntityType("A Concept Type 2");
        rule.addConclusion(ct1).addConclusion(ct2);
        assertEquals(2, rule.getConclusionTypes().size());
        assertTrue(rule.getConclusionTypes().contains(ct1));
        assertTrue(rule.getConclusionTypes().contains(ct2));
    }

}