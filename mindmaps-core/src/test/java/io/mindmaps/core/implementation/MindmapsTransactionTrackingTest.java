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

import io.mindmaps.core.exceptions.ConceptException;
import io.mindmaps.core.model.Concept;
import io.mindmaps.core.model.EntityType;
import io.mindmaps.core.model.Instance;
import io.mindmaps.core.model.Type;
import io.mindmaps.factory.MindmapsTestGraphFactory;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.Stack;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 *
 * Tests to ensure that future code changes do not cause concepts to be missed by the tracking functionality.
 * This is very important to ensure validation is applied to ALL concepts that have been added/changed plus
 * and concepts that have had new vertices added.
 *
 */

public class MindmapsTransactionTrackingTest {

    private MindmapsTransactionImpl mindmapsGraph;
    private Set<ConceptImpl> modifiedConcepts;
    private Stack<Concept> newConcepts;

    @Before
    public void buildGraphAccessManager() {
        mindmapsGraph = (MindmapsTransactionImpl) MindmapsTestGraphFactory.newEmptyGraph().newTransaction();
        Graph graph = mindmapsGraph.getTinkerPopGraph();
        // start standard rootGraph access manager
        mindmapsGraph.initialiseMetaConcepts();
        modifiedConcepts = new HashSet<>();
        newConcepts = new Stack<>();
    }

    @After
    public void destroyGraphAccessManager()  throws Exception{
        mindmapsGraph.close();
    }

    @Test
    public void testAddConcept () {
        // add concepts to rootGraph in as many ways as possible
        newConcepts.push(mindmapsGraph.putEntityType("test item id"));
        newConcepts.push(mindmapsGraph.putRelationType("test subject id"));
        newConcepts.push(mindmapsGraph.putRoleType("1"));
        newConcepts.push(mindmapsGraph.putRuleType("4"));
        newConcepts.push(mindmapsGraph.putEntityType("3"));

        // verify the concepts that we expected are returned in the set
        modifiedConcepts = mindmapsGraph.getModifiedConcepts();
        assertTrue(modifiedConcepts.containsAll(newConcepts));

    }

    @Test
    public void testAddPrimitiveEdge() {
        EntityType c1, c2;
        Instance c3;

        // fetch the existing concepts
        c1 = mindmapsGraph.putEntityType("1");
        newConcepts.push(c1);
        c2 = mindmapsGraph.putEntityType("2");
        newConcepts.push(c2);

        // check the concept tracker is empty
        modifiedConcepts = mindmapsGraph.getModifiedConcepts();
        assertEquals(4, modifiedConcepts.size());

        // add primitive edges in as many ways as possible
        c1.superType(c2);

        c3 = mindmapsGraph.putEntity("instance", c2);
        newConcepts.push(c3);

        // verify the concepts that we expected are returned in the set
        modifiedConcepts = mindmapsGraph.getModifiedConcepts();
        assertTrue(modifiedConcepts.containsAll(newConcepts));

    }

    @Test
    public void testDeleteConceptAfterAddingWithinTransaction () throws ConceptException {
        EntityType entityType = mindmapsGraph.putEntityType("entityType");
        // add concepts to rootGraph
        newConcepts.push(
                mindmapsGraph.putEntity("test id", entityType));
        Type type = mindmapsGraph.putEntityType("test subject id");

        // delete some concepts
        type.delete();

        // verify the concepts that we expected are returned in the set
        modifiedConcepts = mindmapsGraph.getModifiedConcepts();
        assertTrue(modifiedConcepts.containsAll(newConcepts));
    }

    @Test
    public void testDeleteConceptFromPrimitiveEdgeWithinTransaction() throws ConceptException {
        // add concepts and edge to rootGraph
        EntityType type = mindmapsGraph.putEntityType("a type");
        Instance instance = mindmapsGraph.putEntity("instance", type);

        // delete a concept
        newConcepts.push(type);
        instance.delete();

        // verify the concepts that we expected are returned in the set
        modifiedConcepts = mindmapsGraph.getModifiedConcepts();
        assertTrue(modifiedConcepts.containsAll(newConcepts));

    }
}
