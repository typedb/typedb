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

package io.mindmaps.graph.internal;

import io.mindmaps.concept.Concept;
import io.mindmaps.concept.Entity;
import io.mindmaps.concept.EntityType;
import io.mindmaps.concept.Instance;
import io.mindmaps.concept.Relation;
import io.mindmaps.concept.RelationType;
import io.mindmaps.concept.Resource;
import io.mindmaps.concept.ResourceType;
import io.mindmaps.concept.RoleType;
import io.mindmaps.concept.RuleType;
import io.mindmaps.concept.Type;
import io.mindmaps.exception.ConceptException;
import io.mindmaps.exception.MoreThanOneEdgeException;
import io.mindmaps.factory.MindmapsTestGraphFactory;
import io.mindmaps.util.ErrorMessage;
import io.mindmaps.util.Schema;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ConceptTest {

    private AbstractMindmapsGraph mindmapsGraph;
    private ConceptImpl concept;

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setUp(){
        mindmapsGraph = (AbstractMindmapsGraph) MindmapsTestGraphFactory.newEmptyGraph();
        mindmapsGraph.initialiseMetaConcepts();
        concept = (ConceptImpl) mindmapsGraph.putEntityType("main_concept");
    }
    @After
    public void destroyGraphAccessManager() throws Exception {
        mindmapsGraph.close();
    }

    @Test(expected=MoreThanOneEdgeException.class)
    public void testGetEdgeOutgoingOfType(){
        ConceptImpl<?, ?> concept = (ConceptImpl<?, ?>) mindmapsGraph.putEntityType("Thing");
        assertNull(concept.getEdgeOutgoingOfType(Schema.EdgeLabel.AKO));

        TypeImpl type1 = (TypeImpl) mindmapsGraph.putEntityType("Type 1");
        TypeImpl type2 = (TypeImpl) mindmapsGraph.putEntityType("Type 2");
        TypeImpl type3 = (TypeImpl) mindmapsGraph.putEntityType("Type 3");

        assertNotNull(type1.getEdgeOutgoingOfType(Schema.EdgeLabel.ISA));

        type1.type(type2);
        Vertex vertexType1 = mindmapsGraph.getTinkerPopGraph().traversal().V(type1.getBaseIdentifier()).next();
        Vertex vertexType3 = mindmapsGraph.getTinkerPopGraph().traversal().V(type3.getBaseIdentifier()).next();
        vertexType1.addEdge(Schema.EdgeLabel.ISA.getLabel(), vertexType3);
        type1.getEdgeOutgoingOfType(Schema.EdgeLabel.ISA);
    }

    @Test
    public void testItemIdentifier() {
        assertEquals("main_concept", concept.getId());
    }

    @Test
    public void testGetVertex(){
        assertNotNull(concept.getBaseIdentifier());
    }

    @Test
    public void testSetType() {
        concept.setType("test_type");
        Vertex conceptVertex = mindmapsGraph.getTinkerPopGraph().traversal().V(concept.getBaseIdentifier()).next();
        assertEquals("test_type", conceptVertex.property(Schema.ConceptProperty.TYPE.name()).value());
    }

    @Test
    public void testGetType() {
        concept.setType("test_type");
        Vertex conceptVertex = mindmapsGraph.getTinkerPopGraph().traversal().V(concept.getBaseIdentifier()).next();
        assertEquals(concept.getType(), conceptVertex.property(Schema.ConceptProperty.TYPE.name()).value());
    }

    @Test(expected=RuntimeException.class)
    public void updateConceptFailTooManyConcepts()  {
        Vertex vertex = mindmapsGraph.getTinkerPopGraph().addVertex();
        vertex.property(Schema.ConceptProperty.ITEM_IDENTIFIER.name(), "VALUE");
        mindmapsGraph.putEntityType("VALUE");
    }

    @Test
    public void testEquality() {
        ConceptImpl c1= (ConceptImpl) mindmapsGraph.putEntityType("Value_1");
        Concept c1_copy = mindmapsGraph.getConcept("Value_1");
        Concept c1_copy_copy = mindmapsGraph.putEntityType("Value_1");

        Concept c2 = mindmapsGraph.putEntityType("Value_2");

        assertEquals(c1, c1_copy);
        assertNotEquals(c1, c2);
        assertNotEquals(c1.getBaseIdentifier(), concept.getBaseIdentifier());

        HashSet<Concept> concepts = new HashSet<>();

        concepts.add(c1);
        concepts.add(c1_copy);
        concepts.add(c1_copy_copy);
        assertEquals(1, concepts.size());

        concepts.add(c2);
        assertEquals(2, concepts.size());
        Vertex conceptVertex = mindmapsGraph.getTinkerPopGraph().traversal().V(concept.getBaseIdentifier()).next();
        assertNotEquals(concept, conceptVertex);
    }

    @Test
    public void testGetParentIsa(){
        TypeImpl conceptParent = (TypeImpl) mindmapsGraph.putEntityType("conceptParent");
        concept.type(conceptParent);
        ConceptImpl foundConcept = concept.getParentIsa();
        assertEquals(conceptParent, foundConcept);
        assertNull(concept.getParentAko());
    }

    @Test
    public void testGetParentAko(){
        TypeImpl conceptType = (TypeImpl) mindmapsGraph.putEntityType("conceptType");
        assertNull(conceptType.getParentAko());
        TypeImpl conceptParent = (TypeImpl) mindmapsGraph.putEntityType("CP");
        conceptType.superType(conceptParent);
        Concept foundConcept = conceptType.getParentAko();
        assertEquals(conceptParent, foundConcept);
        assertNull(conceptParent.getParentAko());
        assertNull(conceptType.getParentIsa());
    }

    @Test(expected = RuntimeException.class)
    public void getBaseTypeTestFail() {
        RelationType concept = mindmapsGraph.putRelationType("relType");
        mindmapsGraph.putRoleType(concept.getId());
    }

    @Test
    public void testToString() {
        EntityType concept = mindmapsGraph.putEntityType("a");
        Instance concept2 = mindmapsGraph.putEntity("concept2", concept);

        assertFalse(concept2.toString().contains("ConceptType"));
        assertFalse(concept2.toString().contains("Subject Identifier"));
        assertFalse(concept2.toString().contains("Subject Locator"));
    }

    @Test
    public void testDelete() throws ConceptException{
        assertEquals(9, mindmapsGraph.getTinkerPopGraph().traversal().V().toList().size());
        Concept c1 = mindmapsGraph.putEntityType("1");
        assertEquals(10, mindmapsGraph.getTinkerPopGraph().traversal().V().toList().size());
        c1.delete();
        assertEquals(9, mindmapsGraph.getTinkerPopGraph().traversal().V().toList().size());

        Concept c2 = mindmapsGraph.putEntityType("blab");
        assertEquals(10, mindmapsGraph.getTinkerPopGraph().traversal().V().toList().size());
        c2.delete();
        assertEquals(9, mindmapsGraph.getTinkerPopGraph().traversal().V().toList().size());
    }

    @Test(expected = ConceptException.class)
    public void testDeleteFail() throws ConceptException{
        EntityType c1 = mindmapsGraph.putEntityType("C1");
        EntityType c2 = mindmapsGraph.putEntityType("C2");
        c1.superType(c2);
        c2.delete();
    }

    @Test
    public void testGetConceptType(){
        EntityType c1 = mindmapsGraph.putEntityType("c1");
        EntityType c2 = mindmapsGraph.putEntityType("c2");
        EntityTypeImpl c3 = (EntityTypeImpl) mindmapsGraph.putEntityType("c3");
        TypeImpl c4 = (TypeImpl) mindmapsGraph.putEntityType("c4");

        c1.superType(c2);
        c2.superType(c3);
        c3.type(c4);
        assertEquals(c4, c1.type());
    }

    @Test
    public void testGetConceptTypeFakeConceptType() {
        TypeImpl c1 = (TypeImpl) mindmapsGraph.putEntityType("c1");
        c1.type(c1);
        assertEquals(c1, c1.type());
    }

    @Test
    public void testGetConceptTypeFailCycleFoundSimple(){
        TypeImpl c1 = (TypeImpl) mindmapsGraph.putEntityType("c1");

        expectedException.expect(ConceptException.class);
        expectedException.expectMessage(allOf(
                containsString(ErrorMessage.LOOP_DETECTED.getMessage(c1.toString(), Schema.EdgeLabel.AKO.getLabel() + " " + Schema.EdgeLabel.ISA.getLabel()))
        ));

        TypeImpl c2 = (TypeImpl) mindmapsGraph.putEntityType("c2");
        TypeImpl c3 = (TypeImpl) mindmapsGraph.putEntityType("c3");
        Vertex c1_Vertex = mindmapsGraph.getTinkerPopGraph().traversal().V(c1.getBaseIdentifier()).next();
        Vertex c2_Vertex = mindmapsGraph.getTinkerPopGraph().traversal().V(c2.getBaseIdentifier()).next();
        Vertex c3_Vertex = mindmapsGraph.getTinkerPopGraph().traversal().V(c3.getBaseIdentifier()).next();

        c1_Vertex.edges(Direction.BOTH).next().remove();
        c2_Vertex.edges(Direction.BOTH).next().remove();
        c3_Vertex.edges(Direction.BOTH).next().remove();

        c1_Vertex.addEdge(Schema.EdgeLabel.AKO.getLabel(), c2_Vertex);
        c2_Vertex.addEdge(Schema.EdgeLabel.AKO.getLabel(), c3_Vertex);
        c3_Vertex.addEdge(Schema.EdgeLabel.AKO.getLabel(), c1_Vertex);
        c1.type();
    }

    @Test
    public void testGetConceptTypeFailCycleFoundExtended(){
        //Checks the following case c1 -ako-> c2 -ako-> c3 -isa-> c1 is invalid
        TypeImpl c1 = (TypeImpl) mindmapsGraph.putEntityType("c1");
        TypeImpl c2 = (TypeImpl) mindmapsGraph.putEntityType("c2");
        TypeImpl c3 = (TypeImpl) mindmapsGraph.putEntityType("c3");

        c1.superType(c2);
        c2.superType(c3);
        c3.type(c1);

        expectedException.expect(ConceptException.class);
        expectedException.expectMessage(allOf(
                containsString(ErrorMessage.LOOP_DETECTED.getMessage(c1.toString(), Schema.EdgeLabel.AKO.getLabel() + " " + Schema.EdgeLabel.ISA.getLabel()))
        ));

        c1.type();
    }

    @Test
    public void testGetConceptTypeSet(){
        TypeImpl c1 = (TypeImpl) mindmapsGraph.putEntityType("c1");
        TypeImpl c2 = (TypeImpl) mindmapsGraph.putEntityType("c2");
        TypeImpl c3 = (TypeImpl) mindmapsGraph.putEntityType("c3");
        TypeImpl c4 = (TypeImpl) mindmapsGraph.putEntityType("c4");
        TypeImpl c5 = (TypeImpl) mindmapsGraph.putEntityType("c5");
        TypeImpl c6 = (TypeImpl) mindmapsGraph.putEntityType("c6");
        TypeImpl c7 = (TypeImpl) mindmapsGraph.putEntityType("c7");
        TypeImpl c8 = (TypeImpl) mindmapsGraph.putEntityType("c8");

        c1.superType(c2);
        c2.superType(c3);
        c3.type(c4);
        c4.type(c5);
        c5.superType(c6);
        c6.superType(c7);
        c7.superType(c8);
        c8.type(c8);

        Set<Type> types = c1.getConceptTypeHierarchy();

        assertEquals(3, types.size());
        assertTrue(types.contains(c4));
        assertTrue(types.contains(c5));
        assertTrue(types.contains(c8));
    }


    @Test
    public void testGetEdgesIncomingOfType(){
        EntityType entityType = mindmapsGraph.putEntityType("entity type");
        InstanceImpl conceptInstance1 = (InstanceImpl) mindmapsGraph.putEntity("ci1", entityType);
        InstanceImpl conceptInstance2 = (InstanceImpl) mindmapsGraph.putEntity("ci2", entityType);
        InstanceImpl conceptInstance3 = (InstanceImpl) mindmapsGraph.putEntity("ci3", entityType);
        InstanceImpl conceptInstance4 = (InstanceImpl) mindmapsGraph.putEntity("ci4", entityType);
        InstanceImpl conceptInstance5 = (InstanceImpl) mindmapsGraph.putEntity("ci5", entityType);
        InstanceImpl conceptInstance6 = (InstanceImpl) mindmapsGraph.putEntity("ci6", entityType);
        Vertex conceptInstance1_Vertex = mindmapsGraph.getTinkerPopGraph().traversal().V(conceptInstance1.getBaseIdentifier()).next();
        Vertex conceptInstance2_Vertex = mindmapsGraph.getTinkerPopGraph().traversal().V(conceptInstance2.getBaseIdentifier()).next();
        Vertex conceptInstance3_Vertex = mindmapsGraph.getTinkerPopGraph().traversal().V(conceptInstance3.getBaseIdentifier()).next();
        Vertex conceptInstance4_Vertex = mindmapsGraph.getTinkerPopGraph().traversal().V(conceptInstance4.getBaseIdentifier()).next();
        Vertex conceptInstance5_Vertex = mindmapsGraph.getTinkerPopGraph().traversal().V(conceptInstance5.getBaseIdentifier()).next();
        Vertex conceptInstance6_Vertex = mindmapsGraph.getTinkerPopGraph().traversal().V(conceptInstance6.getBaseIdentifier()).next();

        conceptInstance2_Vertex.addEdge(Schema.EdgeLabel.SHORTCUT.getLabel(), conceptInstance1_Vertex);
        conceptInstance3_Vertex.addEdge(Schema.EdgeLabel.SHORTCUT.getLabel(), conceptInstance1_Vertex);
        conceptInstance4_Vertex.addEdge(Schema.EdgeLabel.SHORTCUT.getLabel(), conceptInstance1_Vertex);
        conceptInstance5_Vertex.addEdge(Schema.EdgeLabel.SHORTCUT.getLabel(), conceptInstance1_Vertex);
        conceptInstance6_Vertex.addEdge(Schema.EdgeLabel.SHORTCUT.getLabel(), conceptInstance1_Vertex);

        Set<EdgeImpl> edges = conceptInstance1.getEdgesOfType(Direction.IN, Schema.EdgeLabel.SHORTCUT);

        assertEquals(5, edges.size());
    }

    @Test
    public void testAsConceptType() {
        Concept concept = mindmapsGraph.putEntityType("Test");
        assertTrue(concept.isEntityType());
        Type type = concept.asEntityType();
        assertEquals(type, concept);
    }

    @Test
    public void  testAsRoleType() {
        Concept concept = mindmapsGraph.putRoleType("Test");
        assertTrue(concept.isRoleType());
        RoleType concept2 = concept.asRoleType();
        assertEquals(concept2, concept);
    }

    @Test
    public void  testAsRelationType() {
        Concept concept = mindmapsGraph.putRelationType("Test");
        assertTrue(concept.isRelationType());
        RelationType concept2 = concept.asRelationType();
        assertEquals(concept2, concept);
    }

    @Test
    public void  testAsResourceType() {
        Concept concept = mindmapsGraph.putResourceType("Test", ResourceType.DataType.STRING);
        assertTrue(concept.isResourceType());
        ResourceType concept2 = concept.asResourceType();
        assertEquals(concept2, concept);
    }

    @Test
    public void  testAsRuleType() {
        Concept concept = mindmapsGraph.putRuleType("Test");
        assertTrue(concept.isRuleType());
        RuleType concept2 = concept.asRuleType();
        assertEquals(concept2, concept);
    }

    @Test
    public void  testAsEntity() {
        EntityType entityType = mindmapsGraph.putEntityType("entity type");
        Concept concept = mindmapsGraph.putEntity("Test", entityType);
        assertTrue(concept.isEntity());
        Instance concept2 = concept.asEntity();
        assertEquals(concept2, concept);
    }

    @Test
    public void  testAsRelation() {
        RelationType type = mindmapsGraph.putRelationType("a type");
        Concept concept = mindmapsGraph.putRelation(UUID.randomUUID().toString(), type);
        assertTrue(concept.isRelation());
        Relation concept2 = concept.asRelation();
        assertEquals(concept2, concept);
    }

    @Test
    public void  testAsResource() {
        ResourceType type = mindmapsGraph.putResourceType("a type", ResourceType.DataType.STRING);
        Concept concept = mindmapsGraph.putResource("Test", type);
        assertTrue(concept.isResource());
        Resource concept2 = concept.asResource();
        assertEquals(concept2, concept);
    }

    @Test
    public void  testAsRule() {
        RuleType type = mindmapsGraph.putRuleType("a type");
        Concept concept = mindmapsGraph.putRule("Test", "lhs", "rhs", type);
        assertTrue(concept.isRule());
        io.mindmaps.concept.Rule concept2 = concept.asRule();
        assertEquals(concept2, concept);
    }

    @Test
    public void  testAsType() {
        Concept concept = mindmapsGraph.getMetaType();
        assertTrue(concept.isType());
        Type concept2 = concept.asType();
        assertEquals(concept2, concept);
    }

    @Test
    public void  testAsInstance() {
        RuleType type = mindmapsGraph.putRuleType("a type");
        Concept concept = mindmapsGraph.putRule("Test", "lhs", "rhs", type);
        assertTrue(concept.isInstance());
        Instance concept2 = concept.asInstance();
        assertEquals(concept2, concept);
    }

    @Test
    public void incorrectConversion(){
        EntityType thingType = mindmapsGraph.putEntityType("thing type");
        Entity thing = mindmapsGraph.putEntity("thing", thingType);

        expectedException.expect(RuntimeException.class);
        expectedException.expectMessage(allOf(
                containsString(ErrorMessage.INVALID_OBJECT_TYPE.getMessage(thing.toString(), Type.class.getName()))
        ));

        thing.asType();
    }

    @Test
    public void reservedTest(){
        expectedException.expect(ConceptException.class);
        expectedException.expectMessage(allOf(
                containsString(ErrorMessage.ID_RESERVED.getMessage("concept-type"))
        ));
        mindmapsGraph.putEntityType("concept-type");
    }
}