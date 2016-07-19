package io.mindmaps.core.implementation;

import io.mindmaps.core.model.Type;
import io.mindmaps.factory.MindmapsTestGraphFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class EdgeTest {

    private MindmapsTransactionImpl mindmapsGraph;

    @Before
    public void setUp(){
        mindmapsGraph = (MindmapsTransactionImpl) MindmapsTestGraphFactory.newEmptyGraph().newTransaction();
    }
    @After
    public void destroyGraphAccessManager() throws Exception {
        mindmapsGraph.close();
    }

    @Test
    public void equalityTest() {
        Type c1 = mindmapsGraph.putEntityType("1");
        Type c2 = mindmapsGraph.putEntityType("2");
        mindmapsGraph.putEntityType("3");

        EdgeImpl l1 = createType(c1, c2);
        EdgeImpl l1_copy = new EdgeImpl(mindmapsGraph.getTinkerPopGraph().traversal().V().
                has(DataType.ConceptPropertyUnique.ITEM_IDENTIFIER.name(), c1.getId()).
                outE(DataType.EdgeLabel.ISA.getLabel()).next(), mindmapsGraph);
        EdgeImpl l2 = new EdgeImpl(mindmapsGraph.getTinkerPopGraph().traversal().V().
                has(DataType.ConceptPropertyUnique.ITEM_IDENTIFIER.name(), c1.getId()).
                outE(DataType.EdgeLabel.ISA.getLabel()).next(), mindmapsGraph);
        EdgeImpl l3 = createType(c2, c1);

        assertEquals(l1, l1_copy);
        assertEquals(l1, l2);
        assertNotEquals(l1, l3);
        assertNotEquals(l1, mindmapsGraph);
    }

    private EdgeImpl createType(Type c1, Type c2){
        ((ConceptImpl)c1).type(c2);
        return new EdgeImpl(mindmapsGraph.getTinkerPopGraph().traversal().V().
                has(DataType.ConceptPropertyUnique.ITEM_IDENTIFIER.name(), c1.getId()).
                outE(DataType.EdgeLabel.ISA.getLabel()).next(), mindmapsGraph);
    }

    @Test
    public void hashCodeTest() {
        Type c1 = mindmapsGraph.putEntityType("1");
        Type c2 = mindmapsGraph.putEntityType("2");
        EdgeImpl l1 = createType(c1, c2);
        org.apache.tinkerpop.gremlin.structure.Edge l1edge = mindmapsGraph.getTinkerPopGraph().traversal().E(l1.getId()).next();
        assertEquals(l1.hashCode(), l1edge.hashCode());
    }

    @Test
    public void testGetFromAndToVertices(){
        Type c1 = mindmapsGraph.putEntityType("1");
        Type c2 = mindmapsGraph.putEntityType("2");
        EdgeImpl edge = createType(c1, c2);

        assertEquals(c1, edge.getFromConcept());
        assertEquals(c2, edge.getToConcept());
        assertNotEquals(edge.getFromConcept(), edge.getToConcept());
    }

    @Test
    public void testGetLabelFromEdge(){
        Type c1 = mindmapsGraph.putEntityType("1");
        Type c2 = mindmapsGraph.putEntityType("2");
        EdgeImpl edge = createType(c1, c2);

        assertEquals(DataType.EdgeLabel.ISA, edge.getType());
    }

    @Test
    public void testEdgeProperties(){
        Type c1 = mindmapsGraph.putEntityType("1");
        Type c2 = mindmapsGraph.putEntityType("2");
        EdgeImpl edge = createType(c1, c2);
        assertNull(edge.getEdgePropertyRoleType());
        edge.setEdgePropertyRoleType("test");
        assertEquals("test", edge.getEdgePropertyRoleType());
    }

    @Test
    public void testEdgePropertyToId(){
        Type c1 = mindmapsGraph.putEntityType("1");
        Type c2 = mindmapsGraph.putEntityType("2");
        EdgeImpl edge = createType(c1, c2);

        edge.setEdgePropertyToId("Test");
        assertEquals("Test", edge.getEdgePropertyToId());
    }

    @Test
    public void testEdgePropertyToRole(){
        Type c1 = mindmapsGraph.putEntityType("1");
        Type c2 = mindmapsGraph.putEntityType("2");
        EdgeImpl edge = createType(c1, c2);

        edge.setEdgePropertyToRole("Test");
        assertEquals("Test", edge.getEdgePropertyToRole());
    }

    @Test
    public void testEdgePropertyToType(){
        Type c1 = mindmapsGraph.putEntityType("1");
        Type c2 = mindmapsGraph.putEntityType("2");
        EdgeImpl edge = createType(c1, c2);

        edge.setEdgePropertyToType("Test");
        assertEquals("Test", edge.getEdgePropertyToType());

    }

    @Test
    public void testEdgePropertyFromId(){
        Type c1 = mindmapsGraph.putEntityType("1");
        Type c2 = mindmapsGraph.putEntityType("2");
        EdgeImpl edge = createType(c1, c2);

        edge.setEdgePropertyFromId("Test");
        assertEquals("Test", edge.getEdgePropertyFromId());

    }

    @Test
    public void testEdgePropertyFromRole(){
        Type c1 = mindmapsGraph.putEntityType("1");
        Type c2 = mindmapsGraph.putEntityType("2");
        EdgeImpl edge = createType(c1, c2);

        edge.setEdgePropertyFromRole("Test");
        assertEquals("Test", edge.getEdgePropertyFromRole());

    }

    @Test
    public void testEdgePropertyFromType(){
        Type c1 = mindmapsGraph.putEntityType("1");
        Type c2 = mindmapsGraph.putEntityType("2");
        EdgeImpl edge = createType(c1, c2);

        edge.setEdgePropertyFromType("Test");
        assertEquals("Test", edge.getEdgePropertyFromType());

    }

    @Test
    public void testEdgePropertyRelationId(){
        Type c1 = mindmapsGraph.putEntityType("1");
        Type c2 = mindmapsGraph.putEntityType("2");
        EdgeImpl edge = createType(c1, c2);

        edge.setEdgePropertyRelationId("Test");
        assertEquals("Test", edge.getEdgePropertyRelationId());

    }

    @Test
    public void testEdgePropertyBaseAssertionId(){
        Type c1 = mindmapsGraph.putEntityType("1");
        Type c2 = mindmapsGraph.putEntityType("2");
        EdgeImpl edge = createType(c1, c2);

        edge.setEdgePropertyBaseAssertionId(1L);
        assertTrue(1L == edge.getEdgePropertyBaseAssertionId());
    }

    @Test
    public void testEdgePropertyShortcutHash(){
        Type c1 = mindmapsGraph.putEntityType("1");
        Type c2 = mindmapsGraph.putEntityType("2");
        EdgeImpl edge = createType(c1, c2);
        edge.setEdgePropertyShortcutHash("Test");
        assertEquals("Test", edge.getEdgePropertyShortcutHash());
    }

    @Test
    public void testEdgePropertyValue(){
        Type c1 = mindmapsGraph.putEntityType("1");
        Type c2 = mindmapsGraph.putEntityType("2");
        EdgeImpl edge = createType(c1, c2);
        edge.setEdgePropertyValue("Test");
        assertEquals("Test", edge.getEdgePropertyValue());

    }
}