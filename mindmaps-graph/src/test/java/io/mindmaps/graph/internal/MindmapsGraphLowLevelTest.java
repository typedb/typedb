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

import io.mindmaps.util.Schema;
import io.mindmaps.exception.MindmapsValidationException;
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
import io.mindmaps.factory.MindmapsTestGraphFactory;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.VerificationException;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.UUID;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class MindmapsGraphLowLevelTest {

    private AbstractMindmapsGraph mindmapsGraph;

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Before
    public void buildGraphAccessManager(){
        mindmapsGraph = (AbstractMindmapsGraph) MindmapsTestGraphFactory.newEmptyGraph();
        mindmapsGraph.initialiseMetaConcepts();
    }
    @After
    public void destroyGraphAccessManager()  throws Exception{
        mindmapsGraph.close();
    }

    @Test
    public void testGetGraph(){
        assertThat(mindmapsGraph.getTinkerPopGraph(), instanceOf(Graph.class));
    }

    @Test
    public void testPutConcept() throws Exception {
        int numVerticies = 14;
        for(int i = 0; i < numVerticies; i ++)
            mindmapsGraph.putEntityType("c" + i);
        assertEquals(22, mindmapsGraph.getTinkerPopGraph().traversal().V().toList().size());
    }

    //----------------------------------------------Concept Functionality-----------------------------------------------
    @Test(expected=RuntimeException.class)
    public void testTooManyNodesForId() {
        Graph graph = mindmapsGraph.getTinkerPopGraph();
        Vertex v1 = graph.addVertex();
        v1.property(Schema.ConceptPropertyUnique.ITEM_IDENTIFIER.name(), "value");
        Vertex v2 = graph.addVertex();
        v2.property(Schema.ConceptPropertyUnique.ITEM_IDENTIFIER.name(), "value");
        mindmapsGraph.putEntityType("value");
    }

    @Test
    public void testGetConceptByBaseIdentifier() throws Exception {
        assertNull(mindmapsGraph.getConceptByBaseIdentifier(1000));

        ConceptImpl c1 = (ConceptImpl) mindmapsGraph.putEntityType("c1");
        ConceptImpl c2 = mindmapsGraph.getConceptByBaseIdentifier(c1.getBaseIdentifier());
        assertEquals(c1, c2);
    }

    @Test
    public void testGetConcept() throws Exception {
        Concept c1 = mindmapsGraph.putEntityType("VALUE");
        Concept c2 = mindmapsGraph.getConcept("VALUE");
        assertEquals(c1, c2);
    }

    @Test
    public void testReadOnlyTraversal(){
        expectedException.expect(VerificationException.class);
        expectedException.expectMessage(allOf(
                containsString("not read only")
        ));

        mindmapsGraph.getTinkerTraversal().V().drop().iterate();
    }

    @Test
    public void testAddCastingLong() {
        //Build It
        RelationType relationType = mindmapsGraph.putRelationType("reltype");
        RoleTypeImpl role = (RoleTypeImpl) mindmapsGraph.putRoleType("Role");
        EntityType thing = mindmapsGraph.putEntityType("thing");
        InstanceImpl rolePlayer = (InstanceImpl) mindmapsGraph.putEntity("rolePlayer", thing);
        RelationImpl relation = (RelationImpl) mindmapsGraph.putRelation(UUID.randomUUID().toString(), relationType);
        CastingImpl casting = mindmapsGraph.putCasting(role, rolePlayer, relation);

        //Check it
        Vertex roleVertex = mindmapsGraph.getTinkerPopGraph().traversal().V(role.getBaseIdentifier()).next();
        Vertex rolePlayerVertex = mindmapsGraph.getTinkerPopGraph().traversal().V(rolePlayer.getBaseIdentifier()).next();
        Vertex assertionVertex = mindmapsGraph.getTinkerPopGraph().traversal().V(relation.getBaseIdentifier()).next();

        org.apache.tinkerpop.gremlin.structure.Edge casting_role = roleVertex.edges(Direction.IN).next();
        org.apache.tinkerpop.gremlin.structure.Edge casting_rolePlayer = rolePlayerVertex.edges(Direction.IN).next();

        assertEquals(casting.getBaseIdentifier(), casting_role.outVertex().id());
        assertEquals(casting.getBaseIdentifier(), casting_rolePlayer.outVertex().id());

        assertEquals(Schema.BaseType.ROLE_TYPE.name(), roleVertex.label());
        assertEquals(Schema.BaseType.RELATION.name(), assertionVertex.label());
    }

    @Test
    public void testAddCastingLongDuplicate(){
        RelationType relationType = mindmapsGraph.putRelationType("reltype");
        RoleTypeImpl role = (RoleTypeImpl) mindmapsGraph.putRoleType("Role");
        EntityType thing = mindmapsGraph.putEntityType("thing");
        InstanceImpl rolePlayer = (InstanceImpl) mindmapsGraph.putEntity("rolePlayer", thing);
        RelationImpl relation = (RelationImpl) mindmapsGraph.putRelation(UUID.randomUUID().toString(), relationType);
        CastingImpl casting1 = mindmapsGraph.putCasting(role, rolePlayer, relation);
        CastingImpl casting2 = mindmapsGraph.putCasting(role, rolePlayer, relation);
        assertEquals(casting1, casting2);
    }

    public void makeArtificialCasting(RoleTypeImpl role, InstanceImpl rolePlayer, RelationImpl relation) {
        String id = "FakeCasting " + UUID.randomUUID();
        Vertex vertex = mindmapsGraph.getTinkerPopGraph().addVertex(Schema.BaseType.CASTING.name());
        vertex.property(Schema.ConceptPropertyUnique.ITEM_IDENTIFIER.name(), id);
        vertex.property(Schema.ConceptPropertyUnique.INDEX.name(), CastingImpl.generateNewHash(role, rolePlayer));

        CastingImpl casting = (CastingImpl) mindmapsGraph.getConcept(id);
        EdgeImpl edge = casting.addEdge(role, Schema.EdgeLabel.ISA); // Casting to Role
        edge.setProperty(Schema.EdgeProperty.ROLE_TYPE, role.getId());
        edge = casting.addEdge(rolePlayer, Schema.EdgeLabel.ROLE_PLAYER);// Casting to Roleplayer
        edge.setProperty(Schema.EdgeProperty.ROLE_TYPE, role.getId());
        relation.addEdge(casting, Schema.EdgeLabel.CASTING);// Assertion to Casting
    }

    @Test
    public void testAddCastingLongManyCastingFound() {
        //Artificially Make First Casting
        RelationType relationType = mindmapsGraph.putRelationType("RelationType");
        RoleTypeImpl role = (RoleTypeImpl) mindmapsGraph.putRoleType("role");
        EntityType thing = mindmapsGraph.putEntityType("thing");
        InstanceImpl rolePlayer = (InstanceImpl) mindmapsGraph.putEntity("rolePlayer", thing);
        RelationImpl relation = (RelationImpl) mindmapsGraph.putRelation(UUID.randomUUID().toString(), relationType);

        //First Casting
        makeArtificialCasting(role, rolePlayer, relation);

        //Second Casting Between same entities
        makeArtificialCasting(role, rolePlayer, relation);

        expectedException.expect(RuntimeException.class);
        expectedException.expectMessage(allOf(
                containsString("More than one casting found")
        ));

        mindmapsGraph.putCasting(role, rolePlayer, relation);
    }

    @Test
    public void testExpandingCastingWithRolePlayer() {
        RelationType relationType = mindmapsGraph.putRelationType("RelationType");
        EntityType type = mindmapsGraph.putEntityType("Parent");
        RoleTypeImpl role1 = (RoleTypeImpl) mindmapsGraph.putRoleType("Role1");
        RoleTypeImpl role2 = (RoleTypeImpl) mindmapsGraph.putRoleType("Role2");

        InstanceImpl<?, ?> rolePlayer1 = (InstanceImpl) mindmapsGraph.putEntity("rolePlayer1", type);
        InstanceImpl<?, ?> rolePlayer2 = (InstanceImpl) mindmapsGraph.putEntity("rolePlayer2", type);

        RelationImpl assertion = (RelationImpl) mindmapsGraph.putRelation(UUID.randomUUID().toString(), relationType).
                putRolePlayer(role1, rolePlayer1).putRolePlayer(role2, null);
        CastingImpl casting1 = mindmapsGraph.putCasting(role1, rolePlayer1, assertion);
        CastingImpl casting2 = mindmapsGraph.putCasting(role2, rolePlayer2, assertion);

        assertTrue(assertion.getMappingCasting().contains(casting1));
        assertTrue(assertion.getMappingCasting().contains(casting2));
        assertNotEquals(casting1, casting2);

        Concept rolePlayer2Copy = rolePlayer1.getOutgoingNeighbours(Schema.EdgeLabel.SHORTCUT).iterator().next();
        Concept rolePlayer1Copy = rolePlayer2.getOutgoingNeighbours(Schema.EdgeLabel.SHORTCUT).iterator().next();

        assertEquals(rolePlayer1, rolePlayer1Copy);
        assertEquals(rolePlayer2, rolePlayer2Copy);
    }

    @Test
    public void testGetResourcesByValue(){
        assertEquals(0, mindmapsGraph.getResourcesByValue("Bob").size());
        ResourceType type = mindmapsGraph.putResourceType("Parent", ResourceType.DataType.STRING);
        ResourceType type2 = mindmapsGraph.putResourceType("Parent 2", ResourceType.DataType.STRING);

        Resource c1 = mindmapsGraph.putResource("Bob", type);
        Resource c2 = mindmapsGraph.putResource("Bob", type2);
        Resource c3 = mindmapsGraph.putResource("Bob", type);

        assertEquals(2, mindmapsGraph.getResourcesByValue("Bob").size());
        assertTrue(mindmapsGraph.getResourcesByValue("Bob").contains(c1));
        assertTrue(mindmapsGraph.getResourcesByValue("Bob").contains(c2));
        assertTrue(mindmapsGraph.getResourcesByValue("Bob").contains(c3));
        assertEquals(c1, c3);
        assertNotEquals(c1, c2);
    }

    @Test
    public void testGetConceptInstance(){
        assertNull(mindmapsGraph.getEntity("Bob"));
        EntityType type = mindmapsGraph.putEntityType("Parent");
        Instance c2 = mindmapsGraph.putEntity("Bob", type);
        assertEquals(c2, mindmapsGraph.getEntity("Bob"));
    }

    @Test
    public void testGetRelation(){
        RelationType relationType = mindmapsGraph.putRelationType("Hello");
        Relation c1 = mindmapsGraph.putRelation(UUID.randomUUID().toString(), relationType);
        assertEquals(c1, mindmapsGraph.getRelation(c1.getId()));
        assertNull(mindmapsGraph.getResourceType("BOB"));
    }

    @Test
    public void testGetConceptType(){
        assertNull(mindmapsGraph.getEntityType("Bob"));
        Type c2 = mindmapsGraph.putEntityType("Bob");
        assertEquals(c2, mindmapsGraph.getEntityType("Bob"));
    }

    @Test
    public void testGetRelationType(){
        assertNull(mindmapsGraph.getRelationType("Bob"));
        RelationType c2 = mindmapsGraph.putRelationType("Bob");
        assertEquals(c2, mindmapsGraph.getRelationType("Bob"));
    }

    @Test
    public void testGetRoleType(){
        assertNull(mindmapsGraph.getRoleType("Bob"));
        RoleType c2 = mindmapsGraph.putRoleType("Bob");
        assertEquals(c2, mindmapsGraph.getRoleType("Bob"));
    }

    @Test
    public void testGetResourceType(){
        assertNull(mindmapsGraph.getResourceType("Bob"));
        ResourceType c2 = mindmapsGraph.putResourceType("Bob", ResourceType.DataType.STRING);
        assertEquals(c2, mindmapsGraph.getResourceType("Bob"));
    }

    @Test
    public void testGetRuleType(){
        assertNull(mindmapsGraph.getRuleType("Bob"));
        RuleType c2 = mindmapsGraph.putRuleType("Bob");
        assertEquals(c2, mindmapsGraph.getRuleType("Bob"));
    }

    @Test
    public void testGetResource(){
        assertNull(mindmapsGraph.getResource("Bob"));
        ResourceType type = mindmapsGraph.putResourceType("Type", ResourceType.DataType.STRING);
        Resource c2 = mindmapsGraph.putResource("1", type);
        assertEquals(c2, mindmapsGraph.getResourcesByValue("1").iterator().next());
        assertEquals(1, mindmapsGraph.getResourcesByValue("1").size());
    }

    @Test
    public void testGetRule(){
        assertNull(mindmapsGraph.getRule("Bob"));
        RuleType type = mindmapsGraph.putRuleType("Type");
        RuleImpl c2 = (RuleImpl) mindmapsGraph.putRule("Bob", "lhs", "rhs", type);
        assertEquals(c2, mindmapsGraph.getRule("Bob"));
    }

    @Test
    public void getSuperConceptType(){
        assertEquals(mindmapsGraph.getMetaType().getId(), Schema.MetaType.TYPE.getId());
    }

    @Test
    public void getSuperRelationType(){
        assertEquals(mindmapsGraph.getMetaRelationType().getId(), Schema.MetaType.RELATION_TYPE.getId());
    }

    @Test
    public void getSuperRoleType(){
        assertEquals(mindmapsGraph.getMetaRoleType().getId(), Schema.MetaType.ROLE_TYPE.getId());
    }

    @Test
    public void getSuperResourceType(){
        assertEquals(mindmapsGraph.getMetaResourceType().getId(), Schema.MetaType.RESOURCE_TYPE.getId());
    }

    @Test
    public void testGetMetaRuleInference() {
        assertEquals(mindmapsGraph.getMetaRuleInference().getId(), Schema.MetaType.INFERENCE_RULE.getId());
    }

    @Test
    public void testGetMetaRuleConstraint() {
        assertEquals(mindmapsGraph.getMetaRuleConstraint().getId(), Schema.MetaType.CONSTRAINT_RULE.getId());
    }

    @Test
    public void testMetaOntologyInitialisation(){
        Type type = mindmapsGraph.getMetaType();
        Type relationType = mindmapsGraph.getMetaRelationType();
        Type roleType = mindmapsGraph.getMetaRoleType();
        Type resourceType = mindmapsGraph.getMetaResourceType();

        assertNotNull(type);
        assertNotNull(relationType);
        assertNotNull(roleType);
        assertNotNull(resourceType);

        assertEquals(type, relationType.superType());
        assertEquals(type, roleType.superType());
        assertEquals(type, resourceType.superType());
    }

    @Test
    public void checkTypeCreation(){
        Type testType = mindmapsGraph.putEntityType("Test Concept Type");
        ResourceType testResourceType = mindmapsGraph.putResourceType("Test Resource Type", ResourceType.DataType.STRING);
        RoleType testRoleType = mindmapsGraph.putRoleType("Test Role Type");
        RelationType testRelationType = mindmapsGraph.putRelationType("Test Relation Type");

        assertEquals(Schema.MetaType.TYPE.getId(), testType.type().type().getId());
        assertEquals(Schema.MetaType.ENTITY_TYPE.getId(), testType.type().getId());
        assertEquals(Schema.MetaType.RESOURCE_TYPE.getId(), testResourceType.type().getId());
        assertEquals(Schema.MetaType.ROLE_TYPE.getId(), testRoleType.type().getId());
        assertEquals(Schema.MetaType.RELATION_TYPE.getId(), testRelationType.type().getId());

    }

    @Test
    public void testGetType(){
        EntityType a = mindmapsGraph.putEntityType("a");
        assertEquals(a, mindmapsGraph.getType("a"));
    }

    @Test
    public void testInstance(){
        EntityType a = mindmapsGraph.putEntityType("a");
        RelationType b = mindmapsGraph.putRelationType("b");
        ResourceType<String> c = mindmapsGraph.putResourceType("c", ResourceType.DataType.STRING);

        Entity instanceA = mindmapsGraph.putEntity("instanceA", a);
        Relation instanceB = mindmapsGraph.putRelation(UUID.randomUUID().toString(), b);
        mindmapsGraph.putResource("1", c);

        assertEquals(instanceA, mindmapsGraph.getInstance("instanceA"));
    }

    @Test
    public void testComplexDelete() throws MindmapsValidationException {
        RoleType roleType1 = mindmapsGraph.putRoleType("roleType 1");
        RoleType roleType2 = mindmapsGraph.putRoleType("roleType 2");
        RoleType roleType3 = mindmapsGraph.putRoleType("roleType 3");
        RoleType roleType4 = mindmapsGraph.putRoleType("roleType 4");
        EntityType entityType = mindmapsGraph.putEntityType("entity type").
                playsRole(roleType1).playsRole(roleType2).
                playsRole(roleType3).playsRole(roleType4);
        RelationType relationType1 = mindmapsGraph.putRelationType("relation type 1").hasRole(roleType1).hasRole(roleType2);
        RelationType relationType2 = mindmapsGraph.putRelationType("relation type 2").hasRole(roleType3).hasRole(roleType4);

        Entity entity1 = mindmapsGraph.putEntity("1", entityType);
        Entity entity2 = mindmapsGraph.putEntity("2", entityType);
        Entity entity3 = mindmapsGraph.putEntity("3", entityType);
        Entity entity4 = mindmapsGraph.putEntity("4", entityType);
        Entity entity5 = mindmapsGraph.putEntity("5", entityType);

        mindmapsGraph.addRelation(relationType1).putRolePlayer(roleType1, entity1).putRolePlayer(roleType2, entity2);
        mindmapsGraph.addRelation(relationType1).putRolePlayer(roleType1, entity1).putRolePlayer(roleType2, entity3);
        mindmapsGraph.addRelation(relationType2).putRolePlayer(roleType3, entity1).putRolePlayer(roleType4, entity4);
        mindmapsGraph.addRelation(relationType2).putRolePlayer(roleType3, entity1).putRolePlayer(roleType4, entity5);

        mindmapsGraph.commit();

        entity1.delete();

        mindmapsGraph.commit();

        assertNull(mindmapsGraph.getConcept("1"));
    }
}