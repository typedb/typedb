package io.mindmaps.core.implementation;

import io.mindmaps.core.exceptions.ConceptException;
import io.mindmaps.core.exceptions.ErrorMessage;
import io.mindmaps.core.model.*;
import io.mindmaps.factory.MindmapsTestGraphFactory;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.*;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.*;

public class RelationTest {

    private MindmapsTransactionImpl mindmapsGraph;

    private RelationImpl relation;
    private RoleTypeImpl role1;
    private InstanceImpl rolePlayer1;
    private RoleTypeImpl role2;
    private InstanceImpl rolePlayer2;
    private RoleTypeImpl role3;
    private InstanceImpl rolePlayer3;
    private EntityType type;
    private RelationType relationType;

    private CastingImpl casting1;
    private CastingImpl casting2;

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Before
    public void buildGraph(){
        mindmapsGraph = (MindmapsTransactionImpl) MindmapsTestGraphFactory.newEmptyGraph().newTransaction();
        mindmapsGraph.initialiseMetaConcepts();

        type = mindmapsGraph.putEntityType("Main concept Type");
        relationType = mindmapsGraph.putRelationType("Main relation type");

        relation = (RelationImpl) mindmapsGraph.putRelation("A relation", relationType);
        role1 = (RoleTypeImpl) mindmapsGraph.putRoleType("Role 1");
        rolePlayer1 = (InstanceImpl) mindmapsGraph.putEntity("Main 1", type);
        role2 = (RoleTypeImpl) mindmapsGraph.putRoleType("Role 2");
        rolePlayer2 = (InstanceImpl) mindmapsGraph.putEntity("Main 2", type);
        role3 = (RoleTypeImpl) mindmapsGraph.putRoleType("Role 3");
        rolePlayer3 = null;

        casting1 = mindmapsGraph.putCasting(role1, rolePlayer1, relation);
        casting2 = mindmapsGraph.putCasting(role2, rolePlayer2, relation);
    }
    @After
    public void destroyGraph()  throws Exception{
        mindmapsGraph.close();
    }

    @Test
    public void simplePutRelation(){
        Relation rel = mindmapsGraph.putRelation("id", relationType);
        assertEquals(rel, mindmapsGraph.getRelation("id"));
        assertNull(mindmapsGraph.getRelation("id2"));
    }

    @Test
    public void testPutRolePlayer(){
        Relation relation = mindmapsGraph.putRelation(UUID.randomUUID().toString(), relationType);
        RoleType roleType = mindmapsGraph.putRoleType("A role");
        Instance instance = mindmapsGraph.putEntity("a instance", type);

        relation.putRolePlayer(roleType, instance);
        assertEquals(1, relation.rolePlayers().size());
        assertTrue(relation.rolePlayers().keySet().contains(roleType));
        assertTrue(relation.rolePlayers().values().contains(instance));
    }

    @Test
    public void scopeTest(){
        RelationType relationType = mindmapsGraph.putRelationType("rel type");
        RelationImpl relation = (RelationImpl) mindmapsGraph.putRelation(UUID.randomUUID().toString(), relationType);
        RelationImpl relationValue = (RelationImpl) mindmapsGraph.putRelation(UUID.randomUUID().toString(), relationType);
        InstanceImpl scope = (InstanceImpl) mindmapsGraph.putEntity("scope", type);
        relation.scope(scope);
        relationValue.scope(scope);

        Vertex vertex = mindmapsGraph.getTinkerPopGraph().traversal().V(relation.getBaseIdentifier()).out(DataType.EdgeLabel.HAS_SCOPE.getLabel()).next();
        assertEquals(scope.getBaseIdentifier(), vertex.id());

        vertex = mindmapsGraph.getTinkerPopGraph().traversal().V(relationValue.getBaseIdentifier()).out(DataType.EdgeLabel.HAS_SCOPE.getLabel()).next();
        assertEquals(scope.getBaseIdentifier(), vertex.id());
    }

    @Test
    public void testGetCasting() throws Exception {
        Set<CastingImpl> castings = relation.getMappingCasting();
        assertEquals(2, castings.size());
        assertTrue(castings.contains(casting1));
        assertTrue(castings.contains(casting2));
    }

    @Test
    public void testGetRoleAndRolePlayers() throws Exception {
        Map<RoleType, Instance> roleMap = relation.rolePlayers();
        assertEquals(2, roleMap.size());
        assertTrue(roleMap.keySet().contains(role1));
        assertTrue(roleMap.keySet().contains(role2));

        assertEquals(rolePlayer1, roleMap.get(role1));
        assertEquals(rolePlayer2, roleMap.get(role2));
        assertEquals(rolePlayer3, roleMap.get(role3));
    }

    @Test
    public void testGetScope(){
        assertEquals(0, relation.scopes().size());
        Instance scope1 = mindmapsGraph.putEntity("s1", type);
        Instance scope2 = mindmapsGraph.putEntity("s2", type);
        relation.scope(scope1);
        relation.scope(scope2);
        Collection<Instance> scopes = relation.scopes();
        assertEquals(2, scopes.size());
        assertTrue(scopes.contains(scope1));
        assertTrue(scopes.contains(scope2));
    }

    @Test
    public void testDeleteScope() throws ConceptException {
        RelationType relationType = mindmapsGraph.putRelationType("Relation type");
        RelationImpl relation2 = (RelationImpl) mindmapsGraph.putRelation(UUID.randomUUID().toString(), relationType);
        InstanceImpl scope1 = (InstanceImpl) mindmapsGraph.putEntity("s1", type);
        Instance scope2 = mindmapsGraph.putEntity("s2", type);
        InstanceImpl scope3 = (InstanceImpl) mindmapsGraph.putEntity("s3", type);
        relation2.scope(scope3);
        relation.scope(scope3);
        relation.scope(scope1);
        relation2.scope(scope1);
        relation2.scope(scope2);

        relation2.deleteScope(scope2);
        Vertex assertion2_vertex = mindmapsGraph.getTinkerPopGraph().traversal().V(relation2.getBaseIdentifier()).next();

        int count = 0;
        Iterator<Edge> edges =  assertion2_vertex.edges(Direction.OUT, DataType.EdgeLabel.HAS_SCOPE.getLabel());
        while(edges.hasNext()){
            edges.next();
            count ++;
        }
        assertEquals(2, count);

        relation2.deleteScope(scope3);
        assertTrue(mindmapsGraph.getTinkerPopGraph().traversal().V(scope3.getBaseIdentifier()).hasNext());

        relation.deleteScope(scope1);
        assertTrue(mindmapsGraph.getTinkerPopGraph().traversal().V(scope1.getBaseIdentifier()).hasNext());
        relation2.deleteScope(scope1);
        relation.deleteScope(scope3);
        assertFalse(mindmapsGraph.getTinkerPopGraph().traversal().V(relation.getBaseIdentifier()).next().edges(Direction.OUT, DataType.EdgeLabel.HAS_SCOPE.getLabel()).hasNext());
    }

    @Test
    public void testDeleteAllScopes() throws ConceptException {
        Instance scope2 = mindmapsGraph.putEntity("s2", type);
        Instance scope1 = mindmapsGraph.putEntity("s1", type);
        relation.scope(scope1);
        relation.scope(scope2);

        relation.scopes().forEach(relation::deleteScope);
        assertFalse(mindmapsGraph.getTinkerPopGraph().traversal().V(relation.getBaseIdentifier()).next().edges(Direction.OUT, DataType.EdgeLabel.HAS_SCOPE.getLabel()).hasNext());
    }

    @Test
    public void testDelete() throws ConceptException{
        relation.delete();
        assertNull(mindmapsGraph.getConceptByBaseIdentifier(relation.getBaseIdentifier()));
    }

    @Test
    public void testDeleteShortcuts() {
        EntityType type = mindmapsGraph.putEntityType("A thing");
        ResourceType resourceType = mindmapsGraph.putResourceType("A resource thing", Data.STRING);
        EntityImpl instance1 = (EntityImpl) mindmapsGraph.putEntity("Instance 1", type);
        EntityImpl instance2 = (EntityImpl) mindmapsGraph.putEntity("Instance 2", type);
        EntityImpl instance3 = (EntityImpl) mindmapsGraph.putEntity("Instance 3", type);
        ResourceImpl resource = (ResourceImpl) mindmapsGraph.putResource("Resource 1", resourceType);
        RoleType roleType1 = mindmapsGraph.putRoleType("Role 1");
        RoleType roleType2 = mindmapsGraph.putRoleType("Role 2");
        RoleType roleType3 = mindmapsGraph.putRoleType("Role 3");
        RelationType relationType = mindmapsGraph.putRelationType("Relation Type");

        Relation rel = mindmapsGraph.putRelation(UUID.randomUUID().toString(), relationType).
                putRolePlayer(roleType1, instance1).putRolePlayer(roleType2, instance2).putRolePlayer(roleType3, resource);

        Relation rel2 = mindmapsGraph.addRelation(relationType).putRolePlayer(roleType1, instance1).putRolePlayer(roleType2, instance3);

        assertEquals(1, instance1.resources().size());
        assertEquals(2, resource.ownerInstances().size());

        assertEquals(3, instance1.getEdgesOfType(Direction.IN, DataType.EdgeLabel.SHORTCUT).size());
        assertEquals(3, instance1.getEdgesOfType(Direction.OUT, DataType.EdgeLabel.SHORTCUT).size());

        rel.delete();

        assertEquals(0, instance1.resources().size());
        assertEquals(0, resource.ownerInstances().size());

        assertEquals(1, instance1.getEdgesOfType(Direction.IN, DataType.EdgeLabel.SHORTCUT).size());
        assertEquals(1, instance1.getEdgesOfType(Direction.OUT, DataType.EdgeLabel.SHORTCUT).size());
    }

    @Test
    public void testRelationHash(){
        TreeMap<RoleType, Instance> roleMap = new TreeMap<>();
        EntityType type = mindmapsGraph.putEntityType("concept type");
        RoleType roleType1 = mindmapsGraph.putRoleType("role type 1");
        RoleType roleType2 = mindmapsGraph.putRoleType("role type 2");
        RelationType relationType = mindmapsGraph.putRelationType("relation type").hasRole(roleType1).hasRole(roleType2);
        Instance instance1 = mindmapsGraph.putEntity("instance1", type);
        Instance instance2 = mindmapsGraph.putEntity("instance2", type);

        RelationImpl relation = (RelationImpl) mindmapsGraph.putRelation(UUID.randomUUID().toString(), relationType);
        assertTrue(relation.getIndex().startsWith("RelationBaseId_" + ((RelationImpl) relation).getBaseIdentifier()));

        relation.putRolePlayer(roleType1, instance1);
        roleMap.put(roleType1, instance1);
        roleMap.put(roleType2, null);
        assertEquals(getFakeId(relation.type(), roleMap), relation.getIndex());

        relation.putRolePlayer(roleType2, null);
        roleMap.put(roleType2, null);
        assertEquals(getFakeId(relation.type(), roleMap), relation.getIndex());

        relation.putRolePlayer(roleType2, instance2);
        roleMap.put(roleType2, instance2);
        assertEquals(getFakeId(relation.type(), roleMap), relation.getIndex());
    }
    private String getFakeId(RelationType relationType, TreeMap<RoleType, Instance> roleMap){
        String itemIdentifier = "RelationType_" + relationType.getId() + "_Relation";
        for(Map.Entry<RoleType, Instance> entry: roleMap.entrySet()){
            itemIdentifier = itemIdentifier + "_" + entry.getKey().getId();
            if(entry.getValue() != null)
                itemIdentifier = itemIdentifier + "_" + entry.getValue().getId();
        }
        return itemIdentifier;
    }

    @Test
    public void testCreatingRelationsWithSameRolePlayersButDifferentIds(){
        RoleType roleType1 = mindmapsGraph.putRoleType("role type 1");
        RoleType roleType2 = mindmapsGraph.putRoleType("role type 2");
        EntityType type = mindmapsGraph.putEntityType("concept type").playsRole(roleType1).playsRole(roleType2);
        Instance instance1 = mindmapsGraph.putEntity("instance1", type);
        Instance instance2 = mindmapsGraph.putEntity("instance2", type);
        RelationType relationType = mindmapsGraph.putRelationType("relation type").hasRole(roleType1).hasRole(roleType2);

        Relation relation1 = mindmapsGraph.putRelation("abc", relationType).putRolePlayer(roleType1, instance1).putRolePlayer(roleType2, instance2);

        expectedException.expect(ConceptException.class);
        expectedException.expectMessage(allOf(
                containsString(ErrorMessage.RELATION_EXISTS.getMessage(relation1))
        ));

        mindmapsGraph.putRelation("def", relationType).putRolePlayer(roleType1, instance1).putRolePlayer(roleType2, instance2);
    }

    @Test
    public void testCreatingRelationsWithSameRolePlayersButDifferentIds2(){
        RoleType roleType1 = mindmapsGraph.putRoleType("role type 1");
        RoleType roleType2 = mindmapsGraph.putRoleType("role type 2");
        EntityType type = mindmapsGraph.putEntityType("concept type").playsRole(roleType1).playsRole(roleType2);
        Instance instance1 = mindmapsGraph.putEntity("instance1", type);
        Instance instance2 = mindmapsGraph.putEntity("instance2", type);
        RelationType relationType = mindmapsGraph.putRelationType("relation type").hasRole(roleType1).hasRole(roleType2);

        Relation relation1 = mindmapsGraph.putRelation("abc", relationType).putRolePlayer(roleType1, instance1).putRolePlayer(roleType2, instance2);
        Relation relation2 = mindmapsGraph.putRelation("def", relationType).putRolePlayer(roleType1, instance1);

        expectedException.expect(ConceptException.class);
        expectedException.expectMessage(allOf(
                containsString(ErrorMessage.RELATION_EXISTS.getMessage(relation1))
        ));

        relation2.putRolePlayer(roleType2, instance2);
    }

    @Test
    public void testGetRoleMapWithMissingInstance(){
        RoleType roleType1 = mindmapsGraph.putRoleType("role type 1");
        RoleType roleType2 = mindmapsGraph.putRoleType("role type 2");
        EntityType type = mindmapsGraph.putEntityType("concept type").playsRole(roleType1).playsRole(roleType2);
        RelationType relationType1 = mindmapsGraph.putRelationType("Another relation type").hasRole(roleType1).hasRole(roleType2);
        Instance instance1 = mindmapsGraph.putEntity("instance1", type);

        Relation relation1 = mindmapsGraph.putRelation("abc", relationType1).putRolePlayer(roleType1, instance1).putRolePlayer(roleType2, null);

        Map<RoleType, Instance> roleTypeInstanceMap = new HashMap<>();
        roleTypeInstanceMap.put(roleType1, instance1);
        roleTypeInstanceMap.put(roleType2, null);

        assertEquals(roleTypeInstanceMap, relation1.rolePlayers());
    }
}