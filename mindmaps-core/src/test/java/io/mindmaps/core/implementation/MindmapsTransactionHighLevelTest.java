package io.mindmaps.core.implementation;

import io.mindmaps.core.exceptions.ErrorMessage;
import io.mindmaps.core.model.*;
import io.mindmaps.factory.MindmapsTestGraphFactory;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.*;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

public class MindmapsTransactionHighLevelTest {

    private MindmapsTransactionImpl mindmapsGraph;
    EntityType type;
    RelationTypeImpl relationType;
    RoleTypeImpl role1;
    RoleTypeImpl role2;
    RoleTypeImpl role3;
    InstanceImpl rolePlayer1;
    InstanceImpl rolePlayer2;
    InstanceImpl rolePlayer3;

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Before
    public void buildGraphAccessManager(){
        mindmapsGraph = (MindmapsTransactionImpl) MindmapsTestGraphFactory.newEmptyGraph().newTransaction();
        mindmapsGraph.initialiseMetaConcepts();

        type = mindmapsGraph.putEntityType("Test");
        relationType = (RelationTypeImpl) mindmapsGraph.putRelationType("relationType");
        role1 = (RoleTypeImpl) mindmapsGraph.putRoleType("role1");
        role2 = (RoleTypeImpl) mindmapsGraph.putRoleType("role2");
        role3 = (RoleTypeImpl) mindmapsGraph.putRoleType("role3");
        rolePlayer1 = (InstanceImpl) mindmapsGraph.putEntity("role-player1", type);
        rolePlayer2 = (InstanceImpl) mindmapsGraph.putEntity("role-player2", type);
        rolePlayer3 = (InstanceImpl) mindmapsGraph.putEntity("role-player3", type);
    }
    @After
    public void destroyGraphAccessManager()  throws Exception{
        mindmapsGraph.close();
    }

    //---------------------------------------------Higher Level Functionality-------------------------------------------
    //------------------------------------ putRelation
    @Test
    public void testAddComplexRelationshipSimple(){
        HashSet<Object> validVertices = new HashSet<>();
        validVertices.add(role1.getBaseIdentifier());
        validVertices.add(role2.getBaseIdentifier());
        validVertices.add(role3.getBaseIdentifier());

        relationType.hasRole(role1);
        relationType.hasRole(role2);
        relationType.hasRole(role3);
        RelationImpl assertion = (RelationImpl) mindmapsGraph.putRelation("a thing", relationType).
                putRolePlayer(role1, rolePlayer1).
                putRolePlayer(role2, rolePlayer2).
                putRolePlayer(role3, rolePlayer3);

        //Checking it
        //Counts
        assertEquals(20, mindmapsGraph.getTinkerPopGraph().traversal().V().toList().size());
        assertEquals(35, mindmapsGraph.getTinkerPopGraph().traversal().E().toList().size());

        assertion.getMappingCasting().forEach(casting ->{
            Edge edge = mindmapsGraph.getTinkerPopGraph().traversal().V(casting.getBaseIdentifier()).inE(DataType.EdgeLabel.CASTING.getLabel()).next();
            assertEquals(casting.getRole().getId(), edge.property(DataType.EdgeProperty.ROLE_TYPE.name()).value().toString());
            edge = mindmapsGraph.getTinkerPopGraph().traversal().V(casting.getBaseIdentifier()).outE(DataType.EdgeLabel.ROLE_PLAYER.getLabel()).next();
            assertEquals(casting.getRole().getId(), edge.property(DataType.EdgeProperty.ROLE_TYPE.name()).value().toString());
        });

        //First Check if Roles are set correctly.
        GraphTraversal<Vertex, Vertex> traversal = mindmapsGraph.getTinkerPopGraph().traversal().V(relationType.getBaseIdentifier()).out(DataType.EdgeLabel.HAS_ROLE.getLabel());
        List<Vertex> relationType_to_roles = traversal.toList();

        boolean rolesCorrect = true;
        if(relationType_to_roles.isEmpty())
            rolesCorrect = false;
        for(Vertex v: relationType_to_roles){
            if(!validVertices.contains(v.id())){
                rolesCorrect = false;
            }
        }
        assertTrue(rolesCorrect);

        //Check Roles and Role Players lead to castings
        Vertex casting1 = mindmapsGraph.getTinkerPopGraph().traversal().V(role1.getBaseIdentifier()).in(DataType.EdgeLabel.ISA.getLabel()).next();
        Vertex casting1_copy = mindmapsGraph.getTinkerPopGraph().traversal().V(rolePlayer1.getBaseIdentifier()).in(DataType.EdgeLabel.ROLE_PLAYER.getLabel()).next();
        assertEquals(casting1, casting1_copy);

        Vertex casting2 = mindmapsGraph.getTinkerPopGraph().traversal().V(role2.getBaseIdentifier()).in(DataType.EdgeLabel.ISA.getLabel()).next();
        Vertex casting2_copy = mindmapsGraph.getTinkerPopGraph().traversal().V(rolePlayer2.getBaseIdentifier()).in(DataType.EdgeLabel.ROLE_PLAYER.getLabel()).next();
        assertEquals(casting2, casting2_copy);

        Vertex casting3 = mindmapsGraph.getTinkerPopGraph().traversal().V(role3.getBaseIdentifier()).in(DataType.EdgeLabel.ISA.getLabel()).next();
        Vertex casting3_copy = mindmapsGraph.getTinkerPopGraph().traversal().V(rolePlayer3.getBaseIdentifier()).in(DataType.EdgeLabel.ROLE_PLAYER.getLabel()).next();
        assertEquals(casting3, casting3_copy);

        assertNotEquals(casting1, casting2);
        assertNotEquals(casting1, casting3);

        //Check all castings go to the same assertion and check assertion
        Vertex assertion1 = mindmapsGraph.getTinkerPopGraph().traversal().V(casting1.id()).in(DataType.EdgeLabel.CASTING.getLabel()).next();
        Vertex assertion2 = mindmapsGraph.getTinkerPopGraph().traversal().V(casting2.id()).in(DataType.EdgeLabel.CASTING.getLabel()).next();
        Vertex assertion3 = mindmapsGraph.getTinkerPopGraph().traversal().V(casting3.id()).in(DataType.EdgeLabel.CASTING.getLabel()).next();

        assertEquals(assertion1, assertion2);
        assertEquals(assertion2, assertion3);

        Iterator<Edge> edges = assertion1.edges(Direction.OUT, DataType.EdgeLabel.ISA.getLabel());
        assertTrue(edges.hasNext());
        edges.next();
        assertFalse(edges.hasNext());

        edges = assertion1.edges(Direction.OUT, DataType.EdgeLabel.CASTING.getLabel());
        int count = 0;
        while (edges.hasNext()){
            edges.next();
            count ++;
        }
        assertEquals(3, count);
    }

    @Test
    public void testAddComplexRelationshipMissingRolePlayer(){
        rolePlayer1.type(type);
        rolePlayer2.type(type);

        HashSet<Object> validVertices = new HashSet<>();
        validVertices.add(role1.getBaseIdentifier());
        validVertices.add(role2.getBaseIdentifier());
        validVertices.add(role3.getBaseIdentifier());

        relationType.hasRole(role1);
        relationType.hasRole(role2);
        relationType.hasRole(role3);
        RelationImpl relationConcept1 = (RelationImpl) mindmapsGraph.putRelation("a", relationType).
                putRolePlayer(role1, rolePlayer1).putRolePlayer(role2, rolePlayer2).putRolePlayer(role3, null);

        //Checking it
        //Counts
        long value = mindmapsGraph.getTinkerPopGraph().traversal().V().count().next();
        assertEquals(19, value);
        value = mindmapsGraph.getTinkerPopGraph().traversal().E().count().next();
        assertEquals(28, value);

        //First Check if Roles are set correctly.
        GraphTraversal<Vertex, Vertex> traversal = mindmapsGraph.getTinkerPopGraph().traversal().V(relationType.getBaseIdentifier()).out(DataType.EdgeLabel.HAS_ROLE.getLabel());
        List<Vertex> relationType_to_roles = traversal.toList();

        boolean rolesCorrect = true;
        if(relationType_to_roles.isEmpty())
            rolesCorrect = false;
        for(Vertex v: relationType_to_roles){
            if(!validVertices.contains(v.id())){
                rolesCorrect = false;
            }
        }
        assertTrue(rolesCorrect);

        //Check Roles and Role Players lead to castings
        Vertex casting1 = mindmapsGraph.getTinkerPopGraph().traversal().V(role1.getBaseIdentifier()).in(DataType.EdgeLabel.ISA.getLabel()).next();
        Vertex casting1_copy = mindmapsGraph.getTinkerPopGraph().traversal().V(rolePlayer1.getBaseIdentifier()).in(DataType.EdgeLabel.ROLE_PLAYER.getLabel()).next();
        assertEquals(casting1, casting1_copy);

        Vertex casting2 = mindmapsGraph.getTinkerPopGraph().traversal().V(role2.getBaseIdentifier()).in(DataType.EdgeLabel.ISA.getLabel()).next();
        Vertex casting2_copy = mindmapsGraph.getTinkerPopGraph().traversal().V(rolePlayer2.getBaseIdentifier()).in(DataType.EdgeLabel.ROLE_PLAYER.getLabel()).next();
        assertEquals(casting2, casting2_copy);

        assertNotEquals(casting1, casting2);

        //Check all castings go to the same assertion
        Vertex assertion1 = mindmapsGraph.getTinkerPopGraph().traversal().V(casting1.id()).in(DataType.EdgeLabel.CASTING.getLabel()).next();
        Vertex assertion2 = mindmapsGraph.getTinkerPopGraph().traversal().V(casting2.id()).in(DataType.EdgeLabel.CASTING.getLabel()).next();

        assertEquals(assertion1, assertion2);
        assertEquals(relationConcept1.getBaseIdentifier(), assertion1.id());

        Iterator<Edge> edges = assertion1.edges(Direction.OUT, DataType.EdgeLabel.ISA.getLabel());
        assertTrue(edges.hasNext());
        edges.next();
        assertFalse(edges.hasNext());

        edges = assertion1.edges(Direction.OUT, DataType.EdgeLabel.CASTING.getLabel());
        int count = 0;
        while (edges.hasNext()){
            edges.next();
            count ++;
        }
        assertEquals(2, count);
    }

    @Test
    public void testAddComplexRelationshipNullRole(){
        expectedException.expect(RuntimeException.class);
        expectedException.expectMessage(allOf(
                containsString(ErrorMessage.ROLE_IS_NULL.getMessage(rolePlayer1))
        ));

        mindmapsGraph.putRelation(UUID.randomUUID().toString(), relationType).putRolePlayer(null, rolePlayer1);
    }

    @Test
    public void getRelationTest(){
        rolePlayer1.type(type);
        rolePlayer2.type(type);

        Map<RoleType, Instance> roleMap = new HashMap<>();
        roleMap.put(role1, rolePlayer1);
        roleMap.put(role2, rolePlayer2);

        RelationImpl assertion = (RelationImpl) mindmapsGraph.addRelation(relationType).
                putRolePlayer(role1, rolePlayer1).putRolePlayer(role2, rolePlayer2);
        Relation relationFound = mindmapsGraph.getRelation(relationType, roleMap);
        assertEquals(assertion, relationFound);

        assertion.getMappingCasting().forEach(casting -> {
            Edge edge = mindmapsGraph.getTinkerPopGraph().traversal().V(casting.getBaseIdentifier()).inE(DataType.EdgeLabel.CASTING.getLabel()).next();
            assertEquals(casting.getRole().getId(), edge.property(DataType.EdgeProperty.ROLE_TYPE.name()).value().toString());
            edge = mindmapsGraph.getTinkerPopGraph().traversal().V(casting.getBaseIdentifier()).outE(DataType.EdgeLabel.ROLE_PLAYER.getLabel()).next();
            assertEquals(casting.getRole().getId(), edge.property(DataType.EdgeProperty.ROLE_TYPE.name()).value().toString());
        });

        RelationImpl relationFound2 = (RelationImpl) mindmapsGraph.getRelation(relationType, roleMap);
        assertEquals(DataType.BaseType.RELATION.name(), relationFound2.getBaseType());
        assertThat(relationFound2, instanceOf(Relation.class));
    }

    @Test
    public void testAddLargeAndMultipleRelationships(){

        //Actual Concepts To Appear Linked In Graph
        Type relationType = mindmapsGraph.putEntityType("Relation Type");
        RelationTypeImpl cast = (RelationTypeImpl) mindmapsGraph.putRelationType("Cast");
        EntityType roleType = mindmapsGraph.putEntityType("Role Type");
        EntityType type = mindmapsGraph.putEntityType("Concept Type");
        RoleTypeImpl feature = (RoleTypeImpl) mindmapsGraph.putRoleType("Feature");
        RoleTypeImpl actor = (RoleTypeImpl) mindmapsGraph.putRoleType("Actor");
        EntityType movie = mindmapsGraph.putEntityType("Movie");
        EntityType person = mindmapsGraph.putEntityType("Person");
        InstanceImpl pacino = (InstanceImpl) mindmapsGraph.putEntity("Pacino", person);
        InstanceImpl godfather = (InstanceImpl) mindmapsGraph.putEntity("Godfather", movie);
        EntityType genre = mindmapsGraph.putEntityType("Genre");
        RoleTypeImpl movieOfGenre = (RoleTypeImpl) mindmapsGraph.putRoleType("Movie of Genre");
        RoleTypeImpl movieGenre = (RoleTypeImpl) mindmapsGraph.putRoleType("Movie Genre");
        InstanceImpl crime = (InstanceImpl) mindmapsGraph.putEntity("Crime", genre);
        RelationTypeImpl movieHasGenre = (RelationTypeImpl) mindmapsGraph.putRelationType("Movie Has Genre");

        //Construction
        cast.hasRole(feature);
        cast.hasRole(actor);

        RelationImpl assertion = (RelationImpl) mindmapsGraph.addRelation(cast).
                putRolePlayer(feature, godfather).putRolePlayer(actor, pacino);

        Map<RoleType, Instance> roleMap = new HashMap<>();
        roleMap.clear();
        roleMap.put(movieOfGenre, godfather);
        roleMap.put(movieGenre, crime);
        mindmapsGraph.addRelation(movieHasGenre).
                putRolePlayer(movieOfGenre, godfather).putRolePlayer(movieGenre, crime);

        movieHasGenre.hasRole(movieOfGenre);
        movieHasGenre.hasRole(movieGenre);

        //Validation
        //Counts
        assertEquals(37, mindmapsGraph.getTinkerPopGraph().traversal().V().toList().size());
        assertEquals(53, mindmapsGraph.getTinkerPopGraph().traversal().E().toList().size());

        assertEdgeCountOfVertex(type, DataType.EdgeLabel.ISA, 0, 1);
        assertEdgeCountOfVertex(relationType, DataType.EdgeLabel.ISA, 0, 1);
        assertEdgeCountOfVertex(roleType, DataType.EdgeLabel.ISA, 0, 1);
        assertEdgeCountOfVertex(cast, DataType.EdgeLabel.ISA, 1, 1);
        assertEdgeCountOfVertex(cast, DataType.EdgeLabel.HAS_ROLE, 0, 2);

        assertEdgeCountOfVertex(feature, DataType.EdgeLabel.ISA, 1, 1);
        assertEdgeCountOfVertex(feature, DataType.EdgeLabel.CASTING, 0, 0);

        assertEdgeCountOfVertex(actor, DataType.EdgeLabel.ISA, 1, 1);
        assertEdgeCountOfVertex(actor, DataType.EdgeLabel.CASTING, 0, 0);

        assertEdgeCountOfVertex(person, DataType.EdgeLabel.ISA, 1, 1);
        assertEdgeCountOfVertex(movie, DataType.EdgeLabel.ISA, 1, 1);
        assertEdgeCountOfVertex(crime, DataType.EdgeLabel.ISA, 0, 1);
        assertEdgeCountOfVertex(genre, DataType.EdgeLabel.ISA, 1, 1);

        assertEdgeCountOfVertex(pacino, DataType.EdgeLabel.ISA, 0, 1);
        assertEdgeCountOfVertex(pacino, DataType.EdgeLabel.ROLE_PLAYER, 1, 0);

        assertEdgeCountOfVertex(movieOfGenre, DataType.EdgeLabel.ISA, 1, 1);
        assertEdgeCountOfVertex(movieOfGenre, DataType.EdgeLabel.ROLE_PLAYER, 0, 0);

        assertEdgeCountOfVertex(movieGenre, DataType.EdgeLabel.ISA, 1, 1);
        assertEdgeCountOfVertex(movieGenre, DataType.EdgeLabel.ROLE_PLAYER, 0, 0);

        assertEdgeCountOfVertex(movieHasGenre, DataType.EdgeLabel.ISA, 1, 1);
        assertEdgeCountOfVertex(movieHasGenre, DataType.EdgeLabel.HAS_ROLE, 0, 2);

        assertEdgeCountOfVertex(godfather, DataType.EdgeLabel.ISA, 0, 1);
        assertEdgeCountOfVertex(godfather, DataType.EdgeLabel.ROLE_PLAYER, 2, 0);

        //More Specific Checks
        List<Vertex> assertionsTypes = mindmapsGraph.getTinkerPopGraph().traversal().V(godfather.getBaseIdentifier()).in(DataType.EdgeLabel.ROLE_PLAYER.getLabel()).in(DataType.EdgeLabel.CASTING.getLabel()).out(DataType.EdgeLabel.ISA.getLabel()).toList();
        assertEquals(2, assertionsTypes.size());
        
        List<Object> assertTypeIds = assertionsTypes.stream().map(Vertex::id).collect(Collectors.toList());

        assertTrue(assertTypeIds.contains(cast.getBaseIdentifier()));
        assertTrue(assertTypeIds.contains(movieHasGenre.getBaseIdentifier()));

        List<Vertex> collection = mindmapsGraph.getTinkerPopGraph().traversal().V(cast.getBaseIdentifier(), movieHasGenre.getBaseIdentifier()).in(DataType.EdgeLabel.ISA.getLabel()).out(DataType.EdgeLabel.CASTING.getLabel()).out().toList();
        assertEquals(8, collection.size());

        HashSet<Object> uniqueCollection = new HashSet<>();
        for(Vertex v: collection){
            uniqueCollection.add(v.id());
        }

        assertEquals(7, uniqueCollection.size());
        assertTrue(uniqueCollection.contains(feature.getBaseIdentifier()));
        assertTrue(uniqueCollection.contains(actor.getBaseIdentifier()));
        assertTrue(uniqueCollection.contains(godfather.getBaseIdentifier()));
        assertTrue(uniqueCollection.contains(pacino.getBaseIdentifier()));
        assertTrue(uniqueCollection.contains(movieOfGenre.getBaseIdentifier()));
        assertTrue(uniqueCollection.contains(movieGenre.getBaseIdentifier()));
        assertTrue(uniqueCollection.contains(crime.getBaseIdentifier()));

        mindmapsGraph.getRelation(movieHasGenre, roleMap);
        mindmapsGraph.getRelation(movieHasGenre, roleMap);
        mindmapsGraph.getRelation(movieHasGenre, roleMap);
        mindmapsGraph.getRelation(movieHasGenre, roleMap);

        assertEquals(DataType.BaseType.ENTITY.name(), pacino.getBaseType());
        for(CastingImpl casting: assertion.getMappingCasting()){
            assertEquals(casting.getRolePlayer().getBaseType(), DataType.BaseType.ENTITY.name());
        }

    }
    private void assertEdgeCountOfVertex(Concept concept , DataType.EdgeLabel type, int inCount, int outCount){
        Vertex v= mindmapsGraph.getTinkerPopGraph().traversal().V(((ConceptImpl) concept).getBaseIdentifier()).next();
        assertEquals(inCount, getIteratorCount(v.edges(Direction.IN, type.getLabel())));
        assertEquals(outCount, getIteratorCount(v.edges(Direction.OUT, type.getLabel())));
    }

    private static int getIteratorCount(Iterator iterator){
        int count = 0;
        while(iterator.hasNext()){
            iterator.next();
            count ++;
        }
        return count;
    }

    //------------------------------------ hasAssertion
    @Test
    public void hasRelationComplexTestMultipleRelations(){
        RelationTypeImpl cast = (RelationTypeImpl) mindmapsGraph.putRelationType("Cast");
        EntityType type = mindmapsGraph.putEntityType("Concept Type");
        RoleTypeImpl feature = (RoleTypeImpl) mindmapsGraph.putRoleType("Feature");
        RoleTypeImpl actor = (RoleTypeImpl) mindmapsGraph.putRoleType("Actor");
        EntityType movie = mindmapsGraph.putEntityType("Movie");
        EntityType person = mindmapsGraph.putEntityType("Person");
        InstanceImpl pacino = (InstanceImpl) mindmapsGraph.putEntity("Pacino", person);
        InstanceImpl godfather = (InstanceImpl) mindmapsGraph.putEntity("Godfather", movie);
        EntityType genre = mindmapsGraph.putEntityType("Genre");
        RoleTypeImpl movieOfGenre = (RoleTypeImpl) mindmapsGraph.putRoleType("Movie of Genre");
        RoleTypeImpl movieGenre = (RoleTypeImpl) mindmapsGraph.putRoleType("Movie Genre");
        InstanceImpl crime = (InstanceImpl) mindmapsGraph.putEntity("Crime", genre);
        RelationTypeImpl movieHasGenre = (RelationTypeImpl) mindmapsGraph.putRelationType("Movie Has Genre");

        pacino.type(type);
        godfather.type(type);
        crime.type(type);

        mindmapsGraph.addRelation(cast).putRolePlayer(feature, godfather).putRolePlayer(actor, pacino);
        mindmapsGraph.addRelation(movieHasGenre).putRolePlayer(movieOfGenre, godfather).putRolePlayer(movieGenre, crime);

        //Validation
        HashMap<RoleType, Instance> roleMap = new HashMap<>();
        roleMap.put(feature, godfather);
        roleMap.put(actor, pacino);
        assertNotNull(mindmapsGraph.getRelation(cast, roleMap));
        roleMap.put(actor, null);
        assertNull(mindmapsGraph.getRelation(cast, roleMap));

        assertEquals(godfather, pacino.getOutgoingNeighbours(DataType.EdgeLabel.SHORTCUT).iterator().next());
        assertTrue(godfather.getOutgoingNeighbours(DataType.EdgeLabel.SHORTCUT).contains(pacino));

        roleMap.clear();
        roleMap.put(movieOfGenre, godfather);
        roleMap.put(movieGenre, crime);
        assertNotNull(mindmapsGraph.getRelation(movieHasGenre, roleMap));
        roleMap.put(actor, null);
        assertNull(mindmapsGraph.getRelation(cast, roleMap));
        assertEquals(cast.getBaseType(), DataType.BaseType.RELATION_TYPE.name());
        assertEquals(feature.getBaseType(), DataType.BaseType.ROLE_TYPE.name());
        assertEquals(actor.getBaseType(), DataType.BaseType.ROLE_TYPE.name());
        assertEquals(pacino.getBaseType(),DataType.BaseType.ENTITY.name());
        assertEquals(godfather.getBaseType(),DataType.BaseType.ENTITY.name());
        assertEquals(movieOfGenre.getBaseType(), DataType.BaseType.ROLE_TYPE.name());
        assertEquals(movieGenre.getBaseType(), DataType.BaseType.ROLE_TYPE.name());
        assertEquals(crime.getBaseType(), DataType.BaseType.ENTITY.name());
        assertEquals(movieHasGenre.getBaseType(), DataType.BaseType.RELATION_TYPE.name());
        assertEquals(DataType.BaseType.RELATION_TYPE.name(), "RELATION_TYPE");
    }

    @Test
    public void hasRelationComplexMissingRolePlayersTest(){
        RelationType relationType2 = mindmapsGraph.putRelationType("relationType2");

        Map<RoleType, Instance> roleMap = new HashMap<>();
        roleMap.put(role1, null);
        roleMap.put(role2, rolePlayer2);
        roleMap.put(role3, null);

        relationType.hasRole(role1);
        relationType.hasRole(role2);
        relationType.hasRole(role3);
        Relation relation = mindmapsGraph.addRelation(relationType).
                putRolePlayer(role1, null).putRolePlayer(role2, rolePlayer2).putRolePlayer(role3, null);

        assertNotNull(mindmapsGraph.getRelation(relationType, roleMap));
        assertNull(mindmapsGraph.getRelation(relationType2, roleMap));
    }

    @Test
    public void addRolePlayerToExistingRelation(){
        RelationType cast = mindmapsGraph.putRelationType("Cast");
        RoleType feature = mindmapsGraph.putRoleType("Feature");
        RoleType actor = mindmapsGraph.putRoleType("Actor");
        EntityType movie = mindmapsGraph.putEntityType("Movie");
        EntityType person = mindmapsGraph.putEntityType("Person");
        Instance pacino = mindmapsGraph.putEntity("Pacino", person);
        Instance godfather = mindmapsGraph.putEntity("Godfather", movie);

        RelationImpl assertion = (RelationImpl) mindmapsGraph.addRelation(cast).
                putRolePlayer(feature, null).putRolePlayer(actor, pacino);
        assertion.putRolePlayer(feature, godfather);
        assertEquals(2, assertion.getMappingCasting().size());
        assertTrue(assertion.rolePlayers().values().contains(pacino));
        assertTrue(assertion.rolePlayers().values().contains(godfather));
    }

    @Test
    public void testPutShortcutEdge(){
        RelationTypeImpl cast = (RelationTypeImpl) mindmapsGraph.putRelationType("Cast");
        EntityType type = mindmapsGraph.putEntityType("Concept Type");
        RoleTypeImpl feature = (RoleTypeImpl) mindmapsGraph.putRoleType("Feature");
        RoleTypeImpl actor = (RoleTypeImpl) mindmapsGraph.putRoleType("Actor");
        EntityType movie = mindmapsGraph.putEntityType("Movie");
        EntityType person = mindmapsGraph.putEntityType("Person");
        InstanceImpl<?, ?, ?> pacino = (InstanceImpl) mindmapsGraph.putEntity("Pacino", person);
        InstanceImpl<?, ?, ?> godfather = (InstanceImpl) mindmapsGraph.putEntity("Godfather", movie);
        RoleType actor2 = mindmapsGraph.putRoleType("Actor 2");
        RoleType actor3 = mindmapsGraph.putRoleType("Actor 3");
        RoleType character = mindmapsGraph.putRoleType("Character");
        Instance thing = mindmapsGraph.putEntity("Thing", type);

        RelationImpl relation = (RelationImpl) mindmapsGraph.addRelation(cast).
                putRolePlayer(feature, godfather).putRolePlayer(actor, pacino).putRolePlayer(actor2, pacino);

        Set<EdgeImpl> edges = pacino.getEdgesOfType(Direction.OUT, DataType.EdgeLabel.SHORTCUT);
        for(EdgeImpl edge : edges){
            assertTrue(relation.getBaseIdentifier() == edge.getEdgePropertyBaseAssertionId());
        }

        Set<ConceptImpl> godfatherToOthers = godfather.getOutgoingNeighbours(DataType.EdgeLabel.SHORTCUT);
        Set<ConceptImpl> pacinoToOthers = pacino.getOutgoingNeighbours(DataType.EdgeLabel.SHORTCUT);

        assertEquals(1, godfatherToOthers.size());
        assertTrue(godfatherToOthers.contains(pacino));

        assertEquals(2, pacinoToOthers.size());
        assertTrue(pacinoToOthers.contains(godfather));
        assertTrue(pacinoToOthers.contains(pacino));

        mindmapsGraph.addRelation(cast).putRolePlayer(feature, godfather).putRolePlayer(actor3, pacino).putRolePlayer(character, thing);

        godfatherToOthers = godfather.getOutgoingNeighbours(DataType.EdgeLabel.SHORTCUT);
        pacinoToOthers = pacino.getOutgoingNeighbours(DataType.EdgeLabel.SHORTCUT);

        assertEquals(2, godfatherToOthers.size());
        assertTrue(godfatherToOthers.contains(pacino));
        assertTrue(godfatherToOthers.contains(thing));

        assertEquals(3, pacinoToOthers.size());
        assertTrue(pacinoToOthers.contains(godfather));
        assertTrue(pacinoToOthers.contains(pacino));
        assertTrue(pacinoToOthers.contains(thing));

        mindmapsGraph.getTinkerPopGraph().traversal().V(pacino.getIncomingNeighbours(DataType.EdgeLabel.ROLE_PLAYER).iterator().next().getBaseIdentifier()).next().remove();

        pacino.getEdgesOfType(Direction.OUT, DataType.EdgeLabel.SHORTCUT).forEach(edge -> edge.setEdgePropertyBaseAssertionId(476L));
        godfather.getEdgesOfType(Direction.OUT, DataType.EdgeLabel.SHORTCUT).forEach(edge -> edge.setEdgePropertyBaseAssertionId(476L));
        godfatherToOthers = godfather.getOutgoingNeighbours(DataType.EdgeLabel.SHORTCUT);
        pacinoToOthers = pacino.getOutgoingNeighbours(DataType.EdgeLabel.SHORTCUT);

        assertEquals(2, godfatherToOthers.size());
        assertEquals(3, pacinoToOthers.size());
    }

    @Test
    public void testPutRelationSimple(){
        EntityType type = mindmapsGraph.putEntityType("Test");
        RoleType actor = mindmapsGraph.putRoleType("Actor");
        RoleType actor2 = mindmapsGraph.putRoleType("Actor 2");
        RoleType actor3 = mindmapsGraph.putRoleType("Actor 3");
        RelationType cast = mindmapsGraph.putRelationType("Cast").hasRole(actor).hasRole(actor2).hasRole(actor3);
        Instance pacino = mindmapsGraph.putEntity("Pacino", type);
        Instance thing = mindmapsGraph.putEntity("Thing", type);
        Instance godfather = mindmapsGraph.putEntity("Godfather", type);

        Instance pacino2 = mindmapsGraph.putEntity("Pacino", type);
        Instance thing2 = mindmapsGraph.putEntity("Thing", type);
        Instance godfather2 = mindmapsGraph.putEntity("Godfather", type);

        assertEquals(0, mindmapsGraph.getTinkerPopGraph().traversal().V().hasLabel(DataType.BaseType.RELATION.name()).toList().size());
        RelationImpl relation = (RelationImpl) mindmapsGraph.putRelation("a", cast).
                putRolePlayer(actor, pacino).putRolePlayer(actor2, thing).putRolePlayer(actor3, godfather);
        assertEquals(1, mindmapsGraph.getTinkerPopGraph().traversal().V().hasLabel(DataType.BaseType.RELATION.name()).toList().size());
        assertNotEquals(String.valueOf(relation.getBaseIdentifier()), relation.getId());

        mindmapsGraph.enableBatchLoading();

        relation = (RelationImpl) mindmapsGraph.addRelation(cast).
                putRolePlayer(actor, pacino2).putRolePlayer(actor2, thing2).putRolePlayer(actor3, godfather2);
        assertTrue(relation.getIndex().startsWith("RelationBaseId_" + String.valueOf(relation.getBaseIdentifier())));
    }

    @Test
    public void testCollapsedCasting(){
        RelationTypeImpl cast = (RelationTypeImpl) mindmapsGraph.putRelationType("Cast");
        EntityType type = mindmapsGraph.putEntityType("Concept Type");
        RoleType feature = mindmapsGraph.putRoleType("Feature");
        RoleTypeImpl actor = (RoleTypeImpl) mindmapsGraph.putRoleType("Actor");
        EntityType movie = mindmapsGraph.putEntityType("Movie");
        EntityType person = mindmapsGraph.putEntityType("Person");
        InstanceImpl pacino = (InstanceImpl) mindmapsGraph.putEntity("Pacino", person);
        InstanceImpl godfather = (InstanceImpl) mindmapsGraph.putEntity("Godfather", movie);
        InstanceImpl godfather2 = (InstanceImpl) mindmapsGraph.putEntity("Godfather 2", movie);
        InstanceImpl godfather3 = (InstanceImpl) mindmapsGraph.putEntity("Godfather 3", movie);

        mindmapsGraph.addRelation(cast).putRolePlayer(feature, godfather).putRolePlayer(actor, pacino);
        mindmapsGraph.addRelation(cast).putRolePlayer(feature, godfather2).putRolePlayer(actor, pacino);
        mindmapsGraph.addRelation(cast).putRolePlayer(feature, godfather3).putRolePlayer(actor, pacino);

        Vertex pacinoVertex = mindmapsGraph.getTinkerPopGraph().traversal().V(pacino.getBaseIdentifier()).next();
        Vertex godfatherVertex = mindmapsGraph.getTinkerPopGraph().traversal().V(godfather.getBaseIdentifier()).next();
        Vertex godfather2Vertex = mindmapsGraph.getTinkerPopGraph().traversal().V(godfather2.getBaseIdentifier()).next();
        Vertex godfather3Vertex = mindmapsGraph.getTinkerPopGraph().traversal().V(godfather3.getBaseIdentifier()).next();

        assertEquals(3, mindmapsGraph.getTinkerPopGraph().traversal().V().hasLabel(DataType.BaseType.RELATION.name()).count().next().intValue());
        assertEquals(4, mindmapsGraph.getTinkerPopGraph().traversal().V().hasLabel(DataType.BaseType.CASTING.name()).count().next().intValue());
        assertEquals(7, mindmapsGraph.getTinkerPopGraph().traversal().V().hasLabel(DataType.BaseType.ENTITY.name()).count().next().intValue());
        assertEquals(5, mindmapsGraph.getTinkerPopGraph().traversal().V().hasLabel(DataType.BaseType.ROLE_TYPE.name()).count().next().intValue());
        assertEquals(2, mindmapsGraph.getTinkerPopGraph().traversal().V().hasLabel(DataType.BaseType.RELATION_TYPE.name()).count().next().intValue());

        Iterator<Edge> pacinoCastings = pacinoVertex.edges(Direction.IN, DataType.EdgeLabel.ROLE_PLAYER.getLabel());
        Iterator<Edge> godfatherCastings = godfatherVertex.edges(Direction.IN, DataType.EdgeLabel.ROLE_PLAYER.getLabel());
        Iterator<Edge> godfather2Castings = godfather2Vertex.edges(Direction.IN, DataType.EdgeLabel.ROLE_PLAYER.getLabel());
        Iterator<Edge> godfather3Castings = godfather3Vertex.edges(Direction.IN, DataType.EdgeLabel.ROLE_PLAYER.getLabel());

        checkEdgeCount(1, pacinoCastings);
        checkEdgeCount(1, godfatherCastings);
        checkEdgeCount(1, godfather2Castings);
        checkEdgeCount(1, godfather3Castings);

        Vertex actorVertex = mindmapsGraph.getTinkerPopGraph().traversal().V(pacino.getBaseIdentifier()).in(DataType.EdgeLabel.ROLE_PLAYER.getLabel()).out(DataType.EdgeLabel.ISA.getLabel()).next();

        assertEquals(actor.getBaseIdentifier(), actorVertex.id());
    }
    private void checkEdgeCount(int expectedNumber, Iterator<Edge> it){
        int count = 0;
        while(it.hasNext()){
            it.next();
            count++;
        }
        assertEquals(expectedNumber, count);
    }
}