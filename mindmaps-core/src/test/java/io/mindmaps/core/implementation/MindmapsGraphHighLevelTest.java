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

import io.mindmaps.MindmapsGraph;
import io.mindmaps.constants.DataType;
import io.mindmaps.constants.ErrorMessage;
import io.mindmaps.core.model.Concept;
import io.mindmaps.core.model.EntityType;
import io.mindmaps.core.model.Instance;
import io.mindmaps.core.model.Relation;
import io.mindmaps.core.model.RelationType;
import io.mindmaps.core.model.RoleType;
import io.mindmaps.core.model.Type;
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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class MindmapsGraphHighLevelTest {

    private MindmapsGraph mindmapsGraph;
    private AbstractMindmapsGraph graph;
    private EntityType type;
    private RelationTypeImpl relationType;
    private RoleTypeImpl role1;
    private RoleTypeImpl role2;
    private RoleTypeImpl role3;
    private InstanceImpl rolePlayer1;
    private InstanceImpl rolePlayer2;
    private InstanceImpl rolePlayer3;

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Before
    public void buildGraphAccessManager(){
        mindmapsGraph = MindmapsTestGraphFactory.newEmptyGraph();
        graph = (AbstractMindmapsGraph) mindmapsGraph;
        graph.initialiseMetaConcepts();

        type = graph.putEntityType("Test");
        relationType = (RelationTypeImpl) graph.putRelationType("relationType");
        role1 = (RoleTypeImpl) graph.putRoleType("role1");
        role2 = (RoleTypeImpl) graph.putRoleType("role2");
        role3 = (RoleTypeImpl) graph.putRoleType("role3");
        rolePlayer1 = (InstanceImpl) graph.putEntity("role-player1", type);
        rolePlayer2 = (InstanceImpl) graph.putEntity("role-player2", type);
        rolePlayer3 = (InstanceImpl) graph.putEntity("role-player3", type);
    }
    @After
    public void destroyGraphAccessManager()  throws Exception{
        graph.close();
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
        RelationImpl assertion = (RelationImpl) graph.putRelation("a thing", relationType).
                putRolePlayer(role1, rolePlayer1).
                putRolePlayer(role2, rolePlayer2).
                putRolePlayer(role3, rolePlayer3);

        //Checking it
        //Counts
        assertEquals(20, graph.getTinkerPopGraph().traversal().V().toList().size());
        assertEquals(35, graph.getTinkerPopGraph().traversal().E().toList().size());

        assertion.getMappingCasting().forEach(casting ->{
            Edge edge = graph.getTinkerPopGraph().traversal().V(casting.getBaseIdentifier()).inE(DataType.EdgeLabel.CASTING.getLabel()).next();
            assertEquals(casting.getRole().getId(), edge.property(DataType.EdgeProperty.ROLE_TYPE.name()).value().toString());
            edge = graph.getTinkerPopGraph().traversal().V(casting.getBaseIdentifier()).outE(DataType.EdgeLabel.ROLE_PLAYER.getLabel()).next();
            assertEquals(casting.getRole().getId(), edge.property(DataType.EdgeProperty.ROLE_TYPE.name()).value().toString());
        });

        //First Check if Roles are set correctly.
        GraphTraversal<Vertex, Vertex> traversal = graph.getTinkerPopGraph().traversal().V(relationType.getBaseIdentifier()).out(DataType.EdgeLabel.HAS_ROLE.getLabel());
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
        Vertex casting1 = graph.getTinkerPopGraph().traversal().V(role1.getBaseIdentifier()).in(DataType.EdgeLabel.ISA.getLabel()).next();
        Vertex casting1_copy = graph.getTinkerPopGraph().traversal().V(rolePlayer1.getBaseIdentifier()).in(DataType.EdgeLabel.ROLE_PLAYER.getLabel()).next();
        assertEquals(casting1, casting1_copy);

        Vertex casting2 = graph.getTinkerPopGraph().traversal().V(role2.getBaseIdentifier()).in(DataType.EdgeLabel.ISA.getLabel()).next();
        Vertex casting2_copy = graph.getTinkerPopGraph().traversal().V(rolePlayer2.getBaseIdentifier()).in(DataType.EdgeLabel.ROLE_PLAYER.getLabel()).next();
        assertEquals(casting2, casting2_copy);

        Vertex casting3 = graph.getTinkerPopGraph().traversal().V(role3.getBaseIdentifier()).in(DataType.EdgeLabel.ISA.getLabel()).next();
        Vertex casting3_copy = graph.getTinkerPopGraph().traversal().V(rolePlayer3.getBaseIdentifier()).in(DataType.EdgeLabel.ROLE_PLAYER.getLabel()).next();
        assertEquals(casting3, casting3_copy);

        assertNotEquals(casting1, casting2);
        assertNotEquals(casting1, casting3);

        //Check all castings go to the same assertion and check assertion
        Vertex assertion1 = graph.getTinkerPopGraph().traversal().V(casting1.id()).in(DataType.EdgeLabel.CASTING.getLabel()).next();
        Vertex assertion2 = graph.getTinkerPopGraph().traversal().V(casting2.id()).in(DataType.EdgeLabel.CASTING.getLabel()).next();
        Vertex assertion3 = graph.getTinkerPopGraph().traversal().V(casting3.id()).in(DataType.EdgeLabel.CASTING.getLabel()).next();

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
        RelationImpl relationConcept1 = (RelationImpl) graph.putRelation("a", relationType).
                putRolePlayer(role1, rolePlayer1).putRolePlayer(role2, rolePlayer2).putRolePlayer(role3, null);

        //Checking it
        //Counts
        long value = graph.getTinkerPopGraph().traversal().V().count().next();
        assertEquals(19, value);
        value = graph.getTinkerPopGraph().traversal().E().count().next();
        assertEquals(28, value);

        //First Check if Roles are set correctly.
        GraphTraversal<Vertex, Vertex> traversal = graph.getTinkerPopGraph().traversal().V(relationType.getBaseIdentifier()).out(DataType.EdgeLabel.HAS_ROLE.getLabel());
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
        Vertex casting1 = graph.getTinkerPopGraph().traversal().V(role1.getBaseIdentifier()).in(DataType.EdgeLabel.ISA.getLabel()).next();
        Vertex casting1_copy = graph.getTinkerPopGraph().traversal().V(rolePlayer1.getBaseIdentifier()).in(DataType.EdgeLabel.ROLE_PLAYER.getLabel()).next();
        assertEquals(casting1, casting1_copy);

        Vertex casting2 = graph.getTinkerPopGraph().traversal().V(role2.getBaseIdentifier()).in(DataType.EdgeLabel.ISA.getLabel()).next();
        Vertex casting2_copy = graph.getTinkerPopGraph().traversal().V(rolePlayer2.getBaseIdentifier()).in(DataType.EdgeLabel.ROLE_PLAYER.getLabel()).next();
        assertEquals(casting2, casting2_copy);

        assertNotEquals(casting1, casting2);

        //Check all castings go to the same assertion
        Vertex assertion1 = graph.getTinkerPopGraph().traversal().V(casting1.id()).in(DataType.EdgeLabel.CASTING.getLabel()).next();
        Vertex assertion2 = graph.getTinkerPopGraph().traversal().V(casting2.id()).in(DataType.EdgeLabel.CASTING.getLabel()).next();

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

        graph.putRelation(UUID.randomUUID().toString(), relationType).putRolePlayer(null, rolePlayer1);
    }

    @Test
    public void getRelationTest(){
        rolePlayer1.type(type);
        rolePlayer2.type(type);

        Map<RoleType, Instance> roleMap = new HashMap<>();
        roleMap.put(role1, rolePlayer1);
        roleMap.put(role2, rolePlayer2);

        RelationImpl assertion = (RelationImpl) graph.addRelation(relationType).
                putRolePlayer(role1, rolePlayer1).putRolePlayer(role2, rolePlayer2);
        Relation relationFound = graph.getRelation(relationType, roleMap);
        assertEquals(assertion, relationFound);

        assertion.getMappingCasting().forEach(casting -> {
            Edge edge = graph.getTinkerPopGraph().traversal().V(casting.getBaseIdentifier()).inE(DataType.EdgeLabel.CASTING.getLabel()).next();
            assertEquals(casting.getRole().getId(), edge.property(DataType.EdgeProperty.ROLE_TYPE.name()).value().toString());
            edge = graph.getTinkerPopGraph().traversal().V(casting.getBaseIdentifier()).outE(DataType.EdgeLabel.ROLE_PLAYER.getLabel()).next();
            assertEquals(casting.getRole().getId(), edge.property(DataType.EdgeProperty.ROLE_TYPE.name()).value().toString());
        });

        RelationImpl relationFound2 = (RelationImpl) graph.getRelation(relationType, roleMap);
        assertEquals(DataType.BaseType.RELATION.name(), relationFound2.getBaseType());
        assertThat(relationFound2, instanceOf(Relation.class));
    }

    @Test
    public void testAddLargeAndMultipleRelationships(){

        //Actual Concepts To Appear Linked In Graph
        Type relationType = graph.putEntityType("Relation Type");
        RelationTypeImpl cast = (RelationTypeImpl) graph.putRelationType("Cast");
        EntityType roleType = graph.putEntityType("Role Type");
        EntityType type = graph.putEntityType("Concept Type");
        RoleTypeImpl feature = (RoleTypeImpl) graph.putRoleType("Feature");
        RoleTypeImpl actor = (RoleTypeImpl) graph.putRoleType("Actor");
        EntityType movie = graph.putEntityType("Movie");
        EntityType person = graph.putEntityType("Person");
        InstanceImpl pacino = (InstanceImpl) graph.putEntity("Pacino", person);
        InstanceImpl godfather = (InstanceImpl) graph.putEntity("Godfather", movie);
        EntityType genre = graph.putEntityType("Genre");
        RoleTypeImpl movieOfGenre = (RoleTypeImpl) graph.putRoleType("Movie of Genre");
        RoleTypeImpl movieGenre = (RoleTypeImpl) graph.putRoleType("Movie Genre");
        InstanceImpl crime = (InstanceImpl) graph.putEntity("Crime", genre);
        RelationTypeImpl movieHasGenre = (RelationTypeImpl) graph.putRelationType("Movie Has Genre");

        //Construction
        cast.hasRole(feature);
        cast.hasRole(actor);

        RelationImpl assertion = (RelationImpl) graph.addRelation(cast).
                putRolePlayer(feature, godfather).putRolePlayer(actor, pacino);

        Map<RoleType, Instance> roleMap = new HashMap<>();
        roleMap.clear();
        roleMap.put(movieOfGenre, godfather);
        roleMap.put(movieGenre, crime);
        graph.addRelation(movieHasGenre).
                putRolePlayer(movieOfGenre, godfather).putRolePlayer(movieGenre, crime);

        movieHasGenre.hasRole(movieOfGenre);
        movieHasGenre.hasRole(movieGenre);

        //Validation
        //Counts
        assertEquals(37, graph.getTinkerPopGraph().traversal().V().toList().size());
        assertEquals(53, graph.getTinkerPopGraph().traversal().E().toList().size());

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
        List<Vertex> assertionsTypes = graph.getTinkerPopGraph().traversal().V(godfather.getBaseIdentifier()).in(DataType.EdgeLabel.ROLE_PLAYER.getLabel()).in(DataType.EdgeLabel.CASTING.getLabel()).out(DataType.EdgeLabel.ISA.getLabel()).toList();
        assertEquals(2, assertionsTypes.size());
        
        List<Object> assertTypeIds = assertionsTypes.stream().map(Vertex::id).collect(Collectors.toList());

        assertTrue(assertTypeIds.contains(cast.getBaseIdentifier()));
        assertTrue(assertTypeIds.contains(movieHasGenre.getBaseIdentifier()));

        List<Vertex> collection = graph.getTinkerPopGraph().traversal().V(cast.getBaseIdentifier(), movieHasGenre.getBaseIdentifier()).in(DataType.EdgeLabel.ISA.getLabel()).out(DataType.EdgeLabel.CASTING.getLabel()).out().toList();
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

        graph.getRelation(movieHasGenre, roleMap);
        graph.getRelation(movieHasGenre, roleMap);
        graph.getRelation(movieHasGenre, roleMap);
        graph.getRelation(movieHasGenre, roleMap);

        assertEquals(DataType.BaseType.ENTITY.name(), pacino.getBaseType());
        for(CastingImpl casting: assertion.getMappingCasting()){
            assertEquals(casting.getRolePlayer().getBaseType(), DataType.BaseType.ENTITY.name());
        }

    }
    private void assertEdgeCountOfVertex(Concept concept , DataType.EdgeLabel type, int inCount, int outCount){
        Vertex v= graph.getTinkerPopGraph().traversal().V(((ConceptImpl) concept).getBaseIdentifier()).next();
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
        RelationTypeImpl cast = (RelationTypeImpl) graph.putRelationType("Cast");
        EntityType type = graph.putEntityType("Concept Type");
        RoleTypeImpl feature = (RoleTypeImpl) graph.putRoleType("Feature");
        RoleTypeImpl actor = (RoleTypeImpl) graph.putRoleType("Actor");
        EntityType movie = graph.putEntityType("Movie");
        EntityType person = graph.putEntityType("Person");
        InstanceImpl pacino = (InstanceImpl) graph.putEntity("Pacino", person);
        InstanceImpl godfather = (InstanceImpl) graph.putEntity("Godfather", movie);
        EntityType genre = graph.putEntityType("Genre");
        RoleTypeImpl movieOfGenre = (RoleTypeImpl) graph.putRoleType("Movie of Genre");
        RoleTypeImpl movieGenre = (RoleTypeImpl) graph.putRoleType("Movie Genre");
        InstanceImpl crime = (InstanceImpl) graph.putEntity("Crime", genre);
        RelationTypeImpl movieHasGenre = (RelationTypeImpl) graph.putRelationType("Movie Has Genre");

        pacino.type(type);
        godfather.type(type);
        crime.type(type);

        graph.addRelation(cast).putRolePlayer(feature, godfather).putRolePlayer(actor, pacino);
        graph.addRelation(movieHasGenre).putRolePlayer(movieOfGenre, godfather).putRolePlayer(movieGenre, crime);

        //Validation
        HashMap<RoleType, Instance> roleMap = new HashMap<>();
        roleMap.put(feature, godfather);
        roleMap.put(actor, pacino);
        assertNotNull(graph.getRelation(cast, roleMap));
        roleMap.put(actor, null);
        assertNull(graph.getRelation(cast, roleMap));

        assertEquals(godfather, pacino.getOutgoingNeighbours(DataType.EdgeLabel.SHORTCUT).iterator().next());
        assertTrue(godfather.getOutgoingNeighbours(DataType.EdgeLabel.SHORTCUT).contains(pacino));

        roleMap.clear();
        roleMap.put(movieOfGenre, godfather);
        roleMap.put(movieGenre, crime);
        assertNotNull(graph.getRelation(movieHasGenre, roleMap));
        roleMap.put(actor, null);
        assertNull(graph.getRelation(cast, roleMap));
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
        RelationType relationType2 = graph.putRelationType("relationType2");

        Map<RoleType, Instance> roleMap = new HashMap<>();
        roleMap.put(role1, null);
        roleMap.put(role2, rolePlayer2);
        roleMap.put(role3, null);

        relationType.hasRole(role1);
        relationType.hasRole(role2);
        relationType.hasRole(role3);
        Relation relation = graph.addRelation(relationType).
                putRolePlayer(role1, null).putRolePlayer(role2, rolePlayer2).putRolePlayer(role3, null);

        assertNotNull(graph.getRelation(relationType, roleMap));
        assertNull(graph.getRelation(relationType2, roleMap));
    }

    @Test
    public void addRolePlayerToExistingRelation(){
        RelationType cast = graph.putRelationType("Cast");
        RoleType feature = graph.putRoleType("Feature");
        RoleType actor = graph.putRoleType("Actor");
        EntityType movie = graph.putEntityType("Movie");
        EntityType person = graph.putEntityType("Person");
        Instance pacino = graph.putEntity("Pacino", person);
        Instance godfather = graph.putEntity("Godfather", movie);

        RelationImpl assertion = (RelationImpl) graph.addRelation(cast).
                putRolePlayer(feature, null).putRolePlayer(actor, pacino);
        assertion.putRolePlayer(feature, godfather);
        assertEquals(2, assertion.getMappingCasting().size());
        assertTrue(assertion.rolePlayers().values().contains(pacino));
        assertTrue(assertion.rolePlayers().values().contains(godfather));
    }

    @Test
    public void testPutShortcutEdge(){
        RelationTypeImpl cast = (RelationTypeImpl) graph.putRelationType("Cast");
        EntityType type = graph.putEntityType("Concept Type");
        RoleTypeImpl feature = (RoleTypeImpl) graph.putRoleType("Feature");
        RoleTypeImpl actor = (RoleTypeImpl) graph.putRoleType("Actor");
        EntityType movie = graph.putEntityType("Movie");
        EntityType person = graph.putEntityType("Person");
        InstanceImpl<?, ?> pacino = (InstanceImpl) graph.putEntity("Pacino", person);
        InstanceImpl<?, ?> godfather = (InstanceImpl) graph.putEntity("Godfather", movie);
        RoleType actor2 = graph.putRoleType("Actor 2");
        RoleType actor3 = graph.putRoleType("Actor 3");
        RoleType character = graph.putRoleType("Character");
        Instance thing = graph.putEntity("Thing", type);

        RelationImpl relation = (RelationImpl) graph.addRelation(cast).
                putRolePlayer(feature, godfather).putRolePlayer(actor, pacino).putRolePlayer(actor2, pacino);

        Set<EdgeImpl> edges = pacino.getEdgesOfType(Direction.OUT, DataType.EdgeLabel.SHORTCUT);
        for(EdgeImpl edge : edges){
            assertEquals(relation.type().getId(), edge.getProperty(DataType.EdgeProperty.RELATION_TYPE_ID));
        }

        Set<ConceptImpl> godfatherToOthers = godfather.getOutgoingNeighbours(DataType.EdgeLabel.SHORTCUT);
        Set<ConceptImpl> pacinoToOthers = pacino.getOutgoingNeighbours(DataType.EdgeLabel.SHORTCUT);

        assertEquals(1, godfatherToOthers.size());
        assertTrue(godfatherToOthers.contains(pacino));

        assertEquals(2, pacinoToOthers.size());
        assertTrue(pacinoToOthers.contains(godfather));
        assertTrue(pacinoToOthers.contains(pacino));

        graph.addRelation(cast).putRolePlayer(feature, godfather).putRolePlayer(actor3, pacino).putRolePlayer(character, thing);

        godfatherToOthers = godfather.getOutgoingNeighbours(DataType.EdgeLabel.SHORTCUT);
        pacinoToOthers = pacino.getOutgoingNeighbours(DataType.EdgeLabel.SHORTCUT);

        assertEquals(2, godfatherToOthers.size());
        assertTrue(godfatherToOthers.contains(pacino));
        assertTrue(godfatherToOthers.contains(thing));

        assertEquals(3, pacinoToOthers.size());
        assertTrue(pacinoToOthers.contains(godfather));
        assertTrue(pacinoToOthers.contains(pacino));
        assertTrue(pacinoToOthers.contains(thing));

        graph.getTinkerPopGraph().traversal().V(pacino.getIncomingNeighbours(DataType.EdgeLabel.ROLE_PLAYER).iterator().next().getBaseIdentifier()).next().remove();

        godfatherToOthers = godfather.getOutgoingNeighbours(DataType.EdgeLabel.SHORTCUT);
        pacinoToOthers = pacino.getOutgoingNeighbours(DataType.EdgeLabel.SHORTCUT);

        assertEquals(2, godfatherToOthers.size());
        assertEquals(3, pacinoToOthers.size());
    }

    @Test
    public void testPutRelationSimple(){
        AbstractMindmapsGraph graph = (AbstractMindmapsGraph) MindmapsTestGraphFactory.newBatchLoadingEmptyGraph();

        EntityType type = graph.putEntityType("Test");
        RoleType actor = graph.putRoleType("Actor");
        RoleType actor2 = graph.putRoleType("Actor 2");
        RoleType actor3 = graph.putRoleType("Actor 3");
        RelationType cast = graph.putRelationType("Cast").hasRole(actor).hasRole(actor2).hasRole(actor3);
        Instance pacino = graph.putEntity("Pacino", type);
        Instance thing = graph.putEntity("Thing", type);
        Instance godfather = graph.putEntity("Godfather", type);

        Instance pacino2 = graph.putEntity("Pacino", type);
        Instance thing2 = graph.putEntity("Thing", type);
        Instance godfather2 = graph.putEntity("Godfather", type);

        assertEquals(0, graph.getTinkerPopGraph().traversal().V().hasLabel(DataType.BaseType.RELATION.name()).toList().size());
        RelationImpl relation = (RelationImpl) graph.putRelation("a", cast).
                putRolePlayer(actor, pacino).putRolePlayer(actor2, thing).putRolePlayer(actor3, godfather);
        assertEquals(1, graph.getTinkerPopGraph().traversal().V().hasLabel(DataType.BaseType.RELATION.name()).toList().size());
        assertNotEquals(String.valueOf(relation.getBaseIdentifier()), relation.getId());

        relation = (RelationImpl) graph.addRelation(cast).
                putRolePlayer(actor, pacino2).putRolePlayer(actor2, thing2).putRolePlayer(actor3, godfather2);

        assertTrue(relation.getIndex().startsWith("RelationBaseId_" + String.valueOf(relation.getBaseIdentifier())));
    }

    @Test
    public void testCollapsedCasting(){
        RelationTypeImpl cast = (RelationTypeImpl) graph.putRelationType("Cast");
        EntityType type = graph.putEntityType("Concept Type");
        RoleType feature = graph.putRoleType("Feature");
        RoleTypeImpl actor = (RoleTypeImpl) graph.putRoleType("Actor");
        EntityType movie = graph.putEntityType("Movie");
        EntityType person = graph.putEntityType("Person");
        InstanceImpl pacino = (InstanceImpl) graph.putEntity("Pacino", person);
        InstanceImpl godfather = (InstanceImpl) graph.putEntity("Godfather", movie);
        InstanceImpl godfather2 = (InstanceImpl) graph.putEntity("Godfather 2", movie);
        InstanceImpl godfather3 = (InstanceImpl) graph.putEntity("Godfather 3", movie);

        graph.addRelation(cast).putRolePlayer(feature, godfather).putRolePlayer(actor, pacino);
        graph.addRelation(cast).putRolePlayer(feature, godfather2).putRolePlayer(actor, pacino);
        graph.addRelation(cast).putRolePlayer(feature, godfather3).putRolePlayer(actor, pacino);

        Vertex pacinoVertex = graph.getTinkerPopGraph().traversal().V(pacino.getBaseIdentifier()).next();
        Vertex godfatherVertex = graph.getTinkerPopGraph().traversal().V(godfather.getBaseIdentifier()).next();
        Vertex godfather2Vertex = graph.getTinkerPopGraph().traversal().V(godfather2.getBaseIdentifier()).next();
        Vertex godfather3Vertex = graph.getTinkerPopGraph().traversal().V(godfather3.getBaseIdentifier()).next();

        assertEquals(3, graph.getTinkerPopGraph().traversal().V().hasLabel(DataType.BaseType.RELATION.name()).count().next().intValue());
        assertEquals(4, graph.getTinkerPopGraph().traversal().V().hasLabel(DataType.BaseType.CASTING.name()).count().next().intValue());
        assertEquals(7, graph.getTinkerPopGraph().traversal().V().hasLabel(DataType.BaseType.ENTITY.name()).count().next().intValue());
        assertEquals(5, graph.getTinkerPopGraph().traversal().V().hasLabel(DataType.BaseType.ROLE_TYPE.name()).count().next().intValue());
        assertEquals(2, graph.getTinkerPopGraph().traversal().V().hasLabel(DataType.BaseType.RELATION_TYPE.name()).count().next().intValue());

        Iterator<Edge> pacinoCastings = pacinoVertex.edges(Direction.IN, DataType.EdgeLabel.ROLE_PLAYER.getLabel());
        Iterator<Edge> godfatherCastings = godfatherVertex.edges(Direction.IN, DataType.EdgeLabel.ROLE_PLAYER.getLabel());
        Iterator<Edge> godfather2Castings = godfather2Vertex.edges(Direction.IN, DataType.EdgeLabel.ROLE_PLAYER.getLabel());
        Iterator<Edge> godfather3Castings = godfather3Vertex.edges(Direction.IN, DataType.EdgeLabel.ROLE_PLAYER.getLabel());

        checkEdgeCount(1, pacinoCastings);
        checkEdgeCount(1, godfatherCastings);
        checkEdgeCount(1, godfather2Castings);
        checkEdgeCount(1, godfather3Castings);

        Vertex actorVertex = graph.getTinkerPopGraph().traversal().V(pacino.getBaseIdentifier()).in(DataType.EdgeLabel.ROLE_PLAYER.getLabel()).out(DataType.EdgeLabel.ISA.getLabel()).next();

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