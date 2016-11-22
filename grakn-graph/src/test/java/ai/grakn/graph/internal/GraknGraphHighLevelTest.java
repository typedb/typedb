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

import ai.grakn.concept.Relation;
import ai.grakn.concept.RelationType;
import ai.grakn.Grakn;
import ai.grakn.concept.Concept;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Instance;
import ai.grakn.concept.RoleType;
import ai.grakn.concept.Type;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static ai.grakn.graql.internal.pattern.Patterns.var;
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

public class GraknGraphHighLevelTest extends GraphTestBase{

    private EntityType type;
    private RelationTypeImpl relationType;
    private RoleTypeImpl role1;
    private RoleTypeImpl role2;
    private RoleTypeImpl role3;
    private InstanceImpl rolePlayer1;
    private InstanceImpl rolePlayer2;
    private InstanceImpl rolePlayer3;

    @Before
    public void buildGraphAccessManager(){
        type = graknGraph.putEntityType("Test");
        relationType = (RelationTypeImpl) graknGraph.putRelationType("relationType");
        role1 = (RoleTypeImpl) graknGraph.putRoleType("role1");
        role2 = (RoleTypeImpl) graknGraph.putRoleType("role2");
        role3 = (RoleTypeImpl) graknGraph.putRoleType("role3");
        rolePlayer1 = (InstanceImpl) type.addEntity();
        rolePlayer2 = (InstanceImpl) type.addEntity();
        rolePlayer3 = (InstanceImpl) type.addEntity();
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
        RelationImpl assertion = (RelationImpl) relationType.addRelation().
                putRolePlayer(role1, rolePlayer1).
                putRolePlayer(role2, rolePlayer2).
                putRolePlayer(role3, rolePlayer3);

        //Checking it
        //Counts
        assertEquals(20, graknGraph.getTinkerPopGraph().traversal().V().toList().size());
        assertEquals(34, graknGraph.getTinkerPopGraph().traversal().E().toList().size());

        assertion.getMappingCasting().forEach(casting ->{
            Edge edge = graknGraph.getTinkerPopGraph().traversal().V(casting.getBaseIdentifier()).inE(Schema.EdgeLabel.CASTING.getLabel()).next();
            assertEquals(casting.getRole().getId(), edge.property(Schema.EdgeProperty.ROLE_TYPE.name()).value().toString());
            edge = graknGraph.getTinkerPopGraph().traversal().V(casting.getBaseIdentifier()).outE(Schema.EdgeLabel.ROLE_PLAYER.getLabel()).next();
            assertEquals(casting.getRole().getId(), edge.property(Schema.EdgeProperty.ROLE_TYPE.name()).value().toString());
        });

        //First Check if Roles are set correctly.
        GraphTraversal<Vertex, Vertex> traversal = graknGraph.getTinkerPopGraph().traversal().V(relationType.getBaseIdentifier()).out(Schema.EdgeLabel.HAS_ROLE.getLabel());
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
        Vertex casting1 = graknGraph.getTinkerPopGraph().traversal().V(role1.getBaseIdentifier()).in(Schema.EdgeLabel.ISA.getLabel()).next();
        Vertex casting1_copy = graknGraph.getTinkerPopGraph().traversal().V(rolePlayer1.getBaseIdentifier()).in(Schema.EdgeLabel.ROLE_PLAYER.getLabel()).next();
        assertEquals(casting1, casting1_copy);

        Vertex casting2 = graknGraph.getTinkerPopGraph().traversal().V(role2.getBaseIdentifier()).in(Schema.EdgeLabel.ISA.getLabel()).next();
        Vertex casting2_copy = graknGraph.getTinkerPopGraph().traversal().V(rolePlayer2.getBaseIdentifier()).in(Schema.EdgeLabel.ROLE_PLAYER.getLabel()).next();
        assertEquals(casting2, casting2_copy);

        Vertex casting3 = graknGraph.getTinkerPopGraph().traversal().V(role3.getBaseIdentifier()).in(Schema.EdgeLabel.ISA.getLabel()).next();
        Vertex casting3_copy = graknGraph.getTinkerPopGraph().traversal().V(rolePlayer3.getBaseIdentifier()).in(Schema.EdgeLabel.ROLE_PLAYER.getLabel()).next();
        assertEquals(casting3, casting3_copy);

        assertNotEquals(casting1, casting2);
        assertNotEquals(casting1, casting3);

        //Check all castings go to the same assertion and check assertion
        Vertex assertion1 = graknGraph.getTinkerPopGraph().traversal().V(casting1.id()).in(Schema.EdgeLabel.CASTING.getLabel()).next();
        Vertex assertion2 = graknGraph.getTinkerPopGraph().traversal().V(casting2.id()).in(Schema.EdgeLabel.CASTING.getLabel()).next();
        Vertex assertion3 = graknGraph.getTinkerPopGraph().traversal().V(casting3.id()).in(Schema.EdgeLabel.CASTING.getLabel()).next();

        assertEquals(assertion1, assertion2);
        assertEquals(assertion2, assertion3);

        Iterator<Edge> edges = assertion1.edges(Direction.OUT, Schema.EdgeLabel.ISA.getLabel());
        assertTrue(edges.hasNext());
        edges.next();
        assertFalse(edges.hasNext());

        edges = assertion1.edges(Direction.OUT, Schema.EdgeLabel.CASTING.getLabel());
        int count = 0;
        while (edges.hasNext()){
            edges.next();
            count ++;
        }
        assertEquals(3, count);
    }

    @Test
    public void testAddComplexRelationshipMissingRolePlayer(){
        HashSet<Object> validVertices = new HashSet<>();
        validVertices.add(role1.getBaseIdentifier());
        validVertices.add(role2.getBaseIdentifier());
        validVertices.add(role3.getBaseIdentifier());

        relationType.hasRole(role1);
        relationType.hasRole(role2);
        relationType.hasRole(role3);
        RelationImpl relationConcept1 = (RelationImpl) relationType.addRelation().
                putRolePlayer(role1, rolePlayer1).putRolePlayer(role2, rolePlayer2).putRolePlayer(role3, null);

        //Checking it
        //Counts
        long value = graknGraph.getTinkerPopGraph().traversal().V().count().next();
        assertEquals(19, value);
        value = graknGraph.getTinkerPopGraph().traversal().E().count().next();
        assertEquals(27, value);

        //First Check if Roles are set correctly.
        GraphTraversal<Vertex, Vertex> traversal = graknGraph.getTinkerPopGraph().traversal().V(relationType.getBaseIdentifier()).out(Schema.EdgeLabel.HAS_ROLE.getLabel());
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
        Vertex casting1 = graknGraph.getTinkerPopGraph().traversal().V(role1.getBaseIdentifier()).in(Schema.EdgeLabel.ISA.getLabel()).next();
        Vertex casting1_copy = graknGraph.getTinkerPopGraph().traversal().V(rolePlayer1.getBaseIdentifier()).in(Schema.EdgeLabel.ROLE_PLAYER.getLabel()).next();
        assertEquals(casting1, casting1_copy);

        Vertex casting2 = graknGraph.getTinkerPopGraph().traversal().V(role2.getBaseIdentifier()).in(Schema.EdgeLabel.ISA.getLabel()).next();
        Vertex casting2_copy = graknGraph.getTinkerPopGraph().traversal().V(rolePlayer2.getBaseIdentifier()).in(Schema.EdgeLabel.ROLE_PLAYER.getLabel()).next();
        assertEquals(casting2, casting2_copy);

        assertNotEquals(casting1, casting2);

        //Check all castings go to the same assertion
        Vertex assertion1 = graknGraph.getTinkerPopGraph().traversal().V(casting1.id()).in(Schema.EdgeLabel.CASTING.getLabel()).next();
        Vertex assertion2 = graknGraph.getTinkerPopGraph().traversal().V(casting2.id()).in(Schema.EdgeLabel.CASTING.getLabel()).next();

        assertEquals(assertion1, assertion2);
        assertEquals(relationConcept1.getBaseIdentifier(), assertion1.id());

        Iterator<Edge> edges = assertion1.edges(Direction.OUT, Schema.EdgeLabel.ISA.getLabel());
        assertTrue(edges.hasNext());
        edges.next();
        assertFalse(edges.hasNext());

        edges = assertion1.edges(Direction.OUT, Schema.EdgeLabel.CASTING.getLabel());
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

        relationType.addRelation().putRolePlayer(null, rolePlayer1);
    }

    @Test
    public void getRelationTest(){
        Map<RoleType, Instance> roleMap = new HashMap<>();
        roleMap.put(role1, rolePlayer1);
        roleMap.put(role2, rolePlayer2);

        RelationImpl assertion = (RelationImpl) relationType.addRelation().
                putRolePlayer(role1, rolePlayer1).putRolePlayer(role2, rolePlayer2);
        Relation relationFound = graknGraph.getRelation(relationType, roleMap);
        assertEquals(assertion, relationFound);

        assertion.getMappingCasting().forEach(casting -> {
            Edge edge = graknGraph.getTinkerPopGraph().traversal().V(casting.getBaseIdentifier()).inE(Schema.EdgeLabel.CASTING.getLabel()).next();
            assertEquals(casting.getRole().getId(), edge.property(Schema.EdgeProperty.ROLE_TYPE.name()).value().toString());
            edge = graknGraph.getTinkerPopGraph().traversal().V(casting.getBaseIdentifier()).outE(Schema.EdgeLabel.ROLE_PLAYER.getLabel()).next();
            assertEquals(casting.getRole().getId(), edge.property(Schema.EdgeProperty.ROLE_TYPE.name()).value().toString());
        });

        RelationImpl relationFound2 = (RelationImpl) graknGraph.getRelation(relationType, roleMap);
        assertEquals(Schema.BaseType.RELATION.name(), relationFound2.getBaseType());
        assertThat(relationFound2, instanceOf(Relation.class));
    }

    @Test
    public void testAddLargeAndMultipleRelationships(){

        //Actual Concepts To Appear Linked In Graph
        Type relationType = graknGraph.putEntityType("Relation Type");
        RelationTypeImpl cast = (RelationTypeImpl) graknGraph.putRelationType("Cast");
        EntityType roleType = graknGraph.putEntityType("Role Type");
        EntityType type = graknGraph.putEntityType("Concept Type");
        RoleTypeImpl feature = (RoleTypeImpl) graknGraph.putRoleType("Feature");
        RoleTypeImpl actor = (RoleTypeImpl) graknGraph.putRoleType("Actor");
        EntityType movie = graknGraph.putEntityType("Movie");
        EntityType person = graknGraph.putEntityType("Person");
        InstanceImpl pacino = (InstanceImpl) person.addEntity();
        InstanceImpl godfather = (InstanceImpl) movie.addEntity();
        EntityType genre = graknGraph.putEntityType("Genre");
        RoleTypeImpl movieOfGenre = (RoleTypeImpl) graknGraph.putRoleType("Movie of Genre");
        RoleTypeImpl movieGenre = (RoleTypeImpl) graknGraph.putRoleType("Movie Genre");
        InstanceImpl crime = (InstanceImpl) genre.addEntity();
        RelationTypeImpl movieHasGenre = (RelationTypeImpl) graknGraph.putRelationType("Movie Has Genre");

        //Construction
        cast.hasRole(feature);
        cast.hasRole(actor);

        RelationImpl assertion = (RelationImpl) cast.addRelation().
                putRolePlayer(feature, godfather).putRolePlayer(actor, pacino);

        Map<RoleType, Instance> roleMap = new HashMap<>();
        roleMap.clear();
        roleMap.put(movieOfGenre, godfather);
        roleMap.put(movieGenre, crime);
        movieHasGenre.addRelation().
                putRolePlayer(movieOfGenre, godfather).putRolePlayer(movieGenre, crime);

        movieHasGenre.hasRole(movieOfGenre);
        movieHasGenre.hasRole(movieGenre);

        //Validation
        //Counts
        assertEquals(37, graknGraph.getTinkerPopGraph().traversal().V().toList().size());
        assertEquals(52, graknGraph.getTinkerPopGraph().traversal().E().toList().size());

        assertEdgeCountOfVertex(type, Schema.EdgeLabel.ISA, 0, 1);
        assertEdgeCountOfVertex(relationType, Schema.EdgeLabel.ISA, 0, 1);
        assertEdgeCountOfVertex(roleType, Schema.EdgeLabel.ISA, 0, 1);
        assertEdgeCountOfVertex(cast, Schema.EdgeLabel.ISA, 1, 1);
        assertEdgeCountOfVertex(cast, Schema.EdgeLabel.HAS_ROLE, 0, 2);

        assertEdgeCountOfVertex(feature, Schema.EdgeLabel.ISA, 1, 1);
        assertEdgeCountOfVertex(feature, Schema.EdgeLabel.CASTING, 0, 0);

        assertEdgeCountOfVertex(actor, Schema.EdgeLabel.ISA, 1, 1);
        assertEdgeCountOfVertex(actor, Schema.EdgeLabel.CASTING, 0, 0);

        assertEdgeCountOfVertex(person, Schema.EdgeLabel.ISA, 1, 1);
        assertEdgeCountOfVertex(movie, Schema.EdgeLabel.ISA, 1, 1);
        assertEdgeCountOfVertex(crime, Schema.EdgeLabel.ISA, 0, 1);
        assertEdgeCountOfVertex(genre, Schema.EdgeLabel.ISA, 1, 1);

        assertEdgeCountOfVertex(pacino, Schema.EdgeLabel.ISA, 0, 1);
        assertEdgeCountOfVertex(pacino, Schema.EdgeLabel.ROLE_PLAYER, 1, 0);

        assertEdgeCountOfVertex(movieOfGenre, Schema.EdgeLabel.ISA, 1, 1);
        assertEdgeCountOfVertex(movieOfGenre, Schema.EdgeLabel.ROLE_PLAYER, 0, 0);

        assertEdgeCountOfVertex(movieGenre, Schema.EdgeLabel.ISA, 1, 1);
        assertEdgeCountOfVertex(movieGenre, Schema.EdgeLabel.ROLE_PLAYER, 0, 0);

        assertEdgeCountOfVertex(movieHasGenre, Schema.EdgeLabel.ISA, 1, 1);
        assertEdgeCountOfVertex(movieHasGenre, Schema.EdgeLabel.HAS_ROLE, 0, 2);

        assertEdgeCountOfVertex(godfather, Schema.EdgeLabel.ISA, 0, 1);
        assertEdgeCountOfVertex(godfather, Schema.EdgeLabel.ROLE_PLAYER, 2, 0);

        //More Specific Checks
        List<Vertex> assertionsTypes = graknGraph.getTinkerPopGraph().traversal().V(godfather.getBaseIdentifier()).in(Schema.EdgeLabel.ROLE_PLAYER.getLabel()).in(Schema.EdgeLabel.CASTING.getLabel()).out(Schema.EdgeLabel.ISA.getLabel()).toList();
        assertEquals(2, assertionsTypes.size());
        
        List<Object> assertTypeIds = assertionsTypes.stream().map(Vertex::id).collect(Collectors.toList());

        assertTrue(assertTypeIds.contains(cast.getBaseIdentifier()));
        assertTrue(assertTypeIds.contains(movieHasGenre.getBaseIdentifier()));

        List<Vertex> collection = graknGraph.getTinkerPopGraph().traversal().V(cast.getBaseIdentifier(), movieHasGenre.getBaseIdentifier()).in(Schema.EdgeLabel.ISA.getLabel()).out(Schema.EdgeLabel.CASTING.getLabel()).out().toList();
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

        graknGraph.getRelation(movieHasGenre, roleMap);
        graknGraph.getRelation(movieHasGenre, roleMap);
        graknGraph.getRelation(movieHasGenre, roleMap);
        graknGraph.getRelation(movieHasGenre, roleMap);

        assertEquals(Schema.BaseType.ENTITY.name(), pacino.getBaseType());
        for(CastingImpl casting: assertion.getMappingCasting()){
            assertEquals(casting.getRolePlayer().getBaseType(), Schema.BaseType.ENTITY.name());
        }

    }
    private void assertEdgeCountOfVertex(Concept concept , Schema.EdgeLabel type, int inCount, int outCount){
        Vertex v= graknGraph.getTinkerPopGraph().traversal().V(((ConceptImpl) concept).getBaseIdentifier()).next();
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
        RelationTypeImpl cast = (RelationTypeImpl) graknGraph.putRelationType("Cast");
        EntityType type = graknGraph.putEntityType("Concept Type");
        RoleTypeImpl feature = (RoleTypeImpl) graknGraph.putRoleType("Feature");
        RoleTypeImpl actor = (RoleTypeImpl) graknGraph.putRoleType("Actor");
        EntityType movie = graknGraph.putEntityType("Movie");
        EntityType person = graknGraph.putEntityType("Person");
        InstanceImpl pacino = (InstanceImpl) person.addEntity();
        InstanceImpl godfather = (InstanceImpl) movie.addEntity();
        EntityType genre = graknGraph.putEntityType("Genre");
        RoleTypeImpl movieOfGenre = (RoleTypeImpl) graknGraph.putRoleType("Movie of Genre");
        RoleTypeImpl movieGenre = (RoleTypeImpl) graknGraph.putRoleType("Movie Genre");
        InstanceImpl crime = (InstanceImpl) genre.addEntity();
        RelationTypeImpl movieHasGenre = (RelationTypeImpl) graknGraph.putRelationType("Movie Has Genre");

        cast.addRelation().putRolePlayer(feature, godfather).putRolePlayer(actor, pacino);
        movieHasGenre.addRelation().putRolePlayer(movieOfGenre, godfather).putRolePlayer(movieGenre, crime);

        //Validation
        HashMap<RoleType, Instance> roleMap = new HashMap<>();
        roleMap.put(feature, godfather);
        roleMap.put(actor, pacino);
        assertNotNull(graknGraph.getRelation(cast, roleMap));
        roleMap.put(actor, null);
        assertNull(graknGraph.getRelation(cast, roleMap));

        assertEquals(godfather, pacino.getOutgoingNeighbours(Schema.EdgeLabel.SHORTCUT).iterator().next());
        assertTrue(godfather.getOutgoingNeighbours(Schema.EdgeLabel.SHORTCUT).contains(pacino));

        roleMap.clear();
        roleMap.put(movieOfGenre, godfather);
        roleMap.put(movieGenre, crime);
        assertNotNull(graknGraph.getRelation(movieHasGenre, roleMap));
        roleMap.put(actor, null);
        assertNull(graknGraph.getRelation(cast, roleMap));
        assertEquals(cast.getBaseType(), Schema.BaseType.RELATION_TYPE.name());
        assertEquals(feature.getBaseType(), Schema.BaseType.ROLE_TYPE.name());
        assertEquals(actor.getBaseType(), Schema.BaseType.ROLE_TYPE.name());
        assertEquals(pacino.getBaseType(), Schema.BaseType.ENTITY.name());
        assertEquals(godfather.getBaseType(), Schema.BaseType.ENTITY.name());
        assertEquals(movieOfGenre.getBaseType(), Schema.BaseType.ROLE_TYPE.name());
        assertEquals(movieGenre.getBaseType(), Schema.BaseType.ROLE_TYPE.name());
        assertEquals(crime.getBaseType(), Schema.BaseType.ENTITY.name());
        assertEquals(movieHasGenre.getBaseType(), Schema.BaseType.RELATION_TYPE.name());
        assertEquals(Schema.BaseType.RELATION_TYPE.name(), "RELATION_TYPE");
    }

    @Test
    public void hasRelationComplexMissingRolePlayersTest(){
        RelationType relationType2 = graknGraph.putRelationType("relationType2");

        Map<RoleType, Instance> roleMap = new HashMap<>();
        roleMap.put(role1, null);
        roleMap.put(role2, rolePlayer2);
        roleMap.put(role3, null);

        relationType.hasRole(role1);
        relationType.hasRole(role2);
        relationType.hasRole(role3);
        Relation relation = relationType.addRelation().
                putRolePlayer(role1, null).putRolePlayer(role2, rolePlayer2).putRolePlayer(role3, null);

        assertNotNull(graknGraph.getRelation(relationType, roleMap));
        assertNull(graknGraph.getRelation(relationType2, roleMap));
    }

    @Test
    public void addRolePlayerToExistingRelation(){
        RelationType cast = graknGraph.putRelationType("Cast");
        RoleType feature = graknGraph.putRoleType("Feature");
        RoleType actor = graknGraph.putRoleType("Actor");
        EntityType movie = graknGraph.putEntityType("Movie");
        EntityType person = graknGraph.putEntityType("Person");
        Instance pacino = person.addEntity();
        Instance godfather = movie.addEntity();

        RelationImpl assertion = (RelationImpl) cast.addRelation().
                putRolePlayer(feature, null).putRolePlayer(actor, pacino);
        assertion.putRolePlayer(feature, godfather);
        assertEquals(2, assertion.getMappingCasting().size());
        assertTrue(assertion.rolePlayers().values().contains(pacino));
        assertTrue(assertion.rolePlayers().values().contains(godfather));
    }

    @Test
    public void testPutShortcutEdge(){
        RelationTypeImpl cast = (RelationTypeImpl) graknGraph.putRelationType("Cast");
        EntityType type = graknGraph.putEntityType("Concept Type");
        RoleTypeImpl feature = (RoleTypeImpl) graknGraph.putRoleType("Feature");
        RoleTypeImpl actor = (RoleTypeImpl) graknGraph.putRoleType("Actor");
        EntityType movie = graknGraph.putEntityType("Movie");
        EntityType person = graknGraph.putEntityType("Person");
        InstanceImpl<?, ?> pacino = (InstanceImpl) person.addEntity();
        InstanceImpl<?, ?> godfather = (InstanceImpl) movie.addEntity();
        RoleType actor2 = graknGraph.putRoleType("Actor 2");
        RoleType actor3 = graknGraph.putRoleType("Actor 3");
        RoleType character = graknGraph.putRoleType("Character");
        Instance thing = type.addEntity();

        RelationImpl relation = (RelationImpl) cast.addRelation().
                putRolePlayer(feature, godfather).putRolePlayer(actor, pacino).putRolePlayer(actor2, pacino);

        Set<EdgeImpl> edges = pacino.getEdgesOfType(Direction.OUT, Schema.EdgeLabel.SHORTCUT);
        for(EdgeImpl edge : edges){
            assertEquals(relation.type().getId(), edge.getProperty(Schema.EdgeProperty.RELATION_TYPE_ID));
        }

        Set<ConceptImpl> godfatherToOthers = godfather.getOutgoingNeighbours(Schema.EdgeLabel.SHORTCUT);
        Set<ConceptImpl> pacinoToOthers = pacino.getOutgoingNeighbours(Schema.EdgeLabel.SHORTCUT);

        assertEquals(1, godfatherToOthers.size());
        assertTrue(godfatherToOthers.contains(pacino));

        assertEquals(2, pacinoToOthers.size());
        assertTrue(pacinoToOthers.contains(godfather));
        assertTrue(pacinoToOthers.contains(pacino));

        cast.addRelation().putRolePlayer(feature, godfather).putRolePlayer(actor3, pacino).putRolePlayer(character, thing);

        godfatherToOthers = godfather.getOutgoingNeighbours(Schema.EdgeLabel.SHORTCUT);
        pacinoToOthers = pacino.getOutgoingNeighbours(Schema.EdgeLabel.SHORTCUT);

        assertEquals(2, godfatherToOthers.size());
        assertTrue(godfatherToOthers.contains(pacino));
        assertTrue(godfatherToOthers.contains(thing));

        assertEquals(3, pacinoToOthers.size());
        assertTrue(pacinoToOthers.contains(godfather));
        assertTrue(pacinoToOthers.contains(pacino));
        assertTrue(pacinoToOthers.contains(thing));

        graknGraph.getTinkerPopGraph().traversal().V(pacino.getIncomingNeighbours(Schema.EdgeLabel.ROLE_PLAYER).iterator().next().getBaseIdentifier()).next().remove();

        godfatherToOthers = godfather.getOutgoingNeighbours(Schema.EdgeLabel.SHORTCUT);
        pacinoToOthers = pacino.getOutgoingNeighbours(Schema.EdgeLabel.SHORTCUT);

        assertEquals(2, godfatherToOthers.size());
        assertEquals(3, pacinoToOthers.size());
    }

    @Test
    public void testPutRelationSimple(){
        //Load Ontology
        String keyspace = UUID.randomUUID().toString().replaceAll("-", "a");
        AbstractGraknGraph graph = (AbstractGraknGraph) Grakn.factory(Grakn.IN_MEMORY, keyspace).getGraph();
        graph.putEntityType("Test");
        RoleType actor = graph.putRoleType("Actor");
        RoleType actor2 = graph.putRoleType("Actor 2");
        RoleType actor3 = graph.putRoleType("Actor 3");
        graph.putRelationType("Cast").hasRole(actor).hasRole(actor2).hasRole(actor3);

        //Fetch Ontology
        graph = (AbstractGraknGraph) Grakn.factory(Grakn.IN_MEMORY, keyspace).getGraphBatchLoading();
        EntityType type = graph.getEntityType("Test");
        actor = graph.getRoleType("Actor");
        actor2 = graph.getRoleType("Actor 2");
        actor3 = graph.getRoleType("Actor 3");
        RelationType cast = graph.getRelationType("Cast");

        Instance pacino = type.addEntity();
        Instance thing = type.addEntity();
        Instance godfather = type.addEntity();

        Instance pacino2 = type.addEntity();
        Instance thing2 = type.addEntity();
        Instance godfather2 = type.addEntity();

        assertEquals(0, graph.getTinkerPopGraph().traversal().V().hasLabel(Schema.BaseType.RELATION.name()).toList().size());
        RelationImpl relation = (RelationImpl) cast.addRelation().
                putRolePlayer(actor, pacino).putRolePlayer(actor2, thing).putRolePlayer(actor3, godfather);
        assertEquals(1, graph.getTinkerPopGraph().traversal().V().hasLabel(Schema.BaseType.RELATION.name()).toList().size());
        assertEquals(String.valueOf(relation.getBaseIdentifier()), relation.getId());

        relation = (RelationImpl) cast.addRelation().
                putRolePlayer(actor, pacino2).putRolePlayer(actor2, thing2).putRolePlayer(actor3, godfather2);

        assertTrue(relation.getIndex().startsWith("RelationBaseId_" + String.valueOf(relation.getBaseIdentifier())));
    }

    @Test
    public void testCollapsedCasting(){
        RelationTypeImpl cast = (RelationTypeImpl) graknGraph.putRelationType("Cast");
        EntityType type = graknGraph.putEntityType("Concept Type");
        RoleType feature = graknGraph.putRoleType("Feature");
        RoleTypeImpl actor = (RoleTypeImpl) graknGraph.putRoleType("Actor");
        EntityType movie = graknGraph.putEntityType("Movie");
        EntityType person = graknGraph.putEntityType("Person");
        InstanceImpl pacino = (InstanceImpl) person.addEntity();
        InstanceImpl godfather = (InstanceImpl) movie.addEntity();
        InstanceImpl godfather2 = (InstanceImpl) movie.addEntity();
        InstanceImpl godfather3 = (InstanceImpl) movie.addEntity();

        cast.addRelation().putRolePlayer(feature, godfather).putRolePlayer(actor, pacino);
        cast.addRelation().putRolePlayer(feature, godfather2).putRolePlayer(actor, pacino);
        cast.addRelation().putRolePlayer(feature, godfather3).putRolePlayer(actor, pacino);

        Vertex pacinoVertex = graknGraph.getTinkerPopGraph().traversal().V(pacino.getBaseIdentifier()).next();
        Vertex godfatherVertex = graknGraph.getTinkerPopGraph().traversal().V(godfather.getBaseIdentifier()).next();
        Vertex godfather2Vertex = graknGraph.getTinkerPopGraph().traversal().V(godfather2.getBaseIdentifier()).next();
        Vertex godfather3Vertex = graknGraph.getTinkerPopGraph().traversal().V(godfather3.getBaseIdentifier()).next();

        assertEquals(3, graknGraph.getTinkerPopGraph().traversal().V().hasLabel(Schema.BaseType.RELATION.name()).count().next().intValue());
        assertEquals(4, graknGraph.getTinkerPopGraph().traversal().V().hasLabel(Schema.BaseType.CASTING.name()).count().next().intValue());
        assertEquals(7, graknGraph.getTinkerPopGraph().traversal().V().hasLabel(Schema.BaseType.ENTITY.name()).count().next().intValue());
        assertEquals(5, graknGraph.getTinkerPopGraph().traversal().V().hasLabel(Schema.BaseType.ROLE_TYPE.name()).count().next().intValue());
        assertEquals(2, graknGraph.getTinkerPopGraph().traversal().V().hasLabel(Schema.BaseType.RELATION_TYPE.name()).count().next().intValue());

        Iterator<Edge> pacinoCastings = pacinoVertex.edges(Direction.IN, Schema.EdgeLabel.ROLE_PLAYER.getLabel());
        Iterator<Edge> godfatherCastings = godfatherVertex.edges(Direction.IN, Schema.EdgeLabel.ROLE_PLAYER.getLabel());
        Iterator<Edge> godfather2Castings = godfather2Vertex.edges(Direction.IN, Schema.EdgeLabel.ROLE_PLAYER.getLabel());
        Iterator<Edge> godfather3Castings = godfather3Vertex.edges(Direction.IN, Schema.EdgeLabel.ROLE_PLAYER.getLabel());

        checkEdgeCount(1, pacinoCastings);
        checkEdgeCount(1, godfatherCastings);
        checkEdgeCount(1, godfather2Castings);
        checkEdgeCount(1, godfather3Castings);

        Vertex actorVertex = graknGraph.getTinkerPopGraph().traversal().V(pacino.getBaseIdentifier()).in(Schema.EdgeLabel.ROLE_PLAYER.getLabel()).out(Schema.EdgeLabel.ISA.getLabel()).next();

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


    @Test
    public void testGraqlQuery(){
        String entityType = Schema.MetaSchema.ENTITY_TYPE.getId();
        EntityType type1 = graknGraph.putEntityType("Concept Type ");
        EntityType type2 = graknGraph.putEntityType("Concept Type 1");

        List<Map<String, Concept>> results = graknGraph.graql().match(var("x").isa(entityType)).execute();
        System.out.println();

        boolean found = results.stream().map(Map::values).anyMatch(concepts -> concepts.stream().anyMatch(concept -> concept.equals(type1)));
        assertTrue(found);

        found = results.stream().map(Map::values).anyMatch(concepts -> concepts.stream().anyMatch(concept -> concept.equals(type2)));
        assertTrue(found);
    }

}