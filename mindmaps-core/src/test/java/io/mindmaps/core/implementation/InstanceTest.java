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
import io.mindmaps.core.model.*;
import io.mindmaps.factory.MindmapsTestGraphFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Set;
import java.util.UUID;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNull;
import static org.junit.Assert.*;

public class InstanceTest {
    private MindmapsTransactionImpl mindmapsGraph;

    @org.junit.Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Before
    public void buildGraph(){
        mindmapsGraph = (MindmapsTransactionImpl) MindmapsTestGraphFactory.newEmptyGraph().newTransaction();
        mindmapsGraph.initialiseMetaConcepts();
    }
    @After
    public void destroyGraph()  throws Exception{
        mindmapsGraph.close();
    }

    @Test
    public void testDeleteScope() throws ConceptException{
        EntityType entityType = mindmapsGraph.putEntityType("entity type");
        RelationType relationType = mindmapsGraph.putRelationType("RelationType");
        Instance scope = mindmapsGraph.putEntity("scope", entityType);
        Relation relation = mindmapsGraph.putRelation(UUID.randomUUID().toString(), relationType);
        relation.scope(scope);
        scope.delete();
        assertNull(mindmapsGraph.getConceptByBaseIdentifier(((ConceptImpl) scope).getBaseIdentifier()));
    }

    @Test
    public void testGetCastings(){
        RelationType relationType = mindmapsGraph.putRelationType("rel type");
        EntityType entityType = mindmapsGraph.putEntityType("entity type");
        InstanceImpl rolePlayer1 = (InstanceImpl) mindmapsGraph.putEntity("role-player1", entityType);
        assertEquals(0, rolePlayer1.getIncomingNeighbours(DataType.EdgeLabel.CASTING).size());

        RoleTypeImpl role = (RoleTypeImpl) mindmapsGraph.putRoleType("Role");
        RoleTypeImpl role2 = (RoleTypeImpl) mindmapsGraph.putRoleType("Role 2");
        Relation relation = mindmapsGraph.putRelation(UUID.randomUUID().toString(), relationType);
        Relation relation2 = mindmapsGraph.putRelation(UUID.randomUUID().toString(), relationType);
        CastingImpl casting1 = mindmapsGraph.putCasting(role, rolePlayer1, (RelationImpl) relation);
        CastingImpl casting2 = mindmapsGraph.putCasting(role2, rolePlayer1, (RelationImpl) relation2);

        Set<ConceptImpl> castings = rolePlayer1.getIncomingNeighbours(DataType.EdgeLabel.ROLE_PLAYER);

        assertEquals(2, castings.size());
        assertTrue(castings.contains(casting1));
        assertTrue(castings.contains(casting2));
        assertNotEquals(casting1, casting2);
    }

    @Test
    public void testDeleteConceptInstanceInRelationship() throws ConceptException{
        //Build
        EntityType type = mindmapsGraph.putEntityType("Concept Type");
        RelationType relationType = mindmapsGraph.putRelationType("relationType");
        RoleType role1 = mindmapsGraph.putRoleType("role1");
        RoleType role2 = mindmapsGraph.putRoleType("role2");
        RoleType role3 = mindmapsGraph.putRoleType("role3");
        Instance rolePlayer1 = mindmapsGraph.putEntity("role-player1", type);
        Instance rolePlayer2 = mindmapsGraph.putEntity("role-player2", type);
        Instance rolePlayer3 = mindmapsGraph.putEntity("role-player3", type);

        relationType.hasRole(role1);
        relationType.hasRole(role2);
        relationType.hasRole(role3);
        mindmapsGraph.addRelation(relationType).
                putRolePlayer(role1, rolePlayer1).
                putRolePlayer(role2, rolePlayer2).
                putRolePlayer(role3, rolePlayer3);

        assertEquals(20, mindmapsGraph.getTinkerPopGraph().traversal().V().toList().size());
        assertEquals(35, mindmapsGraph.getTinkerPopGraph().traversal().E().toList().size());

        rolePlayer1.delete();

        assertNull(mindmapsGraph.getConcept("role-player1"));
        assertEquals(18, mindmapsGraph.getTinkerPopGraph().traversal().V().toList().size());
        assertEquals(27, mindmapsGraph.getTinkerPopGraph().traversal().E().toList().size());
    }

    @Test
    public void testDeleteConceptInstanceInRelationshipLastRolePlayer() throws ConceptException {
        EntityType type = mindmapsGraph.putEntityType("Concept Type");
        RelationType relationType = mindmapsGraph.putRelationType("relationType");
        RoleType role1 = mindmapsGraph.putRoleType("role1");
        RoleType role2 = mindmapsGraph.putRoleType("role2");
        RoleType role3 = mindmapsGraph.putRoleType("role3");
        Instance rolePlayer1 = mindmapsGraph.putEntity("role-player1", type);

        relationType.hasRole(role1);
        relationType.hasRole(role2);
        relationType.hasRole(role3);
        mindmapsGraph.addRelation(relationType).
                putRolePlayer(role1, rolePlayer1).
                putRolePlayer(role2, null).
                putRolePlayer(role3, null);

        long value = mindmapsGraph.getTinkerPopGraph().traversal().V().count().next();
        assertEquals(16, value);
        value = mindmapsGraph.getTinkerPopGraph().traversal().E().count().next();
        assertEquals(21, value);

        rolePlayer1.delete();

        assertNull(mindmapsGraph.getConcept("role-player1"));
        assertEquals(13, mindmapsGraph.getTinkerPopGraph().traversal().V().toList().size());
        assertEquals(16, mindmapsGraph.getTinkerPopGraph().traversal().E().toList().size());
    }

    @Test
    public void testRelationsAndPlayedRoleTypes(){
        EntityType entityType = mindmapsGraph.putEntityType("Concept Type");
        RelationType castSinging = mindmapsGraph.putRelationType("Acting Cast");
        RelationType castActing = mindmapsGraph.putRelationType("Singing Cast");
        RoleType feature = mindmapsGraph.putRoleType("Feature");
        RoleType musical = mindmapsGraph.putRoleType("Musical");
        RoleType actor = mindmapsGraph.putRoleType("Actor");
        RoleType singer = mindmapsGraph.putRoleType("Singer");
        Instance pacino = mindmapsGraph.putEntity("Pacino", entityType);
        Instance godfather = mindmapsGraph.putEntity("Godfather", entityType);
        Instance godfather2 = mindmapsGraph.putEntity("Godfather 2", entityType);
        Instance godfather3 = mindmapsGraph.putEntity("Godfather 3", entityType);
        Instance godfather4 = mindmapsGraph.putEntity("Godfather 4", entityType);

        castActing.hasRole(actor).hasRole(feature);
        castSinging.hasRole(singer).hasRole(musical);

        Relation relation1 = mindmapsGraph.addRelation(castActing).putRolePlayer(feature, godfather).putRolePlayer(actor, pacino);
        Relation relation2 = mindmapsGraph.addRelation(castActing).putRolePlayer(feature, godfather2).putRolePlayer(actor, pacino);
        Relation relation3 = mindmapsGraph.addRelation(castActing).putRolePlayer(feature, godfather3).putRolePlayer(actor, pacino);
        Relation relation4 = mindmapsGraph.addRelation(castActing).putRolePlayer(feature, godfather4).putRolePlayer(singer, pacino);

        assertEquals(4, pacino.relations().size());
        assertEquals(1, godfather.relations().size());
        assertEquals(1, godfather2.relations().size());
        assertEquals(1, godfather3.relations().size());
        assertEquals(1, godfather4.relations().size());
        assertEquals(3, pacino.relations(actor).size());
        assertEquals(1, pacino.relations(singer).size());
        assertEquals(4, pacino.relations(actor, singer).size());

        assertTrue(pacino.relations(actor).contains(relation1));
        assertTrue(pacino.relations(actor).contains(relation2));
        assertTrue(pacino.relations(actor).contains(relation3));
        assertFalse(pacino.relations(actor).contains(relation4));
        assertTrue(pacino.relations(singer).contains(relation4));

        assertEquals(2, pacino.playsRoles().size());
        assertEquals(1, godfather.playsRoles().size());
        assertEquals(1, godfather2.playsRoles().size());
        assertEquals(1, godfather3.playsRoles().size());
        assertEquals(1, godfather4.playsRoles().size());

        assertTrue(pacino.playsRoles().contains(actor));
        assertTrue(pacino.playsRoles().contains(singer));
    }

    @Test
    public void testResources(){
        EntityType randomThing = mindmapsGraph.putEntityType("A Thing");
        ResourceType resourceType = mindmapsGraph.putResourceType("A Resource Thing", Data.STRING);
        RelationType hasResource = mindmapsGraph.putRelationType("Has Resource");
        RoleType resourceRole = mindmapsGraph.putRoleType("Resource Role");
        RoleType actorRole = mindmapsGraph.putRoleType("Actor");
        Entity pacino = mindmapsGraph.putEntity("pacino", randomThing);
        Resource birthplace = mindmapsGraph.putResource("a place", resourceType);
        Resource age = mindmapsGraph.putResource("100", resourceType);
        Resource family = mindmapsGraph.putResource("people", resourceType);
        Resource birthDate = mindmapsGraph.putResource("10/10/10", resourceType);
        hasResource.hasRole(resourceRole).hasRole(actorRole);

        assertEquals(0, birthDate.ownerInstances().size());
        mindmapsGraph.addRelation(hasResource).putRolePlayer(actorRole, pacino).putRolePlayer(resourceRole, birthDate);
        mindmapsGraph.addRelation(hasResource).putRolePlayer(actorRole, pacino).putRolePlayer(resourceRole, birthplace);
        mindmapsGraph.addRelation(hasResource).putRolePlayer(actorRole, pacino).putRolePlayer(resourceRole, age);
        mindmapsGraph.addRelation(hasResource).putRolePlayer(actorRole, pacino).putRolePlayer(resourceRole, family);

        assertEquals(1, birthDate.ownerInstances().size());
        assertEquals(4, pacino.resources().size());
        assertTrue(pacino.resources().contains(birthDate));
        assertTrue(pacino.resources().contains(birthplace));
        assertTrue(pacino.resources().contains(age));
        assertTrue(pacino.resources().contains(family));
    }

    @Test
    public void testAutoGeneratedInstanceIds(){
        EntityType entityType = mindmapsGraph.putEntityType("A Thing");
        ResourceType resourceType = mindmapsGraph.putResourceType("A Resource Thing", Data.STRING);
        RelationType relationType = mindmapsGraph.putRelationType("Has Resource");
        RuleType ruleType = mindmapsGraph.putRuleType("Rule Type");

        Entity entity = mindmapsGraph.addEntity(entityType);
        Resource resource = mindmapsGraph.addResource(resourceType);
        Relation relation = mindmapsGraph.addRelation(relationType);
        Rule rule = mindmapsGraph.addRule(ruleType);

        assertTrue(entity.getId().startsWith(DataType.BaseType.ENTITY.name() + "-" + entity.type().getId() + "-"));
        assertTrue(resource.getId().startsWith(DataType.BaseType.RESOURCE.name() + "-" + resource.type().getId() + "-"));
        assertTrue(relation.getId().startsWith(DataType.BaseType.RELATION.name() + "-" + relation.type().getId() + "-"));
        assertTrue(rule.getId().startsWith(DataType.BaseType.RULE.name() + "-" + rule.type().getId() + "-"));
    }
}