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

import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Instance;
import ai.grakn.concept.Relation;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.Resource;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.RoleType;
import ai.grakn.concept.TypeName;
import ai.grakn.exception.ConceptException;
import ai.grakn.exception.GraknValidationException;
import ai.grakn.exception.GraphRuntimeException;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.Schema;
import org.junit.Test;

import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class EntityTest extends GraphTestBase{

    @Test
    public void testDeleteScope() throws ConceptException {
        EntityType entityType = graknGraph.putEntityType("entity type");
        RelationType relationType = graknGraph.putRelationType("RelationType");
        Instance scope = entityType.addEntity();
        Relation relation = relationType.addRelation();
        relation.scope(scope);
        scope.delete();
        assertNull(graknGraph.getConceptByBaseIdentifier(((ConceptImpl) scope).getBaseIdentifier()));
    }

    @Test
    public void testGetCastings(){
        RelationType relationType = graknGraph.putRelationType("rel type");
        EntityType entityType = graknGraph.putEntityType("entity type");
        InstanceImpl<?, ?> rolePlayer1 = (InstanceImpl) entityType.addEntity();
        assertEquals(0, rolePlayer1.getIncomingNeighbours(Schema.EdgeLabel.CASTING).size());

        RoleTypeImpl role = (RoleTypeImpl) graknGraph.putRoleType("Role");
        RoleTypeImpl role2 = (RoleTypeImpl) graknGraph.putRoleType("Role 2");
        Relation relation = relationType.addRelation();
        Relation relation2 = relationType.addRelation();
        CastingImpl casting1 = graknGraph.putCasting(role, rolePlayer1, (RelationImpl) relation);
        CastingImpl casting2 = graknGraph.putCasting(role2, rolePlayer1, (RelationImpl) relation2);

        Set<ConceptImpl> castings = rolePlayer1.getIncomingNeighbours(Schema.EdgeLabel.ROLE_PLAYER);

        assertEquals(2, castings.size());
        assertTrue(castings.contains(casting1));
        assertTrue(castings.contains(casting2));
        assertNotEquals(casting1, casting2);
    }

    @Test
    public void testDeleteConceptInstanceInRelationship() throws ConceptException{
        //Build
        EntityType type = graknGraph.putEntityType("Concept Type");
        RelationType relationType = graknGraph.putRelationType("relationTypes");
        RoleType role1 = graknGraph.putRoleType("role1");
        RoleType role2 = graknGraph.putRoleType("role2");
        RoleType role3 = graknGraph.putRoleType("role3");
        Instance rolePlayer1 = type.addEntity();
        Instance rolePlayer2 = type.addEntity();
        Instance rolePlayer3 = type.addEntity();

        relationType.hasRole(role1);
        relationType.hasRole(role2);
        relationType.hasRole(role3);
        relationType.addRelation().
                putRolePlayer(role1, rolePlayer1).
                putRolePlayer(role2, rolePlayer2).
                putRolePlayer(role3, rolePlayer3);

        assertEquals(20, graknGraph.getTinkerPopGraph().traversal().V().toList().size());
        assertEquals(34, graknGraph.getTinkerPopGraph().traversal().E().toList().size());

        ConceptId idOfDeleted = rolePlayer1.getId();
        rolePlayer1.delete();

        assertNull(graknGraph.getConcept(idOfDeleted));
        assertEquals(18, graknGraph.getTinkerPopGraph().traversal().V().toList().size());
        assertEquals(26, graknGraph.getTinkerPopGraph().traversal().E().toList().size());
    }

    @Test
    public void testDeleteConceptInstanceInRelationshipLastRolePlayer() throws ConceptException {
        EntityType type = graknGraph.putEntityType("Concept Type");
        RelationType relationType = graknGraph.putRelationType("relationTypes");
        RoleType role1 = graknGraph.putRoleType("role1");
        RoleType role2 = graknGraph.putRoleType("role2");
        RoleType role3 = graknGraph.putRoleType("role3");
        Instance rolePlayer1 = type.addEntity();

        relationType.hasRole(role1);
        relationType.hasRole(role2);
        relationType.hasRole(role3);
        relationType.addRelation().
                putRolePlayer(role1, rolePlayer1).
                putRolePlayer(role2, null).
                putRolePlayer(role3, null);

        long value = graknGraph.getTinkerPopGraph().traversal().V().count().next();
        assertEquals(16, value);
        value = graknGraph.getTinkerPopGraph().traversal().E().count().next();
        assertEquals(20, value);

        ConceptId idOfDeleted = rolePlayer1.getId();
        rolePlayer1.delete();

        assertNull(graknGraph.getConcept(idOfDeleted));
        assertEquals(13, graknGraph.getTinkerPopGraph().traversal().V().toList().size());
        assertEquals(15, graknGraph.getTinkerPopGraph().traversal().E().toList().size());
    }

    @Test
    public void testRelationsAndPlayedRoleTypes(){
        EntityType entityType = graknGraph.putEntityType("Concept Type");
        RelationType castSinging = graknGraph.putRelationType("Acting Cast");
        RelationType castActing = graknGraph.putRelationType("Singing Cast");
        RoleType feature = graknGraph.putRoleType("Feature");
        RoleType musical = graknGraph.putRoleType("Musical");
        RoleType actor = graknGraph.putRoleType("Actor");
        RoleType singer = graknGraph.putRoleType("Singer");
        Instance pacino = entityType.addEntity();
        Instance godfather = entityType.addEntity();
        Instance godfather2 = entityType.addEntity();
        Instance godfather3 = entityType.addEntity();
        Instance godfather4 = entityType.addEntity();

        castActing.hasRole(actor).hasRole(feature);
        castSinging.hasRole(singer).hasRole(musical);

        Relation relation1 = castActing.addRelation().putRolePlayer(feature, godfather).putRolePlayer(actor, pacino);
        Relation relation2 = castActing.addRelation().putRolePlayer(feature, godfather2).putRolePlayer(actor, pacino);
        Relation relation3 = castActing.addRelation().putRolePlayer(feature, godfather3).putRolePlayer(actor, pacino);
        Relation relation4 = castActing.addRelation().putRolePlayer(feature, godfather4).putRolePlayer(singer, pacino);

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
        EntityType randomThing = graknGraph.putEntityType("A Thing");
        ResourceType<String> resourceType = graknGraph.putResourceType("A Resource Thing", ResourceType.DataType.STRING);
        ResourceType<String> resourceType2 = graknGraph.putResourceType("A Resource Thing 2", ResourceType.DataType.STRING);

        RelationType hasResource = graknGraph.putRelationType("Has Resource");

        RoleType resourceRole = graknGraph.putRoleType("Resource Role");
        RoleType actorRole = graknGraph.putRoleType("Actor");

        Entity pacino = randomThing.addEntity();
        Resource birthplace = resourceType.putResource("a place");
        Resource age = resourceType.putResource("100");
        Resource family = resourceType.putResource("people");
        Resource birthDate = resourceType.putResource("10/10/10");
        hasResource.hasRole(resourceRole).hasRole(actorRole);

        Resource randomResource = resourceType2.putResource("Random 1");
        Resource randomResource2 = resourceType2.putResource("Random 2");

        assertEquals(0, birthDate.ownerInstances().size());
        hasResource.addRelation().putRolePlayer(actorRole, pacino).putRolePlayer(resourceRole, birthDate);
        hasResource.addRelation().putRolePlayer(actorRole, pacino).putRolePlayer(resourceRole, birthplace);
        hasResource.addRelation().putRolePlayer(actorRole, pacino).putRolePlayer(resourceRole, age);
        hasResource.addRelation().putRolePlayer(actorRole, pacino).putRolePlayer(resourceRole, family);

        hasResource.addRelation().putRolePlayer(actorRole, pacino).putRolePlayer(resourceRole, randomResource);
        hasResource.addRelation().putRolePlayer(actorRole, pacino).putRolePlayer(resourceRole, randomResource2);

        assertEquals(1, birthDate.ownerInstances().size());
        assertEquals(6, pacino.resources().size());
        assertTrue(pacino.resources().contains(birthDate));
        assertTrue(pacino.resources().contains(birthplace));
        assertTrue(pacino.resources().contains(age));
        assertTrue(pacino.resources().contains(family));
        assertTrue(pacino.resources().contains(randomResource));
        assertTrue(pacino.resources().contains(randomResource2));

        assertEquals(2, pacino.resources(resourceType2).size());
        assertTrue(pacino.resources(resourceType2).contains(randomResource));
        assertTrue(pacino.resources(resourceType2).contains(randomResource2));
    }

    @Test
    public void testHasResource(){
        TypeName resourceTypeName = TypeName.of("A Resource Thing");
        EntityType entityType = graknGraph.putEntityType("A Thing");
        ResourceType<String> resourceType = graknGraph.putResourceType(resourceTypeName, ResourceType.DataType.STRING);
        entityType.hasResource(resourceType);

        Entity entity = entityType.addEntity();
        Resource resource = resourceType.putResource("A resource thing");

        Relation relation = entity.hasResource(resource);
        assertEquals(Schema.Resource.HAS_RESOURCE.getName(resourceTypeName), relation.type().getName());

        relation.rolePlayers().entrySet().forEach(entry -> {
            RoleType roleType = entry.getKey();
            Instance instance = entry.getValue();

            if(instance.equals(entity)){
                assertEquals(Schema.Resource.HAS_RESOURCE_OWNER.getName(resourceTypeName), roleType.getName());
            } else {
                assertEquals(Schema.Resource.HAS_RESOURCE_VALUE.getName(resourceTypeName), roleType.getName());
            }
        });
    }

    @Test
    public void testHasResourceWithNoSchema(){
        EntityType entityType = graknGraph.putEntityType("A Thing");
        ResourceType<String> resourceType = graknGraph.putResourceType("A Resource Thing", ResourceType.DataType.STRING);

        Entity entity = entityType.addEntity();
        Resource resource = resourceType.putResource("A resource thing");

        expectedException.expect(GraphRuntimeException.class);
        expectedException.expectMessage(
                ErrorMessage.HAS_RESOURCE_INVALID.getMessage(entityType.getName(), resourceType.getName())
        );

        entity.hasResource(resource);
    }

    @Test
    public void testKey(){
        TypeName resourceTypeName = TypeName.of("A Resource Thing");
        EntityType entityType = graknGraph.putEntityType("A Thing");
        ResourceType<String> resourceType = graknGraph.putResourceType(resourceTypeName, ResourceType.DataType.STRING);
        entityType.key(resourceType);

        Entity entity = entityType.addEntity();
        Resource resource = resourceType.putResource("A resource thing");

        Relation relation = entity.hasResource(resource);
        assertEquals(Schema.Resource.HAS_RESOURCE.getName(resourceTypeName), relation.type().getName());

        relation.rolePlayers().entrySet().forEach(entry -> {
            RoleType roleType = entry.getKey();
            Instance instance = entry.getValue();

            if(instance.equals(entity)){
                assertEquals(Schema.Resource.HAS_RESOURCE_OWNER.getName(resourceTypeName), roleType.getName());
            } else {
                assertEquals(Schema.Resource.HAS_RESOURCE_VALUE.getName(resourceTypeName), roleType.getName());
            }
        });
    }

    @Test
    public void testMultipleResources() throws GraknValidationException {
        String resourceTypeId = "A Resource Thing";
        EntityType entityType = graknGraph.putEntityType("A Thing");
        ResourceType<String> resourceType = graknGraph.putResourceType(resourceTypeId, ResourceType.DataType.STRING);
        entityType.hasResource(resourceType);

        Entity entity = entityType.addEntity();
        Resource resource1 = resourceType.putResource("A resource thing");
        Resource resource2 = resourceType.putResource("Another resource thing");

        Relation relation1 = entity.hasResource(resource1);
        Relation relation2 = entity.hasResource(resource2);

        assertNotEquals(relation1, relation2);

        graknGraph.validateGraph();
    }

    @Test
    public void testMultipleKeys() throws GraknValidationException {
        String resourceTypeId = "A Resource Thing";
        EntityType entityType = graknGraph.putEntityType("A Thing");
        ResourceType<String> resourceType = graknGraph.putResourceType(resourceTypeId, ResourceType.DataType.STRING);
        entityType.key(resourceType);

        Entity entity = entityType.addEntity();
        Resource resource1 = resourceType.putResource("A resource thing");
        Resource resource2 = resourceType.putResource("Another resource thing");

        Relation relation1 = entity.hasResource(resource1);
        Relation relation2 = entity.hasResource(resource2);

        assertNotEquals(relation1, relation2);

        expectedException.expect(GraknValidationException.class);
        graknGraph.validateGraph();
    }

    @Test
    public void testNoKey() throws GraknValidationException {
        String resourceTypeId = "A Resource Thing";
        EntityType entityType = graknGraph.putEntityType("A Thing");
        ResourceType<String> resourceType = graknGraph.putResourceType(resourceTypeId, ResourceType.DataType.STRING);
        entityType.key(resourceType);

        entityType.addEntity();

        expectedException.expect(GraknValidationException.class);
        graknGraph.validateGraph();
    }
}