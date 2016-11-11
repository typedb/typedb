/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs
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

import ai.grakn.concept.Entity;
import ai.grakn.concept.Instance;
import ai.grakn.concept.Relation;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.Resource;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.Rule;
import ai.grakn.concept.RuleType;
import ai.grakn.exception.ConceptException;
import ai.grakn.graql.Pattern;
import ai.grakn.util.Schema;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.RoleType;
import org.junit.Test;

import java.util.Set;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNull;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class EntityTest extends GraphTestBase{

    @Test
    public void testDeleteScope() throws ConceptException {
        EntityType entityType = graknGraph.putEntityType("entity type");
        RelationType relationType = graknGraph.putRelationType("RelationType");
        Instance scope = graknGraph.addEntity(entityType);
        Relation relation = graknGraph.addRelation(relationType);
        relation.scope(scope);
        scope.delete();
        assertNull(graknGraph.getConceptByBaseIdentifier(((ConceptImpl) scope).getBaseIdentifier()));
    }

    @Test
    public void testGetCastings(){
        RelationType relationType = graknGraph.putRelationType("rel type");
        EntityType entityType = graknGraph.putEntityType("entity type");
        InstanceImpl rolePlayer1 = (InstanceImpl) graknGraph.addEntity(entityType);
        assertEquals(0, rolePlayer1.getIncomingNeighbours(Schema.EdgeLabel.CASTING).size());

        RoleTypeImpl role = (RoleTypeImpl) graknGraph.putRoleType("Role");
        RoleTypeImpl role2 = (RoleTypeImpl) graknGraph.putRoleType("Role 2");
        Relation relation = graknGraph.addRelation(relationType);
        Relation relation2 = graknGraph.addRelation(relationType);
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
        RelationType relationType = graknGraph.putRelationType("relationType");
        RoleType role1 = graknGraph.putRoleType("role1");
        RoleType role2 = graknGraph.putRoleType("role2");
        RoleType role3 = graknGraph.putRoleType("role3");
        Instance rolePlayer1 = graknGraph.addEntity(type);
        Instance rolePlayer2 = graknGraph.addEntity(type);
        Instance rolePlayer3 = graknGraph.addEntity(type);

        relationType.hasRole(role1);
        relationType.hasRole(role2);
        relationType.hasRole(role3);
        graknGraph.addRelation(relationType).
                putRolePlayer(role1, rolePlayer1).
                putRolePlayer(role2, rolePlayer2).
                putRolePlayer(role3, rolePlayer3);

        assertEquals(20, graknGraph.getTinkerPopGraph().traversal().V().toList().size());
        assertEquals(34, graknGraph.getTinkerPopGraph().traversal().E().toList().size());

        rolePlayer1.delete();

        assertNull(graknGraph.getConcept("role-player1"));
        assertEquals(18, graknGraph.getTinkerPopGraph().traversal().V().toList().size());
        assertEquals(26, graknGraph.getTinkerPopGraph().traversal().E().toList().size());
    }

    @Test
    public void testDeleteConceptInstanceInRelationshipLastRolePlayer() throws ConceptException {
        EntityType type = graknGraph.putEntityType("Concept Type");
        RelationType relationType = graknGraph.putRelationType("relationType");
        RoleType role1 = graknGraph.putRoleType("role1");
        RoleType role2 = graknGraph.putRoleType("role2");
        RoleType role3 = graknGraph.putRoleType("role3");
        Instance rolePlayer1 = graknGraph.addEntity(type);

        relationType.hasRole(role1);
        relationType.hasRole(role2);
        relationType.hasRole(role3);
        graknGraph.addRelation(relationType).
                putRolePlayer(role1, rolePlayer1).
                putRolePlayer(role2, null).
                putRolePlayer(role3, null);

        long value = graknGraph.getTinkerPopGraph().traversal().V().count().next();
        assertEquals(16, value);
        value = graknGraph.getTinkerPopGraph().traversal().E().count().next();
        assertEquals(20, value);

        rolePlayer1.delete();

        assertNull(graknGraph.getConcept("role-player1"));
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
        Instance pacino = graknGraph.addEntity(entityType);
        Instance godfather = graknGraph.addEntity(entityType);
        Instance godfather2 = graknGraph.addEntity(entityType);
        Instance godfather3 = graknGraph.addEntity(entityType);
        Instance godfather4 = graknGraph.addEntity(entityType);

        castActing.hasRole(actor).hasRole(feature);
        castSinging.hasRole(singer).hasRole(musical);

        Relation relation1 = graknGraph.addRelation(castActing).putRolePlayer(feature, godfather).putRolePlayer(actor, pacino);
        Relation relation2 = graknGraph.addRelation(castActing).putRolePlayer(feature, godfather2).putRolePlayer(actor, pacino);
        Relation relation3 = graknGraph.addRelation(castActing).putRolePlayer(feature, godfather3).putRolePlayer(actor, pacino);
        Relation relation4 = graknGraph.addRelation(castActing).putRolePlayer(feature, godfather4).putRolePlayer(singer, pacino);

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
        ResourceType resourceType = graknGraph.putResourceType("A Resource Thing", ResourceType.DataType.STRING);
        ResourceType resourceType2 = graknGraph.putResourceType("A Resource Thing 2", ResourceType.DataType.STRING);

        RelationType hasResource = graknGraph.putRelationType("Has Resource");

        RoleType resourceRole = graknGraph.putRoleType("Resource Role");
        RoleType actorRole = graknGraph.putRoleType("Actor");

        Entity pacino = graknGraph.addEntity(randomThing);
        Resource birthplace = graknGraph.putResource("a place", resourceType);
        Resource age = graknGraph.putResource("100", resourceType);
        Resource family = graknGraph.putResource("people", resourceType);
        Resource birthDate = graknGraph.putResource("10/10/10", resourceType);
        hasResource.hasRole(resourceRole).hasRole(actorRole);

        Resource randomResource = graknGraph.putResource("Random 1", resourceType2);
        Resource randomResource2 = graknGraph.putResource("Random 2", resourceType2);

        assertEquals(0, birthDate.ownerInstances().size());
        graknGraph.addRelation(hasResource).putRolePlayer(actorRole, pacino).putRolePlayer(resourceRole, birthDate);
        graknGraph.addRelation(hasResource).putRolePlayer(actorRole, pacino).putRolePlayer(resourceRole, birthplace);
        graknGraph.addRelation(hasResource).putRolePlayer(actorRole, pacino).putRolePlayer(resourceRole, age);
        graknGraph.addRelation(hasResource).putRolePlayer(actorRole, pacino).putRolePlayer(resourceRole, family);

        graknGraph.addRelation(hasResource).putRolePlayer(actorRole, pacino).putRolePlayer(resourceRole, randomResource);
        graknGraph.addRelation(hasResource).putRolePlayer(actorRole, pacino).putRolePlayer(resourceRole, randomResource2);

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
    public void testAutoGeneratedInstanceIds(){
        Pattern lhs = graknGraph.graql().parsePattern("$x isa entity-type");
        Pattern rhs = graknGraph.graql().parsePattern("$x isa entity-type");
        EntityType entityType = graknGraph.putEntityType("A Thing");
        ResourceType resourceType = graknGraph.putResourceType("A Resource Thing", ResourceType.DataType.STRING);
        RelationType relationType = graknGraph.putRelationType("Has Resource");
        RuleType ruleType = graknGraph.putRuleType("Rule Type");

        Entity entity = graknGraph.addEntity(entityType);
        Resource resource = graknGraph.putResource("A resource thing", resourceType);
        Relation relation = graknGraph.addRelation(relationType);
        Rule rule = graknGraph.addRule(lhs, rhs, ruleType);

        assertTrue(entity.getId().startsWith(Schema.BaseType.ENTITY.name() + "-" + entity.type().getId() + "-"));
        assertTrue(resource.getId().startsWith(Schema.BaseType.RESOURCE.name() + "-" + resource.type().getId() + "-"));
        assertTrue(relation.getId().startsWith(Schema.BaseType.RELATION.name() + "-" + relation.type().getId() + "-"));
        assertTrue(rule.getId().startsWith(Schema.BaseType.RULE.name() + "-" + rule.type().getId() + "-"));
    }

    @Test
    public void testHasResource(){
        String resourceTypeId = "A Resource Thing";
        EntityType entityType = graknGraph.putEntityType("A Thing");
        ResourceType resourceType = graknGraph.putResourceType(resourceTypeId, ResourceType.DataType.STRING);
        entityType.hasResource(resourceType);

        Entity entity = graknGraph.addEntity(entityType);
        Resource resource = graknGraph.putResource("A resource thing", resourceType);

        Relation relation = entity.hasResource(resource);
        assertEquals(Schema.Resource.HAS_RESOURCE.getId(resourceTypeId), relation.type().getId());

        relation.rolePlayers().entrySet().forEach(entry -> {
            RoleType roleType = entry.getKey();
            Instance instance = entry.getValue();

            if(instance.equals(entity)){
                assertEquals(Schema.Resource.HAS_RESOURCE_OWNER.getId(resourceTypeId), roleType.getId());
            } else {
                assertEquals(Schema.Resource.HAS_RESOURCE_VALUE.getId(resourceTypeId), roleType.getId());
            }
        });
    }
}