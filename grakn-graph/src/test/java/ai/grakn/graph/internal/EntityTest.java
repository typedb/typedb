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
import ai.grakn.concept.TypeLabel;
import ai.grakn.exception.ConceptException;
import ai.grakn.exception.GraknValidationException;
import ai.grakn.exception.GraphRuntimeException;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.Schema;
import org.junit.Test;

import java.util.Set;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

public class EntityTest extends GraphTestBase{

    @Test
    public void whenGettingTypeOfEntity_ReturnEntityType(){
        EntityType entityType = graknGraph.putEntityType("Entiy Type");
        Entity entity = entityType.addEntity();
        assertEquals(entityType, entity.type());
    }

    @Test
    public void whenDeletingInstanceInRelationShip_TheInstanceAndCastingsAreDeleteTheRelationRemains() throws ConceptException{
        //Ontology
        EntityType type = graknGraph.putEntityType("Concept Type");
        RelationType relationType = graknGraph.putRelationType("relationTypes");
        RoleType role1 = graknGraph.putRoleType("role1");
        RoleType role2 = graknGraph.putRoleType("role2");
        RoleType role3 = graknGraph.putRoleType("role3");

        //Data
        Instance rolePlayer1 = type.addEntity();
        Instance rolePlayer2 = type.addEntity();
        Instance rolePlayer3 = type.addEntity();

        relationType.relates(role1);
        relationType.relates(role2);
        relationType.relates(role3);

        //Check Structure is in order
        RelationImpl relation = (RelationImpl) relationType.addRelation().
                addRolePlayer(role1, rolePlayer1).
                addRolePlayer(role2, rolePlayer2).
                addRolePlayer(role3, rolePlayer3);

        CastingImpl casting1 = ((InstanceImpl<?, ?>) rolePlayer1).castings().iterator().next();
        CastingImpl casting2 = ((InstanceImpl<?, ?>) rolePlayer2).castings().iterator().next();
        CastingImpl casting3 = ((InstanceImpl<?, ?>) rolePlayer3).castings().iterator().next();
        Set<CastingImpl> castings = relation.getMappingCasting();

        assertThat(relation.getMappingCasting(), containsInAnyOrder(casting1, casting2, casting3));

        //Delete And Check Again
        ConceptId idOfDeleted = rolePlayer1.getId();
        rolePlayer1.delete();

        assertNull(graknGraph.getConcept(idOfDeleted));
        assertThat(relation.getMappingCasting(), containsInAnyOrder(casting2, casting3));
    }

    @Test
    public void whenDeletingLastRolePlayerInRelation_TheRelationIsDeleted() throws ConceptException {
        EntityType type = graknGraph.putEntityType("Concept Type");
        RelationType relationType = graknGraph.putRelationType("relationTypes");
        RoleType role1 = graknGraph.putRoleType("role1");
        RoleType role2 = graknGraph.putRoleType("role2");
        Instance rolePlayer1 = type.addEntity();

        Relation relation = relationType.addRelation().
                addRolePlayer(role1, rolePlayer1).
                addRolePlayer(role2, null);

        assertNotNull(graknGraph.getConcept(relation.getId()));

        rolePlayer1.delete();

        assertNull(graknGraph.getConcept(relation.getId()));
    }

    @Test
    public void whenAddingResourceToAnEntity_EnsureTheImplicitStructureIsCreated(){
        TypeLabel resourceTypeLabel = TypeLabel.of("A Resource Thing");
        EntityType entityType = graknGraph.putEntityType("A Thing");
        ResourceType<String> resourceType = graknGraph.putResourceType(resourceTypeLabel, ResourceType.DataType.STRING);
        entityType.resource(resourceType);

        Entity entity = entityType.addEntity();
        Resource resource = resourceType.putResource("A resource thing");

        entity.resource(resource);
        Relation relation = entity.relations().iterator().next();

        checkImplicitStructure(resourceType, relation, entity, Schema.ImplicitType.HAS, Schema.ImplicitType.HAS_OWNER, Schema.ImplicitType.HAS_VALUE);
    }

    @Test
    public void whenAddingResourceToEntityWitoutAllowingItBetweenTypes_Throw(){
        EntityType entityType = graknGraph.putEntityType("A Thing");
        ResourceType<String> resourceType = graknGraph.putResourceType("A Resource Thing", ResourceType.DataType.STRING);

        Entity entity = entityType.addEntity();
        Resource resource = resourceType.putResource("A resource thing");

        expectedException.expect(GraphRuntimeException.class);
        expectedException.expectMessage(
                ErrorMessage.HAS_INVALID.getMessage(entityType.getLabel(), "resource", resourceType.getLabel())
        );

        entity.resource(resource);
    }

    @Test
    public void whenAddingMultipleResourcesToEntity_EnsureDifferentRelationsAreBuilt() throws GraknValidationException {
        String resourceTypeId = "A Resource Thing";
        EntityType entityType = graknGraph.putEntityType("A Thing");
        ResourceType<String> resourceType = graknGraph.putResourceType(resourceTypeId, ResourceType.DataType.STRING);
        entityType.resource(resourceType);

        Entity entity = entityType.addEntity();
        Resource resource1 = resourceType.putResource("A resource thing");
        Resource resource2 = resourceType.putResource("Another resource thing");

        assertEquals(0, entity.relations().size());
        entity.resource(resource1);
        assertEquals(1, entity.relations().size());
        entity.resource(resource2);
        assertEquals(2, entity.relations().size());

        graknGraph.validateGraph();
    }

    @Test
    public void checkKeyCreatesCorrectResourceStructure(){
        TypeLabel resourceTypeLabel = TypeLabel.of("A Resource Thing");
        EntityType entityType = graknGraph.putEntityType("A Thing");
        ResourceType<String> resourceType = graknGraph.putResourceType(resourceTypeLabel, ResourceType.DataType.STRING);
        entityType.key(resourceType);

        Entity entity = entityType.addEntity();
        Resource resource = resourceType.putResource("A resource thing");

        entity.resource(resource);
        Relation relation = entity.relations().iterator().next();

        checkImplicitStructure(resourceType, relation, entity, Schema.ImplicitType.KEY, Schema.ImplicitType.KEY_OWNER, Schema.ImplicitType.KEY_VALUE);
    }

    @Test
    public void whenMissingKeyOnEntity_Throw() throws GraknValidationException {
        String resourceTypeId = "A Resource Thing";
        EntityType entityType = graknGraph.putEntityType("A Thing");
        ResourceType<String> resourceType = graknGraph.putResourceType(resourceTypeId, ResourceType.DataType.STRING);
        entityType.key(resourceType);

        entityType.addEntity();

        expectedException.expect(GraknValidationException.class);
        graknGraph.validateGraph();
    }

    private void checkImplicitStructure(ResourceType<?> resourceType, Relation relation, Entity entity, Schema.ImplicitType has, Schema.ImplicitType hasOwner, Schema.ImplicitType hasValue){
        assertEquals(2, relation.allRolePlayers().size());
        assertEquals(has.getLabel(resourceType.getLabel()), relation.type().getLabel());
        relation.allRolePlayers().entrySet().forEach(entry -> {
            RoleType roleType = entry.getKey();
            assertEquals(1, entry.getValue().size());
            entry.getValue().forEach(instance -> {
                if(instance.equals(entity)){
                    assertEquals(hasOwner.getLabel(resourceType.getLabel()), roleType.getLabel());
                } else {
                    assertEquals(hasValue.getLabel(resourceType.getLabel()), roleType.getLabel());
                }
            });
        });
    }

    @Test
    public void whenDeletingEntityInRelations_DeleteAllImplicitRelations(){
        RoleType role1 = graknGraph.putRoleType("Role 1");
        RoleType role2 = graknGraph.putRoleType("Role 2");
        RelationType relationType = graknGraph.putRelationType("A Relation Type Thing").relates(role1).relates(role2);
        EntityType entityType = graknGraph.putEntityType("A Thing").plays(role1).plays(role2);
        ResourceType<String> resourceType = graknGraph.putResourceType("A Resource Thing", ResourceType.DataType.STRING);
        entityType.resource(resourceType);

        Entity entityToDelete = entityType.addEntity();
        Entity entityOther = entityType.addEntity();
        Resource<String> resource1 = resourceType.putResource("1");
        Resource<String> resource2 = resourceType.putResource("2");
        Resource<String> resource3 = resourceType.putResource("3");

        //Create Implicit Relations
        entityToDelete.resource(resource1);
        entityToDelete.resource(resource2);
        entityToDelete.resource(resource3);

        //Create Explicit Relation
        relationType.addRelation().addRolePlayer(role1, entityToDelete).addRolePlayer(role2, entityOther);

        //Check Relation Counts
        RelationType implicitRelationType = graknGraph.getRelationType(Schema.ImplicitType.HAS.getLabel(resourceType.getLabel()).getValue());
        assertEquals(1, relationType.instances().size());
        assertEquals(3, implicitRelationType.instances().size());

        entityToDelete.delete();

        assertEquals(1, relationType.instances().size());
        assertEquals(0, implicitRelationType.instances().size());
    }
}