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

package ai.grakn.graph.internal.concept;

import ai.grakn.concept.Attribute;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Label;
import ai.grakn.concept.Relation;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.Role;
import ai.grakn.concept.Thing;
import ai.grakn.exception.GraphOperationException;
import ai.grakn.exception.InvalidGraphException;
import ai.grakn.graph.internal.GraphTestBase;
import ai.grakn.graph.internal.structure.Casting;
import ai.grakn.util.Schema;
import org.junit.Test;

import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

public class EntityTest extends GraphTestBase {

    @Test
    public void whenGettingTypeOfEntity_ReturnEntityType(){
        EntityType entityType = graknGraph.putEntityType("Entiy Type");
        Entity entity = entityType.addEntity();
        assertEquals(entityType, entity.type());
    }

    @Test
    public void whenDeletingInstanceInRelationShip_TheInstanceAndCastingsAreDeletedAndTheRelationRemains() throws GraphOperationException{
        //Ontology
        EntityType type = graknGraph.putEntityType("Concept Type");
        RelationType relationType = graknGraph.putRelationType("relationTypes");
        Role role1 = graknGraph.putRole("role1");
        Role role2 = graknGraph.putRole("role2");
        Role role3 = graknGraph.putRole("role3");

        //Data
        ThingImpl<?, ?> rolePlayer1 = (ThingImpl) type.addEntity();
        ThingImpl<?, ?> rolePlayer2 = (ThingImpl) type.addEntity();
        ThingImpl<?, ?> rolePlayer3 = (ThingImpl) type.addEntity();

        relationType.relates(role1);
        relationType.relates(role2);
        relationType.relates(role3);

        //Check Structure is in order
        RelationImpl relation = (RelationImpl) relationType.addRelation().
                addRolePlayer(role1, rolePlayer1).
                addRolePlayer(role2, rolePlayer2).
                addRolePlayer(role3, rolePlayer3);

        Casting rp1 = rolePlayer1.castingsInstance().findAny().get();
        Casting rp2 = rolePlayer2.castingsInstance().findAny().get();
        Casting rp3 = rolePlayer3.castingsInstance().findAny().get();

        assertThat(relation.reified().get().castingsRelation().collect(Collectors.toSet()), containsInAnyOrder(rp1, rp2, rp3));

        //Delete And Check Again
        ConceptId idOfDeleted = rolePlayer1.getId();
        rolePlayer1.delete();

        assertNull(graknGraph.getConcept(idOfDeleted));
        assertThat(relation.reified().get().castingsRelation().collect(Collectors.toSet()), containsInAnyOrder(rp2, rp3));
    }

    @Test
    public void whenDeletingLastRolePlayerInRelation_TheRelationIsDeleted() throws GraphOperationException {
        EntityType type = graknGraph.putEntityType("Concept Type");
        RelationType relationType = graknGraph.putRelationType("relationTypes");
        Role role1 = graknGraph.putRole("role1");
        Thing rolePlayer1 = type.addEntity();

        Relation relation = relationType.addRelation().
                addRolePlayer(role1, rolePlayer1);

        assertNotNull(graknGraph.getConcept(relation.getId()));

        rolePlayer1.delete();

        assertNull(graknGraph.getConcept(relation.getId()));
    }

    @Test
    public void whenAddingResourceToAnEntity_EnsureTheImplicitStructureIsCreated(){
        Label resourceLabel = Label.of("A Attribute Thing");
        EntityType entityType = graknGraph.putEntityType("A Thing");
        AttributeType<String> attributeType = graknGraph.putAttributeType(resourceLabel, AttributeType.DataType.STRING);
        entityType.resource(attributeType);

        Entity entity = entityType.addEntity();
        Attribute attribute = attributeType.putAttribute("A attribute thing");

        entity.resource(attribute);
        Relation relation = entity.relations().iterator().next();

        checkImplicitStructure(attributeType, relation, entity, Schema.ImplicitType.HAS, Schema.ImplicitType.HAS_OWNER, Schema.ImplicitType.HAS_VALUE);
    }

    @Test
    public void whenAddingResourceToEntityWithoutAllowingItBetweenTypes_Throw(){
        EntityType entityType = graknGraph.putEntityType("A Thing");
        AttributeType<String> attributeType = graknGraph.putAttributeType("A Attribute Thing", AttributeType.DataType.STRING);

        Entity entity = entityType.addEntity();
        Attribute attribute = attributeType.putAttribute("A attribute thing");

        expectedException.expect(GraphOperationException.class);
        expectedException.expectMessage(GraphOperationException.hasNotAllowed(entity, attribute).getMessage());

        entity.resource(attribute);
    }

    @Test
    public void whenAddingMultipleResourcesToEntity_EnsureDifferentRelationsAreBuilt() throws InvalidGraphException {
        String resourceTypeId = "A Attribute Thing";
        EntityType entityType = graknGraph.putEntityType("A Thing");
        AttributeType<String> attributeType = graknGraph.putAttributeType(resourceTypeId, AttributeType.DataType.STRING);
        entityType.resource(attributeType);

        Entity entity = entityType.addEntity();
        Attribute attribute1 = attributeType.putAttribute("A resource thing");
        Attribute attribute2 = attributeType.putAttribute("Another resource thing");

        assertEquals(0, entity.relations().count());
        entity.resource(attribute1);
        assertEquals(1, entity.relations().count());
        entity.resource(attribute2);
        assertEquals(2, entity.relations().count());

        graknGraph.commit();
    }

    @Test
    public void checkKeyCreatesCorrectResourceStructure(){
        Label resourceLabel = Label.of("A Attribute Thing");
        EntityType entityType = graknGraph.putEntityType("A Thing");
        AttributeType<String> attributeType = graknGraph.putAttributeType(resourceLabel, AttributeType.DataType.STRING);
        entityType.key(attributeType);

        Entity entity = entityType.addEntity();
        Attribute attribute = attributeType.putAttribute("A attribute thing");

        entity.resource(attribute);
        Relation relation = entity.relations().iterator().next();

        checkImplicitStructure(attributeType, relation, entity, Schema.ImplicitType.KEY, Schema.ImplicitType.KEY_OWNER, Schema.ImplicitType.KEY_VALUE);
    }

    @Test
    public void whenCreatingAnEntityAndNotLinkingARequiredKey_Throw() throws InvalidGraphException {
        String resourceTypeId = "A Attribute Thing";
        EntityType entityType = graknGraph.putEntityType("A Thing");
        AttributeType<String> attributeType = graknGraph.putAttributeType(resourceTypeId, AttributeType.DataType.STRING);
        entityType.key(attributeType);

        Entity entity = entityType.addEntity();

        expectedException.expect(InvalidGraphException.class);

        graknGraph.commit();
    }

    private void checkImplicitStructure(AttributeType<?> attributeType, Relation relation, Entity entity, Schema.ImplicitType has, Schema.ImplicitType hasOwner, Schema.ImplicitType hasValue){
        assertEquals(2, relation.allRolePlayers().size());
        assertEquals(has.getLabel(attributeType.getLabel()), relation.type().getLabel());
        relation.allRolePlayers().entrySet().forEach(entry -> {
            Role role = entry.getKey();
            assertEquals(1, entry.getValue().size());
            entry.getValue().forEach(instance -> {
                if(instance.equals(entity)){
                    assertEquals(hasOwner.getLabel(attributeType.getLabel()), role.getLabel());
                } else {
                    assertEquals(hasValue.getLabel(attributeType.getLabel()), role.getLabel());
                }
            });
        });
    }

    @Test
    public void whenAddingEntity_EnsureInternalTypeIsTheSameAsRealType(){
        EntityType et = graknGraph.putEntityType("et");
        EntityImpl e = (EntityImpl) et.addEntity();
        assertEquals(et.getLabel(), e.getInternalType());
    }
}