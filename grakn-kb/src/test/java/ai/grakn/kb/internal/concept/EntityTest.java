/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
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

package ai.grakn.kb.internal.concept;

import ai.grakn.concept.Attribute;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Label;
import ai.grakn.concept.Relationship;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Role;
import ai.grakn.concept.Thing;
import ai.grakn.exception.GraknTxOperationException;
import ai.grakn.exception.InvalidKBException;
import ai.grakn.kb.internal.TxTestBase;
import ai.grakn.kb.internal.structure.Casting;
import ai.grakn.util.Schema;
import org.junit.Test;

import java.util.stream.Collectors;

import static java.util.stream.Collectors.toSet;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class EntityTest extends TxTestBase {

    @Test
    public void whenGettingTypeOfEntity_ReturnEntityType(){
        EntityType entityType = tx.putEntityType("Entiy Type");
        Entity entity = entityType.addEntity();
        assertEquals(entityType, entity.type());
    }

    @Test
    public void whenDeletingInstanceInRelationShip_TheInstanceAndCastingsAreDeletedAndTheRelationRemains() throws GraknTxOperationException {
        //Schema
        EntityType type = tx.putEntityType("Concept Type");
        RelationshipType relationshipType = tx.putRelationshipType("relationTypes");
        Role role1 = tx.putRole("role1");
        Role role2 = tx.putRole("role2");
        Role role3 = tx.putRole("role3");

        //Data
        ThingImpl<?, ?> rolePlayer1 = (ThingImpl) type.addEntity();
        ThingImpl<?, ?> rolePlayer2 = (ThingImpl) type.addEntity();
        ThingImpl<?, ?> rolePlayer3 = (ThingImpl) type.addEntity();

        relationshipType.relates(role1);
        relationshipType.relates(role2);
        relationshipType.relates(role3);

        //Check Structure is in order
        RelationshipImpl relation = (RelationshipImpl) relationshipType.addRelationship().
                addRolePlayer(role1, rolePlayer1).
                addRolePlayer(role2, rolePlayer2).
                addRolePlayer(role3, rolePlayer3);

        Casting rp1 = rolePlayer1.castingsInstance().findAny().get();
        Casting rp2 = rolePlayer2.castingsInstance().findAny().get();
        Casting rp3 = rolePlayer3.castingsInstance().findAny().get();

        assertThat(relation.reified().get().castingsRelation().collect(toSet()), containsInAnyOrder(rp1, rp2, rp3));

        //Delete And Check Again
        ConceptId idOfDeleted = rolePlayer1.getId();
        rolePlayer1.delete();

        assertNull(tx.getConcept(idOfDeleted));
        assertThat(relation.reified().get().castingsRelation().collect(toSet()), containsInAnyOrder(rp2, rp3));
    }

    @Test
    public void whenDeletingLastRolePlayerInRelation_TheRelationIsDeleted() throws GraknTxOperationException {
        EntityType type = tx.putEntityType("Concept Type");
        RelationshipType relationshipType = tx.putRelationshipType("relationTypes");
        Role role1 = tx.putRole("role1");
        Thing rolePlayer1 = type.addEntity();

        Relationship relationship = relationshipType.addRelationship().
                addRolePlayer(role1, rolePlayer1);

        assertNotNull(tx.getConcept(relationship.getId()));

        rolePlayer1.delete();

        assertNull(tx.getConcept(relationship.getId()));
    }

    @Test
    public void whenAddingResourceToAnEntity_EnsureTheImplicitStructureIsCreated(){
        Label resourceLabel = Label.of("A Attribute Thing");
        EntityType entityType = tx.putEntityType("A Thing");
        AttributeType<String> attributeType = tx.putAttributeType(resourceLabel, AttributeType.DataType.STRING);
        entityType.attribute(attributeType);

        Entity entity = entityType.addEntity();
        Attribute attribute = attributeType.putAttribute("A attribute thing");

        entity.attribute(attribute);
        Relationship relationship = entity.relationships().iterator().next();

        checkImplicitStructure(attributeType, relationship, entity, Schema.ImplicitType.HAS, Schema.ImplicitType.HAS_OWNER, Schema.ImplicitType.HAS_VALUE);
    }

    @Test
    public void whenAddingResourceToEntityWithoutAllowingItBetweenTypes_Throw(){
        EntityType entityType = tx.putEntityType("A Thing");
        AttributeType<String> attributeType = tx.putAttributeType("A Attribute Thing", AttributeType.DataType.STRING);

        Entity entity = entityType.addEntity();
        Attribute attribute = attributeType.putAttribute("A attribute thing");

        expectedException.expect(GraknTxOperationException.class);
        expectedException.expectMessage(GraknTxOperationException.hasNotAllowed(entity, attribute).getMessage());

        entity.attribute(attribute);
    }

    @Test
    public void whenAddingMultipleResourcesToEntity_EnsureDifferentRelationsAreBuilt() throws InvalidKBException {
        String resourceTypeId = "A Attribute Thing";
        EntityType entityType = tx.putEntityType("A Thing");
        AttributeType<String> attributeType = tx.putAttributeType(resourceTypeId, AttributeType.DataType.STRING);
        entityType.attribute(attributeType);

        Entity entity = entityType.addEntity();
        Attribute attribute1 = attributeType.putAttribute("A resource thing");
        Attribute attribute2 = attributeType.putAttribute("Another resource thing");

        assertEquals(0, entity.relationships().count());
        entity.attribute(attribute1);
        assertEquals(1, entity.relationships().count());
        entity.attribute(attribute2);
        assertEquals(2, entity.relationships().count());

        tx.commit();
    }

    @Test
    public void checkKeyCreatesCorrectResourceStructure(){
        Label resourceLabel = Label.of("A Attribute Thing");
        EntityType entityType = tx.putEntityType("A Thing");
        AttributeType<String> attributeType = tx.putAttributeType(resourceLabel, AttributeType.DataType.STRING);
        entityType.key(attributeType);

        Entity entity = entityType.addEntity();
        Attribute attribute = attributeType.putAttribute("A attribute thing");

        entity.attribute(attribute);
        Relationship relationship = entity.relationships().iterator().next();

        checkImplicitStructure(attributeType, relationship, entity, Schema.ImplicitType.KEY, Schema.ImplicitType.KEY_OWNER, Schema.ImplicitType.KEY_VALUE);
    }

    @Test
    public void whenCreatingAnEntityAndNotLinkingARequiredKey_Throw() throws InvalidKBException {
        String resourceTypeId = "A Attribute Thing";
        EntityType entityType = tx.putEntityType("A Thing");
        AttributeType<String> attributeType = tx.putAttributeType(resourceTypeId, AttributeType.DataType.STRING);
        entityType.key(attributeType);

        entityType.addEntity();

        expectedException.expect(InvalidKBException.class);

        tx.commit();
    }

    private void checkImplicitStructure(AttributeType<?> attributeType, Relationship relationship, Entity entity, Schema.ImplicitType has, Schema.ImplicitType hasOwner, Schema.ImplicitType hasValue){
        assertEquals(2, relationship.allRolePlayers().size());
        assertEquals(has.getLabel(attributeType.getLabel()), relationship.type().getLabel());
        relationship.allRolePlayers().entrySet().forEach(entry -> {
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
    public void whenGettingEntityKeys_EnsureKeysAreReturned(){
        AttributeType<String> attributeType = tx.putAttributeType("An Attribute", AttributeType.DataType.STRING);
        AttributeType<String> keyType = tx.putAttributeType("A Key", AttributeType.DataType.STRING);
        EntityType entityType = tx.putEntityType("A Thing").attribute(attributeType).key(keyType);

        Attribute<String> a1 = attributeType.putAttribute("a1");
        Attribute<String> a2 = attributeType.putAttribute("a2");

        Attribute<String> k1 = keyType.putAttribute("k1");

        Entity entity = entityType.addEntity().attribute(a1).attribute(a2).attribute(k1);

        assertThat(entity.keys().collect(Collectors.toSet()), containsInAnyOrder(k1));
        assertThat(entity.keys(attributeType, keyType).collect(Collectors.toSet()), containsInAnyOrder(k1));
        assertThat(entity.keys(attributeType).collect(Collectors.toSet()), empty());
    }

    @Test
    public void whenAddingEntity_EnsureInternalTypeIsTheSameAsRealType(){
        EntityType et = tx.putEntityType("et");
        EntityImpl e = (EntityImpl) et.addEntity();
        assertEquals(et.getLabel(), e.getInternalType());
    }

    @Test
    public void whenCreatingAnInferredEntity_EnsureMarkedAsInferred(){
        EntityTypeImpl et = EntityTypeImpl.from(tx.putEntityType("et"));
        Entity entity = et.addEntity();
        Entity entityInferred = et.addEntityInferred();
        assertFalse(entity.isInferred());
        assertTrue(entityInferred.isInferred());
    }

    @Test
    public void whenRemovingAnAttributedFromAnEntity_EnsureTheAttributeIsNoLongerReturned(){
        AttributeType<String> name = tx.putAttributeType("name", AttributeType.DataType.STRING);
        Attribute<String> fim = name.putAttribute("Fim");
        Attribute<String> tim = name.putAttribute("Tim");
        Attribute<String> pim = name.putAttribute("Pim");

        EntityType person = tx.putEntityType("person").attribute(name);
        Entity aPerson = person.addEntity().attribute(fim).attribute(tim).attribute(pim);
        assertThat(aPerson.attributes().collect(toSet()), containsInAnyOrder(fim, tim, pim));

        aPerson.deleteAttribute(tim);
        assertThat(aPerson.attributes().collect(toSet()), containsInAnyOrder(fim, pim));
    }


    @Test
    public void whenCreatingInferredAttributeLink_EnsureMarkedAsInferred(){
        AttributeType<String> name = tx.putAttributeType("name", AttributeType.DataType.STRING);
        Attribute<String> attribute1 = name.putAttribute("An attribute 1");
        Attribute<String> attribute2 = name.putAttribute("An attribute 2");
        EntityType et = tx.putEntityType("et").attribute(name);
        Entity e = et.addEntity();

        //Link Attributes
        e.attribute(attribute1);
        EntityImpl.from(e).attributeInferred(attribute2);

        e.relationships().forEach(relationship -> {
            relationship.rolePlayers().forEach(roleplayer ->{
                if(roleplayer.equals(attribute1)){
                    assertFalse(relationship.isInferred());
                } else if(roleplayer.equals(attribute2)){
                    assertTrue(relationship.isInferred());
                }
            });
        });
    }
}