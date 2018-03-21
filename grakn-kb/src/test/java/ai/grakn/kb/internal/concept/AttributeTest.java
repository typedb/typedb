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
import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Relationship;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Role;
import ai.grakn.concept.Thing;
import ai.grakn.exception.GraknTxOperationException;
import ai.grakn.exception.InvalidKBException;
import ai.grakn.kb.internal.TxTestBase;
import ai.grakn.util.Schema;
import com.google.common.collect.Iterables;
import org.junit.Test;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import static java.util.stream.Collectors.toSet;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class AttributeTest extends TxTestBase {
    @Test
    public void whenCreatingResource_EnsureTheResourcesDataTypeIsTheSameAsItsType() throws Exception {
        AttributeType<String> attributeType = tx.putAttributeType("attributeType", AttributeType.DataType.STRING);
        Attribute attribute = attributeType.putAttribute("resource");
        assertEquals(AttributeType.DataType.STRING, attribute.dataType());
    }

    @Test
    public void whenAttachingResourcesToInstances_EnsureInstancesAreReturnedAsOwners() throws Exception {
        EntityType randomThing = tx.putEntityType("A Thing");
        AttributeType<String> attributeType = tx.putAttributeType("A Attribute Thing", AttributeType.DataType.STRING);
        RelationshipType hasResource = tx.putRelationshipType("Has Attribute");
        Role resourceRole = tx.putRole("Attribute Role");
        Role actorRole = tx.putRole("Actor");
        Thing pacino = randomThing.addEntity();
        Thing jennifer = randomThing.addEntity();
        Thing bob = randomThing.addEntity();
        Thing alice = randomThing.addEntity();
        Attribute<String> birthDate = attributeType.putAttribute("10/10/10");
        hasResource.relates(resourceRole).relates(actorRole);

        assertThat(birthDate.ownerInstances().collect(toSet()), empty());

        hasResource.addRelationship().
                addRolePlayer(resourceRole, birthDate).addRolePlayer(actorRole, pacino);
        hasResource.addRelationship().
                addRolePlayer(resourceRole, birthDate).addRolePlayer(actorRole, jennifer);
        hasResource.addRelationship().
                addRolePlayer(resourceRole, birthDate).addRolePlayer(actorRole, bob);
        hasResource.addRelationship().
                addRolePlayer(resourceRole, birthDate).addRolePlayer(actorRole, alice);

        assertThat(birthDate.ownerInstances().collect(toSet()), containsInAnyOrder(pacino, jennifer, bob, alice));
    }

    // this is due to the generic of getResourcesByValue
    @SuppressWarnings("unchecked")
    @Test
    public void whenCreatingResources_EnsureDataTypesAreEnforced(){
        AttributeType<String> strings = tx.putAttributeType("String Type", AttributeType.DataType.STRING);
        AttributeType<Long> longs = tx.putAttributeType("Long Type", AttributeType.DataType.LONG);
        AttributeType<Double> doubles = tx.putAttributeType("Double Type", AttributeType.DataType.DOUBLE);
        AttributeType<Boolean> booleans = tx.putAttributeType("Boolean Type", AttributeType.DataType.BOOLEAN);

        Attribute<String> attribute1 = strings.putAttribute("1");
        Attribute<Long> attribute2 = longs.putAttribute(1L);
        Attribute<Double> attribute3 = doubles.putAttribute(1.0);
        Attribute<Boolean> attribute4 = booleans.putAttribute(true);

        assertEquals("1", tx.<Attribute>getConcept(attribute1.getId()).getValue());
        assertEquals(1L, tx.<Attribute>getConcept(attribute2.getId()).getValue());
        assertEquals(1.0, tx.<Attribute>getConcept(attribute3.getId()).getValue());
        assertEquals(true, tx.<Attribute>getConcept(attribute4.getId()).getValue());

        assertThat(tx.<Attribute>getConcept(attribute1.getId()).getValue(), instanceOf(String.class));
        assertThat(tx.<Attribute>getConcept(attribute2.getId()).getValue(), instanceOf(Long.class));
        assertThat(tx.<Attribute>getConcept(attribute3.getId()).getValue(), instanceOf(Double.class));
        assertThat(tx.<Attribute>getConcept(attribute4.getId()).getValue(), instanceOf(Boolean.class));

        assertThat(tx.getAttributesByValue("1"), containsInAnyOrder(attribute1));
        assertThat(tx.getAttributesByValue(1L), containsInAnyOrder(attribute2));
        assertThat(tx.getAttributesByValue(1.0), containsInAnyOrder(attribute3));
        assertThat(tx.getAttributesByValue(true), containsInAnyOrder(attribute4));
    }

    // this is deliberately an incorrect type for the test
    @SuppressWarnings("unchecked")
    @Test
    public void whenCreatingResourceWithAnInvalidDataType_Throw(){
        String invalidThing = "Invalid Thing";
        AttributeType longAttributeType = tx.putAttributeType("long", AttributeType.DataType.LONG);
        expectedException.expect(GraknTxOperationException.class);
        expectedException.expectMessage(GraknTxOperationException.invalidAttributeValue(invalidThing, AttributeType.DataType.LONG).getMessage());
        longAttributeType.putAttribute(invalidThing);
    }

    // this is deliberately an incorrect type for the test
    @SuppressWarnings("unchecked")
    @Test
    public void whenCreatingResourceWithAnInvalidDataTypeOnADate_Throw(){
        String invalidThing = "Invalid Thing";
        AttributeType longAttributeType = tx.putAttributeType("date", AttributeType.DataType.DATE);
        expectedException.expect(GraknTxOperationException.class);
        expectedException.expectMessage(GraknTxOperationException.invalidAttributeValue(invalidThing, AttributeType.DataType.DATE).getMessage());
        longAttributeType.putAttribute(invalidThing);
    }

    // this is deliberately an incorrect type for the test
    @SuppressWarnings("unchecked")
    @Test
    public void whenCreatingResourceWithAnInvalidDataType_DoNotCreateTheResource(){
        AttributeType longAttributeType = tx.putAttributeType("long", AttributeType.DataType.LONG);

        try {
            longAttributeType.putAttribute("Invalid Thing");
            fail("Expected to throw");
        } catch (GraknTxOperationException e) {
            // expected failure
        }

        Collection<Attribute> instances = (Collection<Attribute>) longAttributeType.instances().collect(toSet());

        assertThat(instances, empty());
    }

    @Test
    public void whenSavingDateIntoResource_DateIsReturnedInSameFormat(){
        LocalDateTime date = LocalDateTime.now();
        AttributeType<LocalDateTime> attributeType = tx.putAttributeType("My Birthday", AttributeType.DataType.DATE);
        Attribute<LocalDateTime> myBirthday = attributeType.putAttribute(date);

        assertEquals(date, myBirthday.getValue());
        assertEquals(myBirthday, attributeType.getAttribute(date));
        assertThat(tx.getAttributesByValue(date), containsInAnyOrder(myBirthday));
    }

    @Test
    public void whenLinkingResourcesToThings_EnsureTheRelationIsAnEdge(){
        AttributeType<String> attributeType = tx.putAttributeType("My attribute type", AttributeType.DataType.STRING);
        Attribute<String> attribute = attributeType.putAttribute("A String");

        EntityType entityType = tx.putEntityType("My entity type").attribute(attributeType);
        Entity entity = entityType.addEntity();

        entity.attribute(attribute);

        RelationshipStructure relationshipStructure = RelationshipImpl.from(Iterables.getOnlyElement(entity.relationships().collect(toSet()))).structure();
        assertThat(relationshipStructure, instanceOf(RelationshipEdge.class));
        assertTrue("Edge Relationship id not starting with [" + Schema.PREFIX_EDGE + "]", relationshipStructure.getId().getValue().startsWith(Schema.PREFIX_EDGE));
        assertEquals(entity, attribute.owner());
        assertThat(entity.attributes().collect(toSet()), containsInAnyOrder(attribute));
    }

    @Test
    public void whenAddingRolePlayerToRelationEdge_RelationAutomaticallyReifies(){
        //Create boring attribute which creates a relation edge
        AttributeType<String> attributeType = tx.putAttributeType("My attribute type", AttributeType.DataType.STRING);
        Attribute<String> attribute = attributeType.putAttribute("A String");
        EntityType entityType = tx.putEntityType("My entity type").attribute(attributeType);
        Entity entity = entityType.addEntity();
        entity.attribute(attribute);
        RelationshipImpl relation = RelationshipImpl.from(entity.relationships().iterator().next());

        //Check it's a relation edge.
        RelationshipStructure relationshipStructureBefore = relation.structure();
        assertThat(relationshipStructureBefore, instanceOf(RelationshipEdge.class));

        //Get the roles and role players via the relation edge:
        Map<Role, Set<Thing>> allRolePlayerBefore = relationshipStructureBefore.allRolePlayers();

        //Expand Schema to allow new role
        Role newRole = tx.putRole("My New Role");
        entityType.plays(newRole);
        relation.type().relates(newRole);
        Entity newEntity = entityType.addEntity();

        //Now actually add the new role player
        relation.addRolePlayer(newRole, newEntity);

        //Check it's a relation reified now.
        RelationshipStructure relationshipStructureAfter = relation.structure();
        assertThat(relationshipStructureAfter, instanceOf(RelationshipReified.class));

        //Check IDs are equal
        assertEquals(relationshipStructureBefore.getId(), relation.getId());
        assertEquals(relationshipStructureBefore.getId(), relationshipStructureAfter.getId());

        //Check Role Players have been transferred
        allRolePlayerBefore.forEach((role, player) -> assertEquals(player, relationshipStructureAfter.rolePlayers(role).collect(toSet())));

        //Check Type Has Been Transferred
        assertEquals(relationshipStructureBefore.type(), relationshipStructureAfter.type());

        //Check new role player has been added as well
        assertEquals(newEntity, Iterables.getOnlyElement(relationshipStructureAfter.rolePlayers(newRole).collect(toSet())));
    }

    @Test
    public void whenInsertingAThingWithTwoKeys_Throw(){
        AttributeType<String> attributeType = tx.putAttributeType("Key Thingy", AttributeType.DataType.STRING);
        EntityType entityType = tx.putEntityType("Entity Type Thingy").key(attributeType);
        Entity entity = entityType.addEntity();

        Attribute<String> key1 = attributeType.putAttribute("key 1");
        Attribute<String> key2 = attributeType.putAttribute("key 2");

        entity.attribute(key1);
        entity.attribute(key2);

        expectedException.expect(InvalidKBException.class);

        tx.commit();
    }

    @Test
    public void whenGettingTheRelationsOfResources_EnsureIncomingResourceEdgesAreTakingIntoAccount(){
        AttributeType<String> attributeType = tx.putAttributeType("Attribute Type Thingy", AttributeType.DataType.STRING);
        Attribute<String> attribute = attributeType.putAttribute("Thingy");

        EntityType entityType = tx.putEntityType("Entity Type Thingy").key(attributeType);
        Entity e1 = entityType.addEntity();
        Entity e2 = entityType.addEntity();

        assertThat(attribute.relationships().collect(toSet()), empty());

        e1.attribute(attribute);
        e2.attribute(attribute);

        Relationship rel1 = Iterables.getOnlyElement(e1.relationships().collect(toSet()));
        Relationship rel2 = Iterables.getOnlyElement(e2.relationships().collect(toSet()));

        assertThat(attribute.relationships().collect(toSet()), containsInAnyOrder(rel1, rel2));
    }

    @Test
    public void whenCreatingAnInferredAttribute_EnsureMarkedAsInferred(){
        AttributeTypeImpl at = AttributeTypeImpl.from(tx.putAttributeType("at", AttributeType.DataType.STRING));
        Attribute attribute = at.putAttribute("blergh");
        Attribute attributeInferred = at.putAttributeInferred("bloorg");
        assertFalse(attribute.isInferred());
        assertTrue(attributeInferred.isInferred());
    }

    @Test
    public void whenCreatingAnInferredAttributeWhichIsAlreadyNonInferred_Throw(){
        AttributeTypeImpl at = AttributeTypeImpl.from(tx.putAttributeType("at", AttributeType.DataType.STRING));
        Attribute attribute = at.putAttribute("blergh");

        expectedException.expect(GraknTxOperationException.class);
        expectedException.expectMessage(GraknTxOperationException.nonInferredThingExists(attribute).getMessage());

        at.putAttributeInferred("blergh");
    }

}