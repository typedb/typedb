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
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
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
        Attribute attribute = attributeType.create("resource");
        assertEquals(AttributeType.DataType.STRING, attribute.dataType());
    }

    @Test
    public void whenAttachingResourcesToInstances_EnsureInstancesAreReturnedAsOwners() throws Exception {
        EntityType randomThing = tx.putEntityType("A Thing");
        AttributeType<String> attributeType = tx.putAttributeType("A Attribute Thing", AttributeType.DataType.STRING);
        RelationshipType hasResource = tx.putRelationshipType("Has Attribute");
        Role resourceRole = tx.putRole("Attribute Role");
        Role actorRole = tx.putRole("Actor");
        Thing pacino = randomThing.create();
        Thing jennifer = randomThing.create();
        Thing bob = randomThing.create();
        Thing alice = randomThing.create();
        Attribute<String> birthDate = attributeType.create("10/10/10");
        hasResource.relates(resourceRole).relates(actorRole);

        assertThat(birthDate.owners().collect(toSet()), empty());

        hasResource.create().
                assign(resourceRole, birthDate).assign(actorRole, pacino);
        hasResource.create().
                assign(resourceRole, birthDate).assign(actorRole, jennifer);
        hasResource.create().
                assign(resourceRole, birthDate).assign(actorRole, bob);
        hasResource.create().
                assign(resourceRole, birthDate).assign(actorRole, alice);

        assertThat(birthDate.owners().collect(toSet()), containsInAnyOrder(pacino, jennifer, bob, alice));
    }

    // this is due to the generic of getResourcesByValue
    @SuppressWarnings("unchecked")
    @Test
    public void whenCreatingResources_EnsureDataTypesAreEnforced(){
        AttributeType<String> strings = tx.putAttributeType("String Type", AttributeType.DataType.STRING);
        AttributeType<Long> longs = tx.putAttributeType("Long Type", AttributeType.DataType.LONG);
        AttributeType<Double> doubles = tx.putAttributeType("Double Type", AttributeType.DataType.DOUBLE);
        AttributeType<Boolean> booleans = tx.putAttributeType("Boolean Type", AttributeType.DataType.BOOLEAN);

        Attribute<String> attribute1 = strings.create("1");
        Attribute<Long> attribute2 = longs.create(1L);
        Attribute<Double> attribute3 = doubles.create(1.0);
        Attribute<Boolean> attribute4 = booleans.create(true);

        assertEquals("1", tx.<Attribute>getConcept(attribute1.id()).value());
        assertEquals(1L, tx.<Attribute>getConcept(attribute2.id()).value());
        assertEquals(1.0, tx.<Attribute>getConcept(attribute3.id()).value());
        assertEquals(true, tx.<Attribute>getConcept(attribute4.id()).value());

        assertThat(tx.<Attribute>getConcept(attribute1.id()).value(), instanceOf(String.class));
        assertThat(tx.<Attribute>getConcept(attribute2.id()).value(), instanceOf(Long.class));
        assertThat(tx.<Attribute>getConcept(attribute3.id()).value(), instanceOf(Double.class));
        assertThat(tx.<Attribute>getConcept(attribute4.id()).value(), instanceOf(Boolean.class));

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
        longAttributeType.create(invalidThing);
    }

    // this is deliberately an incorrect type for the test
    @SuppressWarnings("unchecked")
    @Test
    public void whenCreatingResourceWithAnInvalidDataTypeOnADate_Throw(){
        String invalidThing = "Invalid Thing";
        AttributeType longAttributeType = tx.putAttributeType("date", AttributeType.DataType.DATE);
        expectedException.expect(GraknTxOperationException.class);
        expectedException.expectMessage(GraknTxOperationException.invalidAttributeValue(invalidThing, AttributeType.DataType.DATE).getMessage());
        longAttributeType.create(invalidThing);
    }

    // this is deliberately an incorrect type for the test
    @SuppressWarnings("unchecked")
    @Test
    public void whenCreatingResourceWithAnInvalidDataType_DoNotCreateTheResource(){
        AttributeType longAttributeType = tx.putAttributeType("long", AttributeType.DataType.LONG);

        try {
            longAttributeType.create("Invalid Thing");
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
        Attribute<LocalDateTime> myBirthday = attributeType.create(date);

        assertEquals(date, myBirthday.value());
        assertEquals(myBirthday, attributeType.attribute(date));
        assertThat(tx.getAttributesByValue(date), containsInAnyOrder(myBirthday));
    }

    @Test
    public void whenLinkingResourcesToThings_EnsureTheRelationIsAnEdge(){
        AttributeType<String> attributeType = tx.putAttributeType("My attribute type", AttributeType.DataType.STRING);
        Attribute<String> attribute = attributeType.create("A String");

        EntityType entityType = tx.putEntityType("My entity type").has(attributeType);
        Entity entity = entityType.create();

        entity.has(attribute);

        RelationshipStructure relationshipStructure = RelationshipImpl.from(Iterables.getOnlyElement(entity.relationships().collect(toSet()))).structure();
        assertThat(relationshipStructure, instanceOf(RelationshipEdge.class));
        assertTrue("Edge Relationship id not starting with [" + Schema.PREFIX_EDGE + "]", relationshipStructure.id().getValue().startsWith(Schema.PREFIX_EDGE));
        assertEquals(entity, attribute.owner());
        assertThat(entity.attributes().collect(toSet()), containsInAnyOrder(attribute));
    }

    @Test
    public void whenAddingRolePlayerToRelationEdge_RelationAutomaticallyReifies(){
        //Create boring attribute which creates a relation edge
        AttributeType<String> attributeType = tx.putAttributeType("My attribute type", AttributeType.DataType.STRING);
        Attribute<String> attribute = attributeType.create("A String");
        EntityType entityType = tx.putEntityType("My entity type").has(attributeType);
        Entity entity = entityType.create();
        entity.has(attribute);
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
        Entity newEntity = entityType.create();

        //Now actually add the new role player
        relation.assign(newRole, newEntity);

        //Check it's a relation reified now.
        RelationshipStructure relationshipStructureAfter = relation.structure();
        assertThat(relationshipStructureAfter, instanceOf(RelationshipReified.class));

        //Check IDs are equal
        assertEquals(relationshipStructureBefore.id(), relation.id());
        assertEquals(relationshipStructureBefore.id(), relationshipStructureAfter.id());

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
        Entity entity = entityType.create();

        Attribute<String> key1 = attributeType.create("key 1");
        Attribute<String> key2 = attributeType.create("key 2");

        entity.has(key1);
        entity.has(key2);

        expectedException.expect(InvalidKBException.class);

        tx.commit();
    }

    @Test
    public void whenGettingTheRelationsOfResources_EnsureIncomingResourceEdgesAreTakingIntoAccount(){
        AttributeType<String> attributeType = tx.putAttributeType("Attribute Type Thingy", AttributeType.DataType.STRING);
        Attribute<String> attribute = attributeType.create("Thingy");

        EntityType entityType = tx.putEntityType("Entity Type Thingy").key(attributeType);
        Entity e1 = entityType.create();
        Entity e2 = entityType.create();

        assertThat(attribute.relationships().collect(toSet()), empty());

        e1.has(attribute);
        e2.has(attribute);

        Relationship rel1 = Iterables.getOnlyElement(e1.relationships().collect(toSet()));
        Relationship rel2 = Iterables.getOnlyElement(e2.relationships().collect(toSet()));

        assertThat(attribute.relationships().collect(toSet()), containsInAnyOrder(rel1, rel2));
    }

    @Test
    public void whenCreatingAnInferredAttribute_EnsureMarkedAsInferred(){
        AttributeTypeImpl at = AttributeTypeImpl.from(tx.putAttributeType("at", AttributeType.DataType.STRING));
        Attribute attribute = at.create("blergh");
        Attribute attributeInferred = at.putAttributeInferred("bloorg");
        assertFalse(attribute.isInferred());
        assertTrue(attributeInferred.isInferred());
    }

    @Test
    public void whenCreatingAnInferredAttributeWhichIsAlreadyNonInferred_Throw(){
        AttributeTypeImpl at = AttributeTypeImpl.from(tx.putAttributeType("at", AttributeType.DataType.STRING));
        Attribute attribute = at.create("blergh");

        expectedException.expect(GraknTxOperationException.class);
        expectedException.expectMessage(GraknTxOperationException.nonInferredThingExists(attribute).getMessage());

        at.putAttributeInferred("blergh");
    }

}