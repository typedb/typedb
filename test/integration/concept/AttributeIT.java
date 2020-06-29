/*
 * Copyright (C) 2020 Grakn Labs
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 */

package grakn.core.concept;

import grakn.core.concept.impl.AttributeTypeImpl;
import grakn.core.kb.concept.api.Attribute;
import grakn.core.kb.concept.api.AttributeType;
import grakn.core.kb.concept.api.Entity;
import grakn.core.kb.concept.api.EntityType;
import grakn.core.kb.concept.api.GraknConceptException;
import grakn.core.kb.concept.api.RelationType;
import grakn.core.kb.concept.api.Role;
import grakn.core.kb.concept.api.Thing;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;
import grakn.core.kb.server.exception.InvalidKBException;
import grakn.core.test.rule.GraknTestServer;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.List;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class AttributeIT {

    @ClassRule
    public static final GraknTestServer server = new GraknTestServer();

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    private Transaction tx;
    private Session session;

    @Before
    public void setUp() {
        session = server.sessionWithNewKeyspace();
        tx = session.transaction(Transaction.Type.WRITE);
    }

    @After
    public void tearDown() {
        tx.close();
        session.close();
    }

    @Test
    public void whenSubTypeSharesAttributes_noDuplicatesAreProducedWhenRetrievingAttributes(){
        AttributeType<String> resource = tx.putAttributeType("resource", AttributeType.ValueType.STRING);
        AttributeType<String> anotherResource = tx.putAttributeType("anotherResource", AttributeType.ValueType.STRING);
        EntityType someEntity = tx.putEntityType("someEntity")
                .has(resource)
                .has(anotherResource);
        EntityType subEntity = tx.putEntityType("subEntity").sup(someEntity)
                .has(resource)
                .has(anotherResource);

        List<AttributeType> entityAttributes = someEntity.has().collect(toList());
        List<AttributeType> subEntityAttributes = subEntity.has().collect(toList());
        assertTrue(entityAttributes.containsAll(subEntityAttributes));
        assertTrue(subEntityAttributes.containsAll(entityAttributes));
    }

    @Test
    public void whenCreatingResource_EnsureTheResourcesValueTypeIsTheSameAsItsType() {
        AttributeType<String> attributeType = tx.putAttributeType("attributeType", AttributeType.ValueType.STRING);
        Attribute attribute = attributeType.create("resource");
        assertEquals(AttributeType.ValueType.STRING, attribute.valueType());
    }

    @Test
    public void whenAttachingResourcesToInstances_EnsureInstancesAreReturnedAsRelated(){
        EntityType randomThing = tx.putEntityType("A Thing");
        AttributeType<String> attributeType = tx.putAttributeType("A Attribute Thing", AttributeType.ValueType.STRING);
        RelationType hasResource = tx.putRelationType("Has Attribute");
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

        List<Thing> neighbors = birthDate.relations(resourceRole).flatMap(rel -> rel.rolePlayers(actorRole)).collect(toList());
        assertThat(neighbors, containsInAnyOrder(pacino, jennifer, bob, alice));
    }

    // this is due to the generic of getResourcesByValue
    @SuppressWarnings("unchecked")
    @Test
    public void whenCreatingResources_EnsureValueTypesAreEnforced() {
        AttributeType<String> strings = tx.putAttributeType("String Type", AttributeType.ValueType.STRING);
        AttributeType<Long> longs = tx.putAttributeType("Long Type", AttributeType.ValueType.LONG);
        AttributeType<Double> doubles = tx.putAttributeType("Double Type", AttributeType.ValueType.DOUBLE);
        AttributeType<Boolean> booleans = tx.putAttributeType("Boolean Type", AttributeType.ValueType.BOOLEAN);

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
    public void whenCreatingResourceWithAnInvalidValueType_Throw() {
        String invalidThing = "Invalid Thing";
        AttributeType longAttributeType = tx.putAttributeType("long", AttributeType.ValueType.LONG);
        expectedException.expect(GraknConceptException.class);
        expectedException.expectMessage(GraknConceptException.invalidAttributeValue(longAttributeType, invalidThing).getMessage());
        longAttributeType.create(invalidThing);
    }

    // this is deliberately an incorrect type for the test
    @SuppressWarnings("unchecked")
    @Test
    public void whenCreatingResourceWithAnInvalidValueTypeOnADate_Throw() {
        String invalidThing = "Invalid Thing";
        AttributeType dateAttributeType = tx.putAttributeType("date", AttributeType.ValueType.DATETIME);
        expectedException.expect(GraknConceptException.class);
        expectedException.expectMessage(GraknConceptException.invalidAttributeValue(dateAttributeType, invalidThing).getMessage());
        dateAttributeType.create(invalidThing);
    }

    // this is deliberately an incorrect type for the test
    @SuppressWarnings("unchecked")
    @Test
    public void whenCreatingResourceWithAnInvalidValueType_DoNotCreateTheResource() {
        AttributeType longAttributeType = tx.putAttributeType("long", AttributeType.ValueType.LONG);

        try {
            longAttributeType.create("Invalid Thing");
            fail("Expected to throw");
        } catch (GraknConceptException e) {
            // expected failure
        }

        Collection<Attribute> instances = (Collection<Attribute>) longAttributeType.instances().collect(toSet());

        assertThat(instances, empty());
    }

    @Test
    public void whenSavingDateIntoResource_DateIsReturnedInSameFormat() {
        LocalDateTime date = LocalDateTime.now();
        AttributeType<LocalDateTime> attributeType = tx.putAttributeType("My Birthday", AttributeType.ValueType.DATETIME);
        Attribute<LocalDateTime> myBirthday = attributeType.create(date);

        assertEquals(date, myBirthday.value());
        assertEquals(myBirthday, attributeType.attribute(date));
        assertThat(tx.getAttributesByValue(date), containsInAnyOrder(myBirthday));
    }

    @Test
    public void whenLinkingResourcesToThings_EnsureTheEdgeExists() {
        AttributeType<String> attributeType = tx.putAttributeType("My attribute type", AttributeType.ValueType.STRING);
        Attribute<String> attribute = attributeType.create("A String");

        EntityType entityType = tx.putEntityType("My entity type").has(attributeType);
        Entity entity = entityType.create();

        entity.has(attribute);

        assertEquals(entity, attribute.owner());
        assertThat(entity.attributes().collect(toSet()), containsInAnyOrder(attribute));
    }

    @Test
    public void whenInsertingAThingWithTwoKeys_Throw() {
        AttributeType<String> attributeType = tx.putAttributeType("Key Thingy", AttributeType.ValueType.STRING);
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
    public void whenCreatingAnInferredAttribute_EnsureMarkedAsInferred() {
        AttributeTypeImpl at = AttributeTypeImpl.from(tx.putAttributeType("at", AttributeType.ValueType.STRING));
        Attribute attribute = at.create("blergh");
        Attribute attributeInferred = at.putAttributeInferred("bloorg");
        assertFalse(attribute.isInferred());
        assertTrue(attributeInferred.isInferred());
    }

    @Test
    public void whenDeletingAnAttribute_associatedEdgesAreDeleted() {
        AttributeType<String> attributeType = tx.putAttributeType("resource", AttributeType.ValueType.STRING);
        Attribute<String> attribute = attributeType.create("polok");

        EntityType entityType = tx.putEntityType("someEntity").has(attributeType);
        Entity e1 = entityType.create();
        Entity e2 = entityType.create();

        assertThat(attribute.owners().collect(toSet()), empty());

        e1.has(attribute);
        e2.has(attribute);

        assertTrue(e1.attributes().findFirst().isPresent());
        assertTrue(e2.attributes().findFirst().isPresent());

        attribute.delete();

        assertFalse(e1.attributes().findFirst().isPresent());
        assertFalse(e2.attributes().findFirst().isPresent());
    }
}