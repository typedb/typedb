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
import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Relation;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.Role;
import ai.grakn.concept.Thing;
import ai.grakn.exception.GraphOperationException;
import ai.grakn.exception.InvalidGraphException;
import ai.grakn.graph.internal.GraphTestBase;
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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class AttributeTest extends GraphTestBase {
    @Test
    public void whenCreatingResource_EnsureTheResourcesDataTypeIsTheSameAsItsType() throws Exception {
        AttributeType<String> attributeType = graknGraph.putResourceType("attributeType", AttributeType.DataType.STRING);
        Attribute attribute = attributeType.putResource("resource");
        assertEquals(AttributeType.DataType.STRING, attribute.dataType());
    }

    @Test
    public void whenAttachingResourcesToInstances_EnsureInstancesAreReturnedAsOwners() throws Exception {
        EntityType randomThing = graknGraph.putEntityType("A Thing");
        AttributeType<String> attributeType = graknGraph.putResourceType("A Attribute Thing", AttributeType.DataType.STRING);
        RelationType hasResource = graknGraph.putRelationType("Has Attribute");
        Role resourceRole = graknGraph.putRole("Attribute Role");
        Role actorRole = graknGraph.putRole("Actor");
        Thing pacino = randomThing.addEntity();
        Thing jennifer = randomThing.addEntity();
        Thing bob = randomThing.addEntity();
        Thing alice = randomThing.addEntity();
        Attribute<String> birthDate = attributeType.putResource("10/10/10");
        hasResource.relates(resourceRole).relates(actorRole);

        assertThat(birthDate.ownerInstances().collect(toSet()), empty());

        hasResource.addRelation().
                addRolePlayer(resourceRole, birthDate).addRolePlayer(actorRole, pacino);
        hasResource.addRelation().
                addRolePlayer(resourceRole, birthDate).addRolePlayer(actorRole, jennifer);
        hasResource.addRelation().
                addRolePlayer(resourceRole, birthDate).addRolePlayer(actorRole, bob);
        hasResource.addRelation().
                addRolePlayer(resourceRole, birthDate).addRolePlayer(actorRole, alice);

        assertThat(birthDate.ownerInstances().collect(toSet()), containsInAnyOrder(pacino, jennifer, bob, alice));
    }

    // this is due to the generic of getResourcesByValue
    @SuppressWarnings("unchecked")
    @Test
    public void whenCreatingResources_EnsureDataTypesAreEnforced(){
        AttributeType<String> strings = graknGraph.putResourceType("String Type", AttributeType.DataType.STRING);
        AttributeType<Long> longs = graknGraph.putResourceType("Long Type", AttributeType.DataType.LONG);
        AttributeType<Double> doubles = graknGraph.putResourceType("Double Type", AttributeType.DataType.DOUBLE);
        AttributeType<Boolean> booleans = graknGraph.putResourceType("Boolean Type", AttributeType.DataType.BOOLEAN);

        Attribute<String> attribute1 = strings.putResource("1");
        Attribute<Long> attribute2 = longs.putResource(1L);
        Attribute<Double> attribute3 = doubles.putResource(1.0);
        Attribute<Boolean> attribute4 = booleans.putResource(true);

        assertEquals("1", graknGraph.<Attribute>getConcept(attribute1.getId()).getValue());
        assertEquals(1L, graknGraph.<Attribute>getConcept(attribute2.getId()).getValue());
        assertEquals(1.0, graknGraph.<Attribute>getConcept(attribute3.getId()).getValue());
        assertEquals(true, graknGraph.<Attribute>getConcept(attribute4.getId()).getValue());

        assertThat(graknGraph.<Attribute>getConcept(attribute1.getId()).getValue(), instanceOf(String.class));
        assertThat(graknGraph.<Attribute>getConcept(attribute2.getId()).getValue(), instanceOf(Long.class));
        assertThat(graknGraph.<Attribute>getConcept(attribute3.getId()).getValue(), instanceOf(Double.class));
        assertThat(graknGraph.<Attribute>getConcept(attribute4.getId()).getValue(), instanceOf(Boolean.class));

        assertThat(graknGraph.getResourcesByValue("1"), containsInAnyOrder(attribute1));
        assertThat(graknGraph.getResourcesByValue(1L), containsInAnyOrder(attribute2));
        assertThat(graknGraph.getResourcesByValue(1.0), containsInAnyOrder(attribute3));
        assertThat(graknGraph.getResourcesByValue(true), containsInAnyOrder(attribute4));
    }

    // this is deliberately an incorrect type for the test
    @SuppressWarnings("unchecked")
    @Test
    public void whenCreatingResourceWithAnInvalidDataType_Throw(){
        String invalidThing = "Invalid Thing";
        AttributeType longAttributeType = graknGraph.putResourceType("long", AttributeType.DataType.LONG);
        expectedException.expect(GraphOperationException.class);
        expectedException.expectMessage(GraphOperationException.invalidResourceValue(invalidThing, AttributeType.DataType.LONG).getMessage());
        longAttributeType.putResource(invalidThing);
    }

    // this is deliberately an incorrect type for the test
    @SuppressWarnings("unchecked")
    @Test
    public void whenCreatingResourceWithAnInvalidDataType_DoNotCreateTheResource(){
        AttributeType longAttributeType = graknGraph.putResourceType("long", AttributeType.DataType.LONG);

        try {
            longAttributeType.putResource("Invalid Thing");
            fail("Expected to throw");
        } catch (GraphOperationException e) {
            // expected failure
        }

        Collection<Attribute> instances = (Collection<Attribute>) longAttributeType.instances().collect(toSet());

        assertThat(instances, empty());
    }

    @Test
    public void whenSavingDateIntoResource_DateIsReturnedInSameFormat(){
        LocalDateTime date = LocalDateTime.now();
        AttributeType<LocalDateTime> attributeType = graknGraph.putResourceType("My Birthday", AttributeType.DataType.DATE);
        Attribute<LocalDateTime> myBirthday = attributeType.putResource(date);

        assertEquals(date, myBirthday.getValue());
        assertEquals(myBirthday, attributeType.getResource(date));
        assertThat(graknGraph.getResourcesByValue(date), containsInAnyOrder(myBirthday));
    }

    @Test
    public void whenLinkingResourcesToThings_EnsureTheRelationIsAnEdge(){
        AttributeType<String> attributeType = graknGraph.putResourceType("My attribute type", AttributeType.DataType.STRING);
        Attribute<String> attribute = attributeType.putResource("A String");

        EntityType entityType = graknGraph.putEntityType("My entity type").resource(attributeType);
        Entity entity = entityType.addEntity();

        entity.resource(attribute);

        RelationStructure relationStructure = RelationImpl.from(Iterables.getOnlyElement(entity.relations().collect(toSet()))).structure();
        assertThat(relationStructure, instanceOf(RelationEdge.class));
        assertTrue("Edge Relation id not starting with [" + Schema.PREFIX_EDGE + "]",relationStructure.getId().getValue().startsWith(Schema.PREFIX_EDGE));
        assertEquals(entity, attribute.owner());
        assertThat(entity.resources().collect(toSet()), containsInAnyOrder(attribute));
    }

    @Test
    public void whenAddingRolePlayerToRelationEdge_RelationAutomaticallyReifies(){
        //Create boring attribute which creates a relation edge
        AttributeType<String> attributeType = graknGraph.putResourceType("My attribute type", AttributeType.DataType.STRING);
        Attribute<String> attribute = attributeType.putResource("A String");
        EntityType entityType = graknGraph.putEntityType("My entity type").resource(attributeType);
        Entity entity = entityType.addEntity();
        entity.resource(attribute);
        RelationImpl relation = RelationImpl.from(entity.relations().iterator().next());

        //Check it's a relation edge.
        RelationStructure relationStructureBefore = relation.structure();
        assertThat(relationStructureBefore, instanceOf(RelationEdge.class));

        //Get the roles and role players via the relation edge:
        Map<Role, Set<Thing>> allRolePlayerBefore = relationStructureBefore.allRolePlayers();

        //Expand Ontology to allow new role
        Role newRole = graknGraph.putRole("My New Role");
        entityType.plays(newRole);
        relation.type().relates(newRole);
        Entity newEntity = entityType.addEntity();

        //Now actually add the new role player
        relation.addRolePlayer(newRole, newEntity);

        //Check it's a relation reified now.
        RelationStructure relationStructureAfter = relation.structure();
        assertThat(relationStructureAfter, instanceOf(RelationReified.class));

        //Check IDs are equal
        assertEquals(relationStructureBefore.getId(), relation.getId());
        assertEquals(relationStructureBefore.getId(), relationStructureAfter.getId());

        //Check Role Players have been transferred
        allRolePlayerBefore.forEach((role, player) -> assertEquals(player, relationStructureAfter.rolePlayers(role).collect(toSet())));

        //Check Type Has Been Transferred
        assertEquals(relationStructureBefore.type(), relationStructureAfter.type());

        //Check new role player has been added as well
        assertEquals(newEntity, Iterables.getOnlyElement(relationStructureAfter.rolePlayers(newRole).collect(toSet())));
    }

    @Test
    public void whenInsertingAThingWithTwoKeys_Throw(){
        AttributeType<String> attributeType = graknGraph.putResourceType("Key Thingy", AttributeType.DataType.STRING);
        EntityType entityType = graknGraph.putEntityType("Entity Type Thingy").key(attributeType);
        Entity entity = entityType.addEntity();

        Attribute<String> key1 = attributeType.putResource("key 1");
        Attribute<String> key2 = attributeType.putResource("key 2");

        entity.resource(key1);
        entity.resource(key2);

        expectedException.expect(InvalidGraphException.class);

        graknGraph.commit();
    }

    @Test
    public void whenGettingTheRelationsOfResources_EnsureIncomingResourceEdgesAreTakingIntoAccount(){
        AttributeType<String> attributeType = graknGraph.putResourceType("Attribute Type Thingy", AttributeType.DataType.STRING);
        Attribute<String> attribute = attributeType.putResource("Thingy");

        EntityType entityType = graknGraph.putEntityType("Entity Type Thingy").key(attributeType);
        Entity e1 = entityType.addEntity();
        Entity e2 = entityType.addEntity();

        assertThat(attribute.relations().collect(toSet()), empty());

        e1.resource(attribute);
        e2.resource(attribute);

        Relation rel1 = Iterables.getOnlyElement(e1.relations().collect(toSet()));
        Relation rel2 = Iterables.getOnlyElement(e2.relations().collect(toSet()));

        assertThat(attribute.relations().collect(toSet()), containsInAnyOrder(rel1, rel2));
    }

}