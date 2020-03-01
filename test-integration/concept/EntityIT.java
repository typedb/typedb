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

import grakn.core.core.Schema;
import grakn.core.kb.concept.api.Attribute;
import grakn.core.kb.concept.api.AttributeType;
import grakn.core.kb.concept.api.ConceptId;
import grakn.core.kb.concept.api.Entity;
import grakn.core.kb.concept.api.EntityType;
import grakn.core.kb.concept.api.GraknConceptException;
import grakn.core.kb.concept.api.Label;
import grakn.core.kb.concept.api.Relation;
import grakn.core.kb.concept.api.RelationType;
import grakn.core.kb.concept.api.Role;
import grakn.core.kb.concept.api.Thing;
import grakn.core.kb.concept.structure.Casting;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;
import grakn.core.kb.server.exception.InvalidKBException;
import grakn.core.rule.GraknTestServer;
import grakn.core.util.ConceptDowncasting;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

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

public class EntityIT {

    @ClassRule
    public static final GraknTestServer server = new GraknTestServer();

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();
    private Transaction tx;
    private Session session;

    @Before
    public void setUp(){
        session = server.sessionWithNewKeyspace();
        tx = session.writeTransaction();
    }

    @After
    public void tearDown(){
        tx.close();
        session.close();
    }

    @Test
    public void whenGettingTypeOfEntity_ReturnEntityType(){
        EntityType entityType = tx.putEntityType("Entiy Type");
        Entity entity = entityType.create();
        assertEquals(entityType, entity.type());
    }

    @Test
    public void whenDeletingInstanceInRelation_TheInstanceAndCastingsAreDeletedAndTheRelationRemains() throws GraknConceptException {
        //Schema
        EntityType type = tx.putEntityType("Concept Type");
        RelationType relationType = tx.putRelationType("relationTypes");
        Role role1 = tx.putRole("role1");
        Role role2 = tx.putRole("role2");
        Role role3 = tx.putRole("role3");

        //Data
        Thing rolePlayer1 = type.create();
        Thing rolePlayer2 = type.create();
        Thing rolePlayer3 = type.create();

        relationType.relates(role1);
        relationType.relates(role2);
        relationType.relates(role3);

        //Check Structure is in order
        Relation relation = relationType.create().
                assign(role1, rolePlayer1).
                assign(role2, rolePlayer2).
                assign(role3, rolePlayer3);

        Casting rp1 = ConceptDowncasting.thing(rolePlayer1).castingsInstance().findAny().get();
        Casting rp2 = ConceptDowncasting.thing(rolePlayer2).castingsInstance().findAny().get();
        Casting rp3 = ConceptDowncasting.thing(rolePlayer3).castingsInstance().findAny().get();

        assertThat(ConceptDowncasting.relation(relation).reified().castingsRelation().collect(toSet()), containsInAnyOrder(rp1, rp2, rp3));

        //Delete And Check Again
        ConceptId idOfDeleted = rolePlayer1.id();
        rolePlayer1.delete();

        assertNull(tx.getConcept(idOfDeleted));
        assertThat(ConceptDowncasting.relation(relation).reified().castingsRelation().collect(toSet()), containsInAnyOrder(rp2, rp3));
    }

    @Test
    public void whenDeletingLastRolePlayerInRelation_TheRelationIsDeleted() throws GraknConceptException {
        EntityType type = tx.putEntityType("Concept Type");
        RelationType relationType = tx.putRelationType("relationTypes");
        Role role1 = tx.putRole("role1");
        Thing rolePlayer1 = type.create();

        Relation relation = relationType.create().
                assign(role1, rolePlayer1);

        assertNotNull(tx.getConcept(relation.id()));

        rolePlayer1.delete();

        assertNull(tx.getConcept(relation.id()));
    }

    @Test
    public void whenAddingResourceToAnEntity_EnsureTheicitStructureIsCreated(){
        Label resourceLabel = Label.of("A Attribute Thing");
        EntityType entityType = tx.putEntityType("A Thing");
        AttributeType<String> attributeType = tx.putAttributeType(resourceLabel, AttributeType.DataType.STRING);
        entityType.has(attributeType);

        Entity entity = entityType.create();
        Attribute attribute = attributeType.create("A attribute thing");

        entity.has(attribute);
        Relation relation = entity.relations().iterator().next();

        checkicitStructure(attributeType, relation, entity, Schema.ImplicitType.HAS, Schema.ImplicitType.HAS_OWNER, Schema.ImplicitType.HAS_VALUE);
    }

    @Test
    public void whenAddingResourceToEntityWithoutAllowingItBetweenTypes_Throw(){
        EntityType entityType = tx.putEntityType("A Thing");
        AttributeType<String> attributeType = tx.putAttributeType("A Attribute Thing", AttributeType.DataType.STRING);

        Entity entity = entityType.create();
        Attribute attribute = attributeType.create("A attribute thing");

        expectedException.expect(GraknConceptException.class);
        expectedException.expectMessage(GraknConceptException.hasNotAllowed(entity, attribute).getMessage());

        entity.has(attribute);
    }

    @Test
    public void whenAddingMultipleResourcesToEntity_EnsureDifferentRelationsAreBuilt() throws InvalidKBException {
        String resourceTypeId = "A Attribute Thing";
        EntityType entityType = tx.putEntityType("A Thing");
        AttributeType<String> attributeType = tx.putAttributeType(resourceTypeId, AttributeType.DataType.STRING);
        entityType.has(attributeType);

        Entity entity = entityType.create();
        Attribute attribute1 = attributeType.create("A resource thing");
        Attribute attribute2 = attributeType.create("Another resource thing");

        assertEquals(0, entity.relations().count());
        entity.has(attribute1);
        assertEquals(1, entity.relations().count());
        entity.has(attribute2);
        assertEquals(2, entity.relations().count());

        tx.commit();
    }

    @Test
    public void checkKeyCreatesCorrectResourceStructure(){
        Label resourceLabel = Label.of("A Attribute Thing");
        EntityType entityType = tx.putEntityType("A Thing");
        AttributeType<String> attributeType = tx.putAttributeType(resourceLabel, AttributeType.DataType.STRING);
        entityType.key(attributeType);

        Entity entity = entityType.create();
        Attribute attribute = attributeType.create("A attribute thing");

        entity.has(attribute);
        Relation relation = entity.relations().iterator().next();

        checkicitStructure(attributeType, relation, entity, Schema.ImplicitType.KEY, Schema.ImplicitType.KEY_OWNER, Schema.ImplicitType.KEY_VALUE);
    }

    @Test
    public void whenCreatingAnEntityAndNotLinkingARequiredKey_Throw() throws InvalidKBException {
        String resourceTypeId = "A Attribute Thing";
        EntityType entityType = tx.putEntityType("A Thing");
        AttributeType<String> attributeType = tx.putAttributeType(resourceTypeId, AttributeType.DataType.STRING);
        entityType.key(attributeType);

        entityType.create();

        expectedException.expect(InvalidKBException.class);

        tx.commit();
    }

    private void checkicitStructure(AttributeType<?> attributeType, Relation relation, Entity entity, Schema.ImplicitType has, Schema.ImplicitType hasOwner, Schema.ImplicitType hasValue){
        assertEquals(2, relation.rolePlayersMap().size());
        assertEquals(has.getLabel(attributeType.label()), relation.type().label());
        relation.rolePlayersMap().entrySet().forEach(entry -> {
            Role role = entry.getKey();
            assertEquals(1, entry.getValue().size());
            entry.getValue().forEach(instance -> {
                if(instance.equals(entity)){
                    assertEquals(hasOwner.getLabel(attributeType.label()), role.label());
                } else {
                    assertEquals(hasValue.getLabel(attributeType.label()), role.label());
                }
            });
        });
    }

    @Test
    public void whenGettingEntityKeys_EnsureKeysAreReturned(){
        AttributeType<String> attributeType = tx.putAttributeType("An Attribute", AttributeType.DataType.STRING);
        AttributeType<String> keyType = tx.putAttributeType("A Key", AttributeType.DataType.STRING);
        EntityType entityType = tx.putEntityType("A Thing").has(attributeType).key(keyType);

        Attribute<String> a1 = attributeType.create("a1");
        Attribute<String> a2 = attributeType.create("a2");

        Attribute<String> k1 = keyType.create("k1");

        Entity entity = entityType.create().has(a1).has(a2).has(k1);

        assertThat(entity.keys().collect(Collectors.toSet()), containsInAnyOrder(k1));
        assertThat(entity.keys(attributeType, keyType).collect(Collectors.toSet()), containsInAnyOrder(k1));
        assertThat(entity.keys(attributeType).collect(Collectors.toSet()), empty());
    }

    @Test
    public void whenCreatingAnInferredEntity_EnsureMarkedAsInferred(){
        EntityType et = tx.putEntityType("et");
        Entity entity = et.create();
        Entity entityInferred = et.addEntityInferred();
        assertFalse(entity.isInferred());
        assertTrue(entityInferred.isInferred());
    }

    @Test
    public void whenRemovingAnAttributedFromAnEntity_EnsureTheAttributeIsNoLongerReturned(){
        AttributeType<String> name = tx.putAttributeType("name", AttributeType.DataType.STRING);
        Attribute<String> fim = name.create("Fim");
        Attribute<String> tim = name.create("Tim");
        Attribute<String> pim = name.create("Pim");

        EntityType person = tx.putEntityType("person").has(name);
        Entity aPerson = person.create().has(fim).has(tim).has(pim);
        assertThat(aPerson.attributes().collect(toSet()), containsInAnyOrder(fim, tim, pim));

        aPerson.unhas(tim);
        assertThat(aPerson.attributes().collect(toSet()), containsInAnyOrder(fim, pim));
    }


    @Test
    public void whenCreatingInferredAttributeLink_EnsureMarkedAsInferred(){
        AttributeType<String> name = tx.putAttributeType("name", AttributeType.DataType.STRING);
        Attribute<String> attribute1 = name.create("An attribute 1");
        Attribute<String> attribute2 = name.create("An attribute 2");
        EntityType et = tx.putEntityType("et").has(name);
        Entity e = et.create();

        //Link Attributes
        e.has(attribute1);
        e.attributeInferred(attribute2);

        e.relations().forEach(relation -> {
            relation.rolePlayers().forEach(roleplayer ->{
                if(roleplayer.equals(attribute1)){
                    assertFalse(relation.isInferred());
                } else if(roleplayer.equals(attribute2)){
                    assertTrue(relation.isInferred());
                }
            });
        });
    }
}