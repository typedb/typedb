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

package ai.grakn.remote.concept;

import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.concept.Attribute;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.AttributeType.DataType;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Label;
import ai.grakn.concept.Relationship;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Role;
import ai.grakn.concept.SchemaConcept;
import ai.grakn.concept.Thing;
import ai.grakn.concept.Type;
import ai.grakn.graql.Pattern;
import ai.grakn.grpc.ConceptMethod;
import ai.grakn.grpc.GrpcUtil;
import ai.grakn.grpc.RolePlayer;
import ai.grakn.remote.GrpcServerMock;
import ai.grakn.remote.RemoteGraknSession;
import ai.grakn.remote.RemoteGraknTx;
import ai.grakn.rpc.generated.GrpcGrakn.TxResponse;
import ai.grakn.util.SimpleURI;
import com.google.common.collect.ImmutableSet;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static ai.grakn.graql.Graql.var;
import static ai.grakn.grpc.ConceptMethod.GET_ATTRIBUTE_TYPES;
import static ai.grakn.grpc.ConceptMethod.GET_DATA_TYPE_OF_ATTRIBUTE;
import static ai.grakn.grpc.ConceptMethod.GET_DATA_TYPE_OF_TYPE;
import static ai.grakn.grpc.ConceptMethod.GET_DIRECT_SUPER;
import static ai.grakn.grpc.ConceptMethod.GET_DIRECT_TYPE;
import static ai.grakn.grpc.ConceptMethod.GET_KEY_TYPES;
import static ai.grakn.grpc.ConceptMethod.GET_REGEX;
import static ai.grakn.grpc.ConceptMethod.GET_ROLE_PLAYERS;
import static ai.grakn.grpc.ConceptMethod.GET_THEN;
import static ai.grakn.grpc.ConceptMethod.GET_VALUE;
import static ai.grakn.grpc.ConceptMethod.GET_WHEN;
import static ai.grakn.grpc.ConceptMethod.IS_ABSTRACT;
import static ai.grakn.grpc.ConceptMethod.IS_IMPLICIT;
import static ai.grakn.grpc.ConceptMethod.IS_INFERRED;
import static ai.grakn.grpc.GrpcUtil.convertOptionalConcept;
import static java.util.stream.Collectors.toSet;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;

/**
 * @author Felix Chapman
 */
public class RemoteConceptsTest {

    private static final ConceptId ID = ConceptId.of("V123");
    private static final Pattern PATTERN = var("x").isa("person");

    private static final ConceptId A = ConceptId.of("A");
    private static final ConceptId B = ConceptId.of("B");
    private static final ConceptId C = ConceptId.of("C");

    @Rule
    public final GrpcServerMock server = GrpcServerMock.create();

    private RemoteGraknSession session;
    private RemoteGraknTx tx;
    private static final SimpleURI URI = new SimpleURI("localhost", 999);
    private static final Label LABEL = Label.of("too-tired-for-funny-test-names-today");

    private SchemaConcept schemaConcept;
    private Type type;
    private EntityType entityType;
    private AttributeType<String> attributeType;
    private RelationshipType relationshipType;
    private Role role;
    private ai.grakn.concept.Rule rule;
    private Entity entity;
    private Attribute<String> attribute;
    private Relationship relationship;
    private Thing thing;
    private Concept concept;

    @Before
    public void setUp() {
        session = RemoteGraknSession.create(Keyspace.of("whatever"), URI, server.channel());
        tx = session.open(GraknTxType.WRITE);
        verify(server.requests()).onNext(any()); // The open request

        entityType = RemoteConcepts.createEntityType(tx, ID);
        attributeType = RemoteConcepts.createAttributeType(tx, ID);
        relationshipType = RemoteConcepts.createRelationshipType(tx, ID);
        role = RemoteConcepts.createRole(tx, ID);
        rule = RemoteConcepts.createRule(tx, ID);
        schemaConcept = role;
        type = entityType;

        entity = RemoteConcepts.createEntity(tx, ID);
        attribute = RemoteConcepts.createAttribute(tx, ID);
        relationship = RemoteConcepts.createRelationship(tx, ID);
        thing = entity;
        concept = entity;
    }

    @After
    public void closeTx() {
        tx.close();
    }

    @After
    public void closeSession() {
        session.close();
    }

    @Test
    public void whenGettingLabel_ReturnTheExpectedLabel() {
        mockConceptMethod(ConceptMethod.GET_LABEL, LABEL);
        assertEquals(LABEL, schemaConcept.getLabel());
    }

    @Test
    public void whenCallingIsImplicit_GetTheExpectedResult() {
        mockConceptMethod(IS_IMPLICIT, true);
        assertTrue(schemaConcept.isImplicit());

        mockConceptMethod(IS_IMPLICIT, false);
        assertFalse(schemaConcept.isImplicit());
    }

    @Test
    public void whenCallingIsInferred_GetTheExpectedResult() {
        mockConceptMethod(IS_INFERRED, true);
        assertTrue(thing.isInferred());

        mockConceptMethod(IS_INFERRED, false);
        assertFalse(thing.isInferred());
    }

    @Test
    public void whenCallingIsAbstract_GetTheExpectedResult() {
        mockConceptMethod(IS_ABSTRACT, true);
        assertTrue(type.isAbstract());

        mockConceptMethod(IS_ABSTRACT, false);
        assertFalse(type.isAbstract());
    }

    @Test
    public void whenCallingGetValue_GetTheExpectedResult() {
        mockConceptMethod(GET_VALUE, 123);
        assertEquals(123, ((Attribute<?>) attribute).getValue());
    }

    @Test
    public void whenCallingGetDataTypeOnAttributeType_GetTheExpectedResult() {
        mockConceptMethod(GET_DATA_TYPE_OF_TYPE, Optional.of(DataType.LONG));
        assertEquals(DataType.LONG, ((AttributeType<?>) attributeType).getDataType());
    }

    @Test
    public void whenCallingGetDataTypeOnAttribute_GetTheExpectedResult() {
        mockConceptMethod(GET_DATA_TYPE_OF_ATTRIBUTE, DataType.LONG);
        assertEquals(DataType.LONG, ((Attribute<?>) attribute).dataType());
    }

    @Test
    public void whenCallingGetRegex_GetTheExpectedResult() {
        mockConceptMethod(GET_REGEX, Optional.of("hello"));
        assertEquals("hello", attributeType.getRegex());
    }

    @Test
    public void whenCallingGetAttribute_GetTheExpectedResult() {
        String value = "Dunstan again";
        Attribute<String> attribute = RemoteConcepts.createAttribute(tx, A);

        mockConceptMethod(ConceptMethod.getAttribute(value), Optional.of(attribute));

        assertEquals(attribute, attributeType.getAttribute(value));
    }

    @Test
    public void whenCallingGetAttributeWhenThereIsNoResult_ReturnNull() {
        String value = "Dunstan > Oliver";
        mockConceptMethod(ConceptMethod.getAttribute(value), Optional.empty());
        assertNull(attributeType.getAttribute(value));
    }

    @Test
    public void whenCallingGetWhen_GetTheExpectedResult() {
        mockConceptMethod(GET_WHEN, Optional.of(PATTERN));
        assertEquals(PATTERN, rule.getWhen());
    }

    @Test
    public void whenCallingGetThen_GetTheExpectedResult() {
        mockConceptMethod(GET_THEN, Optional.of(PATTERN));
        assertEquals(PATTERN, rule.getThen());
    }

    @Test
    public void whenCallingIsDeleted_GetTheExpectedResult() {
        TxResponse response =
                TxResponse.newBuilder().setOptionalConcept(convertOptionalConcept(Optional.of(concept))).build();

        server.setResponse(GrpcUtil.getConceptRequest(ID), response);

        assertFalse(entity.isDeleted());

        TxResponse nullResponse =
                TxResponse.newBuilder().setOptionalConcept(convertOptionalConcept(Optional.empty())).build();

        server.setResponse(GrpcUtil.getConceptRequest(ID), nullResponse);

        assertTrue(entity.isDeleted());
    }

    @Test
    public void whenCallingSups_GetTheExpectedResult() {
        Type me = entityType;
        Type mySuper = RemoteConcepts.createEntityType(tx, A);
        Type mySupersSuper = RemoteConcepts.createEntityType(tx, B);
        Type metaType = RemoteConcepts.createMetaType(tx, C);

        mockConceptMethod(ConceptMethod.GET_SUPER_CONCEPTS, Stream.of(me, mySuper, mySupersSuper, metaType));

        Set<Type> sups = entityType.sups().collect(toSet());
        assertThat(sups, containsInAnyOrder(me, mySuper, mySupersSuper));
        assertThat(sups, not(hasItem(metaType)));
    }

    @Test
    public void whenCallingSubs_GetTheExpectedResult() {
        Type me = relationshipType;
        Type mySub = RemoteConcepts.createRelationshipType(tx, A);
        Type mySubsSub = RemoteConcepts.createRelationshipType(tx, B);

        mockConceptMethod(ConceptMethod.GET_SUB_CONCEPTS, Stream.of(me, mySub, mySubsSub));

        assertThat(relationshipType.subs().collect(toSet()), containsInAnyOrder(me, mySub, mySubsSub));
    }

    @Test
    public void whenCallingSup_GetTheExpectedResult() {
        SchemaConcept sup = RemoteConcepts.createEntityType(tx, A);
        mockConceptMethod(GET_DIRECT_SUPER, Optional.of(sup));
        assertEquals(sup, entityType.sup());
    }

    @Test
    public void whenCallingSupOnMetaType_GetNull() {
        mockConceptMethod(GET_DIRECT_SUPER, Optional.empty());
        assertNull(schemaConcept.sup());
    }

    @Test
    public void whenCallingType_GetTheExpectedResult() {
        Type type = RemoteConcepts.createEntityType(tx, A);

        mockConceptMethod(GET_DIRECT_TYPE, type);

        assertEquals(type, thing.type());
    }

    @Test
    public void whenCallingAttributesWithNoArguments_GetTheExpectedResult() {
        Attribute<?> a = RemoteConcepts.createAttribute(tx, A);
        Attribute<?> b = RemoteConcepts.createAttribute(tx, B);
        Attribute<?> c = RemoteConcepts.createAttribute(tx, C);

        mockConceptMethod(ConceptMethod.GET_ATTRIBUTES, Stream.of(a, b, c));

        assertThat(thing.attributes().collect(toSet()), containsInAnyOrder(a, b, c));
    }

    @Test
    public void whenCallingAttributesWithArguments_GetTheExpectedResult() {
        AttributeType<?> foo = RemoteConcepts.createAttributeType(tx, ConceptId.of("foo"));
        AttributeType<?> bar = RemoteConcepts.createAttributeType(tx, ConceptId.of("bar"));
        AttributeType<?> baz = RemoteConcepts.createAttributeType(tx, ConceptId.of("baz"));

        Attribute<?> a = RemoteConcepts.createAttribute(tx, A);
        Attribute<?> b = RemoteConcepts.createAttribute(tx, B);
        Attribute<?> c = RemoteConcepts.createAttribute(tx, C);

        mockConceptMethod(ConceptMethod.getAttributesByTypes(foo, bar, baz), Stream.of(a, b, c));

        assertThat(thing.attributes(foo, bar, baz).collect(toSet()), containsInAnyOrder(a, b, c));
    }

    @Test
    public void whenCallingKeysWithNoArguments_GetTheExpectedResult() {
        Attribute<?> a = RemoteConcepts.createAttribute(tx, A);
        Attribute<?> b = RemoteConcepts.createAttribute(tx, B);
        Attribute<?> c = RemoteConcepts.createAttribute(tx, C);

        mockConceptMethod(ConceptMethod.GET_KEYS, Stream.of(a, b, c));

        assertThat(thing.keys().collect(toSet()), containsInAnyOrder(a, b, c));
    }

    @Test
    public void whenCallingKeysWithArguments_GetTheExpectedResult() {
        AttributeType<?> foo = RemoteConcepts.createAttributeType(tx, ConceptId.of("foo"));
        AttributeType<?> bar = RemoteConcepts.createAttributeType(tx, ConceptId.of("bar"));
        AttributeType<?> baz = RemoteConcepts.createAttributeType(tx, ConceptId.of("baz"));

        Attribute<?> a = RemoteConcepts.createAttribute(tx, A);
        Attribute<?> b = RemoteConcepts.createAttribute(tx, B);
        Attribute<?> c = RemoteConcepts.createAttribute(tx, C);

        mockConceptMethod(ConceptMethod.getKeysByTypes(foo, bar, baz), Stream.of(a, b, c));

        assertThat(thing.keys(foo, bar, baz).collect(toSet()), containsInAnyOrder(a, b, c));
    }

    @Test
    public void whenCallingPlays_GetTheExpectedResult() {
        Role a = RemoteConcepts.createRole(tx, A);
        Role b = RemoteConcepts.createRole(tx, B);
        Role c = RemoteConcepts.createRole(tx, C);

        mockConceptMethod(ConceptMethod.GET_ROLES_PLAYED_BY_TYPE, Stream.of(a, b, c));

        assertThat(type.plays().collect(toSet()), containsInAnyOrder(a, b, c));
    }

    @Test
    public void whenCallingInstances_GetTheExpectedResult() {
        Thing a = RemoteConcepts.createRelationship(tx, A);
        Thing b = RemoteConcepts.createRelationship(tx, B);
        Thing c = RemoteConcepts.createRelationship(tx, C);

        mockConceptMethod(ConceptMethod.GET_INSTANCES, Stream.of(a, b, c));

        assertThat(relationshipType.instances().collect(toSet()), containsInAnyOrder(a, b, c));
    }

    @Test
    public void whenCallingThingPlays_GetTheExpectedResult() {
        Role a = RemoteConcepts.createRole(tx, A);
        Role b = RemoteConcepts.createRole(tx, B);
        Role c = RemoteConcepts.createRole(tx, C);

        mockConceptMethod(ConceptMethod.GET_ROLES_PLAYED_BY_THING, Stream.of(a, b, c));

        assertThat(thing.plays().collect(toSet()), containsInAnyOrder(a, b, c));
    }

    @Test
    public void whenCallingRelationshipsWithNoArguments_GetTheExpectedResult() {
        Relationship a = RemoteConcepts.createRelationship(tx, A);
        Relationship b = RemoteConcepts.createRelationship(tx, B);
        Relationship c = RemoteConcepts.createRelationship(tx, C);

        mockConceptMethod(ConceptMethod.GET_RELATIONSHIPS, Stream.of(a, b, c));

        assertThat(thing.relationships().collect(toSet()), containsInAnyOrder(a, b, c));
    }

    @Test
    public void whenCallingRelationshipsWithRoles_GetTheExpectedResult() {
        Role foo = RemoteConcepts.createRole(tx, ConceptId.of("foo"));
        Role bar = RemoteConcepts.createRole(tx, ConceptId.of("bar"));
        Role baz = RemoteConcepts.createRole(tx, ConceptId.of("baz"));

        Relationship a = RemoteConcepts.createRelationship(tx, A);
        Relationship b = RemoteConcepts.createRelationship(tx, B);
        Relationship c = RemoteConcepts.createRelationship(tx, C);

        mockConceptMethod(ConceptMethod.getRelationshipsByRoles(foo, bar, baz), Stream.of(a, b, c));

        assertThat(thing.relationships(foo, bar, baz).collect(toSet()), containsInAnyOrder(a, b, c));
    }

    @Test
    public void whenCallingRelationshipTypes_GetTheExpectedResult() {
        RelationshipType a = RemoteConcepts.createRelationshipType(tx, A);
        RelationshipType b = RemoteConcepts.createRelationshipType(tx, B);
        RelationshipType c = RemoteConcepts.createRelationshipType(tx, C);

        mockConceptMethod(ConceptMethod.GET_RELATIONSHIP_TYPES_THAT_RELATE_ROLE, Stream.of(a, b, c));

        assertThat(role.relationshipTypes().collect(toSet()), containsInAnyOrder(a, b, c));
    }

    @Test
    public void whenCallingPlayedByTypes_GetTheExpectedResult() {
        Type a = RemoteConcepts.createEntityType(tx, A);
        Type b = RemoteConcepts.createRelationshipType(tx, B);
        Type c = RemoteConcepts.createAttributeType(tx, C);

        mockConceptMethod(ConceptMethod.GET_TYPES_THAT_PLAY_ROLE, Stream.of(a, b, c));

        assertThat(role.playedByTypes().collect(toSet()), containsInAnyOrder(a, b, c));
    }

    @Test
    public void whenCallingRelates_GetTheExpectedResult() {
        Role a = RemoteConcepts.createRole(tx, A);
        Role b = RemoteConcepts.createRole(tx, B);
        Role c = RemoteConcepts.createRole(tx, C);

        mockConceptMethod(ConceptMethod.GET_RELATED_ROLES, Stream.of(a, b, c));

        assertThat(relationshipType.relates().collect(toSet()), containsInAnyOrder(a, b, c));
    }

    @Test
    public void whenCallingAllRolePlayers_GetTheExpectedResult() {
        Role foo = RemoteConcepts.createRole(tx, ConceptId.of("foo"));
        Role bar = RemoteConcepts.createRole(tx, ConceptId.of("bar"));

        Thing a = RemoteConcepts.createEntity(tx, A);
        Thing b = RemoteConcepts.createRelationship(tx, B);
        Thing c = RemoteConcepts.createAttribute(tx, C);

        Stream<RolePlayer> expected = Stream.of(
                RolePlayer.create(foo, a),
                RolePlayer.create(bar, b),
                RolePlayer.create(bar, c)
        );

        TxResponse response = GET_ROLE_PLAYERS.createTxResponse(server.grpcIterators(), expected);

        server.setResponse(GrpcUtil.runConceptMethodRequest(ID, GET_ROLE_PLAYERS), response);

        Map<Role, Set<Thing>> allRolePlayers = relationship.allRolePlayers();
        assertEquals(expected, allRolePlayers);
    }

    @Test
    public void whenCallingRolePlayersWithNoArguments_GetTheExpectedResult() {
        Role foo = RemoteConcepts.createRole(tx, ConceptId.of("foo"));

        Thing a = RemoteConcepts.createEntity(tx, A);
        Thing b = RemoteConcepts.createRelationship(tx, B);
        Thing c = RemoteConcepts.createAttribute(tx, C);

        Stream<RolePlayer> expected = Stream.of(
                RolePlayer.create(foo, a), RolePlayer.create(foo, b), RolePlayer.create(foo, c)
        );

        mockConceptMethod(ConceptMethod.GET_ROLE_PLAYERS, expected);

        assertThat(relationship.rolePlayers().collect(toSet()), containsInAnyOrder(a, b, c));
    }

    @Test
    public void whenCallingRolePlayersWithRoles_GetTheExpectedResult() {
        Role foo = RemoteConcepts.createRole(tx, ConceptId.of("foo"));
        Role bar = RemoteConcepts.createRole(tx, ConceptId.of("bar"));
        Role baz = RemoteConcepts.createRole(tx, ConceptId.of("baz"));

        Thing a = RemoteConcepts.createEntity(tx, A);
        Thing b = RemoteConcepts.createRelationship(tx, B);
        Thing c = RemoteConcepts.createAttribute(tx, C);

        mockConceptMethod(ConceptMethod.getRolePlayersByRoles(foo, bar, baz), Stream.of(a, b, c));

        assertThat(relationship.rolePlayers(foo, bar, baz).collect(toSet()), containsInAnyOrder(a, b, c));
    }

    @Test
    public void whenCallingOwnerInstances_GetTheExpectedResult() {
        Thing a = RemoteConcepts.createEntity(tx, A);
        Thing b = RemoteConcepts.createRelationship(tx, A);
        Thing c = RemoteConcepts.createAttribute(tx, A);

        mockConceptMethod(ConceptMethod.GET_OWNERS, Stream.of(a, b, c));

        assertThat(attribute.ownerInstances().collect(toSet()), containsInAnyOrder(a, b, c));
    }

    @Test
    public void whenCallingAttributeTypes_GetTheExpectedResult() {

        ImmutableSet<AttributeType> attributeTypes = ImmutableSet.of(
                RemoteConcepts.createAttributeType(tx, A),
                RemoteConcepts.createAttributeType(tx, B),
                RemoteConcepts.createAttributeType(tx, C)
        );

        mockConceptMethod(GET_ATTRIBUTE_TYPES, attributeTypes.stream());

        assertEquals(attributeTypes, type.attributes().collect(toSet()));
    }

    @Test
    public void whenCallingKeyTypes_GetTheExpectedResult() {

        ImmutableSet<AttributeType> keyTypes = ImmutableSet.of(
                RemoteConcepts.createAttributeType(tx, A),
                RemoteConcepts.createAttributeType(tx, B),
                RemoteConcepts.createAttributeType(tx, C)
        );

        mockConceptMethod(GET_KEY_TYPES, keyTypes.stream());

        assertEquals(keyTypes, type.keys().collect(toSet()));
    }

    @Test
    public void whenCallingDelete_ExecuteAConceptMethod() {
        concept.delete();
        verifyConceptMethodCalled(ConceptMethod.DELETE);
    }

    @Test
    public void whenSettingSuper_ExecuteAConceptMethod() {
        EntityType sup = RemoteConcepts.createEntityType(tx, A);
        assertEquals(entityType, entityType.sup(sup));
        verifyConceptMethodCalled(ConceptMethod.setDirectSuperConcept(sup));
    }

    @Test
    public void whenSettingSub_ExecuteAConceptMethod() {
        EntityType sup = RemoteConcepts.createEntityType(tx, A);
        assertEquals(sup, sup.sub(entityType));
        verifyConceptMethodCalled(ConceptMethod.setDirectSuperConcept(sup));
    }

    @Test
    public void whenSettingLabel_ExecuteAConceptMethod() {
        Label label = Label.of("Dunstan");
        assertEquals(schemaConcept, schemaConcept.setLabel(label));
        verifyConceptMethodCalled(ConceptMethod.setLabel(label));
    }

    @Test
    public void whenSettingRelates_ExecuteAConceptMethod() {
        Role role = RemoteConcepts.createRole(tx, A);
        assertEquals(relationshipType, relationshipType.relates(role));
        verifyConceptMethodCalled(ConceptMethod.setRelatedRole(role));
    }

    @Test
    public void whenSettingPlays_ExecuteAConceptMethod() {
        Role role = RemoteConcepts.createRole(tx, A);
        assertEquals(attributeType, attributeType.plays(role));
        verifyConceptMethodCalled(ConceptMethod.setRolePlayedByType(role));
    }

    @Test
    public void whenSettingAbstractOn_ExecuteAConceptMethod() {
        assertEquals(attributeType, attributeType.setAbstract(true));
        verifyConceptMethodCalled(ConceptMethod.setAbstract(true));
    }

    @Test
    public void whenSettingAbstractOff_ExecuteAConceptMethod() {
        assertEquals(attributeType, attributeType.setAbstract(false));
        verifyConceptMethodCalled(ConceptMethod.setAbstract(false));
    }

    @Test
    public void whenSettingAttributeType_ExecuteAConceptMethod() {
        AttributeType<?> attributeType = RemoteConcepts.createAttributeType(tx, A);
        assertEquals(type, type.attribute(attributeType));
        verifyConceptMethodCalled(ConceptMethod.setAttributeType(attributeType));
    }

    @Test
    public void whenSettingKeyType_ExecuteAConceptMethod() {
        AttributeType<?> attributeType = RemoteConcepts.createAttributeType(tx, A);
        assertEquals(type, type.key(attributeType));
        verifyConceptMethodCalled(ConceptMethod.setKeyType(attributeType));
    }

    @Test
    public void whenDeletingAttributeType_ExecuteAConceptMethod() {
        AttributeType<?> attributeType = RemoteConcepts.createAttributeType(tx, A);
        assertEquals(type, type.deleteAttribute(attributeType));
        verifyConceptMethodCalled(ConceptMethod.unsetAttributeType(attributeType));
    }

    @Test
    public void whenDeletingKeyType_ExecuteAConceptMethod() {
        AttributeType<?> attributeType = RemoteConcepts.createAttributeType(tx, A);
        assertEquals(type, type.deleteKey(attributeType));
        verifyConceptMethodCalled(ConceptMethod.unsetKeyType(attributeType));
    }

    @Test
    public void whenDeletingPlays_ExecuteAConceptMethod() {
        Role role = RemoteConcepts.createRole(tx, A);
        assertEquals(type, type.deletePlays(role));
        verifyConceptMethodCalled(ConceptMethod.unsetRolePlayedByType(role));
    }

    @Test
    public void whenCallingAddEntity_ExecuteAConceptMethod() {
        Entity entity = RemoteConcepts.createEntity(tx, A);
        mockConceptMethod(ConceptMethod.ADD_ENTITY, entity);
        assertEquals(entity, entityType.addEntity());
    }

    @Test
    public void whenCallingAddRelationship_ExecuteAConceptMethod() {
        Relationship relationship = RemoteConcepts.createRelationship(tx, A);
        mockConceptMethod(ConceptMethod.ADD_RELATIONSHIP, relationship);
        assertEquals(relationship, relationshipType.addRelationship());
    }

    @Test
    public void whenCallingPutAttribute_ExecuteAConceptMethod() {
        String value = "Dunstan";
        Attribute<String> attribute = RemoteConcepts.createAttribute(tx, A);
        mockConceptMethod(ConceptMethod.putAttribute(value), attribute);
        assertEquals(attribute, attributeType.putAttribute(value));
    }

    @Test
    public void whenCallingDeleteRelates_ExecuteAConceptMethod() {
        Role role = RemoteConcepts.createRole(tx, A);
        assertEquals(relationshipType, relationshipType.deleteRelates(role));
        verifyConceptMethodCalled(ConceptMethod.unsetRelatedRole(role));
    }

    @Test
    public void whenSettingRegex_ExecuteAConceptMethod() {
        String regex = "[abc]";
        assertEquals(attributeType, attributeType.setRegex(regex));
        verifyConceptMethodCalled(ConceptMethod.setRegex(Optional.of(regex)));
    }

    @Test
    public void whenResettingRegex_ExecuteAQuery() {
        assertEquals(attributeType, attributeType.setRegex(null));
        verifyConceptMethodCalled(ConceptMethod.setRegex(Optional.empty()));
    }

    @Test
    public void whenCallingAddAttributeOnThing_ExecuteAConceptMethod() {
        Attribute<Long> attribute = RemoteConcepts.createAttribute(tx, A);
        Relationship relationship = RemoteConcepts.createRelationship(tx, C);
        mockConceptMethod(ConceptMethod.setAttribute(attribute), relationship);

        assertEquals(thing, thing.attribute(attribute));

        verifyConceptMethodCalled(ConceptMethod.setAttribute(attribute));
    }

    @Test
    public void whenCallingAddAttributeRelationshipOnThing_ExecuteAConceptMethod() {
        Attribute<Long> attribute = RemoteConcepts.createAttribute(tx, A);
        Relationship relationship = RemoteConcepts.createRelationship(tx, C);
        mockConceptMethod(ConceptMethod.setAttribute(attribute), relationship);
        assertEquals(relationship, thing.attributeRelationship(attribute));
    }

    @Test
    public void whenCallingDeleteAttribute_ExecuteAConceptMethod() {
        Attribute<Long> attribute = RemoteConcepts.createAttribute(tx, A);
        assertEquals(thing, thing.deleteAttribute(attribute));
        verifyConceptMethodCalled(ConceptMethod.unsetAttribute(attribute));
    }

    @Test
    public void whenCallingAddRolePlayer_ExecuteAConceptMethod() {
        Role role = RemoteConcepts.createRole(tx, A);
        Thing thing = RemoteConcepts.createEntity(tx, B);
        assertEquals(relationship, relationship.addRolePlayer(role, thing));

        verifyConceptMethodCalled(ConceptMethod.setRolePlayer(RolePlayer.create(role, thing)));
    }

    @Test
    public void whenCallingRemoveRolePlayer_ExecuteAConceptMethod() {
        Role role = RemoteConcepts.createRole(tx, A);
        Thing thing = RemoteConcepts.createEntity(tx, B);
        relationship.removeRolePlayer(role, thing);
        verifyConceptMethodCalled(ConceptMethod.removeRolePlayer(RolePlayer.create(role, thing)));
    }

    private void verifyConceptMethodCalled(ConceptMethod<?> conceptMethod) {
        verify(server.requests()).onNext(GrpcUtil.runConceptMethodRequest(ID, conceptMethod));
    }

    private <T> void mockConceptMethod(ConceptMethod<T> property, T value) {
        server.setResponse(
                GrpcUtil.runConceptMethodRequest(ID, property),
                property.createTxResponse(server.grpcIterators(), value)
        );
    }
}