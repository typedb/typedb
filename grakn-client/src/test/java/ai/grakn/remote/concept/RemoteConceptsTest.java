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
import ai.grakn.graql.Query;
import ai.grakn.graql.Var;
import ai.grakn.graql.VarPattern;
import ai.grakn.grpc.ConceptMethod;
import ai.grakn.grpc.GrpcUtil;
import ai.grakn.remote.GrpcServerMock;
import ai.grakn.remote.RemoteGraknSession;
import ai.grakn.remote.RemoteGraknTx;
import ai.grakn.rpc.generated.GrpcConcept;
import ai.grakn.rpc.generated.GrpcConcept.BaseType;
import ai.grakn.rpc.generated.GrpcGrakn;
import ai.grakn.rpc.generated.GrpcGrakn.QueryResult;
import ai.grakn.rpc.generated.GrpcGrakn.TxResponse;
import ai.grakn.util.SimpleURI;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static ai.grakn.graql.Graql.define;
import static ai.grakn.graql.Graql.insert;
import static ai.grakn.graql.Graql.match;
import static ai.grakn.graql.Graql.undefine;
import static ai.grakn.graql.Graql.var;
import static ai.grakn.grpc.ConceptMethod.GET_ATTRIBUTE_TYPES;
import static ai.grakn.grpc.ConceptMethod.GET_DATA_TYPE;
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
import static ai.grakn.remote.concept.RemoteConcept.ME;
import static ai.grakn.remote.concept.RemoteConcept.TARGET;
import static ai.grakn.util.CommonUtil.toImmutableSet;
import static ai.grakn.util.Schema.MetaSchema.THING;
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
    private static final VarPattern ME_ID = ME.id(ID);
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
        mockLabelResponse(ID, LABEL);
        assertEquals(LABEL, schemaConcept.getLabel());
    }

    @Test
    public void whenCallingIsImplicit_GetTheExpectedResult() {
        mockPropertyResponse(IS_IMPLICIT, true);
        assertTrue(schemaConcept.isImplicit());

        mockPropertyResponse(IS_IMPLICIT, false);
        assertFalse(schemaConcept.isImplicit());
    }

    @Test
    public void whenCallingIsInferred_GetTheExpectedResult() {
        mockPropertyResponse(IS_INFERRED, true);
        assertTrue(thing.isInferred());

        mockPropertyResponse(IS_INFERRED, false);
        assertFalse(thing.isInferred());
    }

    @Test
    public void whenCallingIsAbstract_GetTheExpectedResult() {
        mockPropertyResponse(IS_ABSTRACT, true);
        assertTrue(type.isAbstract());

        mockPropertyResponse(IS_ABSTRACT, false);
        assertFalse(type.isAbstract());
    }

    @Test
    public void whenCallingGetValue_GetTheExpectedResult() {
        mockPropertyResponse(GET_VALUE, 123);
        assertEquals(123, ((Attribute<?>) attribute).getValue());
    }

    @Test
    public void whenCallingGetDataTypeOnAttributeType_GetTheExpectedResult() {
        mockPropertyResponse(GET_DATA_TYPE, DataType.LONG);
        assertEquals(DataType.LONG, ((AttributeType<?>) attributeType).getDataType());
    }

    @Test
    public void whenCallingGetDataTypeOnAttribute_GetTheExpectedResult() {
        mockPropertyResponse(GET_DATA_TYPE, DataType.LONG);
        assertEquals(DataType.LONG, ((Attribute<?>) attribute).dataType());
    }

    @Test
    public void whenCallingGetRegex_GetTheExpectedResult() {
        mockPropertyResponse(GET_REGEX, "hello");
        assertEquals("hello", attributeType.getRegex());
    }

    @Test
    public void whenCallingGetAttribute_GetTheExpectedResult() {
        String value = "Dunstan again";
        Attribute<String> attribute = RemoteConcepts.createAttribute(tx, A);

        mockPropertyResponse(ConceptMethod.getAttribute(value), attribute);

        assertEquals(attribute, attributeType.getAttribute(value));
    }

    @Test
    public void whenCallingGetAttributeWhenThereIsNoResult_ReturnNull() {
        String value = "Dunstan > Oliver";
        mockPropertyResponse(ConceptMethod.getAttribute(value), null);
        assertNull(attributeType.getAttribute(value));
    }

    @Test
    public void whenCallingGetWhen_GetTheExpectedResult() {
        mockPropertyResponse(GET_WHEN, PATTERN);
        assertEquals(PATTERN, rule.getWhen());
    }

    @Test
    public void whenCallingGetThen_GetTheExpectedResult() {
        mockPropertyResponse(GET_THEN, PATTERN);
        assertEquals(PATTERN, rule.getThen());
    }

    @Test
    public void whenCallingIsDeleted_GetTheExpectedResult() {
        mockPropertyResponse(ConceptMethod.EXISTS, false);
        assertFalse(entity.isDeleted());
        mockPropertyResponse(ConceptMethod.EXISTS, true);
        assertTrue(entity.isDeleted());
    }

    @Test
    public void whenCallingSups_GetTheExpectedResult() {
        Type me = entityType;
        Type mySuper = RemoteConcepts.createEntityType(tx, A);
        Type mySupersSuper = RemoteConcepts.createEntityType(tx, B);
        Type metaType = RemoteConcepts.createMetaType(tx, C);

        mockPropertyResponse(ConceptMethod.GET_SUPER_CONCEPTS, Stream.of(me, mySuper, mySupersSuper, metaType));

        Set<Type> sups = entityType.sups().collect(toSet());
        assertThat(sups, containsInAnyOrder(me, mySuper, mySupersSuper));
        assertThat(sups, not(hasItem(metaType)));
    }

    @Test
    public void whenCallingSubs_GetTheExpectedResult() {
        Type me = relationshipType;
        Type mySub = RemoteConcepts.createRelationshipType(tx, A);
        Type mySubsSub = RemoteConcepts.createRelationshipType(tx, B);

        mockPropertyResponse(ConceptMethod.GET_SUB_CONCEPTS, Stream.of(me, mySub, mySubsSub));

        assertThat(relationshipType.subs().collect(toSet()), containsInAnyOrder(me, mySub, mySubsSub));
    }

    @Test
    public void whenCallingSup_GetTheExpectedResult() {
        SchemaConcept sup = RemoteConcepts.createEntityType(tx, A);
        mockLabelResponse(A, Label.of("A"));

        mockPropertyResponse(GET_DIRECT_SUPER, sup);

        assertEquals(sup, entityType.sup());
    }

    @Test
    public void whenCallingSupOnMetaType_GetNull() {
        SchemaConcept sup = RemoteConcepts.createMetaType(tx, A);
        mockLabelResponse(A, THING.getLabel());

        mockPropertyResponse(GET_DIRECT_SUPER, sup);

        assertNull(schemaConcept.sup());
    }

    @Test
    public void whenCallingType_GetTheExpectedResult() {
        Type type = RemoteConcepts.createEntityType(tx, A);

        mockPropertyResponse(GET_DIRECT_TYPE, type);

        assertEquals(type, thing.type());
    }

    @Test
    public void whenCallingAttributesWithNoArguments_GetTheExpectedResult() {
        Attribute<?> a = RemoteConcepts.createAttribute(tx, A);
        Attribute<?> b = RemoteConcepts.createAttribute(tx, B);
        Attribute<?> c = RemoteConcepts.createAttribute(tx, C);

        mockPropertyResponse(ConceptMethod.GET_ATTRIBUTES, Stream.of(a, b, c));

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

        mockPropertyResponse(ConceptMethod.getAttributesByTypes(foo, bar, baz), Stream.of(a, b, c));

        assertThat(thing.attributes(foo, bar, baz).collect(toSet()), containsInAnyOrder(a, b, c));
    }

    @Test
    public void whenCallingPlays_GetTheExpectedResult() {
        Role a = RemoteConcepts.createRole(tx, A);
        Role b = RemoteConcepts.createRole(tx, B);
        Role c = RemoteConcepts.createRole(tx, C);

        mockPropertyResponse(ConceptMethod.GET_ROLES_PLAYED_BY_TYPE, Stream.of(a, b, c));

        assertThat(type.plays().collect(toSet()), containsInAnyOrder(a, b, c));
    }

    @Test
    public void whenCallingInstances_GetTheExpectedResult() {
        Thing a = RemoteConcepts.createRelationship(tx, A);
        Thing b = RemoteConcepts.createRelationship(tx, B);
        Thing c = RemoteConcepts.createRelationship(tx, C);

        mockPropertyResponse(ConceptMethod.GET_INSTANCES, Stream.of(a, b, c));

        assertThat(relationshipType.instances().collect(toSet()), containsInAnyOrder(a, b, c));
    }

    @Test
    public void whenCallingThingPlays_GetTheExpectedResult() {
        Role a = RemoteConcepts.createRole(tx, A);
        Role b = RemoteConcepts.createRole(tx, B);
        Role c = RemoteConcepts.createRole(tx, C);

        mockPropertyResponse(ConceptMethod.GET_ROLES_PLAYED_BY_THING, Stream.of(a, b, c));

        assertThat(thing.plays().collect(toSet()), containsInAnyOrder(a, b, c));
    }

    @Test
    public void whenCallingRelationshipsWithNoArguments_GetTheExpectedResult() {
        Relationship a = RemoteConcepts.createRelationship(tx, A);
        Relationship b = RemoteConcepts.createRelationship(tx, B);
        Relationship c = RemoteConcepts.createRelationship(tx, C);

        mockPropertyResponse(ConceptMethod.GET_RELATIONSHIPS, Stream.of(a, b, c));

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

        mockPropertyResponse(ConceptMethod.getRelationshipsByRoles(foo, bar, baz), Stream.of(a, b, c));

        assertThat(thing.relationships(foo, bar, baz).collect(toSet()), containsInAnyOrder(a, b, c));
    }

    @Test
    public void whenCallingRelationshipTypes_GetTheExpectedResult() {
        RelationshipType a = RemoteConcepts.createRelationshipType(tx, A);
        RelationshipType b = RemoteConcepts.createRelationshipType(tx, B);
        RelationshipType c = RemoteConcepts.createRelationshipType(tx, C);

        mockPropertyResponse(ConceptMethod.GET_RELATIONSHIP_TYPES_THAT_RELATE_ROLE, Stream.of(a, b, c));

        assertThat(role.relationshipTypes().collect(toSet()), containsInAnyOrder(a, b, c));
    }

    @Test
    public void whenCallingPlayedByTypes_GetTheExpectedResult() {
        Type a = RemoteConcepts.createEntityType(tx, A);
        Type b = RemoteConcepts.createRelationshipType(tx, B);
        Type c = RemoteConcepts.createAttributeType(tx, C);

        mockPropertyResponse(ConceptMethod.GET_TYPES_THAT_PLAY_ROLE, Stream.of(a, b, c));

        assertThat(role.playedByTypes().collect(toSet()), containsInAnyOrder(a, b, c));
    }

    @Test
    public void whenCallingRelates_GetTheExpectedResult() {
        Role a = RemoteConcepts.createRole(tx, A);
        Role b = RemoteConcepts.createRole(tx, B);
        Role c = RemoteConcepts.createRole(tx, C);

        mockPropertyResponse(ConceptMethod.GET_RELATED_ROLES, Stream.of(a, b, c));

        assertThat(relationshipType.relates().collect(toSet()), containsInAnyOrder(a, b, c));
    }

    @Test
    public void whenCallingAllRolePlayers_GetTheExpectedResult() {
        Role foo = RemoteConcepts.createRole(tx, ConceptId.of("foo"));
        Role bar = RemoteConcepts.createRole(tx, ConceptId.of("bar"));

        Thing a = RemoteConcepts.createEntity(tx, A);
        Thing b = RemoteConcepts.createRelationship(tx, B);
        Thing c = RemoteConcepts.createAttribute(tx, C);

        Map<Role, Set<Thing>> expected = ImmutableMap.of(
                foo, ImmutableSet.of(a),
                bar, ImmutableSet.of(b, c)
        );

        TxResponse response = GET_ROLE_PLAYERS.createTxResponse(expected);

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

        mockPropertyResponse(ConceptMethod.GET_ROLE_PLAYERS, ImmutableMap.of(foo, ImmutableSet.of(a, b, c)));

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

        mockPropertyResponse(ConceptMethod.getRolePlayersByRoles(foo, bar, baz), Stream.of(a, b, c));

        assertThat(relationship.rolePlayers(foo, bar, baz).collect(toSet()), containsInAnyOrder(a, b, c));
    }

    @Test
    public void whenCallingOwnerInstances_GetTheExpectedResult() {
        Thing a = RemoteConcepts.createEntity(tx, A);
        Thing b = RemoteConcepts.createRelationship(tx, A);
        Thing c = RemoteConcepts.createAttribute(tx, A);

        mockPropertyResponse(ConceptMethod.GET_OWNERS, Stream.of(a, b, c));

        assertThat(attribute.ownerInstances().collect(toSet()), containsInAnyOrder(a, b, c));
    }

    @Test
    public void whenCallingAttributeTypes_GetTheExpectedResult() {

        ImmutableSet<AttributeType> attributeTypes = ImmutableSet.of(
                RemoteConcepts.createAttributeType(tx, A),
                RemoteConcepts.createAttributeType(tx, B),
                RemoteConcepts.createAttributeType(tx, C)
        );

        mockPropertyResponse(GET_ATTRIBUTE_TYPES, attributeTypes.stream());

        assertEquals(attributeTypes, type.attributes().collect(toSet()));
    }

    @Test
    public void whenCallingKeyTypes_GetTheExpectedResult() {

        ImmutableSet<AttributeType> keyTypes = ImmutableSet.of(
                RemoteConcepts.createAttributeType(tx, A),
                RemoteConcepts.createAttributeType(tx, B),
                RemoteConcepts.createAttributeType(tx, C)
        );

        mockPropertyResponse(GET_KEY_TYPES, keyTypes.stream());

        assertEquals(keyTypes, type.keys().collect(toSet()));
    }

    @Test
    public void whenCallingDelete_ExecuteAConceptMethod() {
        concept.delete();
        verify(server.requests()).onNext(GrpcUtil.runConceptMethodRequest(ID, ConceptMethod.DELETE));
    }

    @Test
    public void whenSettingSuper_ExecuteAQuery() {
        Query<?> query = define(ME_ID, TARGET.id(A), ME.sub(TARGET));

        EntityType sup = RemoteConcepts.createEntityType(tx, A);

        server.setResponseSequence(GrpcUtil.execQueryRequest(query), emptyQueryResultResponse());

        assertEquals(entityType, entityType.sup(sup));

        verify(server.requests()).onNext(GrpcUtil.execQueryRequest(query));
    }

    @Test
    public void whenSettingSub_ExecuteAQuery() {
        Query<?> query = define(ME_ID, TARGET.id(A), TARGET.sub(ME));

        EntityType sub = RemoteConcepts.createEntityType(tx, A);

        server.setResponseSequence(GrpcUtil.execQueryRequest(query), emptyQueryResultResponse());

        assertEquals(entityType, entityType.sub(sub));

        verify(server.requests()).onNext(GrpcUtil.execQueryRequest(query));
    }

    @Test
    public void whenSettingLabel_ExecuteAQuery() {
        Label label = Label.of("Dunstan");

        Query<?> query = define(ME_ID, ME.label(label));

        server.setResponseSequence(GrpcUtil.execQueryRequest(query), emptyQueryResultResponse());

        assertEquals(schemaConcept, schemaConcept.setLabel(label));

        verify(server.requests()).onNext(GrpcUtil.execQueryRequest(query));
    }

    @Test
    public void whenSettingRelates_ExecuteAQuery() {
        Query<?> query = define(ME_ID, TARGET.id(A), ME.relates(TARGET));

        Role role = RemoteConcepts.createRole(tx, A);

        server.setResponseSequence(GrpcUtil.execQueryRequest(query), emptyQueryResultResponse());

        assertEquals(relationshipType, relationshipType.relates(role));

        verify(server.requests()).onNext(GrpcUtil.execQueryRequest(query));
    }

    @Test
    public void whenSettingPlays_ExecuteAQuery() {
        Query<?> query = define(ME_ID, TARGET.id(A), ME.plays(TARGET));

        Role role = RemoteConcepts.createRole(tx, A);

        server.setResponseSequence(GrpcUtil.execQueryRequest(query), emptyQueryResultResponse());

        assertEquals(attributeType, attributeType.plays(role));

        verify(server.requests()).onNext(GrpcUtil.execQueryRequest(query));
    }

    @Test
    public void whenSettingAbstractOn_ExecuteAQuery() {
        Query<?> query = define(ME_ID, ME.isAbstract());

        server.setResponseSequence(GrpcUtil.execQueryRequest(query), emptyQueryResultResponse());

        assertEquals(attributeType, attributeType.setAbstract(true));

        verify(server.requests()).onNext(GrpcUtil.execQueryRequest(query));
    }

    @Test
    public void whenSettingAbstractOff_ExecuteAQuery() {
        Query<?> query = undefine(ME_ID, ME.isAbstract());

        server.setResponseSequence(GrpcUtil.execQueryRequest(query), emptyQueryResultResponse());

        assertEquals(attributeType, attributeType.setAbstract(false));

        verify(server.requests()).onNext(GrpcUtil.execQueryRequest(query));
    }

    @Test
    public void whenSettingAttributeType_ExecuteAQuery() {
        Label label = Label.of("yes");

        Query<?> query = define(ME_ID, ME.has(var().label(label)));

        AttributeType<?> attributeType = RemoteConcepts.createAttributeType(tx, A);
        mockLabelResponse(A, label);

        server.setResponseSequence(GrpcUtil.execQueryRequest(query), emptyQueryResultResponse());

        assertEquals(type, type.attribute(attributeType));

        verify(server.requests()).onNext(GrpcUtil.execQueryRequest(query));
    }

    @Test
    public void whenSettingKeyType_ExecuteAQuery() {
        Label label = Label.of("yes");

        Query<?> query = define(ME_ID, ME.key(var().label(label)));

        AttributeType<?> attributeType = RemoteConcepts.createAttributeType(tx, A);
        mockLabelResponse(A, label);

        server.setResponseSequence(GrpcUtil.execQueryRequest(query), emptyQueryResultResponse());

        assertEquals(type, type.key(attributeType));

        verify(server.requests()).onNext(GrpcUtil.execQueryRequest(query));
    }

    @Test
    public void whenDeletingAttributeType_ExecuteAQuery() {
        Label label = Label.of("yes");

        Query<?> query = undefine(ME_ID, ME.has(var().label(label)));

        AttributeType<?> attributeType = RemoteConcepts.createAttributeType(tx, A);
        mockLabelResponse(A, label);

        server.setResponseSequence(GrpcUtil.execQueryRequest(query), emptyQueryResultResponse());

        assertEquals(type, type.deleteAttribute(attributeType));

        verify(server.requests()).onNext(GrpcUtil.execQueryRequest(query));
    }

    @Test
    public void whenDeletingKeyType_ExecuteAQuery() {
        Label label = Label.of("yes");

        Query<?> query = undefine(ME_ID, ME.key(var().label(label)));

        AttributeType<?> attributeType = RemoteConcepts.createAttributeType(tx, A);
        mockLabelResponse(A, label);

        server.setResponseSequence(GrpcUtil.execQueryRequest(query), emptyQueryResultResponse());

        assertEquals(type, type.deleteKey(attributeType));

        verify(server.requests()).onNext(GrpcUtil.execQueryRequest(query));
    }

    @Test
    public void whenDeletingPlays_ExecuteAQuery() {
        Query<?> query = undefine(ME_ID, TARGET.id(A), ME.plays(TARGET));

        Role role = RemoteConcepts.createRole(tx, A);

        server.setResponseSequence(GrpcUtil.execQueryRequest(query), emptyQueryResultResponse());

        assertEquals(type, type.deletePlays(role));

        verify(server.requests()).onNext(GrpcUtil.execQueryRequest(query));
    }

    @Test
    public void whenCallingAddEntity_ExecuteAQuery() {
        Query<?> query = insert(ME_ID, TARGET.isa(ME));

        Entity entity = RemoteConcepts.createEntity(tx, A);

        server.setResponseSequence(GrpcUtil.execQueryRequest(query), queryResultResponse(entity));

        assertEquals(entity, entityType.addEntity());
    }

    @Test
    public void whenCallingAddRelationship_ExecuteAQuery() {
        Query<?> query = insert(ME_ID, TARGET.isa(ME));

        Relationship relationship = RemoteConcepts.createRelationship(tx, A);

        server.setResponseSequence(GrpcUtil.execQueryRequest(query), queryResultResponse(relationship));

        assertEquals(relationship, relationshipType.addRelationship());
    }

    @Test
    public void whenCallingPutAttribute_ExecuteAQuery() {
        String value = "Dunstan";

        Query<?> query = insert(ME_ID, TARGET.val(value).isa(ME));

        Attribute<String> attribute = RemoteConcepts.createAttribute(tx, A);

        server.setResponseSequence(GrpcUtil.execQueryRequest(query), queryResultResponse(attribute));

        assertEquals(attribute, attributeType.putAttribute(value));
    }

    @Test
    public void whenCallingDeleteRelates_ExecuteAQuery() {
        Query<?> query = undefine(ME_ID, TARGET.id(A), ME.relates(TARGET));

        Role role = RemoteConcepts.createRole(tx, A);

        server.setResponseSequence(GrpcUtil.execQueryRequest(query), emptyQueryResultResponse());

        assertEquals(relationshipType, relationshipType.deleteRelates(role));

        verify(server.requests()).onNext(GrpcUtil.execQueryRequest(query));
    }

    @Test
    public void whenSettingRegex_ExecuteAQuery() {
        String regex = "[abc]";

        Query<?> query = define(ME_ID, ME.regex(regex));

        server.setResponseSequence(GrpcUtil.execQueryRequest(query), emptyQueryResultResponse());

        assertEquals(attributeType, attributeType.setRegex(regex));

        verify(server.requests()).onNext(GrpcUtil.execQueryRequest(query));
    }

    @Test
    public void whenResettingRegex_ExecuteAQuery() {
        String regex = "[abc]";

        server.setResponse(
                GrpcUtil.runConceptMethodRequest(ID, ConceptMethod.GET_REGEX),
                ConceptMethod.GET_REGEX.createTxResponse(regex)
        );

        Query<?> query = undefine(ME_ID, ME.regex(regex));

        server.setResponseSequence(GrpcUtil.execQueryRequest(query), emptyQueryResultResponse());

        assertEquals(attributeType, attributeType.setRegex(null));

        verify(server.requests()).onNext(GrpcUtil.execQueryRequest(query));
    }

    @Test
    public void whenResettingUnsetRegex_DontCrash() {
        server.setResponse(
                GrpcUtil.runConceptMethodRequest(ID, ConceptMethod.GET_REGEX),
                ConceptMethod.GET_REGEX.createTxResponse((String) null)
        );

        assertEquals(attributeType, attributeType.setRegex(null));
    }

    @Test
    public void whenCallingAddAttributeOnThing_ExecuteAQuery() {
        Label label = Label.of("yes");

        Var attributeVar = var("attribute");

        Query<?> query = insert(ME_ID, attributeVar.id(A), ME.has(label, attributeVar, TARGET));

        Attribute<Long> attribute = RemoteConcepts.createAttribute(tx, A);
        AttributeType<Long> attributeType = RemoteConcepts.createAttributeType(tx, B);

        server.setResponse(
                GrpcUtil.runConceptMethodRequest(A, ConceptMethod.GET_DIRECT_TYPE),
                ConceptMethod.GET_DIRECT_TYPE.createTxResponse(attributeType)
        );
        mockLabelResponse(attributeType, label);

        server.setResponseSequence(GrpcUtil.execQueryRequest(query), queryResultResponse(C, BaseType.Relationship));

        assertEquals(thing, thing.attribute(attribute));

        verify(server.requests()).onNext(GrpcUtil.execQueryRequest(query));
    }

    @Test
    public void whenCallingAddAttributeRelationshipOnThing_ExecuteAQuery() {
        Label label = Label.of("yes");

        Var attributeVar = var("attribute");

        Query<?> query = insert(ME_ID, attributeVar.id(A), ME.has(label, attributeVar, TARGET));

        Attribute<Long> attribute = RemoteConcepts.createAttribute(tx, A);
        AttributeType<Long> attributeType = RemoteConcepts.createAttributeType(tx, B);
        Relationship relationship = RemoteConcepts.createRelationship(tx, C);

        server.setResponse(
                GrpcUtil.runConceptMethodRequest(A, ConceptMethod.GET_DIRECT_TYPE),
                ConceptMethod.GET_DIRECT_TYPE.createTxResponse(attributeType)
        );
        mockLabelResponse(attributeType, label);

        server.setResponseSequence(GrpcUtil.execQueryRequest(query), queryResultResponse(relationship));

        assertEquals(relationship, thing.attributeRelationship(attribute));

        verify(server.requests()).onNext(GrpcUtil.execQueryRequest(query));
    }

    @Test
    public void whenCallingDeleteAttribute_ExecuteAQuery() {
        Label label = Label.of("yes");

        Var attributeVar = var("attribute");

        Query<?> query = match(ME_ID, attributeVar.id(A), ME.has(label, attributeVar, TARGET)).delete(TARGET);

        Attribute<Long> attribute = RemoteConcepts.createAttribute(tx, A);
        AttributeType<Long> attributeType = RemoteConcepts.createAttributeType(tx, B);

        server.setResponse(
                GrpcUtil.runConceptMethodRequest(A, ConceptMethod.GET_DIRECT_TYPE),
                ConceptMethod.GET_DIRECT_TYPE.createTxResponse(attributeType)
        );
        mockLabelResponse(attributeType, label);

        server.setResponseSequence(GrpcUtil.execQueryRequest(query));

        assertEquals(thing, thing.deleteAttribute(attribute));

        verify(server.requests()).onNext(GrpcUtil.execQueryRequest(query));
    }

    @Test
    public void whenCallingAddRolePlayer_ExecuteAQuery() {
        Var roleVar = var("role");

        Query<?> query = insert(ME_ID, roleVar.id(A), TARGET.id(B), ME.rel(roleVar, TARGET));

        Role role = RemoteConcepts.createRole(tx, A);
        Thing thing = RemoteConcepts.createEntity(tx, B);

        server.setResponseSequence(GrpcUtil.execQueryRequest(query), queryResultResponse(thing));

        assertEquals(relationship, relationship.addRolePlayer(role, thing));

        verify(server.requests()).onNext(GrpcUtil.execQueryRequest(query));
    }

    @Test
    public void whenCallingRemoveRolePlayer_ExecuteAConceptMethod() {
        Role role = RemoteConcepts.createRole(tx, A);
        Thing thing = RemoteConcepts.createEntity(tx, B);

        relationship.removeRolePlayer(role, thing);

        GrpcGrakn.TxRequest request = GrpcUtil.runConceptMethodRequest(ID, ConceptMethod.removeRolePlayer(role, thing));
        verify(server.requests()).onNext(request);
    }

    private void mockLabelResponse(Concept concept, Label label) {
        mockLabelResponse(concept.getId(), label);
    }

    private void mockLabelResponse(ConceptId id, Label label) {
        server.setResponse(
                GrpcUtil.runConceptMethodRequest(id, ConceptMethod.GET_LABEL),
                ConceptMethod.GET_LABEL.createTxResponse(label)
        );
    }

    private static TxResponse emptyQueryResultResponse() {
        return queryResultResponse(GrpcGrakn.Answer.getDefaultInstance());
    }

    private static TxResponse queryResultResponse(Concept concept) {
        return queryResultResponse(concept.getId(), GrpcUtil.getBaseType(concept));
    }

    private static TxResponse queryResultResponse(ConceptId id, BaseType baseType) {
        GrpcConcept.ConceptId conceptId = GrpcConcept.ConceptId.newBuilder().setValue(id.getValue()).build();

        GrpcConcept.Concept concept =
                GrpcConcept.Concept.newBuilder().setId(conceptId).setBaseType(baseType).build();

        GrpcGrakn.Answer answer =
                GrpcGrakn.Answer.newBuilder().putAnswer(TARGET.getValue(), concept).build();

        return queryResultResponse(answer);
    }

    private static TxResponse queryResultResponse(GrpcGrakn.Answer answer) {
        QueryResult queryResult = QueryResult.newBuilder().setAnswer(answer).build();
        return TxResponse.newBuilder().setQueryResult(queryResult).build();
    }

    private Matcher<? super Stream<? extends Concept>> containsIds(ConceptId... expectedIds) {
        Set<ConceptId> expectedSet = ImmutableSet.copyOf(expectedIds);

        return new TypeSafeDiagnosingMatcher<Stream<? extends Concept>>() {
            @Override
            protected boolean matchesSafely(Stream<? extends Concept> stream, Description mismatchDescription) {
                Set<ConceptId> ids = stream.map(Concept::getId).collect(toImmutableSet());

                if (!ids.equals(expectedSet)) {
                    mismatchDescription.appendText("Contains IDs " + ids.toString());
                    return false;
                } else {
                    return true;
                }
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("Contains IDs " + Arrays.toString(expectedIds));
            }
        };
    }

    private <T> void mockPropertyResponse(ConceptMethod<T> property, @Nullable T value) {
        server.setResponse(GrpcUtil.runConceptMethodRequest(ID, property), property.createTxResponse(value));
    }
}