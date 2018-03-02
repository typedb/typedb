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
import ai.grakn.grpc.ConceptProperty;
import ai.grakn.grpc.GrpcUtil;
import ai.grakn.remote.GrpcServerMock;
import ai.grakn.remote.RemoteGraknSession;
import ai.grakn.remote.RemoteGraknTx;
import ai.grakn.rpc.generated.GraknOuterClass;
import ai.grakn.rpc.generated.GraknOuterClass.BaseType;
import ai.grakn.rpc.generated.GraknOuterClass.QueryResult;
import ai.grakn.rpc.generated.GraknOuterClass.TxResponse;
import ai.grakn.util.SimpleURI;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static ai.grakn.graql.Graql.ask;
import static ai.grakn.graql.Graql.define;
import static ai.grakn.graql.Graql.insert;
import static ai.grakn.graql.Graql.match;
import static ai.grakn.graql.Graql.or;
import static ai.grakn.graql.Graql.undefine;
import static ai.grakn.graql.Graql.var;
import static ai.grakn.grpc.ConceptProperty.ALL_ROLE_PLAYERS;
import static ai.grakn.grpc.ConceptProperty.ATTRIBUTE_TYPES;
import static ai.grakn.grpc.ConceptProperty.DATA_TYPE;
import static ai.grakn.grpc.ConceptProperty.DIRECT_SUPER;
import static ai.grakn.grpc.ConceptProperty.DIRECT_TYPE;
import static ai.grakn.grpc.ConceptProperty.IS_ABSTRACT;
import static ai.grakn.grpc.ConceptProperty.IS_IMPLICIT;
import static ai.grakn.grpc.ConceptProperty.IS_INFERRED;
import static ai.grakn.grpc.ConceptProperty.KEY_TYPES;
import static ai.grakn.grpc.ConceptProperty.REGEX;
import static ai.grakn.grpc.ConceptProperty.THEN;
import static ai.grakn.grpc.ConceptProperty.VALUE;
import static ai.grakn.grpc.ConceptProperty.WHEN;
import static ai.grakn.remote.concept.RemoteConcept.ME;
import static ai.grakn.remote.concept.RemoteConcept.TARGET;
import static ai.grakn.rpc.generated.GraknOuterClass.BaseType.Attribute;
import static ai.grakn.rpc.generated.GraknOuterClass.BaseType.MetaType;
import static ai.grakn.rpc.generated.GraknOuterClass.BaseType.Relationship;
import static ai.grakn.util.CommonUtil.toImmutableSet;
import static ai.grakn.util.Schema.MetaSchema.ATTRIBUTE;
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
        mockPropertyResponse(VALUE, 123);
        assertEquals(123, ((Attribute<?>) attribute).getValue());
    }

    @Test
    public void whenCallingGetDataTypeOnAttributeType_GetTheExpectedResult() {
        mockPropertyResponse(DATA_TYPE, DataType.LONG);
        assertEquals(DataType.LONG, ((AttributeType<?>) attributeType).getDataType());
    }

    @Test
    public void whenCallingGetDataTypeOnAttribute_GetTheExpectedResult() {
        mockPropertyResponse(DATA_TYPE, DataType.LONG);
        assertEquals(DataType.LONG, ((Attribute<?>) attribute).dataType());
    }

    @Test
    public void whenCallingGetRegex_GetTheExpectedResult() {
        mockPropertyResponse(REGEX, "hello");
        assertEquals("hello", attributeType.getRegex());
    }

    @Test
    public void whenCallingGetAttribute_ExecuteAQuery() {
        String value = "Dunstan again";

        Query<?> query = match(ME_ID, TARGET.val(value).isa(ME)).get();

        Attribute<String> attribute = RemoteConcepts.createAttribute(tx, A);

        server.setResponseSequence(GrpcUtil.execQueryRequest(query), queryResultResponse(attribute));

        assertEquals(attribute, attributeType.getAttribute(value));
    }

    @Test
    public void whenCallingGetAttributeWhenThereIsNoResult_ExecuteAQuery() {
        String value = "Dunstan > Oliver";

        Query<?> query = match(ME_ID, TARGET.val(value).isa(ME)).get();

        server.setResponseSequence(GrpcUtil.execQueryRequest(query));

        assertNull(attributeType.getAttribute(value));
    }

    @Test
    public void whenCallingGetWhen_GetTheExpectedResult() {
        mockPropertyResponse(WHEN, PATTERN);
        assertEquals(PATTERN, rule.getWhen());
    }

    @Test
    public void whenCallingGetThen_GetTheExpectedResult() {
        mockPropertyResponse(THEN, PATTERN);
        assertEquals(PATTERN, rule.getThen());
    }

    @Test
    public void whenCallingIsDeleted_ExecuteAnAskQuery() {
        Query<?> query = match(ME_ID).aggregate(ask());

        server.setResponseSequence(GrpcUtil.execQueryRequest(query), queryResultResponse("true"));
        assertFalse(entity.isDeleted());

        server.setResponseSequence(GrpcUtil.execQueryRequest(query), queryResultResponse("false"));
        assertTrue(entity.isDeleted());
    }

    @Test
    public void whenCallingSups_ExecuteAQuery() {
        Query<?> query = match(ME_ID, ME.sub(TARGET)).get();

        mockLabelResponse(ID, Label.of("yes"));
        mockLabelResponse(A, Label.of("A"));
        mockLabelResponse(B, Label.of("B"));
        mockLabelResponse(C, THING.getLabel());

        server.setResponseSequence(GrpcUtil.execQueryRequest(query),
                queryResultResponse(entityType),
                queryResultResponse(A, BaseType.EntityType),
                queryResultResponse(B, BaseType.EntityType),
                queryResultResponse(C, MetaType)
        );

        Set<ConceptId> sups = entityType.sups().map(Concept::getId).collect(toSet());
        assertThat(sups, containsInAnyOrder(ID, A, B));
        assertThat(sups, not(hasItem(C)));
    }

    @Test
    public void whenCallingSubs_ExecuteAQuery() {
        Query<?> query = match(ME_ID, TARGET.sub(ME)).get();

        mockLabelResponse(ID, Label.of("yes"));
        mockLabelResponse(A, Label.of("A"));
        mockLabelResponse(B, Label.of("B"));

        server.setResponseSequence(GrpcUtil.execQueryRequest(query),
                queryResultResponse(ID, BaseType.RelationshipType),
                queryResultResponse(A, BaseType.RelationshipType),
                queryResultResponse(B, BaseType.RelationshipType)
        );

        assertThat(relationshipType.subs(), containsIds(ID, A, B));
    }

    @Test
    public void whenCallingSup_GetTheExpectedResult() {
        SchemaConcept sup = RemoteConcepts.createEntityType(tx, A);
        mockLabelResponse(A, Label.of("A"));

        mockPropertyResponse(DIRECT_SUPER, sup);

        assertEquals(sup, entityType.sup());
    }

    @Test
    public void whenCallingSupOnMetaType_GetNull() {
        SchemaConcept sup = RemoteConcepts.createMetaType(tx, A);
        mockLabelResponse(A, THING.getLabel());

        mockPropertyResponse(DIRECT_SUPER, sup);

        assertNull(schemaConcept.sup());
    }

    @Test
    public void whenCallingType_GetTheExpectedResult() {
        Type type = RemoteConcepts.createEntityType(tx, A);

        mockPropertyResponse(DIRECT_TYPE, type);

        assertEquals(type, thing.type());
    }

    @Test
    public void whenCallingAttributesWithNoArguments_ExecuteAQueryForAllAttributes() {
        Query<?> query = match(ME_ID, ME.has(ATTRIBUTE.getLabel(), TARGET)).get();

        server.setResponseSequence(GrpcUtil.execQueryRequest(query),
                queryResultResponse(A, Attribute),
                queryResultResponse(B, Attribute),
                queryResultResponse(C, Attribute)
        );

        assertThat(entity.attributes(), containsIds(A, B, C));
    }

    @Test
    public void whenCallingAttributesWithArguments_ExecuteAQueryForThoseTypesOnly() {
        AttributeType<?> foo = RemoteConcepts.createAttributeType(tx, ConceptId.of("foo"));
        AttributeType<?> bar = RemoteConcepts.createAttributeType(tx, ConceptId.of("bar"));
        AttributeType<?> baz = RemoteConcepts.createAttributeType(tx, ConceptId.of("baz"));

        mockLabelResponse(foo, Label.of("foo"));
        mockLabelResponse(bar, Label.of("bar"));
        mockLabelResponse(baz, Label.of("baz"));

        String query = match(
                ME_ID,
                or(
                        ME.has(foo.getLabel(), TARGET),
                        ME.has(bar.getLabel(), TARGET),
                        ME.has(baz.getLabel(), TARGET)
                )
        ).get().toString();

        server.setResponseSequence(GrpcUtil.execQueryRequest(query),
                queryResultResponse(A, Attribute),
                queryResultResponse(B, Attribute),
                queryResultResponse(C, Attribute)
        );

        assertThat(thing.attributes(foo, bar, baz), containsIds(A, B, C));
    }

    @Test
    public void whenCallingPlays_ExecuteAQuery() {
        Query<?> query = match(ME_ID, ME.plays(TARGET)).get();

        server.setResponseSequence(GrpcUtil.execQueryRequest(query),
                queryResultResponse(A, BaseType.Role),
                queryResultResponse(B, BaseType.Role),
                queryResultResponse(C, BaseType.Role)
        );

        assertThat(type.plays(), containsIds(A, B, C));
    }

    @Test
    public void whenCallingInstances_ExecuteAQuery() {
        Query<?> query = match(ME_ID, TARGET.isa(ME)).get();

        server.setResponseSequence(GrpcUtil.execQueryRequest(query),
                queryResultResponse(A, Relationship),
                queryResultResponse(B, Relationship),
                queryResultResponse(C, Relationship)
        );

        assertThat(((Type) relationshipType).instances(), containsIds(A, B, C));
    }

    @Test
    public void whenCallingThingPlays_ExecuteAQuery() {
        Query<?> query = match(ME_ID, var().rel(TARGET, ME)).get();

        server.setResponseSequence(GrpcUtil.execQueryRequest(query),
                queryResultResponse(A, BaseType.Role),
                queryResultResponse(B, BaseType.Role),
                queryResultResponse(C, BaseType.Role)
        );

        assertThat(thing.plays(), containsIds(A, B, C));
    }

    @Test
    public void whenCallingRelationshipsWithNoArguments_ExecuteAQueryForAllRelationships() {
        Query<?> query = match(ME_ID, TARGET.rel(ME)).get();

        server.setResponseSequence(GrpcUtil.execQueryRequest(query),
                queryResultResponse(A, Relationship),
                queryResultResponse(B, Relationship),
                queryResultResponse(C, Relationship)
        );

        assertThat(thing.relationships(), containsIds(A, B, C));
    }

    @Test
    public void whenCallingRelationshipsWithRoles_ExecuteAQueryForAllRelationshipsOfTheSpecifiedRoles() {
        Role foo = RemoteConcepts.createRole(tx, ConceptId.of("foo"));
        Role bar = RemoteConcepts.createRole(tx, ConceptId.of("bar"));
        Role baz = RemoteConcepts.createRole(tx, ConceptId.of("baz"));

        mockLabelResponse(foo, Label.of("foo"));
        mockLabelResponse(bar, Label.of("bar"));
        mockLabelResponse(baz, Label.of("baz"));

        Var role = var("role");
        String query = match(
                ME_ID,
                TARGET.rel(role, ME),
                or(
                        role.label(foo.getLabel()),
                        role.label(bar.getLabel()),
                        role.label(baz.getLabel())
                )
        ).get().toString();

        server.setResponseSequence(GrpcUtil.execQueryRequest(query),
                queryResultResponse(A, Relationship),
                queryResultResponse(B, Relationship),
                queryResultResponse(C, Relationship)
        );

        assertThat(thing.relationships(foo, bar, baz), containsIds(A, B, C));
    }

    @Test
    public void whenCallingRelationshipTypes_ExecuteAQuery() {
        Query<?> query = match(ME_ID, TARGET.relates(ME)).get();

        server.setResponseSequence(GrpcUtil.execQueryRequest(query),
                queryResultResponse(A, BaseType.RelationshipType),
                queryResultResponse(B, BaseType.RelationshipType),
                queryResultResponse(C, BaseType.RelationshipType)
        );

        assertThat(role.relationshipTypes(), containsIds(A, B, C));
    }

    @Test
    public void whenCallingPlayedByTypes_ExecuteAQuery() {
        Query<?> query = match(ME_ID, TARGET.plays(ME)).get();

        server.setResponseSequence(GrpcUtil.execQueryRequest(query),
                queryResultResponse(A, BaseType.EntityType),
                queryResultResponse(B, BaseType.RelationshipType),
                queryResultResponse(C, BaseType.AttributeType)
        );

        assertThat(role.playedByTypes(), containsIds(A, B, C));
    }

    @Test
    public void whenCallingRelates_ExecuteAQuery() {
        Query<?> query = match(ME_ID, ME.relates(TARGET)).get();

        server.setResponseSequence(GrpcUtil.execQueryRequest(query),
                queryResultResponse(A, BaseType.Role),
                queryResultResponse(B, BaseType.Role),
                queryResultResponse(C, BaseType.Role)
        );

        assertThat(relationshipType.relates(), containsIds(A, B, C));
    }

    @Test
    public void whenCallingAllRolePlayers_ExecuteAQuery() {
        Role foo = RemoteConcepts.createRole(tx, ConceptId.of("foo"));
        Role bar = RemoteConcepts.createRole(tx, ConceptId.of("bar"));

        Thing a = RemoteConcepts.createEntity(tx, A);
        Thing b = RemoteConcepts.createRelationship(tx, B);
        Thing c = RemoteConcepts.createAttribute(tx, C);

        Map<Role, Set<Thing>> expected = ImmutableMap.of(
                foo, ImmutableSet.of(a),
                bar, ImmutableSet.of(b, c)
        );

        TxResponse response = ALL_ROLE_PLAYERS.createTxResponse(expected);

        server.setResponse(GrpcUtil.getConceptPropertyRequest(ID, ALL_ROLE_PLAYERS), response);

        Map<Role, Set<Thing>> allRolePlayers = relationship.allRolePlayers();
        assertEquals(expected, allRolePlayers);
    }

    @Test
    public void whenCallingRolePlayersWithNoArguments_ExecuteAQueryForAllRolePlayers() {
        Query<?> query = match(ME_ID, ME.rel(TARGET)).get();

        server.setResponseSequence(GrpcUtil.execQueryRequest(query),
                queryResultResponse(A, BaseType.Entity),
                queryResultResponse(B, Relationship),
                queryResultResponse(C, Attribute)
        );

        assertThat(relationship.rolePlayers(), containsIds(A, B, C));
    }

    @Test
    public void whenCallingRolePlayersWithRoles_ExecuteAQueryForAllSpecifiedRoles() {
        Role foo = RemoteConcepts.createRole(tx, ConceptId.of("foo"));
        Role bar = RemoteConcepts.createRole(tx, ConceptId.of("bar"));
        Role baz = RemoteConcepts.createRole(tx, ConceptId.of("baz"));

        mockLabelResponse(foo, Label.of("foo"));
        mockLabelResponse(bar, Label.of("bar"));
        mockLabelResponse(baz, Label.of("baz"));

        Var role = var("role");
        String query = match(
                ME_ID,
                ME.rel(role, TARGET),
                or(
                        role.label(foo.getLabel()),
                        role.label(bar.getLabel()),
                        role.label(baz.getLabel())
                )
        ).get().toString();

        server.setResponseSequence(GrpcUtil.execQueryRequest(query),
                queryResultResponse(A, BaseType.Entity),
                queryResultResponse(B, Relationship),
                queryResultResponse(C, Attribute)
        );

        assertThat(relationship.rolePlayers(foo, bar, baz), containsIds(A, B, C));
    }

    @Test
    public void whenCallingOwnerInstances_ExecuteAQuery() {
        Query<?> query = match(ME_ID, TARGET.has(ATTRIBUTE.getLabel(), ME)).get();

        server.setResponseSequence(GrpcUtil.execQueryRequest(query),
                queryResultResponse(A, BaseType.Entity),
                queryResultResponse(B, Relationship),
                queryResultResponse(C, Attribute)
        );

        assertThat(attribute.ownerInstances(), containsIds(A, B, C));
    }

    @Test
    public void whenCallingAttributeTypes_GetTheExpectedResult() {

        ImmutableSet<AttributeType> attributeTypes = ImmutableSet.of(
                RemoteConcepts.createAttributeType(tx, A),
                RemoteConcepts.createAttributeType(tx, B),
                RemoteConcepts.createAttributeType(tx, C)
        );

        mockPropertyResponse(ATTRIBUTE_TYPES, attributeTypes.stream());

        assertEquals(attributeTypes, type.attributes().collect(toSet()));
    }

    @Test
    public void whenCallingKeyTypes_GetTheExpectedResult() {

        ImmutableSet<AttributeType> keyTypes = ImmutableSet.of(
                RemoteConcepts.createAttributeType(tx, A),
                RemoteConcepts.createAttributeType(tx, B),
                RemoteConcepts.createAttributeType(tx, C)
        );

        mockPropertyResponse(KEY_TYPES, keyTypes.stream());

        assertEquals(keyTypes, type.keys().collect(toSet()));
    }

    @Test
    public void whenCallingDelete_ExecuteAQuery() {
        Query<?> query = match(ME_ID).delete(ME);

        server.setResponseSequence(GrpcUtil.execQueryRequest(query));

        concept.delete();

        verify(server.requests()).onNext(GrpcUtil.execQueryRequest(query));
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
                GrpcUtil.getConceptPropertyRequest(ID, ConceptProperty.REGEX),
                ConceptProperty.REGEX.createTxResponse(regex)
        );

        Query<?> query = undefine(ME_ID, ME.regex(regex));

        server.setResponseSequence(GrpcUtil.execQueryRequest(query), emptyQueryResultResponse());

        assertEquals(attributeType, attributeType.setRegex(null));

        verify(server.requests()).onNext(GrpcUtil.execQueryRequest(query));
    }

    @Test
    public void whenResettingUnsetRegex_DontCrash() {
        server.setResponse(
                GrpcUtil.getConceptPropertyRequest(ID, ConceptProperty.REGEX),
                ConceptProperty.REGEX.createTxResponse((String) null)
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
                GrpcUtil.getConceptPropertyRequest(A, ConceptProperty.DIRECT_TYPE),
                ConceptProperty.DIRECT_TYPE.createTxResponse(attributeType)
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
                GrpcUtil.getConceptPropertyRequest(A, ConceptProperty.DIRECT_TYPE),
                ConceptProperty.DIRECT_TYPE.createTxResponse(attributeType)
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
                GrpcUtil.getConceptPropertyRequest(A, ConceptProperty.DIRECT_TYPE),
                ConceptProperty.DIRECT_TYPE.createTxResponse(attributeType)
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

    private void mockLabelResponse(Concept concept, Label label) {
        mockLabelResponse(concept.getId(), label);
    }

    private void mockLabelResponse(ConceptId id, Label label) {
        server.setResponse(
                GrpcUtil.getConceptPropertyRequest(id, ConceptProperty.LABEL),
                ConceptProperty.LABEL.createTxResponse(label)
        );
    }

    private static TxResponse emptyQueryResultResponse() {
        return queryResultResponse(GraknOuterClass.Answer.getDefaultInstance());
    }

    private static TxResponse queryResultResponse(String value) {
        QueryResult queryResult = QueryResult.newBuilder().setOtherResult(value).build();
        return TxResponse.newBuilder().setQueryResult(queryResult).build();
    }

    private static TxResponse queryResultResponse(Concept concept) {
        return queryResultResponse(concept.getId(), GrpcUtil.getBaseType(concept));
    }

    private static TxResponse queryResultResponse(ConceptId id, BaseType baseType) {
        GraknOuterClass.ConceptId conceptId = GraknOuterClass.ConceptId.newBuilder().setValue(id.getValue()).build();

        GraknOuterClass.Concept concept =
                GraknOuterClass.Concept.newBuilder().setId(conceptId).setBaseType(baseType).build();

        GraknOuterClass.Answer answer =
                GraknOuterClass.Answer.newBuilder().putAnswer(TARGET.getValue(), concept).build();

        return queryResultResponse(answer);
    }

    private static TxResponse queryResultResponse(GraknOuterClass.Answer answer) {
        QueryResult queryResult = QueryResult.newBuilder().setAnswer(answer).build();
        return TxResponse.newBuilder().setQueryResult(queryResult).build();
    }

    private Matcher<? super Stream<? extends Concept>> containsIds(ConceptId... expectedIds) {
        Set<ConceptId> expectedSet = ImmutableSet.copyOf(expectedIds);

        return new TypeSafeMatcher<Stream<? extends Concept>>() {
            @Override
            protected boolean matchesSafely(Stream<? extends Concept> stream) {
                Set<ConceptId> ids = stream.map(Concept::getId).collect(toImmutableSet());
                return ids.equals(expectedSet);
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("Contains IDs " + Arrays.toString(expectedIds));
            }
        };
    }

    private <T> void mockPropertyResponse(ConceptProperty<T> property, T value) {
        server.setResponse(GrpcUtil.getConceptPropertyRequest(ID, property), property.createTxResponse(value));
    }
}