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
import ai.grakn.graql.admin.Answer;
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
import static ai.grakn.graql.Graql.match;
import static ai.grakn.graql.Graql.or;
import static ai.grakn.graql.Graql.var;
import static ai.grakn.grpc.ConceptProperty.ALL_ROLE_PLAYERS;
import static ai.grakn.grpc.ConceptProperty.DATA_TYPE;
import static ai.grakn.grpc.ConceptProperty.IS_ABSTRACT;
import static ai.grakn.grpc.ConceptProperty.IS_IMPLICIT;
import static ai.grakn.grpc.ConceptProperty.IS_INFERRED;
import static ai.grakn.grpc.ConceptProperty.REGEX;
import static ai.grakn.grpc.ConceptProperty.THEN;
import static ai.grakn.grpc.ConceptProperty.VALUE;
import static ai.grakn.grpc.ConceptProperty.WHEN;
import static ai.grakn.remote.concept.RemoteConcept.ME;
import static ai.grakn.remote.concept.RemoteConcept.TARGET;
import static ai.grakn.rpc.generated.GraknOuterClass.BaseType.Attribute;
import static ai.grakn.rpc.generated.GraknOuterClass.BaseType.EntityType;
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

    @Before
    public void setUp() {
        session = RemoteGraknSession.create(Keyspace.of("whatever"), URI, server.channel());
        tx = session.open(GraknTxType.WRITE);
        verify(server.requests()).onNext(any()); // The open request
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
        SchemaConcept concept = RemoteConcepts.createEntityType(tx, ID);
        mockLabelResponse(ID, LABEL);

        assertEquals(LABEL, concept.getLabel());
    }

    @Test
    public void whenCallingIsImplicit_GetTheExpectedResult() {
        SchemaConcept concept = RemoteConcepts.createEntityType(tx, ID);

        server.setResponse(GrpcUtil.getConceptPropertyRequest(ID, IS_IMPLICIT), IS_IMPLICIT.createTxResponse(true));
        assertTrue(concept.isImplicit());

        server.setResponse(GrpcUtil.getConceptPropertyRequest(ID, IS_IMPLICIT), IS_IMPLICIT.createTxResponse(false));
        assertFalse(concept.isImplicit());
    }

    @Test
    public void whenCallingIsInferred_GetTheExpectedResult() {
        Thing concept = RemoteConcepts.createEntity(tx, ID);

        server.setResponse(GrpcUtil.getConceptPropertyRequest(ID, IS_INFERRED), IS_INFERRED.createTxResponse(true));
        assertTrue(concept.isInferred());

        server.setResponse(GrpcUtil.getConceptPropertyRequest(ID, IS_INFERRED), IS_INFERRED.createTxResponse(false));
        assertFalse(concept.isInferred());
    }

    @Test
    public void whenCallingIsAbstract_GetTheExpectedResult() {
        Type concept = RemoteConcepts.createEntityType(tx, ID);

        server.setResponse(GrpcUtil.getConceptPropertyRequest(ID, IS_ABSTRACT), IS_ABSTRACT.createTxResponse(true));
        assertTrue(concept.isAbstract());

        server.setResponse(GrpcUtil.getConceptPropertyRequest(ID, IS_ABSTRACT), IS_ABSTRACT.createTxResponse(false));
        assertFalse(concept.isAbstract());
    }

    @Test
    public void whenCallingGetValue_GetTheExpectedResult() {
        Attribute<?> concept = RemoteConcepts.createAttribute(tx, ID);

        server.setResponse(GrpcUtil.getConceptPropertyRequest(ID, VALUE), VALUE.createTxResponse(123));
        assertEquals(123, concept.getValue());
    }

    @Test
    public void whenCallingGetDataTypeOnAttributeType_GetTheExpectedResult() {
        AttributeType<?> concept = RemoteConcepts.createAttributeType(tx, ID);

        server.setResponse(GrpcUtil.getConceptPropertyRequest(ID, DATA_TYPE), DATA_TYPE.createTxResponse(DataType.LONG));
        assertEquals(DataType.LONG, concept.getDataType());
    }

    @Test
    public void whenCallingGetDataTypeOnAttribute_GetTheExpectedResult() {
        Attribute<?> concept = RemoteConcepts.createAttribute(tx, ID);

        server.setResponse(GrpcUtil.getConceptPropertyRequest(ID, DATA_TYPE), DATA_TYPE.createTxResponse(DataType.LONG));
        assertEquals(DataType.LONG, concept.dataType());
    }

    @Test
    public void whenCallingGetRegex_GetTheExpectedResult() {
        AttributeType<?> concept = RemoteConcepts.createAttributeType(tx, ID);

        server.setResponse(GrpcUtil.getConceptPropertyRequest(ID, REGEX), REGEX.createTxResponse("hello"));
        assertEquals("hello", concept.getRegex());
    }

    @Test
    public void whenCallingGetWhen_GetTheExpectedResult() {
        ai.grakn.concept.Rule concept = RemoteConcepts.createRule(tx, ID);

        server.setResponse(GrpcUtil.getConceptPropertyRequest(ID, WHEN), WHEN.createTxResponse(PATTERN));
        assertEquals(PATTERN, concept.getWhen());
    }

    @Test
    public void whenCallingGetThen_GetTheExpectedResult() {
        ai.grakn.concept.Rule concept = RemoteConcepts.createRule(tx, ID);

        server.setResponse(GrpcUtil.getConceptPropertyRequest(ID, THEN), THEN.createTxResponse(PATTERN));
        assertEquals(PATTERN, concept.getThen());
    }

    @Test
    public void whenCallingIsDeleted_ExecuteAnAskQuery() {
        Query<?> query = match(var().id(ID)).aggregate(ask());

        Concept concept = RemoteConcepts.createEntity(tx, ID);

        server.setResponseSequence(GrpcUtil.execQueryRequest(query), queryResultResponse("true"));
        assertFalse(concept.isDeleted());

        server.setResponseSequence(GrpcUtil.execQueryRequest(query), queryResultResponse("false"));
        assertTrue(concept.isDeleted());
    }

    @Test
    public void whenCallingSups_ExecuteAQuery() {
        Query<?> query = match(ME.id(ID), ME.sub(TARGET)).get();

        SchemaConcept concept = RemoteConcepts.createEntityType(tx, ID);

        mockLabelResponse(ID, Label.of("yes"));
        mockLabelResponse(A, Label.of("A"));
        mockLabelResponse(B, Label.of("B"));
        mockLabelResponse(C, THING.getLabel());

        server.setResponseSequence(GrpcUtil.execQueryRequest(query),
                queryResultResponse(ID, EntityType),
                queryResultResponse(A, EntityType),
                queryResultResponse(B, EntityType),
                queryResultResponse(C, MetaType)
        );

        Set<ConceptId> sups = concept.sups().map(Concept::getId).collect(toSet());
        assertThat(sups, containsInAnyOrder(ID, A, B));
        assertThat(sups, not(hasItem(C)));
    }

    @Test
    public void whenCallingSubs_ExecuteAQuery() {
        Query<?> query = match(ME.id(ID), TARGET.sub(ME)).get();

        SchemaConcept concept = RemoteConcepts.createRelationshipType(tx, ID);

        mockLabelResponse(ID, Label.of("yes"));
        mockLabelResponse(A, Label.of("A"));
        mockLabelResponse(B, Label.of("B"));

        server.setResponseSequence(GrpcUtil.execQueryRequest(query),
                queryResultResponse(ID, BaseType.RelationshipType),
                queryResultResponse(A, BaseType.RelationshipType),
                queryResultResponse(B, BaseType.RelationshipType)
        );

        assertThat(concept.subs(), containsIds(ID, A, B));
    }

    @Test
    public void whenCallingSup_ExecuteSeveralQueries() {
        SchemaConcept concept = RemoteConcepts.createEntityType(tx, ID);

        String supsId = match(ME.id(ID), ME.sub(TARGET)).get().toString();
        String supsA = match(ME.id(A), ME.sub(TARGET)).get().toString();
        String supsB = match(ME.id(B), ME.sub(TARGET)).get().toString();
        String supsMetaType = match(ME.id(C), ME.sub(TARGET)).get().toString();

        mockLabelResponse(ID, Label.of("yes"));
        mockLabelResponse(A, Label.of("A"));
        mockLabelResponse(B, Label.of("B"));
        mockLabelResponse(C, THING.getLabel());

        server.setResponseSequence(GrpcUtil.execQueryRequest(supsId),
                queryResultResponse(ID, EntityType),
                queryResultResponse(A, EntityType),
                queryResultResponse(B, EntityType),
                queryResultResponse(C, MetaType)
        );

        server.setResponseSequence(GrpcUtil.execQueryRequest(supsA),
                queryResultResponse(A, EntityType),
                queryResultResponse(B, EntityType),
                queryResultResponse(C, MetaType)
        );

        server.setResponseSequence(GrpcUtil.execQueryRequest(supsB),
                queryResultResponse(B, EntityType),
                queryResultResponse(C, MetaType)
        );

        server.setResponseSequence(GrpcUtil.execQueryRequest(supsMetaType),
                queryResultResponse(C, MetaType)
        );

        assertEquals(A, concept.sup().getId());
    }

    @Test
    public void whenCallingIsa_ExecuteSeveralQueries() {
        Thing concept = RemoteConcepts.createEntity(tx, ID);

        String typeId = match(ME.id(ID), ME.isa(TARGET)).get().toString();
        String supsA = match(ME.id(A), ME.sub(TARGET)).get().toString();
        String supsB = match(ME.id(B), ME.sub(TARGET)).get().toString();
        String supsMetaType = match(ME.id(C), ME.sub(TARGET)).get().toString();

        mockLabelResponse(A, Label.of("A"));
        mockLabelResponse(B, Label.of("B"));
        mockLabelResponse(C, THING.getLabel());

        server.setResponseSequence(GrpcUtil.execQueryRequest(typeId),
                queryResultResponse(A, EntityType),
                queryResultResponse(B, EntityType),
                queryResultResponse(C, MetaType)
        );

        server.setResponseSequence(GrpcUtil.execQueryRequest(supsA),
                queryResultResponse(A, EntityType),
                queryResultResponse(B, EntityType),
                queryResultResponse(C, MetaType)
        );

        server.setResponseSequence(GrpcUtil.execQueryRequest(supsB),
                queryResultResponse(B, EntityType),
                queryResultResponse(C, MetaType)
        );

        server.setResponseSequence(GrpcUtil.execQueryRequest(supsMetaType),
                queryResultResponse(C, MetaType)
        );

        assertEquals(A, concept.type().getId());
    }

    @Test
    public void whenCallingAttributesWithNoArguments_ExecuteAQueryForAllAttributes() {
        Query<?> query = match(ME.id(ID), ME.has(ATTRIBUTE.getLabel(), TARGET)).get();

        Thing concept = RemoteConcepts.createEntity(tx, ID);

        server.setResponseSequence(GrpcUtil.execQueryRequest(query),
                queryResultResponse(A, Attribute),
                queryResultResponse(B, Attribute),
                queryResultResponse(C, Attribute)
        );

        assertThat(concept.attributes(), containsIds(A, B, C));
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
                ME.id(ID),
                or(
                        ME.has(foo.getLabel(), TARGET),
                        ME.has(bar.getLabel(), TARGET),
                        ME.has(baz.getLabel(), TARGET)
                )
        ).get().toString();

        Thing concept = RemoteConcepts.createEntity(tx, ID);

        server.setResponseSequence(GrpcUtil.execQueryRequest(query),
                queryResultResponse(A, Attribute),
                queryResultResponse(B, Attribute),
                queryResultResponse(C, Attribute)
        );

        assertThat(concept.attributes(foo, bar, baz), containsIds(A, B, C));
    }

    @Test
    public void whenCallingPlays_ExecuteAQuery() {
        Query<?> query = match(ME.id(ID), ME.plays(TARGET)).get();

        Type concept = RemoteConcepts.createEntityType(tx, ID);

        server.setResponseSequence(GrpcUtil.execQueryRequest(query),
                queryResultResponse(A, BaseType.Role),
                queryResultResponse(B, BaseType.Role),
                queryResultResponse(C, BaseType.Role)
        );

        assertThat(concept.plays(), containsIds(A, B, C));
    }

    @Test
    public void whenCallingInstances_ExecuteAQuery() {
        Query<?> query = match(ME.id(ID), TARGET.isa(ME)).get();

        Type concept = RemoteConcepts.createRelationshipType(tx, ID);

        server.setResponseSequence(GrpcUtil.execQueryRequest(query),
                queryResultResponse(A, Relationship),
                queryResultResponse(B, Relationship),
                queryResultResponse(C, Relationship)
        );

        assertThat(concept.instances(), containsIds(A, B, C));
    }

    @Test
    public void whenCallingThingPlays_ExecuteAQuery() {
        Query<?> query = match(ME.id(ID), var().rel(TARGET, ME)).get();

        Thing concept = RemoteConcepts.createAttribute(tx, ID);

        server.setResponseSequence(GrpcUtil.execQueryRequest(query),
                queryResultResponse(A, BaseType.Role),
                queryResultResponse(B, BaseType.Role),
                queryResultResponse(C, BaseType.Role)
        );

        assertThat(concept.plays(), containsIds(A, B, C));
    }

    @Test
    public void whenCallingRelationshipsWithNoArguments_ExecuteAQueryForAllRelationships() {
        Query<?> query = match(ME.id(ID), TARGET.rel(ME)).get();

        Thing concept = RemoteConcepts.createEntity(tx, ID);

        server.setResponseSequence(GrpcUtil.execQueryRequest(query),
                queryResultResponse(A, Relationship),
                queryResultResponse(B, Relationship),
                queryResultResponse(C, Relationship)
        );

        assertThat(concept.relationships(), containsIds(A, B, C));
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
                ME.id(ID),
                TARGET.rel(role, ME),
                or(
                        role.label(foo.getLabel()),
                        role.label(bar.getLabel()),
                        role.label(baz.getLabel())
                )
        ).get().toString();

        Thing concept = RemoteConcepts.createEntity(tx, ID);

        server.setResponseSequence(GrpcUtil.execQueryRequest(query),
                queryResultResponse(A, Relationship),
                queryResultResponse(B, Relationship),
                queryResultResponse(C, Relationship)
        );

        assertThat(concept.relationships(foo, bar, baz), containsIds(A, B, C));
    }

    @Test
    public void whenCallingRelationshipTypes_ExecuteAQuery() {
        Query<?> query = match(ME.id(ID), TARGET.relates(ME)).get();

        Role concept = RemoteConcepts.createRole(tx, ID);

        server.setResponseSequence(GrpcUtil.execQueryRequest(query),
                queryResultResponse(A, BaseType.RelationshipType),
                queryResultResponse(B, BaseType.RelationshipType),
                queryResultResponse(C, BaseType.RelationshipType)
        );

        assertThat(concept.relationshipTypes(), containsIds(A, B, C));
    }

    @Test
    public void whenCallingPlayedByTypes_ExecuteAQuery() {
        Query<?> query = match(ME.id(ID), TARGET.plays(ME)).get();

        Role concept = RemoteConcepts.createRole(tx, ID);

        server.setResponseSequence(GrpcUtil.execQueryRequest(query),
                queryResultResponse(A, EntityType),
                queryResultResponse(B, BaseType.RelationshipType),
                queryResultResponse(C, BaseType.AttributeType)
        );

        assertThat(concept.playedByTypes(), containsIds(A, B, C));
    }

    @Test
    public void whenCallingRelates_ExecuteAQuery() {
        Query<?> query = match(ME.id(ID), ME.relates(TARGET)).get();

        RelationshipType concept = RemoteConcepts.createRelationshipType(tx, ID);

        server.setResponseSequence(GrpcUtil.execQueryRequest(query),
                queryResultResponse(A, BaseType.Role),
                queryResultResponse(B, BaseType.Role),
                queryResultResponse(C, BaseType.Role)
        );

        assertThat(concept.relates(), containsIds(A, B, C));
    }

    @Test
    public void whenCallingAllRolePlayers_ExecuteAQuery() {
        Relationship concept = RemoteConcepts.createRelationship(tx, ID);

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

        Map<Role, Set<Thing>> allRolePlayers = concept.allRolePlayers();
        assertEquals(expected, allRolePlayers);
    }

    @Test
    public void whenCallingRolePlayersWithNoArguments_ExecuteAQueryForAllRolePlayers() {
        Query<?> query = match(ME.id(ID), ME.rel(TARGET)).get();

        Relationship concept = RemoteConcepts.createRelationship(tx, ID);

        server.setResponseSequence(GrpcUtil.execQueryRequest(query),
                queryResultResponse(A, BaseType.Entity),
                queryResultResponse(B, Relationship),
                queryResultResponse(C, Attribute)
        );

        assertThat(concept.rolePlayers(), containsIds(A, B, C));
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
                ME.id(ID),
                ME.rel(role, TARGET),
                or(
                        role.label(foo.getLabel()),
                        role.label(bar.getLabel()),
                        role.label(baz.getLabel())
                )
        ).get().toString();

        Relationship concept = RemoteConcepts.createRelationship(tx, ID);

        server.setResponseSequence(GrpcUtil.execQueryRequest(query),
                queryResultResponse(A, BaseType.Entity),
                queryResultResponse(B, Relationship),
                queryResultResponse(C, Attribute)
        );

        assertThat(concept.rolePlayers(foo, bar, baz), containsIds(A, B, C));
    }

    @Test
    public void whenCallingOwnerInstances_ExecuteAQuery() {
        Query<?> query = match(ME.id(ID), TARGET.has(ATTRIBUTE.getLabel(), ME)).get();

        Attribute<?> concept = RemoteConcepts.createAttribute(tx, ID);

        server.setResponseSequence(GrpcUtil.execQueryRequest(query),
                queryResultResponse(A, BaseType.Entity),
                queryResultResponse(B, Relationship),
                queryResultResponse(C, Attribute)
        );

        assertThat(concept.ownerInstances(), containsIds(A, B, C));
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

    private static TxResponse queryResultResponse(String value) {
        QueryResult queryResult = QueryResult.newBuilder().setOtherResult(value).build();
        return TxResponse.newBuilder().setQueryResult(queryResult).build();
    }

    private static TxResponse queryResultResponse(Answer answer) {
        GraknOuterClass.Answer.Builder grpcAnswer = GraknOuterClass.Answer.newBuilder();

        answer.forEach((var, concept) -> {
            grpcAnswer.putAnswer(var.getValue(), GrpcUtil.convert(concept));
        });

        return queryResultResponse(grpcAnswer.build());
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
}