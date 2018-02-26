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
import ai.grakn.concept.SchemaConcept;
import ai.grakn.concept.Thing;
import ai.grakn.concept.Type;
import ai.grakn.graql.Pattern;
import ai.grakn.grpc.ConceptProperty;
import ai.grakn.grpc.GrpcUtil;
import ai.grakn.remote.GrpcServerMock;
import ai.grakn.remote.RemoteGraknSession;
import ai.grakn.remote.RemoteGraknTx;
import ai.grakn.rpc.generated.GraknOuterClass.QueryResult;
import ai.grakn.rpc.generated.GraknOuterClass.TxResponse;
import ai.grakn.util.SimpleURI;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static ai.grakn.graql.Graql.ask;
import static ai.grakn.graql.Graql.match;
import static ai.grakn.graql.Graql.var;
import static ai.grakn.grpc.ConceptProperty.DATA_TYPE;
import static ai.grakn.grpc.ConceptProperty.IS_ABSTRACT;
import static ai.grakn.grpc.ConceptProperty.IS_IMPLICIT;
import static ai.grakn.grpc.ConceptProperty.IS_INFERRED;
import static ai.grakn.grpc.ConceptProperty.REGEX;
import static ai.grakn.grpc.ConceptProperty.THEN;
import static ai.grakn.grpc.ConceptProperty.VALUE;
import static ai.grakn.grpc.ConceptProperty.WHEN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;

/**
 * @author Felix Chapman
 */
public class RemoteConceptsTest {

    private static final ConceptId ID = ConceptId.of("V123");
    private static final Pattern PATTERN = var("x").isa("person");

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
        server.setResponse(GrpcUtil.getConceptPropertyRequest(ID, ConceptProperty.LABEL), ConceptProperty.LABEL.createTxResponse(LABEL));

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
        String query = match(var().id(ID)).aggregate(ask()).toString();

        Concept concept = RemoteConcepts.createEntity(tx, ID);

        server.setResponse(GrpcUtil.execQueryRequest(query), queryResultResponse("true"));
        assertFalse(concept.isDeleted());

        server.setResponse(GrpcUtil.execQueryRequest(query), queryResultResponse("false"));
        assertTrue(concept.isDeleted());
    }

    private static TxResponse queryResultResponse(String value) {
        QueryResult queryResult = QueryResult.newBuilder().setOtherResult(value).build();
        return TxResponse.newBuilder().setQueryResult(queryResult).build();
    }
}