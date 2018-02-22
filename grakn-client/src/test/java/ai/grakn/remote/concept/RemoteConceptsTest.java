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
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Label;
import ai.grakn.concept.SchemaConcept;
import ai.grakn.grpc.GrpcUtil;
import ai.grakn.remote.GrpcServerMock;
import ai.grakn.remote.RemoteGraknSession;
import ai.grakn.remote.RemoteGraknTx;
import ai.grakn.util.SimpleURI;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static ai.grakn.rpc.generated.GraknOuterClass.ConceptProperty.IsImplicit;
import static ai.grakn.rpc.generated.GraknOuterClass.ConceptProperty.LabelProperty;
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
    public void whenGettingLabel_MakeAGrpcRequest() {
        SchemaConcept concept = RemoteConcepts.createEntityType(tx, ID);

        concept.getLabel();

        verify(server.requests()).onNext(GrpcUtil.getConceptPropertyRequest(ID, LabelProperty));
    }

    @Test
    public void whenGettingLabel_ReturnTheExpectedLabel() {
        SchemaConcept concept = RemoteConcepts.createEntityType(tx, ID);
        server.setResponse(GrpcUtil.getConceptPropertyRequest(ID, LabelProperty), GrpcUtil.conceptPropertyLabelResponse(LABEL));

        assertEquals(LABEL, concept.getLabel());
    }

    @Test
    public void whenCallingIsImplicit_MakeAGrpcRequest() {
        SchemaConcept concept = RemoteConcepts.createEntityType(tx, ID);

        concept.isImplicit();

        verify(server.requests()).onNext(GrpcUtil.getConceptPropertyRequest(ID, IsImplicit));
    }

    @Test
    public void whenCallingIsImplicit_GetTheExpectedResult() {
        SchemaConcept concept = RemoteConcepts.createEntityType(tx, ID);

        server.setResponse(GrpcUtil.getConceptPropertyRequest(ID, IsImplicit), GrpcUtil.conceptPropertyIsImplicitResponse(true));
        assertTrue(concept.isImplicit());

        server.setResponse(GrpcUtil.getConceptPropertyRequest(ID, IsImplicit), GrpcUtil.conceptPropertyIsImplicitResponse(false));
        assertFalse(concept.isImplicit());
    }
}