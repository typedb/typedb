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

package ai.grakn.test.engine;

import ai.grakn.GraknSession;
import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.engine.rpc.BidirectionalObserver;
import ai.grakn.engine.rpc.GrpcUtil;
import ai.grakn.graql.internal.printer.Printers;
import ai.grakn.rpc.GraknGrpc;
import ai.grakn.rpc.GraknGrpc.GraknStub;
import ai.grakn.rpc.GraknOuterClass;
import ai.grakn.rpc.GraknOuterClass.TxRequest;
import ai.grakn.rpc.GraknOuterClass.TxResponse;
import ai.grakn.test.rule.EngineContext;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import mjson.Json;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * @author Felix Chapman
 */
public class GrpcServerIT {

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @ClassRule
    public static final EngineContext engine = EngineContext.createWithInMemoryRedis();

    private final GraknSession session = engine.sessionWithNewKeyspace();

    // TODO: usePlainText is not secure
    private final ManagedChannel channel =
            ManagedChannelBuilder.forAddress(engine.host(), engine.grpcPort()).usePlaintext(true).build();

    private final GraknStub stub = GraknGrpc.newStub(channel);

    @Test
    public void whenExecutingAndCommittingAQuery_TheQueryIsCommitted() {
        try (BidirectionalObserver<TxRequest, TxResponse> tx = startTx()) {
            tx.send(openRequest(session.keyspace().getValue()));
            tx.send(execQueryRequest("define person sub entity;"));
            tx.send(commitRequest());
        }

        try (GraknTx tx = session.open(GraknTxType.READ)) {
            assertNotNull(tx.getEntityType("person"));
        }
    }

    @Test
    public void whenExecutingAQueryAndNotCommitting_TheQueryIsNotCommitted() {
        try (BidirectionalObserver<TxRequest, TxResponse> tx = startTx()) {
            tx.send(openRequest(session.keyspace().getValue()));
            tx.send(execQueryRequest("define person sub entity;"));
        }

        try (GraknTx tx = session.open(GraknTxType.READ)) {
            assertNull(tx.getEntityType("person"));
        }
    }

    @Test
    public void whenExecutingAQuery_ResultsAreReturned() {
        Json response;

        try (BidirectionalObserver<TxRequest, TxResponse> tx = startTx()) {
            tx.send(openRequest(session.keyspace().getValue()));
            tx.send(execQueryRequest("match $x sub thing; get;"));
            response = Json.read(tx.receive().elem().getQueryResult().getValue());
        }

        Json expected;

        try (GraknTx tx = session.open(GraknTxType.READ)) {
            expected = Json.read(Printers.json().graqlString(tx.graql().parse("match $x sub thing; get;").execute()));
        }

        assertEquals(expected, response);
    }

    @Test
    public void whenExecutingAnInvalidQuery_Throw() throws Throwable {
        try (BidirectionalObserver<TxRequest, TxResponse> tx = startTx()) {
            tx.send(openRequest(session.keyspace().getValue()));
            tx.send(execQueryRequest("match $x sub thing; get $y;"));

            exception.expect(GrpcUtil.hasMessage("boo"));

            throw tx.receive().throwable();
        }
    }

    private BidirectionalObserver<TxRequest, TxResponse> startTx() {
        return BidirectionalObserver.create(stub::tx);
    }

    private TxRequest openRequest(String keyspaceString) {
        GraknOuterClass.Keyspace keyspace = GraknOuterClass.Keyspace.newBuilder().setValue(keyspaceString).build();
        return TxRequest.newBuilder().setOpen(TxRequest.Open.newBuilder().setKeyspace(keyspace)).build();
    }

    private TxRequest commitRequest() {
        return TxRequest.newBuilder().setCommit(TxRequest.Commit.getDefaultInstance()).build();
    }

    private TxRequest execQueryRequest(String queryString) {
        GraknOuterClass.Query query = GraknOuterClass.Query.newBuilder().setValue(queryString).build();
        return TxRequest.newBuilder().setExecQuery(TxRequest.ExecQuery.newBuilder().setQuery(query)).build();
    }
}
