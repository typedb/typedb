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
 *
 */

package ai.grakn.engine.rpc;

import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.concept.Concept;
import ai.grakn.engine.factory.EngineGraknTxFactory;
import ai.grakn.exception.GraknTxOperationException;
import ai.grakn.graql.Graql;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.internal.query.QueryAnswer;
import ai.grakn.rpc.GraknGrpc;
import ai.grakn.rpc.GraknGrpc.GraknStub;
import ai.grakn.rpc.GraknOuterClass;
import ai.grakn.rpc.GraknOuterClass.Query;
import ai.grakn.rpc.GraknOuterClass.TxRequest;
import ai.grakn.rpc.GraknOuterClass.TxResponse;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.isA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Felix Chapman
 */
public class GrpcServerTest {

    private static final int PORT = 5555;
    private static final Keyspace KEYSPACE = Keyspace.of("myks");
    private static final GraknOuterClass.Keyspace KEYSPACE_RPC =
            GraknOuterClass.Keyspace.newBuilder().setValue(KEYSPACE.getValue()).build();
    private static final Query QUERY = Query.newBuilder().setValue("match $x isa person; get;").build();

    private final EngineGraknTxFactory txFactory = mock(EngineGraknTxFactory.class);
    private final GraknTx tx = mock(GraknTx.class);
    private final ai.grakn.graql.Query<?> query = mock(ai.grakn.graql.Query.class);

    private GrpcServer server;

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    // TODO: usePlainText is not secure
    private final ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", PORT).usePlaintext(true).build();
    private final GraknStub stub = GraknGrpc.newStub(channel);

    @Before
    public void setUp() throws IOException {
        server = GrpcServer.create(PORT, txFactory);

        QueryBuilder qb = mock(QueryBuilder.class);

        when(txFactory.tx(KEYSPACE, GraknTxType.WRITE)).thenReturn(tx);
        when(tx.graql()).thenReturn(qb);
        when(qb.parse(QUERY.getValue())).thenReturn(query);
    }

    @After
    public void tearDown() throws InterruptedException {
        server.close();
    }

    @Test
    public void whenOpeningATransactionRemotely_ATransactionIsOpened() {
        TxRequest.Open.Builder openRequest = TxRequest.Open.newBuilder().setKeyspace(KEYSPACE_RPC);

        try (BidirectionalObserver<TxRequest, TxResponse> tx = BidirectionalObserver.create(stub::tx)) {
            tx.send(TxRequest.newBuilder().setOpen(openRequest).build());
        }

        verify(txFactory).tx(KEYSPACE, GraknTxType.WRITE);
    }

    @Test
    public void whenCommittingATransactionRemotely_TheTransactionIsCommitted() {
        TxRequest.Open.Builder openRequest = TxRequest.Open.newBuilder().setKeyspace(KEYSPACE_RPC);

        try (BidirectionalObserver<TxRequest, TxResponse> tx = BidirectionalObserver.create(stub::tx)) {
            tx.send(TxRequest.newBuilder().setOpen(openRequest).build());

            tx.send(TxRequest.newBuilder().setCommit(TxRequest.Commit.getDefaultInstance()).build());
        }

        verify(tx).commit();
    }

    @Test
    public void whenOpeningTwoTransactions_TransactionsAreOpenedInDifferentThreads() {
        List<Thread> threads = new ArrayList<>();

        when(txFactory.tx(KEYSPACE, GraknTxType.WRITE)).thenAnswer(invocation -> {
            threads.add(Thread.currentThread());
            return tx;
        });

        TxRequest.Open.Builder openRequest = TxRequest.Open.newBuilder().setKeyspace(KEYSPACE_RPC);

        try (
            BidirectionalObserver<TxRequest, TxResponse> tx1 = BidirectionalObserver.create(stub::tx);
            BidirectionalObserver<TxRequest, TxResponse> tx2 = BidirectionalObserver.create(stub::tx)
        ) {
            tx1.send(TxRequest.newBuilder().setOpen(openRequest).build());
            tx2.send(TxRequest.newBuilder().setOpen(openRequest).build());
        }

        assertNotEquals(threads.get(0), threads.get(1));
    }

    @Test
    public void whenOpeningATransactionRemotelyWithAnInvalidKeyspace_AnErrorOccurs() throws Throwable {
        TxRequest.Open openRequest = TxRequest.Open.getDefaultInstance();

        try (BidirectionalObserver<TxRequest, TxResponse> tx = BidirectionalObserver.create(stub::tx)) {
            tx.send(TxRequest.newBuilder().setOpen(openRequest).build());

            exception.expect(hasMessage(GraknTxOperationException.invalidKeyspace("").getMessage()));

            throw tx.error().get();
        }
    }

    @Test
    public void whenClosingATransactionRemotely_TheTransactionIsClosedWithinTheThreadItWasCreated() {
        final Thread[] threadOpenedWith = new Thread[1];
        final Thread[] threadClosedWith = new Thread[1];

        when(txFactory.tx(KEYSPACE, GraknTxType.WRITE)).thenAnswer(invocation -> {
            threadOpenedWith[0] = Thread.currentThread();
            return tx;
        });

        doAnswer(invocation -> {
            threadClosedWith[0] = Thread.currentThread();
            return null;
        }).when(tx).close();

        TxRequest.Open.Builder openRequest = TxRequest.Open.newBuilder().setKeyspace(KEYSPACE_RPC);

        try (BidirectionalObserver<TxRequest, TxResponse> tx = BidirectionalObserver.create(stub::tx)) {
            tx.send(TxRequest.newBuilder().setOpen(openRequest).build());
        }

        verify(tx).close();
        assertEquals(threadOpenedWith[0], threadClosedWith[0]);
    }

    @Test
    public void whenExecutingAQueryRemotely_TheQueryIsParsedAndExecuted() {
        TxRequest.Open.Builder openRequest = TxRequest.Open.newBuilder().setKeyspace(KEYSPACE_RPC);
        TxRequest.ExecQuery execQueryRequest = TxRequest.ExecQuery.newBuilder().setQuery(QUERY).build();

        try (BidirectionalObserver<TxRequest, TxResponse> tx = BidirectionalObserver.create(stub::tx)) {
            tx.send(TxRequest.newBuilder().setOpen(openRequest).build());
            tx.send(TxRequest.newBuilder().setExecQuery(execQueryRequest).build());
        }

        ai.grakn.graql.Query<?> query = tx.graql().parse(QUERY.getValue());
        verify(query).execute();
    }

    @Test
    public void whenExecutingAQueryRemotely_AResultIsReturned() {
        TxRequest.Open.Builder openRequest = TxRequest.Open.newBuilder().setKeyspace(KEYSPACE_RPC);
        TxRequest.ExecQuery execQueryRequest = TxRequest.ExecQuery.newBuilder().setQuery(QUERY).build();

        Concept conceptX = mock(Concept.class);
        Concept conceptY = mock(Concept.class);

        ImmutableList<Answer> answers = ImmutableList.of(
                new QueryAnswer(ImmutableMap.of(Graql.var("x"), conceptX)),
                new QueryAnswer(ImmutableMap.of(Graql.var("y"), conceptY))
        );

        when(query.execute()).thenReturn(answers);

        try (BidirectionalObserver<TxRequest, TxResponse> tx = BidirectionalObserver.create(stub::tx)) {
            tx.send(TxRequest.newBuilder().setOpen(openRequest).build());

            tx.send(TxRequest.newBuilder().setExecQuery(execQueryRequest).build());

            TxResponse response = tx.receive();
        }
    }

    private Matcher<StatusRuntimeException> hasMessage(String message) {
        return allOf(
                isA(StatusRuntimeException.class),
                new TypeSafeMatcher<StatusRuntimeException>() {
                    @Override
                    public void describeTo(Description description) {
                        description.appendText("has message " + message);
                    }

                    @Override
                    protected boolean matchesSafely(StatusRuntimeException item) {
                        return message.equals(item.getTrailers().get(GrpcServer.MESSAGE));
                    }
                }
        );
    }
}

