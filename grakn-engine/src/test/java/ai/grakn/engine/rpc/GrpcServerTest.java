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
import ai.grakn.engine.factory.EngineGraknTxFactory;
import ai.grakn.rpc.GraknGrpc;
import ai.grakn.rpc.GraknGrpc.GraknBlockingStub;
import ai.grakn.rpc.GraknOuterClass;
import ai.grakn.rpc.GraknOuterClass.CloseTxRequest;
import ai.grakn.rpc.GraknOuterClass.OpenTxRequest;
import ai.grakn.rpc.GraknOuterClass.OpenTxResponse;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.is;
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

    private final EngineGraknTxFactory txFactory = mock(EngineGraknTxFactory.class);
    private final GraknTx tx = mock(GraknTx.class);

    private GrpcServer server;

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    // TODO: usePlainText is not secure
    private final ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", PORT).usePlaintext(true).build();
    private final GraknBlockingStub blockingStub = GraknGrpc.newBlockingStub(channel);

    @Before
    public void setUp() throws IOException {
        server = GrpcServer.create(PORT, txFactory);

        when(txFactory.tx(KEYSPACE, GraknTxType.WRITE)).thenReturn(tx);
    }

    @After
    public void tearDown() throws InterruptedException {
        server.close();
    }

    @Test
    public void whenOpeningATransactionRemotely_ATransactionIsOpened() {
        OpenTxRequest request = OpenTxRequest.newBuilder().setKeyspace(KEYSPACE_RPC).build();
        blockingStub.openTx(request);

        verify(txFactory).tx(KEYSPACE, GraknTxType.WRITE);
    }

    @Test
    public void whenOpeningTwoTransactions_TransactionsHaveDifferentIDs() {
        OpenTxRequest request = OpenTxRequest.newBuilder().setKeyspace(KEYSPACE_RPC).build();

        OpenTxResponse tx1 = blockingStub.openTx(request);
        OpenTxResponse tx2 = blockingStub.openTx(request);

        assertNotEquals(tx1, tx2);
    }

    @Test
    public void whenOpeningTwoTransactions_TransactionsAreOpenedInDifferentThreads() {
        List<Thread> threads = new ArrayList<>();

        when(txFactory.tx(KEYSPACE, GraknTxType.WRITE)).thenAnswer(invocation -> {
            threads.add(Thread.currentThread());
            return tx;
        });

        OpenTxRequest request = OpenTxRequest.newBuilder().setKeyspace(KEYSPACE_RPC).build();

        blockingStub.openTx(request);
        blockingStub.openTx(request);

        assertNotEquals(threads.get(0), threads.get(1));
    }

    @Test
    public void whenOpeningATransactionRemotelyWithAnInvalidKeyspace_AnErrorOccurs() {
        OpenTxRequest request = OpenTxRequest.newBuilder().build();
        OpenTxResponse response = blockingStub.openTx(request);

        assertEquals(OpenTxResponse.ResponseCase.INVALIDKEYSPACE, response.getResponseCase());
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

        OpenTxRequest openRequest = OpenTxRequest.newBuilder().setKeyspace(KEYSPACE_RPC).build();
        GraknOuterClass.TxId txId = blockingStub.openTx(openRequest).getSuccess().getTx();

        CloseTxRequest request = CloseTxRequest.newBuilder().setTx(txId).build();
        blockingStub.closeTx(request);

        verify(tx).close();

        assertEquals(threadOpenedWith[0], threadClosedWith[0]);
    }

    @Test
    public void whenClosingATransactionThatDoesntExist_Throw() {
        CloseTxRequest closeRequest = CloseTxRequest.getDefaultInstance();

        exception.expect(hasStatus(Status.FAILED_PRECONDITION));

        blockingStub.closeTx(closeRequest);
    }

    private Matcher<StatusRuntimeException> hasStatus(Status status) {
        return allOf(
                isA(StatusRuntimeException.class),
                hasProperty("status", is(status))
        );
    }
}
