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

import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.engine.factory.EngineGraknTxFactory;
import ai.grakn.rpc.GraknGrpc;
import ai.grakn.rpc.GraknGrpc.GraknBlockingStub;
import ai.grakn.rpc.GraknOuterClass;
import ai.grakn.rpc.GraknOuterClass.OpenTxRequest;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * @author Felix Chapman
 */
public class GrpcServerTest {

    private static final int PORT = 5555;

    private final EngineGraknTxFactory txFactory = mock(EngineGraknTxFactory.class);

    private GrpcServer server;

    // TODO: usePlainText is not secure
    private final ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", PORT).usePlaintext(true).build();
    private final GraknBlockingStub blockingStub = GraknGrpc.newBlockingStub(channel);

    @Before
    public void setUp() throws IOException {
        server = GrpcServer.create(PORT, txFactory);
    }

    @After
    public void tearDown() throws InterruptedException {
        server.close();
    }

    @Test
    public void whenOpeningATransactionRemotely_ATransactionIsOpened() {
        GraknOuterClass.Keyspace keyspace = GraknOuterClass.Keyspace.newBuilder().setValue("myks").build();
        OpenTxRequest request = OpenTxRequest.newBuilder().setKeyspace(keyspace).build();

        blockingStub.openTx(request);

        verify(txFactory).tx(Keyspace.of("myks"), GraknTxType.WRITE);
    }
}
