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
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.remote;

import ai.grakn.GraknSession;
import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.exception.GraknTxOperationException;
import ai.grakn.remote.rpc.RequestBuilder;
import ai.grakn.rpc.generated.GraknGrpc;
import ai.grakn.util.SimpleURI;
import com.google.common.annotations.VisibleForTesting;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

/**
 * Entry-point and remote equivalent of {@link ai.grakn.Grakn}. Communicates with a running Grakn server using gRPC.
 *
 * <p>
 *     In the future, this will likely become the default entry-point over {@link ai.grakn.Grakn}. For now, only a
 *     subset of {@link GraknSession} and {@link ai.grakn.GraknTx} features are supported.
 * </p>
 *
 * @author Felix Chapman
 */
public final class Grakn {

    private Grakn() {}

    public static Grakn.Session getSession(SimpleURI uri, Keyspace keyspace) {
        return Session.create(uri, keyspace);
    }

    /**
     * Remote implementation of {@link GraknSession} that communicates with a Grakn server using gRPC.
     *
     * @see Transaction
     * @see Grakn
     */
    public static class Session implements GraknSession {

        private final Keyspace keyspace;
        private final SimpleURI uri;
        private final ManagedChannel channel;

        private Session(Keyspace keyspace, SimpleURI uri, ManagedChannel channel) {
            this.keyspace = keyspace;
            this.uri = uri;
            this.channel = channel;
        }

        @VisibleForTesting
        public static Session create(SimpleURI uri, Keyspace keyspace, ManagedChannel channel) {
            return new Session(keyspace, uri, channel);
        }

        private static Session create(SimpleURI uri, Keyspace keyspace){
            ManagedChannel channel =
                    ManagedChannelBuilder.forAddress(uri.getHost(), uri.getPort()).usePlaintext(true).build();

            return create(uri, keyspace, channel);
        }

        GraknGrpc.GraknStub stub() {
            return GraknGrpc.newStub(channel);
        }

        GraknGrpc.GraknBlockingStub blockingStub() {
            return GraknGrpc.newBlockingStub(channel);
        }

        @Override
        public Transaction open(GraknTxType transactionType) {
            return Transaction.create(this, RequestBuilder.open(keyspace, transactionType));
        }

        @Override
        public void close() throws GraknTxOperationException {
            channel.shutdown();
        }

        @Override
        public String uri() {
            return uri.toString();
        }

        @Override
        public Keyspace keyspace() {
            return keyspace;
        }
    }
}
