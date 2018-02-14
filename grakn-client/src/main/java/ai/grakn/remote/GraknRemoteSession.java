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

package ai.grakn.remote;

import ai.grakn.GraknComputer;
import ai.grakn.GraknSession;
import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.engine.GraknConfig;
import ai.grakn.exception.GraknTxOperationException;
import ai.grakn.rpc.generated.GraknGrpc.GraknStub;
import ai.grakn.util.SimpleURI;

/**
 * @author Felix Chapman
 */
final class GraknRemoteSession implements GraknSession {

    private final Keyspace keyspace;
    private final SimpleURI uri;

    private GraknRemoteSession(Keyspace keyspace, SimpleURI uri) {
        this.keyspace = keyspace;
        this.uri = uri;
    }

    public static GraknRemoteSession create(Keyspace keyspace, SimpleURI engineUri){
        return new GraknRemoteSession(keyspace, engineUri);
    }

    GraknStub stub() {
        return null;
    }

    @Override
    public GraknTx open(GraknTxType transactionType) {
        throw new UnsupportedOperationException();
    }

    @Override
    public GraknComputer getGraphComputer() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws GraknTxOperationException {

    }

    @Override
    public String uri() {
        return uri.toString();
    }

    @Override
    public Keyspace keyspace() {
        return keyspace;
    }

    @Override
    public GraknConfig config() {
        throw new UnsupportedOperationException();
    }
}
