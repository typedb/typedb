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

package ai.grakn.engine.rpc;

import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.engine.factory.EngineGraknTxFactory;
import ai.grakn.kb.internal.EmbeddedGraknTx;
import ai.grakn.rpc.generated.GrpcGrakn.Open;

/**
 * Class used to handle gRPC Open requests. It extracts keyspace and tx type from gRPC request
 * and open new tx using GraknTxFactory
 */

public class OpenRequestImpl implements OpenRequest {

    private final EngineGraknTxFactory txFactory;

    public OpenRequestImpl(EngineGraknTxFactory txFactory) {
        this.txFactory = txFactory;
    }

    @Override
    public EmbeddedGraknTx<?> open(Open request) {
        Keyspace keyspace = Keyspace.of(request.getKeyspace());
        GraknTxType txType = GraknTxType.of(request.getTxType().getNumber());
        return txFactory.tx(keyspace, txType);
    }
}
