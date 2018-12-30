/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package ai.grakn.engine.rpc;

import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.engine.factory.EngineGraknTxFactory;
import ai.grakn.kb.internal.EmbeddedGraknTx;
import ai.grakn.rpc.proto.SessionProto;

/**
 * A request transaction opener for RPC Services. It requires the keyspace and transaction type from the argument object
 * to open a new transaction.
 */
public class ServerOpenRequest implements OpenRequest {

    private final EngineGraknTxFactory txFactory;

    public ServerOpenRequest(EngineGraknTxFactory txFactory) {
        this.txFactory = txFactory;
    }

    @Override
    public EmbeddedGraknTx<?> open(SessionProto.Transaction.Open.Req request) {
        Keyspace keyspace = Keyspace.of(request.getKeyspace());
        GraknTxType txType = GraknTxType.of(request.getType().getNumber());
        return txFactory.tx(keyspace, txType);
    }

}
