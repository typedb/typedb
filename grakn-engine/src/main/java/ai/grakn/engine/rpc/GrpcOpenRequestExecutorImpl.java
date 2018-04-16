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

package ai.grakn.engine.rpc;

/*-
 * #%L
 * grakn-engine
 * %%
 * Copyright (C) 2016 - 2018 Grakn Labs Ltd
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */

import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.engine.factory.EngineGraknTxFactory;
import ai.grakn.grpc.GrpcOpenRequestExecutor;
import ai.grakn.grpc.GrpcUtil;
import ai.grakn.kb.internal.EmbeddedGraknTx;
import ai.grakn.rpc.generated.GrpcGrakn.Open;

/**
 * Class used to handle gRPC Open requests. It extracts keyspace and tx type from gRPC request
 * and open new tx using GraknTxFactory
 *
 * @author marcoscoppetta
 */

public class GrpcOpenRequestExecutorImpl implements GrpcOpenRequestExecutor {

    private final EngineGraknTxFactory txFactory;

    public GrpcOpenRequestExecutorImpl(EngineGraknTxFactory txFactory) {
        this.txFactory=txFactory;
    }

    @Override
    public EmbeddedGraknTx<?> execute(Open request) {
        Keyspace keyspace = GrpcUtil.getKeyspace(request);
        GraknTxType txType = GrpcUtil.getTxType(request);
        return txFactory.tx(keyspace, txType);
    }
}
