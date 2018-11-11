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

package grakn.core.kb.internal;

import grakn.core.GraknTx;
import grakn.core.factory.EmbeddedGraknSession;
import grakn.core.util.ErrorMessage;
import grakn.core.util.Schema;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

/**
 * <p>
 *     A {@link GraknTx} using {@link TinkerGraph} as a vendor backend.
 * </p>
 *
 * <p>
 *     Wraps up a {@link TinkerGraph} as a method of storing the {@link GraknTx} object Model.
 *     With this vendor some exceptions are in place:
 *     1. Transactions do not exists and all threads work on the same graph at the same time.
 * </p>
 *
 * @author fppt
 */
public class GraknTxTinker extends EmbeddedGraknTx<TinkerGraph> {
    private final TinkerGraph rootGraph;

    public GraknTxTinker(EmbeddedGraknSession session, TinkerGraph tinkerGraph){
        super(session, tinkerGraph);
        rootGraph = tinkerGraph;
    }

    @Override
    public boolean isTinkerPopGraphClosed() {
        return !rootGraph.traversal().V().has(Schema.VertexProperty.SCHEMA_LABEL.name(), Schema.MetaSchema.ENTITY.getLabel().getValue()).hasNext();
    }

    @Override
    public void commit(){
        LOG.warn(ErrorMessage.TRANSACTIONS_NOT_SUPPORTED.getMessage(TinkerGraph.class.getName(), "committed"));
        super.commit();
    }

    @Override
    public void abort(){
        LOG.warn(ErrorMessage.TRANSACTIONS_NOT_SUPPORTED.getMessage(TinkerGraph.class.getName(), "aborted"));
        super.abort();
    }
}
