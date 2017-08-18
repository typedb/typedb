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
 */

package ai.grakn.kb.internal;

import ai.grakn.concept.Concept;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

import java.util.Properties;

/**
 * <p>
 *     A Grakn Graph using {@link TinkerGraph} as a vendor backend.
 * </p>
 *
 * <p>
 *     Wraps up a {@link TinkerGraph} as a method of storing the Grakn Graph object Model.
 *     With this vendor some exceptions are in place:
 *     1. Transactions do not exists and all threads work on the same graph at the same time.
 * </p>
 *
 * @author fppt
 */
public class GraknTxTinker extends GraknTxAbstract<TinkerGraph> {
    private final TinkerGraph rootGraph;

    public GraknTxTinker(TinkerGraph tinkerGraph, String name, String engineUrl, Properties properties){
        super(tinkerGraph, name, engineUrl, properties);
        rootGraph = tinkerGraph;
    }

    /**
     *
     * @param concept A concept in the graph
     * @return true all the time. There is no way to know if a
     * {@link org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerVertex} has been modified or not.
     */
    @Override
    public boolean isConceptModified(Concept concept) {
        return true;
    }

    @Override
    public int numOpenTx() {
        return 1;
    }

    @Override
    public boolean isSessionClosed() {
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
