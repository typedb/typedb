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

package ai.grakn.graph.internal;

import ai.grakn.util.ErrorMessage;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

/**
 * A grakn graph which uses a Tinkergraph backend.
 * Primarily used for testing
 */
public class GraknTinkerGraph extends AbstractGraknGraph<TinkerGraph> {
    public GraknTinkerGraph(TinkerGraph tinkerGraph, String name, String engineUrl, boolean batchLoading){
        super(tinkerGraph, name, engineUrl, batchLoading);
    }

    @Override
    public ConceptImpl getConceptByBaseIdentifier(Object baseIdentifier) {
        try {
            return super.getConceptByBaseIdentifier(Long.valueOf(baseIdentifier.toString()));
        } catch (NumberFormatException e){
            return null;
        }
    }

    @Override
    public void rollback(){
        throw new UnsupportedOperationException(ErrorMessage.UNSUPPORTED_GRAPH.getMessage(getTinkerPopGraph().getClass().getName(), "rollback"));
    }
}
