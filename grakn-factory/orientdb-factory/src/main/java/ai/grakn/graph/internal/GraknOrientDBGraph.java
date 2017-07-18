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

import ai.grakn.concept.Concept;
import org.apache.tinkerpop.gremlin.orientdb.OrientGraph;

import java.util.Properties;

/**
 * <p>
 *     A Grakn Graph using {@link OrientGraph} as a vendor backend.
 * </p>
 *
 * <p>
 *     Wraps up a {@link OrientGraph} as a method of storing the Grakn Graph object Model.
 *     With this vendor some issues to be aware of:
 *     1. {@link AbstractGraknGraph#isConceptModified(ConceptImpl)} always returns true due to methods not available
 *        yet on orient's side.
 *     2. Indexing is done across labels as opposed to global indices
 * </p>
 *
 * @author fppt
 */
public class GraknOrientDBGraph extends AbstractGraknGraph<OrientGraph> {
    public GraknOrientDBGraph(OrientGraph graph, String name, String engineUrl, Properties properties){
        super(graph, name, engineUrl, properties);
    }

    /**
     *
     * @param concept A concept in the graph
     * @return True because at the moment there is no method in
     * {@link org.apache.tinkerpop.gremlin.orientdb.OrientElement} which helps us to determine this.
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
        //TODO: determine if the connection is closed
        return false;
    }

    @Override
    protected void commitTransactionInternal(){
        getTinkerPopGraph().commit();
    }
}
