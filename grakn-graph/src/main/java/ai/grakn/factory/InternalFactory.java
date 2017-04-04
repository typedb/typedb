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

package ai.grakn.factory;

import ai.grakn.GraknTxType;
import ai.grakn.graph.internal.AbstractGraknGraph;
import org.apache.tinkerpop.gremlin.structure.Graph;

/**
 * <p>
 *     Graph Building Interface
 * </p>
 *
 * <p>
 *     The interface used to build new graphs from different vendors.
 *     Adding new vendor support means implementing this interface.
 * </p>
 *
 * @author fppt
 *
 * @param <T> A vendor implementation of a Tinkerpop {@link Graph}
 */
interface InternalFactory<T extends Graph> {
    /**
     *
     * @param txType The type of transaction to open on the graph
     * @return An instance of Grakn graph
     */
    AbstractGraknGraph<T> open(GraknTxType txType);

    /**
     *
     * @param batchLoading A flag which indicates if the graph has batch loading enabled or not.
     * @return An instance of a tinker graph
     */
    T getTinkerPopGraph(boolean batchLoading);


}
