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

package grakn.core.server.session;

import grakn.core.server.Transaction;
import org.apache.tinkerpop.gremlin.structure.Graph;

/**
 * <p>
 *     Transaction Building Interface
 * </p>
 *
 * <p>
 *     The interface used to build new graphs from different vendors.
 *     Adding new vendor support means implementing this interface.
 * </p>
 *
 *
 * @param <T> A vendor implementation of a Tinkerpop {@link Graph}
 */
public interface TransactionFactory<T extends Graph> {
    /**
     *
     * @param txType The type of transaction to open on the graph
     * @return An instance of Grakn graph
     */
    TransactionImpl<T> open(Transaction.Type txType);

    /**
     *
     * @param batchLoading A flag which indicates if the graph has batch loading enabled or not.
     * @return An instance of a tinker graph
     */
    T getTinkerPopGraph(boolean batchLoading);
}
