/*
 * Copyright (C) 2020 Grakn Labs
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

package grakn.core.graph.graphdb.internal;

import grakn.core.graph.core.JanusGraphElement;
import grakn.core.graph.core.JanusGraphTransaction;
import grakn.core.graph.graphdb.transaction.StandardJanusGraphTx;

/**
 * Internal Element interface adding methods that should only be used by JanusGraph
 */
public interface InternalElement extends JanusGraphElement {

    /**
     * Returns this element in the context of the current transaction.
     */
    InternalElement it();

    /**
     * Returns the transaction to which the element is currently bound or should be refreshed into
     */
    StandardJanusGraphTx tx();


    @Override
    default JanusGraphTransaction graph() {
        return tx();
    }

    void setId(long id);

    /**
     * @return The lifecycle of this element
     * see ElementLifeCycle
     */
    byte getLifeCycle();

    /**
     * Whether this element is invisible and should only be returned to queries that explicitly ask for invisible elements.
     */
    boolean isInvisible();

}
