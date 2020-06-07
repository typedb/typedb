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

import grakn.core.graph.core.JanusGraphRelation;
import grakn.core.graph.core.PropertyKey;

/**
 * Internal Relation interface adding methods that should only be used by JanusGraph.
 * <p>
 * The "direct" qualifier in the method names indicates that the corresponding action is executed on this relation
 * object and not migrated to a different transactional context. It also means that access returns the "raw" value of
 * what is stored on this relation
 */
public interface InternalRelation extends JanusGraphRelation, InternalElement {

    /**
     * Returns this relation in the current transactional context
     */
    @Override
    InternalRelation it();

    /**
     * Returns the vertex at the given position (0=OUT, 1=IN) of this relation
     */
    InternalVertex getVertex(int pos);

    /**
     * Number of vertices on this relation.
     */
    int getArity();

    /**
     * Number of vertices on this relation that are aware of its existence. This number will
     * differ from #getArity()
     */
    int getLen();


    <O> O getValueDirect(PropertyKey key);

    void setPropertyDirect(PropertyKey key, Object value);

    Iterable<PropertyKey> getPropertyKeysDirect();

    <O> O removePropertyDirect(PropertyKey key);

}
