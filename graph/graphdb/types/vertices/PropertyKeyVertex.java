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

package grakn.core.graph.graphdb.types.vertices;

import grakn.core.graph.core.Cardinality;
import grakn.core.graph.core.PropertyKey;
import grakn.core.graph.graphdb.transaction.StandardJanusGraphTx;
import grakn.core.graph.graphdb.types.TypeDefinitionCategory;
import org.apache.tinkerpop.gremlin.structure.Direction;

public class PropertyKeyVertex extends RelationTypeVertex implements PropertyKey {

    public PropertyKeyVertex(StandardJanusGraphTx tx, long id, byte lifecycle) {
        super(tx, id, lifecycle);
    }

    @Override
    public Class<?> dataType() {
        return getDefinition().getValue(TypeDefinitionCategory.DATATYPE,Class.class);
    }

    @Override
    public Cardinality cardinality() {
        return super.multiplicity().getCardinality();
    }

    @Override
    public final boolean isPropertyKey() {
        return true;
    }

    @Override
    public final boolean isEdgeLabel() {
        return false;
    }

    @Override
    public boolean isUnidirected(Direction dir) {
        return dir== Direction.OUT;
    }
}
