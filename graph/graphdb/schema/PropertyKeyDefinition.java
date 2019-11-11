/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2019 Grakn Labs Ltd
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

package grakn.core.graph.graphdb.schema;

import org.apache.tinkerpop.gremlin.structure.Direction;
import grakn.core.graph.core.Cardinality;
import grakn.core.graph.core.Multiplicity;
import grakn.core.graph.core.PropertyKey;
import grakn.core.graph.graphdb.schema.RelationTypeDefinition;


public class PropertyKeyDefinition extends RelationTypeDefinition {

    private final Class<?> dataType;

    public PropertyKeyDefinition(String name, long id, Cardinality cardinality, Class dataType) {
        this(name,id, Multiplicity.convert(cardinality),dataType);
    }

    public PropertyKeyDefinition(String name, long id, Multiplicity multiplicity, Class dataType) {
        super(name, id, multiplicity);
        this.dataType = dataType;
    }

    public PropertyKeyDefinition(PropertyKey key) {
        this(key.name(),key.longId(),key.cardinality(),key.dataType());
    }

    public Class<?> getDataType() {
        return dataType;
    }

    @Override
    public boolean isUnidirected(Direction dir) {
        return dir== Direction.OUT;
    }

}
