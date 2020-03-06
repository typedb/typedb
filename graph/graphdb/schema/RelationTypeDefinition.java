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

package grakn.core.graph.graphdb.schema;

import grakn.core.graph.core.Cardinality;
import grakn.core.graph.core.Multiplicity;
import org.apache.tinkerpop.gremlin.structure.Direction;


public abstract class RelationTypeDefinition extends SchemaElementDefinition {

    private final Multiplicity multiplicity;

    public RelationTypeDefinition(String name, long id, Multiplicity multiplicity) {
        super(name, id);
        this.multiplicity = multiplicity;
    }

    public Multiplicity getMultiplicity() {
        return multiplicity;
    }

    public Cardinality getCardinality() {
        return multiplicity.getCardinality();
    }

    public abstract boolean isUnidirected(Direction dir);

}
