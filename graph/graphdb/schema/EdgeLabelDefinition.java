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

import grakn.core.graph.core.EdgeLabel;
import grakn.core.graph.core.Multiplicity;
import org.apache.tinkerpop.gremlin.structure.Direction;


public class EdgeLabelDefinition extends RelationTypeDefinition {

    private final boolean unidirected;

    public EdgeLabelDefinition(String name, long id, Multiplicity multiplicity, boolean unidirected) {
        super(name, id, multiplicity);
        this.unidirected = unidirected;
    }

    public EdgeLabelDefinition(EdgeLabel label) {
        this(label.name(),label.longId(),label.multiplicity(),label.isUnidirected());
    }

    public boolean isDirected() {
        return !unidirected;
    }

    public boolean isUnidirected() {
        return unidirected;
    }

    @Override
    public boolean isUnidirected(Direction dir) {
        if (unidirected) return dir== Direction.OUT;
        else return dir== Direction.BOTH;
    }



}
