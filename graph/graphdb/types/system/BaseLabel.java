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

package grakn.core.graph.graphdb.types.system;

import grakn.core.graph.core.Connection;
import grakn.core.graph.core.EdgeLabel;
import grakn.core.graph.core.Multiplicity;
import grakn.core.graph.core.PropertyKey;
import grakn.core.graph.graphdb.internal.JanusGraphSchemaCategory;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.util.ArrayList;
import java.util.Collection;

public class BaseLabel extends BaseRelationType implements EdgeLabel {

    public static final BaseLabel SchemaDefinitionEdge = new BaseLabel("SchemaRelated", 36, Direction.BOTH, Multiplicity.MULTI);
    public static final BaseLabel VertexLabelEdge = new BaseLabel("vertexlabel", 2, Direction.OUT, Multiplicity.MANY2ONE);

    private final Direction directionality;
    private final Multiplicity multiplicity;

    private BaseLabel(String name, int id, Direction uniDirectionality, Multiplicity multiplicity) {
        super(name, id, JanusGraphSchemaCategory.EDGELABEL);
        this.directionality = uniDirectionality;
        this.multiplicity = multiplicity;
    }

    @Override
    public long[] getSignature() {
        return new long[]{BaseKey.SchemaDefinitionDesc.longId()};
    }

    @Override
    public Multiplicity multiplicity() {
        return multiplicity;
    }

    @Override
    public Collection<PropertyKey> mappedProperties() {
        return new ArrayList<>();
    }

    @Override
    public Collection<Connection> mappedConnections() {
        return new ArrayList<>();
    }

    @Override
    public final boolean isPropertyKey() {
        return false;
    }

    @Override
    public final boolean isEdgeLabel() {
        return true;
    }

    @Override
    public boolean isDirected() {
        return true;
    }

    @Override
    public boolean isUnidirected() {
        return isUnidirected(Direction.OUT);
    }

    @Override
    public boolean isUnidirected(Direction dir) {
        return dir == directionality;
    }


}
