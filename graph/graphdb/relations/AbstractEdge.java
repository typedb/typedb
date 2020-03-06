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

package grakn.core.graph.graphdb.relations;

import com.google.common.base.Preconditions;
import grakn.core.graph.core.EdgeLabel;
import grakn.core.graph.core.JanusGraphEdge;
import grakn.core.graph.core.JanusGraphVertex;
import grakn.core.graph.graphdb.internal.InternalVertex;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

public abstract class AbstractEdge extends AbstractTypedRelation implements JanusGraphEdge {

    private InternalVertex start;
    private InternalVertex end;

    AbstractEdge(long id, EdgeLabel label, InternalVertex start, InternalVertex end) {
        super(id, label);
        this.start = start;
        this.end = end;
    }

    @Override
    public String toString() {
        return StringFactory.edgeString(this);
    }

    @Override
    public String label() {
        return type.name();
    }

    public void setVertexAt(int pos, InternalVertex vertex) {
        Preconditions.checkArgument(vertex != null && getVertex(pos).equals(vertex));
        switch (pos) {
            case 0:
                start = vertex;
                break;
            case 1:
                end = vertex;
                break;
            default:
                throw new IllegalArgumentException("Invalid position: " + pos);
        }
    }

    @Override
    public InternalVertex getVertex(int pos) {
        switch (pos) {
            case 0:
                return start;
            case 1:
                return end;
            default:
                throw new IllegalArgumentException("Invalid position: " + pos);
        }
    }

    @Override
    public int getArity() {
        return 2;
    }

    @Override
    public int getLen() {
        return type.isUnidirected(Direction.OUT) ? 1 : 2;
    }

    @Override
    public JanusGraphVertex vertex(Direction dir) {
        return getVertex(EdgeDirection.position(dir));
    }


    @Override
    public JanusGraphVertex otherVertex(Vertex vertex) {
        if (start.equals(vertex)) {
            return end;
        }

        if (end.equals(vertex)) {
            return start;
        }

        throw new IllegalArgumentException("Edge is not incident on vertex");
    }

    @Override
    public boolean isProperty() {
        return false;
    }

    @Override
    public boolean isEdge() {
        return true;
    }


}
