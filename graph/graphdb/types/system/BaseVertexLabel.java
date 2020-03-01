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

import grakn.core.graph.graphdb.internal.InternalVertexLabel;
import org.apache.tinkerpop.gremlin.structure.Vertex;


public class BaseVertexLabel extends EmptyVertex implements InternalVertexLabel {

    public static final BaseVertexLabel DEFAULT_VERTEXLABEL = new BaseVertexLabel(Vertex.DEFAULT_LABEL);

    private final String name;

    public BaseVertexLabel(String name) {
        this.name = name;
    }

    @Override
    public boolean isPartitioned() {
        return false;
    }

    @Override
    public boolean isStatic() {
        return false;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public boolean hasDefaultConfiguration() {
        return true;
    }

    @Override
    public int getTTL() {
        return 0;
    }

    @Override
    public String toString() {
        return name();
    }
}
