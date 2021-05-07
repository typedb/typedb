/*
 * Copyright (C) 2021 Vaticle
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
 *
 */

package com.vaticle.typedb.core.graph;

import com.vaticle.typedb.core.common.exception.ErrorMessage;
import com.vaticle.typedb.core.common.exception.TypeDBException;

import java.util.Objects;

public class GraphManager {

    private final TypeGraph typeGraph;

    private final ThingGraph thingGraph;

    public GraphManager(TypeGraph typeGraph, ThingGraph thingGraph) {
        this.typeGraph = typeGraph;
        this.thingGraph = thingGraph;
    }

    public TypeGraph schema() {
        return typeGraph;
    }

    public ThingGraph data() {
        return thingGraph;
    }

    public void clear() {
        typeGraph.clear();
        thingGraph.clear();
    }

    public TypeDBException exception(ErrorMessage error) {
        return thingGraph.storage().exception(error);
    }

    public TypeDBException exception(Exception exception) {
        return thingGraph.storage().exception(exception);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        GraphManager graphMgr = (GraphManager) o;
        return typeGraph.equals(graphMgr.typeGraph) && thingGraph.equals(graphMgr.thingGraph);
    }

    @Override
    public int hashCode() {
        return Objects.hash(typeGraph, thingGraph);
    }
}
