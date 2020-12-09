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
 *
 */

package grakn.core.graph;

import grakn.core.common.exception.ErrorMessage;
import grakn.core.common.exception.GraknException;

import java.util.Objects;

public class GraphManager {

    private final SchemaGraph schemaGraph;

    private final DataGraph dataGraph;

    public GraphManager(SchemaGraph schemaGraph, DataGraph dataGraph) {
        this.schemaGraph = schemaGraph;
        this.dataGraph = dataGraph;
    }

    public SchemaGraph schema() {
        return schemaGraph;
    }

    public DataGraph data() {
        return dataGraph;
    }

    public void clear() {
        schemaGraph.clear();
        dataGraph.clear();
    }

    public GraknException exception(ErrorMessage error) {
        return dataGraph.storage().exception(error);
    }

    public GraknException exception(Exception exception) {
        return dataGraph.storage().exception(exception);
    }

    public GraknException exception(GraknException exception) {
        return dataGraph.storage().exception(exception);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final GraphManager graphMgr = (GraphManager) o;
        return schemaGraph.equals(graphMgr.schemaGraph) && dataGraph.equals(graphMgr.dataGraph);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schemaGraph, dataGraph);
    }
}
