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

package grakn.core.graph.graphdb.database.management;

import grakn.core.graph.core.schema.SchemaStatus;
import grakn.core.graph.graphdb.database.management.AbstractIndexStatusReport;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class GraphIndexStatusReport extends AbstractIndexStatusReport {
    private final Map<String, SchemaStatus> notConverged;
    private final Map<String, SchemaStatus> converged;

    public GraphIndexStatusReport(boolean success, String indexName, List<SchemaStatus> targetStatuses,
                                  Map<String, SchemaStatus> notConverged,
                                  Map<String, SchemaStatus> converged, Duration elapsed) {
        super(success, indexName, targetStatuses, elapsed);
        this.notConverged = notConverged;
        this.converged = converged;
    }

    public Map<String, SchemaStatus> getNotConvergedKeys() {
        return notConverged;
    }

    public Map<String, SchemaStatus> getConvergedKeys() {
        return converged;
    }

    @Override
    public String toString() {
        return "GraphIndexStatusReport[" +
                "success=" + success +
                ", indexName='" + indexName + '\'' +
                ", targetStatus=" + targetStatuses +
                ", notConverged=" + notConverged +
                ", converged=" + converged +
                ", elapsed=" + elapsed +
                ']';
    }
}

