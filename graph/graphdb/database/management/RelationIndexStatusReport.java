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

public class RelationIndexStatusReport extends AbstractIndexStatusReport {

    private final String relationTypeName;
    private final SchemaStatus actualStatus;

    public RelationIndexStatusReport(boolean success, String indexName, String relationTypeName, SchemaStatus actualStatus,
                                     List<SchemaStatus>targetStatuses, Duration elapsed) {
        super(success, indexName, targetStatuses, elapsed);
        this.relationTypeName = relationTypeName;
        this.actualStatus = actualStatus;
    }

    public String getRelationTypeName() {
        return relationTypeName;
    }

    public SchemaStatus getActualStatus() {
        return actualStatus;
    }

    @Override
    public String toString() {
        return "RelationIndexStatusReport[" +
                "succeeded=" + success +
                ", indexName='" + indexName + '\'' +
                ", relationTypeName='" + relationTypeName + '\'' +
                ", actualStatus=" + actualStatus +
                ", targetStatus=" + targetStatuses +
                ", elapsed=" + elapsed +
                ']';
    }
}
