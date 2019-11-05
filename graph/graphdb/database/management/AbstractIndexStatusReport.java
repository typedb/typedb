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

import java.time.Duration;
import java.util.List;

public abstract class AbstractIndexStatusReport {
    protected final boolean success;
    protected final String indexName;
    protected final List<SchemaStatus> targetStatuses;
    protected final Duration elapsed;

    public AbstractIndexStatusReport(boolean success, String indexName, List<SchemaStatus> targetStatuses, Duration elapsed) {
        this.success = success;
        this.indexName = indexName;
        this.targetStatuses = targetStatuses;
        this.elapsed = elapsed;
    }

    public boolean getSucceeded() {
        return success;
    }

    public String getIndexName() {
        return indexName;
    }

    public List<SchemaStatus> getTargetStatuses() {
        return targetStatuses;
    }

    public Duration getElapsed() {
        return elapsed;
    }
}

