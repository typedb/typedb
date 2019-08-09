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

package grakn.core.graql.gremlin.spanningtree.graph;

import grakn.core.server.session.TransactionOLTP;

public class SchemaNode extends Node {

    private static final int SCHEMA_NODE_PRIORITY = 1;

    public SchemaNode(NodeId nodeId) {
        super(nodeId);
    }

    @Override
    public long matchingElementsEstimate(TransactionOLTP tx) {
        // only 1 node ever matches a schema node
        return 1;
    }

    @Override
    public int getNodeTypePriority() {
        return SCHEMA_NODE_PRIORITY;
    }
}
