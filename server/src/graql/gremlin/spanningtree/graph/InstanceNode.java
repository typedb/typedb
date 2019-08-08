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

import grakn.core.concept.Label;
import grakn.core.server.kb.Schema;
import grakn.core.server.session.TransactionOLTP;

public class InstanceNode extends Node {

    private static final int INSTANCE_NODE_PRIORTIY = 3;
    // null instance type label indicates we have no information and we the total of all instance counts;
    private Label instanceTypeLabel = null;

    public InstanceNode(NodeId nodeId) {
        super(nodeId);
    }

    @Override
    public long matchingElementsEstimate(TransactionOLTP tx) {
        if (instanceTypeLabel == null) {
            return tx.session().keyspaceStatistics().count(tx, Schema.MetaSchema.THING.getLabel());
        } else {
            return tx.session().keyspaceStatistics().count(tx, instanceTypeLabel);
        }
    }

    @Override
    public int getNodeTypePriority() {
        return INSTANCE_NODE_PRIORTIY;
    }

    public void setInstanceLabel(Label instanceTypeLabel) {
        this.instanceTypeLabel = instanceTypeLabel;
    }

    public Label getInstanceLabel() {
        return instanceTypeLabel;
    }

}

