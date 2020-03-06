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

package grakn.core.kb.graql.planning.spanningtree.graph;

import grakn.core.core.Schema;
import grakn.core.kb.concept.api.Label;
import grakn.core.kb.concept.manager.ConceptManager;
import grakn.core.kb.keyspace.KeyspaceStatistics;

public class InstanceNode extends Node {

    private static final int INSTANCE_NODE_PRIORTIY = 3;
    // null instance type label indicates we have no information and we the total of all instance counts;
    private Label instanceTypeLabel = null;

    public InstanceNode(NodeId nodeId) {
        super(nodeId);
    }

    @Override
    public long matchingElementsEstimate(ConceptManager conceptManager, KeyspaceStatistics statistics) {
        if (instanceTypeLabel == null) {
            return statistics.count(conceptManager, Schema.MetaSchema.THING.getLabel());
        } else {
            return statistics.count(conceptManager, instanceTypeLabel);
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

