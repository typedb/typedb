/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
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
import grakn.core.concept.type.SchemaConcept;
import grakn.core.server.kb.Schema;
import grakn.core.server.session.TransactionOLTP;

import java.util.HashSet;
import java.util.Set;

public class InstanceNode extends Node {

    private Long cachedMatchingElementsEstimate = null;

    private static final int INSTANCE_NODE_PRIORTIY = 4;
    private Set<Label> instanceTypeLabels = new HashSet<>();

    public InstanceNode(NodeId nodeId) {
        super(nodeId);
    }

    /*
    This can be an expensive operation - cache it
     */
    @Override
    public long matchingElementsEstimate(TransactionOLTP tx) {
        if (cachedMatchingElementsEstimate == null) {
            if (instanceTypeLabels.isEmpty()) {
                cachedMatchingElementsEstimate = tx.session().keyspaceStatistics().count(tx, Schema.MetaSchema.THING.getLabel());
            } else {
                long count = 0;
                for (Label possibleLabel : instanceTypeLabels) {
                    count += tx.session().keyspaceStatistics().count(tx, possibleLabel);
                }
                cachedMatchingElementsEstimate = count;
            }
        }
        return cachedMatchingElementsEstimate;
    }

    @Override
    public int getNodeTypePriority() {
        return INSTANCE_NODE_PRIORTIY;
    }

    public void setInstanceLabels(Set<Label> instanceTypeLabels) {
        this.instanceTypeLabels = instanceTypeLabels;
    }

    public Set<Label> getInstanceLabels() {
        return instanceTypeLabels;
    }

}

