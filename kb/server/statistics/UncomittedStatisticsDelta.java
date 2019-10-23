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
 *
 */

package grakn.core.kb.server.statistics;

import grakn.core.kb.concept.api.AttributeType;
import grakn.core.kb.concept.api.EntityType;
import grakn.core.kb.concept.api.Label;
import grakn.core.core.Schema;
import grakn.core.kb.concept.api.RelationType;
import grakn.core.kb.concept.api.Type;

import java.util.HashMap;

/**
 * A transaction-bound tracker of created and deleted types. A simple implementation that increments and decrements
 * and doesn't need to be thread safe as transactions are currently bound to a single thread.
 *
 * Written into `KeyspaceStatistics` on commit
 */
public class UncomittedStatisticsDelta {

    private HashMap<Label, Long> instanceDeltas;
    // keep these outside of the hashmap to avoid a large number of hash() method calls
    private long thingCount = 0;
    private long entityCount = 0;
    private long relationCount = 0;
    private long attributeCount = 0;

    public UncomittedStatisticsDelta() {
        instanceDeltas = new HashMap<>();
    }

    public long delta(Label label) {
        return instanceDeltas.getOrDefault(label, 0L);
    }

    public void increment(Type type) {
        Label label = type.label();
        Long currentCount = instanceDeltas.getOrDefault(label, 0L);
        instanceDeltas.put(label, currentCount + 1);
        thingCount++;
        if (type.getClass().equals(EntityType.class)) {
            entityCount++;
        } else if (type.getClass().equals(RelationType.class)) {
            relationCount++;
        } else if (type.getClass().equals(AttributeType.class)) {
            attributeCount++;
        } else {
            // some exception
        }
    }

    public void decrement(Type type) {
        Label label = type.label();
        Long currentCount = instanceDeltas.getOrDefault(label, 0L);
        instanceDeltas.put(label, currentCount - 1);
        thingCount--;
        if (type.getClass().equals(EntityType.class)) {
            entityCount--;
        } else if (type.getClass().equals(RelationType.class)) {
            relationCount--;
        } else if (type.getClass().equals(AttributeType.class)) {
            attributeCount--;
        } else {
            // some exception
        }
    }

    /**
     * Special case decrement for attribute deduplication
     * @param label
     */
    public void decrementAttribute(Label label) {
        Long currentCount = instanceDeltas.getOrDefault(label, 0L);
        instanceDeltas.put(label, currentCount - 1);
        thingCount--;
        attributeCount--;
    }

    public HashMap<Label, Long> instanceDeltas() {
        // copy the meta type counts into the map on retrieval
        instanceDeltas.put(Schema.MetaSchema.THING.getLabel(), thingCount);
        instanceDeltas.put(Schema.MetaSchema.ENTITY.getLabel(), entityCount);
        instanceDeltas.put(Schema.MetaSchema.RELATION.getLabel(), relationCount);
        instanceDeltas.put(Schema.MetaSchema.ATTRIBUTE.getLabel(), attributeCount);
        return instanceDeltas;
    }
}
