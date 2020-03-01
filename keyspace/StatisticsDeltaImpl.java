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

package grakn.core.keyspace;

import grakn.core.core.Schema;
import grakn.core.kb.concept.api.AttributeType;
import grakn.core.kb.concept.api.EntityType;
import grakn.core.kb.concept.api.GraknConceptException;
import grakn.core.kb.concept.api.Label;
import grakn.core.kb.concept.api.RelationType;
import grakn.core.kb.concept.api.Type;
import grakn.core.kb.keyspace.StatisticsDelta;

import java.util.HashMap;

/**
 * A transaction-bound tracker of created and deleted types. A simple implementation that increments and decrements
 * and doesn't need to be thread safe as transactions are currently bound to a single thread.
 *
 * Written into `KeyspaceStatistics` on commit
 */
public class StatisticsDeltaImpl implements StatisticsDelta {

    private HashMap<Label, Long> instanceDeltas;
    // keep these outside of the hashmap to avoid a large number of hash() method calls
    private long thingCount = 0;
    private long entityCount = 0;
    private long relationCount = 0;
    private long attributeCount = 0;

    public StatisticsDeltaImpl() {
        instanceDeltas = new HashMap<>();
    }

    @Override
    public long delta(Label label) {
        return instanceDeltas.getOrDefault(label, 0L);
    }

    @Override
    public void increment(Type type) {
        Label label = type.label();
        Long currentCount = instanceDeltas.getOrDefault(label, 0L);
        instanceDeltas.put(label, currentCount + 1);
        thingCount++;
        if (type instanceof EntityType) {
            entityCount++;
        } else if (type instanceof RelationType) {
            relationCount++;
        } else if (type instanceof AttributeType) {
            attributeCount++;
        } else {
            throw GraknConceptException.unknownTypeMetaType(type);
        }
    }

    @Override
    public void decrement(Type type) {
        Label label = type.label();
        Long currentCount = instanceDeltas.getOrDefault(label, 0L);
        instanceDeltas.put(label, currentCount - 1);
        thingCount--;
        if (type instanceof EntityType) {
            entityCount--;
        } else if (type instanceof RelationType) {
            relationCount--;
        } else if (type instanceof AttributeType) {
            attributeCount--;
        } else {
            throw GraknConceptException.unknownTypeMetaType(type);
        }
    }

    /**
     * Special case decrement for attribute deduplication
     * @param label
     */
    @Override
    public void decrementAttribute(Label label) {
        Long currentCount = instanceDeltas.getOrDefault(label, 0L);
        instanceDeltas.put(label, currentCount - 1);
        thingCount--;
        attributeCount--;
    }

    @Override
    public HashMap<Label, Long> instanceDeltas() {
        // copy the meta type counts into the map on retrieval
        if (thingCount != 0) {
            instanceDeltas.put(Schema.MetaSchema.THING.getLabel(), thingCount);
        }
        if (entityCount != 0) {
            instanceDeltas.put(Schema.MetaSchema.ENTITY.getLabel(), entityCount);
        }
        if (relationCount != 0) {
            instanceDeltas.put(Schema.MetaSchema.RELATION.getLabel(), relationCount);
        }
        if (attributeCount != 0) {
            instanceDeltas.put(Schema.MetaSchema.ATTRIBUTE.getLabel(), attributeCount);
        }
        return instanceDeltas;
    }
}
