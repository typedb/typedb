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
 */

package grakn.core.graph.graphdb.transaction;

import com.google.common.base.Preconditions;
import grakn.core.graph.core.EdgeLabel;
import grakn.core.graph.core.JanusGraphRelation;
import grakn.core.graph.core.PropertyKey;
import grakn.core.graph.diskstorage.Entry;
import grakn.core.graph.graphdb.internal.InternalRelation;
import grakn.core.graph.graphdb.internal.InternalRelationType;
import grakn.core.graph.graphdb.internal.InternalVertex;
import grakn.core.graph.graphdb.relations.CacheEdge;
import grakn.core.graph.graphdb.relations.CacheVertexProperty;
import grakn.core.graph.graphdb.relations.RelationCache;
import grakn.core.graph.graphdb.types.TypeUtil;

import java.util.Iterator;


public class RelationConstructor {

    public static RelationCache readRelationCache(Entry data, StandardJanusGraphTx tx) {
        return tx.getEdgeSerializer().readRelation(data, false, tx);
    }

    public static Iterable<JanusGraphRelation> readRelation(InternalVertex vertex, Iterable<Entry> data, StandardJanusGraphTx tx) {
        return () -> new Iterator<JanusGraphRelation>() {

            private Iterator<Entry> iterator = data.iterator();
            private JanusGraphRelation current = null;

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public JanusGraphRelation next() {
                current = readRelation(vertex, iterator.next(), tx);
                return current;
            }

            @Override
            public void remove() {
                Preconditions.checkNotNull(current);
                current.remove();
            }
        };
    }

    private static InternalRelation readRelation(InternalVertex vertex, Entry data, StandardJanusGraphTx tx) {
        RelationCache relation = tx.getEdgeSerializer().readRelation(data, true, tx);
        return readRelation(vertex, relation, data, tx);
    }


    private static InternalRelation readRelation(InternalVertex vertex, RelationCache relation, Entry data, StandardJanusGraphTx tx) {
        InternalRelationType type = TypeUtil.getBaseType((InternalRelationType) tx.getExistingRelationType(relation.typeId));

        if (type.isPropertyKey()) {
            return new CacheVertexProperty(relation.relationId, (PropertyKey) type, vertex, relation.getValue(), data);
        }

        if (type.isEdgeLabel()) {
            InternalVertex otherVertex = tx.getInternalVertex(relation.getOtherVertexId());
            switch (relation.direction) {
                case IN:
                    return new CacheEdge(relation.relationId, (EdgeLabel) type, otherVertex, vertex, data);

                case OUT:
                    return new CacheEdge(relation.relationId, (EdgeLabel) type, vertex, otherVertex, data);

                default:
                    throw new AssertionError();
            }
        }

        throw new AssertionError();
    }

}
