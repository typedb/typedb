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

package grakn.core.graph.graphdb.relations;

import com.google.common.base.Preconditions;
import grakn.core.graph.core.JanusGraphEdge;
import grakn.core.graph.core.JanusGraphRelation;
import grakn.core.graph.core.JanusGraphTransaction;
import grakn.core.graph.core.JanusGraphVertex;
import grakn.core.graph.core.JanusGraphVertexProperty;
import grakn.core.graph.core.RelationType;
import grakn.core.graph.graphdb.internal.InternalRelation;
import grakn.core.graph.graphdb.query.vertex.VertexCentricQueryBuilder;
import grakn.core.graph.graphdb.transaction.StandardJanusGraphTx;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.io.Serializable;
import java.util.Arrays;


public final class RelationIdentifier implements Serializable {

    public static final String TOSTRING_DELIMITER = "-";

    private final long outVertexId;
    private final long typeId;
    private final long relationId;
    private final long inVertexId;

    private RelationIdentifier() {
        outVertexId = 0;
        typeId = 0;
        relationId = 0;
        inVertexId = 0;
    }

    private RelationIdentifier(long outVertexId, long typeId, long relationId, long inVertexId) {
        this.outVertexId = outVertexId;
        this.typeId = typeId;
        this.relationId = relationId;
        this.inVertexId = inVertexId;
    }

    static RelationIdentifier get(InternalRelation r) {
        if (r.hasId()) {
            return new RelationIdentifier(r.getVertex(0).longId(),
                    r.getType().longId(),
                    r.longId(), (r.isEdge() ? r.getVertex(1).longId() : 0));
        } else return null;
    }

    public long getRelationId() {
        return relationId;
    }

    public long getTypeId() {
        return typeId;
    }

    public long getOutVertexId() {
        return outVertexId;
    }

    public long getInVertexId() {
        Preconditions.checkState(inVertexId != 0);
        return inVertexId;
    }

    public static RelationIdentifier get(long[] ids) {
        if (ids.length != 3 && ids.length != 4) {
            throw new IllegalArgumentException("Not a valid relation identifier: " + Arrays.toString(ids));
        }
        for (int i = 0; i < 3; i++) {
            if (ids[i] < 0) {
                throw new IllegalArgumentException("Not a valid relation identifier: " + Arrays.toString(ids));
            }
        }
        return new RelationIdentifier(ids[1], ids[2], ids[0], ids.length == 4 ? ids[3] : 0);
    }

    public static RelationIdentifier get(int[] ids) {
        if (ids.length != 3 && ids.length != 4) {
            throw new IllegalArgumentException("Not a valid relation identifier: " + Arrays.toString(ids));
        }
        for (int i = 0; i < 3; i++) {
            if (ids[i] < 0) {
                throw new IllegalArgumentException("Not a valid relation identifier: " + Arrays.toString(ids));
            }
        }
        return new RelationIdentifier(ids[1], ids[2], ids[0], ids.length == 4 ? ids[3] : 0);
    }

    public long[] getLongRepresentation() {
        long[] r = new long[3 + (inVertexId != 0 ? 1 : 0)];
        r[0] = relationId;
        r[1] = outVertexId;
        r[2] = typeId;
        if (inVertexId != 0) r[3] = inVertexId;
        return r;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(relationId);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        } else if (!getClass().isInstance(other)) {
            return false;
        }
        RelationIdentifier oth = (RelationIdentifier) other;
        return relationId == oth.relationId && typeId == oth.typeId;
    }

    @Override
    public String toString() {
        StringBuilder s = new StringBuilder();
        s.append(relationId).append(TOSTRING_DELIMITER).append(outVertexId)
                .append(TOSTRING_DELIMITER).append(typeId);
        if (inVertexId != 0) {
            s.append(TOSTRING_DELIMITER).append(inVertexId);
        }
        return s.toString();
    }

    public static RelationIdentifier parse(String id) {
        String[] elements = id.split(TOSTRING_DELIMITER);
        if (elements.length != 3 && elements.length != 4) {
            throw new IllegalArgumentException("Not a valid relation identifier: " + id);
        }
        try {
            return new RelationIdentifier(Long.parseLong(elements[1]),
                    Long.parseLong(elements[2]),
                    Long.parseLong(elements[0]),
                    elements.length == 4 ? Long.parseLong(elements[3]) : 0);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid id - each token expected to be a number", e);
        }
    }

    JanusGraphRelation findRelation(JanusGraphTransaction tx) {
        JanusGraphVertex v = ((StandardJanusGraphTx) tx).getInternalVertex(outVertexId);
        if (v == null || v.isRemoved()) {
            return null;
        }
        JanusGraphVertex typeVertex = tx.getVertex(typeId);
        if (typeVertex == null) {
            return null;
        }
        if (!(typeVertex instanceof RelationType)) {
            throw new IllegalArgumentException("Invalid RelationIdentifier: typeID does not reference a type");
        }
        RelationType type = (RelationType) typeVertex;
        Iterable<? extends JanusGraphRelation> relations;
        if (((RelationType) typeVertex).isEdgeLabel()) {
            Direction dir = Direction.OUT;
            JanusGraphVertex other = ((StandardJanusGraphTx) tx).getInternalVertex(inVertexId);
            if (other == null || other.isRemoved()) {
                return null;
            }
            if (((StandardJanusGraphTx) tx).isPartitionedVertex(v) && !((StandardJanusGraphTx) tx).isPartitionedVertex(other)) { //Swap for likely better performance
                JanusGraphVertex tmp = other;
                other = v;
                v = tmp;
                dir = Direction.IN;
            }
            relations = ((VertexCentricQueryBuilder) v.query()).noPartitionRestriction().types(type).direction(dir).adjacent(other).edges();
        } else {
            relations = ((VertexCentricQueryBuilder) v.query()).noPartitionRestriction().types(type).properties();
        }

        for (JanusGraphRelation r : relations) {
            //Find current or previous relation
            if (r.longId() == relationId || ((r instanceof StandardRelation) && ((StandardRelation) r).getPreviousID() == relationId)) {
                return r;
            }
        }
        return null;
    }

    public JanusGraphEdge findEdge(JanusGraphTransaction tx) {
        JanusGraphRelation r = findRelation(tx);
        if (r == null) {
            return null;
        } else if (r instanceof JanusGraphEdge) {
            return (JanusGraphEdge) r;
        } else {
            throw new UnsupportedOperationException("Referenced relation is a property not an edge");
        }
    }

    public JanusGraphVertexProperty findProperty(JanusGraphTransaction tx) {
        JanusGraphRelation r = findRelation(tx);
        if (r == null) {
            return null;
        } else if (r instanceof JanusGraphVertexProperty) {
            return (JanusGraphVertexProperty) r;
        } else {
            throw new UnsupportedOperationException("Referenced relation is a edge not a property");
        }
    }


}
