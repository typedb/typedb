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
import grakn.core.graph.core.JanusGraphRelation;
import grakn.core.graph.core.JanusGraphVertexProperty;
import grakn.core.graph.core.PropertyKey;
import grakn.core.graph.graphdb.internal.AbstractElement;
import grakn.core.graph.graphdb.internal.InternalRelation;
import grakn.core.graph.graphdb.internal.InternalRelationType;
import grakn.core.graph.graphdb.internal.InternalVertex;
import grakn.core.graph.graphdb.internal.Order;
import grakn.core.graph.graphdb.internal.OrderList;
import grakn.core.graph.graphdb.transaction.StandardJanusGraphTx;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.util.Comparator;

/**
 * A Comparator for JanusGraphRelation that uses a defined order to compare the relations with
 * or otherwise uses the natural order of relations.
 */
public class RelationComparator implements Comparator<InternalRelation> {

    private final StandardJanusGraphTx tx;
    private final InternalVertex vertex;
    private final OrderList orders;

    public RelationComparator(InternalVertex v) {
        this(v, OrderList.NO_ORDER);
    }

    public RelationComparator(InternalVertex v, OrderList orders) {
        this.vertex = Preconditions.checkNotNull(v);
        this.orders = Preconditions.checkNotNull(orders);
        this.tx = v.tx();
    }

    @Override
    public int compare(InternalRelation r1, InternalRelation r2) {
        if (r1.equals(r2)) return 0;

        //1) Based on orders (if any)
        if (!orders.isEmpty()) {
            for (OrderList.OrderEntry order : orders) {
                int orderCompare = compareOnKey(r1, r2, order.getKey(), order.getOrder());
                if (orderCompare != 0) return orderCompare;
            }
        }

        //2) RelationType (determine if property or edge - properties come first)
        int relationTypeCompare = (r1.isProperty() ? 1 : 2) - (r2.isProperty() ? 1 : 2);
        if (relationTypeCompare != 0) return relationTypeCompare;

        //3) JanusGraphType
        InternalRelationType t1 = (InternalRelationType) r1.getType(), t2 = (InternalRelationType) r2.getType();
        int typeCompare = AbstractElement.compare(t1, t2);
        if (typeCompare != 0) return typeCompare;

        //4) Direction
        Direction dir1 = null, dir2 = null;
        for (int i = 0; i < r1.getLen(); i++) {
            if (r1.getVertex(i).equals(vertex)) {
                dir1 = EdgeDirection.fromPosition(i);
                break;
            }
        }
        for (int i = 0; i < r2.getLen(); i++) {
            if (r2.getVertex(i).equals(vertex)) {
                dir2 = EdgeDirection.fromPosition(i);
                break;
            }
        }
        // ("Either relation is not incident on vertex [%s]", vertex);
        int dirCompare = EdgeDirection.position(dir1) - EdgeDirection.position(dir2);
        if (dirCompare != 0) return dirCompare;

        // Breakout: If type&direction are the same and the type is unique in the direction it follows that the relations are the same
        if (t1.multiplicity().isUnique(dir1)) return 0;

        // 5) Compare sort key values (this is empty and hence skipped if the type multiplicity is constrained)
        for (long typeId : t1.getSortKey()) {
            int keyCompare = compareOnKey(r1, r2, typeId, t1.getSortOrder());
            if (keyCompare != 0) return keyCompare;
        }
        // 6) Compare property objects or other vertices
        if (r1.isProperty()) {
            Object o1 = ((JanusGraphVertexProperty) r1).value();
            Object o2 = ((JanusGraphVertexProperty) r2).value();
            Preconditions.checkArgument(o1 != null && o2 != null);
            if (!o1.equals(o2)) {
                int objectCompare;
                if (Comparable.class.isAssignableFrom(((PropertyKey) t1).dataType())) {
                    objectCompare = ((Comparable) o1).compareTo(o2);
                } else {
                    objectCompare = System.identityHashCode(o1) - System.identityHashCode(o2);
                }
                if (objectCompare != 0) return objectCompare;
            }
        } else {
            Preconditions.checkArgument(r1.isEdge() && r2.isEdge());
            int vertexCompare = AbstractElement.compare(r1.getVertex(EdgeDirection.position(dir1.opposite())),
                    r2.getVertex(EdgeDirection.position(dir1.opposite())));
            if (vertexCompare != 0) return vertexCompare;
        }
        // Breakout: if type&direction are the same, and the end points of the relation are the same and the type is constrained, the relations must be the same
        if (t1.multiplicity().isConstrained()) return 0;

        // 7)compare relation ids
        return AbstractElement.compare(r1, r2);
    }

    private static int compareValues(Object v1, Object v2, Order order) {
        return compareValues(v1, v2) * (order == Order.DESC ? -1 : 1);
    }

    private static int compareValues(Object v1, Object v2) {
        if (v1 == null || v2 == null) {
            if (v1 != null) {
                return -1;
            } else if (v2 != null) {
                return 1;
            } else {
                return 0;
            }
        } else {
            Preconditions.checkArgument(v1 instanceof Comparable && v2 instanceof Comparable, "Encountered invalid values");
            return ((Comparable) v1).compareTo(v2);
        }
    }

    private int compareOnKey(JanusGraphRelation r1, JanusGraphRelation r2, long typeId, Order order) {
        return compareOnKey(r1, r2, tx.getExistingPropertyKey(typeId), order);
    }

    private int compareOnKey(JanusGraphRelation r1, JanusGraphRelation r2, PropertyKey type, Order order) {
        Object v1 = r1.valueOrNull(type), v2 = r2.valueOrNull(type);
        return compareValues(v1, v2, order);
    }
}
