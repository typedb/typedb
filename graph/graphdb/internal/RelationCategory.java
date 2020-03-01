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

package grakn.core.graph.graphdb.internal;

import com.google.common.collect.ImmutableList;
import grakn.core.graph.core.JanusGraphRelation;
import grakn.core.graph.core.JanusGraphVertexQuery;
import grakn.core.graph.graphdb.query.condition.Condition;


public enum RelationCategory implements Condition<JanusGraphRelation> {

    EDGE, PROPERTY, RELATION;

    public boolean isProper() {
        switch(this) {
            case EDGE:
            case PROPERTY:
                return true;
            case RELATION:
                return false;
            default: throw new AssertionError("Unrecognized type: " + this);
        }
    }

    public Iterable<JanusGraphRelation> executeQuery(JanusGraphVertexQuery query) {
        switch (this) {
            case EDGE: return (Iterable)query.edges();
            case PROPERTY: return (Iterable)query.properties();
            case RELATION: return query.relations();
            default: throw new AssertionError();
        }
    }

    /*
    ########### CONDITION DEFINITION #################
     */

    @Override
    public Type getType() {
        return Type.LITERAL;
    }

    @Override
    public Iterable getChildren() {
        return ImmutableList.of();
    }

    @Override
    public boolean hasChildren() {
        return false;
    }

    @Override
    public int numChildren() {
        return 0;
    }

    @Override
    public boolean evaluate(JanusGraphRelation relation) {
        switch(this) {
            case EDGE:
                return relation.isEdge();
            case PROPERTY:
                return relation.isProperty();
            case RELATION:
                return true;
            default: throw new AssertionError("Unrecognized type: " + this);
        }
    }
}
