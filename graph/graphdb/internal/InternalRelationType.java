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

import grakn.core.graph.core.Multiplicity;
import grakn.core.graph.core.RelationType;
import grakn.core.graph.core.schema.ConsistencyModifier;
import grakn.core.graph.core.schema.SchemaStatus;
import grakn.core.graph.graphdb.types.IndexType;
import org.apache.tinkerpop.gremlin.structure.Direction;

/**
 * Internal Type interface adding methods that should only be used by JanusGraph
 *
 */
public interface InternalRelationType extends RelationType, InternalVertex {

    boolean isInvisibleType();

    long[] getSignature();

    long[] getSortKey();

    Order getSortOrder();

    Multiplicity multiplicity();

    ConsistencyModifier getConsistencyModifier();

    Integer getTTL();

    boolean isUnidirected(Direction dir);

    InternalRelationType getBaseType();

    Iterable<InternalRelationType> getRelationIndexes();

    SchemaStatus getStatus();

    Iterable<IndexType> getKeyIndexes();
}
