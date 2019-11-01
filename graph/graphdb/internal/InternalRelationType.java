// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package grakn.core.graph.graphdb.internal;

import org.apache.tinkerpop.gremlin.structure.Direction;
import grakn.core.graph.core.Multiplicity;
import grakn.core.graph.core.RelationType;
import grakn.core.graph.core.schema.ConsistencyModifier;
import grakn.core.graph.core.schema.SchemaStatus;
import grakn.core.graph.graphdb.internal.InternalVertex;
import grakn.core.graph.graphdb.internal.Order;
import grakn.core.graph.graphdb.types.IndexType;

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
