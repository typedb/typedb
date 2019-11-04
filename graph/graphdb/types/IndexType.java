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

package grakn.core.graph.graphdb.types;

import grakn.core.graph.core.PropertyKey;
import grakn.core.graph.core.schema.JanusGraphSchemaType;
import grakn.core.graph.graphdb.database.management.ManagementSystem;
import grakn.core.graph.graphdb.internal.ElementCategory;


public interface IndexType {

    ElementCategory getElement();

    IndexField[] getFieldKeys();

    IndexField getField(PropertyKey key);

    boolean indexesKey(PropertyKey key);

    boolean isCompositeIndex();

    boolean isMixedIndex();

    boolean hasSchemaTypeConstraint();

    JanusGraphSchemaType getSchemaTypeConstraint();

    String getBackingIndexName();

    String getName();

    /**
     * Resets the internal caches used to speed up lookups on this index.
     * This is needed when the index gets modified in {@link ManagementSystem}.
     */
    void resetCache();

    //TODO: Add in the future
    //public And getCondition();
}
