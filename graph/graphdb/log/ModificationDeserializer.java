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

package grakn.core.graph.graphdb.log;

import grakn.core.graph.core.EdgeLabel;
import grakn.core.graph.core.PropertyKey;
import grakn.core.graph.core.log.Change;
import grakn.core.graph.diskstorage.Entry;
import grakn.core.graph.graphdb.database.log.TransactionLogHeader;
import grakn.core.graph.graphdb.internal.ElementLifeCycle;
import grakn.core.graph.graphdb.internal.InternalRelation;
import grakn.core.graph.graphdb.internal.InternalRelationType;
import grakn.core.graph.graphdb.internal.InternalVertex;
import grakn.core.graph.graphdb.relations.CacheEdge;
import grakn.core.graph.graphdb.relations.CacheVertexProperty;
import grakn.core.graph.graphdb.relations.RelationCache;
import grakn.core.graph.graphdb.relations.StandardEdge;
import grakn.core.graph.graphdb.relations.StandardVertexProperty;
import grakn.core.graph.graphdb.transaction.StandardJanusGraphTx;

public class ModificationDeserializer {

    public static InternalRelation parseRelation(TransactionLogHeader.Modification modification, StandardJanusGraphTx tx) {
        Change state = modification.state;
        long outVertexId = modification.outVertexId;
        Entry relEntry = modification.relationEntry;
        InternalVertex outVertex = tx.getInternalVertex(outVertexId);
        //Special relation parsing, compare to {@link RelationConstructor}
        RelationCache relCache = tx.getEdgeSerializer().readRelation(relEntry, false, tx);
        InternalRelationType type = (InternalRelationType) tx.getExistingRelationType(relCache.typeId);
        InternalRelation rel;
        if (type.isPropertyKey()) {
            if (state == Change.REMOVED) {
                rel = new StandardVertexProperty(relCache.relationId, (PropertyKey) type, outVertex, relCache.getValue(), ElementLifeCycle.Removed);
            } else {
                rel = new CacheVertexProperty(relCache.relationId, (PropertyKey) type, outVertex, relCache.getValue(), relEntry);
            }
        } else {
            InternalVertex otherVertex = tx.getInternalVertex(relCache.getOtherVertexId());
            if (state == Change.REMOVED) {
                rel = new StandardEdge(relCache.relationId, (EdgeLabel) type, outVertex, otherVertex, ElementLifeCycle.Removed);
            } else {
                rel = new CacheEdge(relCache.relationId, (EdgeLabel) type, outVertex, otherVertex, relEntry);
            }
        }
        if (state == Change.REMOVED && relCache.hasProperties()) { //copy over properties
            relCache.properties().entrySet().forEach((entry -> {
                rel.setPropertyDirect(tx.getExistingPropertyKey(entry.getKey()), entry.getValue());
            }));
        }
        return rel;
    }

}
