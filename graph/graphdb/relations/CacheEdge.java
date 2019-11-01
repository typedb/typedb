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

package grakn.core.graph.graphdb.relations;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import grakn.core.graph.core.EdgeLabel;
import grakn.core.graph.core.PropertyKey;
import grakn.core.graph.core.schema.ConsistencyModifier;
import grakn.core.graph.diskstorage.Entry;
import grakn.core.graph.graphdb.internal.ElementLifeCycle;
import grakn.core.graph.graphdb.internal.InternalRelation;
import grakn.core.graph.graphdb.internal.InternalVertex;
import grakn.core.graph.graphdb.transaction.RelationConstructor;
import grakn.core.graph.graphdb.types.system.ImplicitKey;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.util.Map;
import java.util.stream.Collectors;


public class CacheEdge extends AbstractEdge {

    public CacheEdge(long id, EdgeLabel label, InternalVertex start, InternalVertex end, Entry data) {
        super(id, label, start.it(), end.it());
        this.data = data;
    }

    public Direction getVertexCentricDirection() {
        final RelationCache cache = data.getCache();
        Preconditions.checkNotNull(cache, "Cache is null");
        return cache.direction;
    }

    //############## Similar code as CacheProperty but be careful when copying #############################

    private final Entry data;

    @Override
    public InternalRelation it() {
        InternalRelation it = null;
        InternalVertex startVertex = getVertex(0);

        if (startVertex.hasAddedRelations() && startVertex.hasRemovedRelations()) {
            //Test whether this relation has been replaced
            final long id = super.longId();
            final Iterable<InternalRelation> previous = startVertex.getAddedRelations(
                    internalRelation -> (internalRelation instanceof StandardEdge) && ((StandardEdge) internalRelation).getPreviousID() == id);
            it = Iterables.getFirst(previous, null);
        }

        if (it != null) return it;

        return super.it();
    }

    private void copyProperties(InternalRelation to) {
        for (Map.Entry<Long, Object> entry : getPropertyMap().properties().entrySet()) {
            PropertyKey type = tx().getExistingPropertyKey(entry.getKey());
            if (!(type instanceof ImplicitKey))
                to.setPropertyDirect(type, entry.getValue());
        }
    }

    private synchronized InternalRelation update() {
        StandardEdge copy = new StandardEdge(super.longId(), edgeLabel(), getVertex(0), getVertex(1), ElementLifeCycle.Loaded);
        copyProperties(copy);
        copy.remove();

        StandardEdge u = (StandardEdge) tx().addEdge(getVertex(0), getVertex(1), edgeLabel());
        if (type.getConsistencyModifier() != ConsistencyModifier.FORK) u.setId(super.longId());
        u.setPreviousID(super.longId());
        copyProperties(u);
        setId(u.longId());
        return u;
    }

    private RelationCache getPropertyMap() {
        RelationCache map = data.getCache();
        if (map == null || !map.hasProperties()) {
            map = RelationConstructor.readRelationCache(data, tx());
        }
        return map;
    }

    @Override
    public <O> O getValueDirect(PropertyKey key) {
        return getPropertyMap().get(key.longId());
    }

    @Override
    public Iterable<PropertyKey> getPropertyKeysDirect() {
        return getPropertyMap().properties().keySet().stream()
                .map(key -> tx().getExistingPropertyKey(key))
                .collect(Collectors.toList());
    }

    @Override
    public void setPropertyDirect(PropertyKey key, Object value) {
        update().setPropertyDirect(key, value);
    }

    @Override
    public <O> O removePropertyDirect(PropertyKey key) {
        return update().removePropertyDirect(key);
    }

    @Override
    public byte getLifeCycle() {
        InternalVertex startVertex = getVertex(0);
        return ((startVertex.hasRemovedRelations() || startVertex.isRemoved()) && tx().isRemovedRelation(super.longId()))
                ? ElementLifeCycle.Removed : ElementLifeCycle.Loaded;
    }

    @Override
    public void remove() {
        if (!tx().isRemovedRelation(super.longId())) {
            tx().removeRelation(this);
        }
    }

}
