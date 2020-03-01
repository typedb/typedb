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
            if (!(type instanceof ImplicitKey)) {
                to.setPropertyDirect(type, entry.getValue());
            }
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
