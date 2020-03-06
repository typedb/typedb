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

import com.google.common.collect.Iterables;
import grakn.core.graph.core.PropertyKey;
import grakn.core.graph.core.schema.ConsistencyModifier;
import grakn.core.graph.diskstorage.Entry;
import grakn.core.graph.graphdb.internal.ElementLifeCycle;
import grakn.core.graph.graphdb.internal.InternalRelation;
import grakn.core.graph.graphdb.internal.InternalVertex;
import grakn.core.graph.graphdb.transaction.RelationConstructor;
import grakn.core.graph.graphdb.types.system.ImplicitKey;

import java.util.Map;
import java.util.stream.Collectors;


public class CacheVertexProperty extends AbstractVertexProperty {

    public CacheVertexProperty(long id, PropertyKey key, InternalVertex start, Object value, Entry data) {
        super(id, key, start.it(), value);
        this.data = data;
    }

    //############## Similar code as CacheEdge but be careful when copying #############################

    private final Entry data;

    @Override
    public InternalRelation it() {
        InternalRelation it = null;
        InternalVertex startVertex = getVertex(0);

        if (startVertex.hasAddedRelations() && startVertex.hasRemovedRelations()) {
            //Test whether this relation has been replaced
            final long id = longId();
            it = Iterables.getOnlyElement(startVertex.getAddedRelations(
                    internalRelation -> (internalRelation instanceof StandardVertexProperty) && ((StandardVertexProperty) internalRelation).getPreviousID() == id), null);
        }

        return (it != null) ? it : super.it();
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
        StandardVertexProperty copy = new StandardVertexProperty(longId(), propertyKey(), getVertex(0), value(), ElementLifeCycle.Loaded);
        copyProperties(copy);
        copy.remove();

        StandardVertexProperty u = (StandardVertexProperty) tx().addProperty(getVertex(0), propertyKey(), value());
        if (type.getConsistencyModifier() != ConsistencyModifier.FORK) u.setId(longId());
        u.setPreviousID(longId());
        copyProperties(u);
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
        if ((getVertex(0).hasRemovedRelations() || getVertex(0).isRemoved()) && tx().isRemovedRelation(longId())) {
            return ElementLifeCycle.Removed;
        } else {
            return ElementLifeCycle.Loaded;
        }
    }

    @Override
    public void remove() {
        if (!tx().isRemovedRelation(longId())) {
            tx().removeRelation(this);
        }
    }


}
