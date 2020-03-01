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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import grakn.core.graph.core.PropertyKey;
import grakn.core.graph.graphdb.internal.ElementLifeCycle;
import grakn.core.graph.graphdb.internal.InternalVertex;
import grakn.core.graph.graphdb.types.system.ImplicitKey;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


public class StandardVertexProperty extends AbstractVertexProperty implements StandardRelation, ReassignableRelation {

    public StandardVertexProperty(long id, PropertyKey type, InternalVertex vertex, Object value, byte lifecycle) {
        super(id, type, vertex, value);
        this.lifecycle = lifecycle;
    }

    //############## SAME CODE AS StandardEdge #############################

    private static final Map<PropertyKey, Object> EMPTY_PROPERTIES = ImmutableMap.of();

    private byte lifecycle;
    private long previousID = 0;
    private volatile Map<PropertyKey, Object> properties = EMPTY_PROPERTIES;

    @Override
    public long getPreviousID() {
        return previousID;
    }

    @Override
    public void setPreviousID(long previousID) {
        Preconditions.checkArgument(previousID > 0);
        Preconditions.checkArgument(this.previousID == 0);
        this.previousID = previousID;
    }

    @Override
    public <O> O getValueDirect(PropertyKey key) {
        return (O) properties.get(key);
    }

    @Override
    public void setPropertyDirect(PropertyKey key, Object value) {
        Preconditions.checkArgument(!(key instanceof ImplicitKey), "Cannot use implicit type [%s] when setting property", key.name());
        if (properties == EMPTY_PROPERTIES) {
            if (tx().getConfiguration().isSingleThreaded()) {
                properties = new HashMap<>(5);
            } else {
                synchronized (this) {
                    if (properties == EMPTY_PROPERTIES) {
                        properties = Collections.synchronizedMap(new HashMap<PropertyKey, Object>(5));
                    }
                }
            }
        }
        properties.put(key, value);
    }

    @Override
    public Iterable<PropertyKey> getPropertyKeysDirect() {
        return Lists.newArrayList(properties.keySet());
    }

    @Override
    public <O> O removePropertyDirect(PropertyKey key) {
        if (!properties.isEmpty()) {
            return (O) properties.remove(key);
        } else {
            return null;
        }
    }

    @Override
    public byte getLifeCycle() {
        return lifecycle;
    }

    @Override
    public synchronized void remove() {
        if (!ElementLifeCycle.isRemoved(lifecycle)) {
            tx().removeRelation(this);
            lifecycle = ElementLifeCycle.update(lifecycle, ElementLifeCycle.Event.REMOVED);
        } //else throw InvalidElementException.removedException(this);
    }

}
