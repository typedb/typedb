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

import grakn.core.graph.core.InvalidElementException;
import grakn.core.graph.core.PropertyKey;
import grakn.core.graph.core.RelationType;
import grakn.core.graph.graphdb.internal.AbstractElement;
import grakn.core.graph.graphdb.internal.InternalRelation;
import grakn.core.graph.graphdb.internal.InternalRelationType;
import grakn.core.graph.graphdb.internal.InternalVertex;
import grakn.core.graph.graphdb.transaction.StandardJanusGraphTx;
import grakn.core.graph.graphdb.types.system.ImplicitKey;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Iterator;
import java.util.stream.Stream;

public abstract class AbstractTypedRelation extends AbstractElement implements InternalRelation {

    protected final InternalRelationType type;

    AbstractTypedRelation(long id, RelationType type) {
        super(id);
        this.type = (InternalRelationType) type;
    }

    @Override
    public InternalRelation it() {
        if (isLoadedInThisTx()) {
            return this;
        }

        InternalRelation next = (InternalRelation) RelationIdentifier.get(this).findRelation(tx());
        if (next == null) {
            throw InvalidElementException.removedException(this);
        }

        return next;
    }

    private boolean isLoadedInThisTx() {
        InternalVertex v = getVertex(0);
        return v == v.it();
    }

    @Override
    public final StandardJanusGraphTx tx() {
        return getVertex(0).tx();
    }

    /**
     * Cannot currently throw exception when removed since internal logic relies on access to the edge
     * beyond its removal. TODO: reconcile with access validation logic
     */
    private void verifyAccess() {
    }

    /* ---------------------------------------------------------------
     * Immutable Aspects of Relation
     * ---------------------------------------------------------------
     */

    @Override
    public Direction direction(Vertex vertex) {
        for (int i = 0; i < getArity(); i++) {
            if (it().getVertex(i).equals(vertex)) {
                return EdgeDirection.fromPosition(i);
            }
        }
        throw new IllegalArgumentException("Relation is not incident on vertex");
    }

    @Override
    public boolean isIncidentOn(Vertex vertex) {
        for (int i = 0; i < getArity(); i++) {
            if (it().getVertex(i).equals(vertex)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean isInvisible() {
        return type.isInvisibleType();
    }

    @Override
    public boolean isLoop() {
        return getArity() == 2 && getVertex(0).equals(getVertex(1));
    }

    @Override
    public RelationType getType() {
        return type;
    }

    @Override
    public RelationIdentifier id() {
        return RelationIdentifier.get(this);
    }

    /* ---------------------------------------------------------------
     * Mutable Aspects of Relation
     * ---------------------------------------------------------------
     */

    @Override
    public <V> Property<V> property(String key, V value) {
        verifyAccess();

        PropertyKey propertyKey = tx().getOrCreatePropertyKey(key, value);
        Object normalizedValue = tx().verifyAttribute(propertyKey, value);
        it().setPropertyDirect(propertyKey, normalizedValue);
        return new SimpleJanusGraphProperty<>(this, propertyKey, value);
    }

    @Override
    public <O> O valueOrNull(PropertyKey key) {
        verifyAccess();
        if (key instanceof ImplicitKey) {
            return ((ImplicitKey) key).computeProperty(this);
        }
        return it().getValueDirect(key);
    }

    @Override
    public <O> O value(String key) {
        verifyAccess();
        O val = valueInternal(tx().getPropertyKey(key));
        if (val == null) {
            throw Property.Exceptions.propertyDoesNotExist(this, key);
        }
        return val;
    }

    private <O> O valueInternal(PropertyKey type) {
        if (type == null) {
            return null;
        }
        return valueOrNull(type);
    }

    @Override
    public <V> Iterator<Property<V>> properties(String... keyNames) {
        verifyAccess();

        Stream<PropertyKey> keys;

        if (keyNames == null || keyNames.length == 0) {
            keys = IteratorUtils.stream(it().getPropertyKeysDirect().iterator());
        } else {
            keys = Stream.of(keyNames)
                    .map(s -> tx().getPropertyKey(s)).filter(rt -> rt != null && getValueDirect(rt) != null);
        }
        return keys.map(rt -> (Property<V>) new SimpleJanusGraphProperty<V>(this, rt, valueInternal(rt))).iterator();
    }
}
