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
import grakn.core.graph.core.JanusGraphElement;
import grakn.core.graph.core.JanusGraphProperty;
import grakn.core.graph.core.PropertyKey;
import grakn.core.graph.graphdb.internal.InternalRelation;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.NoSuchElementException;


public class SimpleJanusGraphProperty<V> implements JanusGraphProperty<V> {

    private final PropertyKey key;
    private final V value;
    private final InternalRelation relation;

    public SimpleJanusGraphProperty(InternalRelation relation, PropertyKey key, V value) {
        this.key = key;
        this.value = value;
        this.relation = relation;
    }

    @Override
    public PropertyKey propertyKey() {
        return key;
    }

    @Override
    public V value() throws NoSuchElementException {
        return value;
    }

    @Override
    public boolean isPresent() {
        return true;
    }

    @Override
    public JanusGraphElement element() {
        return relation;
    }

    @Override
    public void remove() {
        Preconditions.checkArgument(!relation.isRemoved(), "Cannot modified removed relation");
        relation.it().removePropertyDirect(key);
    }

    @Override
    public String toString() {
        return StringFactory.propertyString(this);
    }

    @Override
    public int hashCode() {
        return ElementHelper.hashCode(this);
    }

    @Override
    public boolean equals(Object oth) {
        return ElementHelper.areEqual(this, oth);
    }

}
