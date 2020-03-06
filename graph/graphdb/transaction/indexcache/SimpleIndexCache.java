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

package grakn.core.graph.graphdb.transaction.indexcache;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import grakn.core.graph.core.JanusGraphVertexProperty;
import grakn.core.graph.core.PropertyKey;


public class SimpleIndexCache implements IndexCache {

    private final HashMultimap<Object, JanusGraphVertexProperty> map;

    public SimpleIndexCache() {
        this.map = HashMultimap.create();
    }

    @Override
    public void add(JanusGraphVertexProperty property) {
        map.put(property.value(),property);
    }

    @Override
    public void remove(JanusGraphVertexProperty property) {
        map.remove(property.value(),property);
    }

    @Override
    public Iterable<JanusGraphVertexProperty> get(Object value, PropertyKey key) {
        return Iterables.filter(map.get(value), janusgraphProperty -> janusgraphProperty.propertyKey().equals(key));
    }
}
