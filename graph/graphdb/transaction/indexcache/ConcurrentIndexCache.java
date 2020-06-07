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
import grakn.core.graph.core.JanusGraphVertexProperty;
import grakn.core.graph.core.PropertyKey;

import java.util.ArrayList;
import java.util.List;


public class ConcurrentIndexCache implements IndexCache {

    private final HashMultimap<Object, JanusGraphVertexProperty> map;

    public ConcurrentIndexCache() {
        this.map = HashMultimap.create();
    }

    @Override
    public synchronized void add(JanusGraphVertexProperty property) {
        map.put(property.value(),property);
    }

    @Override
    public synchronized void remove(JanusGraphVertexProperty property) {
        map.remove(property.value(),property);
    }

    @Override
    public synchronized Iterable<JanusGraphVertexProperty> get(Object value, PropertyKey key) {
        final List<JanusGraphVertexProperty> result = new ArrayList<>(4);
        for (JanusGraphVertexProperty p : map.get(value)) {
            if (p.propertyKey().equals(key)) result.add(p);
        }
        return result;
    }
}
