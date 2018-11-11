/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
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


package grakn.core.graql.internal.reasoner.utils;

import java.util.AbstractMap;

/**
 *
 * <p>
 * Convenience pair class wrapping a SimpleImmutableEntry object.
 * </p>
 *
 * @param <K> key type
 * @param <V> value type
 *
 * @author Kasper Piskorski
 *
 */
public class Pair<K, V> {

    private final AbstractMap.SimpleImmutableEntry<K, V> data;

    public Pair(K key, V value){
        data = new AbstractMap.SimpleImmutableEntry<>(key, value);
    }

    @Override
    public int hashCode(){ return data.hashCode();}

    @Override
    public boolean equals(Object obj){
        if (obj == null || this.getClass() != obj.getClass()) return false;
        if (obj == this) return true;
        Pair p = (Pair) obj;
        return data.equals(p.data);
    }

    public K getKey(){ return data.getKey();}
    public V getValue(){ return data.getValue();}
}