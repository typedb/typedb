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

package grakn.core.server.kb.cache;


import grakn.core.concept.Concept;
import grakn.core.concept.ConceptId;
import grakn.core.concept.Label;
import grakn.core.concept.LabelId;
import grakn.core.server.kb.structure.Shard;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.UnaryOperator;

/**
 * Contains the functionality needed to add an Object to Cache
 * This is used to wrap functionality needed by Cache.
 * Specifically it is used to ensure that when flushing Cache#valueGlobal into Cache#valueTx
 * no cache leaks can occur. For example this is needed when caching java.util.Collection
 *
 * @param <V>
 */
public class Cacheable<V> {
    private final UnaryOperator<V> copier;

    private Cacheable(UnaryOperator<V> copier) {
        this.copier = copier;
    }

    // Constructors for supported cache-able items
    public static Cacheable<ConceptId> conceptId() {
        return new Cacheable<>((o) -> o);
    }

    public static Cacheable<Long> number() {
        return new Cacheable<>((o) -> o);
    }

    public static Cacheable<Label> label() {
        return new Cacheable<>((o) -> o);
    }

    public static Cacheable<LabelId> labelId() {
        return new Cacheable<>((o) -> o);
    }

    public static Cacheable<Boolean> bool() {
        return new Cacheable<>((o) -> o);
    }

    public static Cacheable<Shard> shard() {
        return new Cacheable<>((o) -> o);
    }

    public static <T extends Concept> Cacheable<T> concept() {
        return new Cacheable<>((o) -> o);
    }

    public static <T> Cacheable<Set<T>> set() {
        return new Cacheable<>(HashSet::new);
    }

    public static <K, T> Cacheable<Map<K, T>> map() {
        return new Cacheable<>(HashMap::new);
    }

    /**
     * Copies the old value into a new value. How this copying is done is dictated by Cacheable#copier
     *
     * @param oldValue The old value
     * @return the new value as defined by Cacheable#copier
     */
    public V copy(V oldValue) {
        return copier.apply(oldValue);
    }
}
