/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.graph.internal.cache;


import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Label;
import ai.grakn.concept.LabelId;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.UnaryOperator;

/**
 * <p>
 *     Contains the functionality needed to add an {@link Object} to {@link Cache}
 * </p>
 *
 * <p>
 *     This is used to wrap functionality needed by {@link Cache}.
 *     Specifically it is used to ensure that when flushing {@link Cache#valueGlobal} into {@link Cache#valueTx}
 *     no cache leaks can occur. For example this is needed when caching {@link java.util.Collection}
 * </p>
 *
 * @author fppt
 *
 * @param <V>
 */
public class Cacheable<V> {
    private final UnaryOperator<V> copier;

    private Cacheable(UnaryOperator<V> copier){
        this.copier = copier;
    }

    // Constructors for supported cache-able items
    public static Cacheable<ConceptId> conceptId(){
        return new Cacheable<>((o) -> o);
    }

    public static Cacheable<Label> label(){
        return new Cacheable<>((o) -> o);
    }

    public static Cacheable<LabelId> labelId(){
        return new Cacheable<>((o) -> o);
    }

    public static Cacheable<Boolean> bool(){
        return new Cacheable<>((o) -> o);
    }

    public static <T extends Concept> Cacheable<T> concept(){
        return new Cacheable<>((o) -> o);
    }

    public static <T> Cacheable<Set<T>> set(){
        return new Cacheable<>(HashSet::new);
    }

    public static <K, T> Cacheable<Map<K, T>> map(){
        return new Cacheable<>(HashMap::new);
    }

    /**
     * Copies the old value into a new value. How this copying is done is dictated by {@link Cacheable#copier}
     *
     * @param oldValue The old value
     * @return the new value as defined by {@link Cacheable#copier}
     */
    public V copy(V oldValue){
        return copier.apply(oldValue);
    }
}
