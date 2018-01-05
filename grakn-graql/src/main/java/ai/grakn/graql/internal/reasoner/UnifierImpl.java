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

package ai.grakn.graql.internal.reasoner;

import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Unifier;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.ImmutableSetMultimap.Builder;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import ai.grakn.graql.internal.reasoner.utils.Pair;

/**
 *
 * <p>
 * Implementation of the {@link Unifier} interface.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public class UnifierImpl implements Unifier {
    
    private final ImmutableSetMultimap<Var, Var> unifier;

    /**
     * Identity unifier.
     */
    public UnifierImpl(){
        this.unifier = ImmutableSetMultimap.of();
    }
    public UnifierImpl(Collection<Map.Entry<Var, Var>> mappings){
        Builder<Var, Var> builder = ImmutableSetMultimap.builder();
        mappings.forEach(entry -> builder.put(entry.getKey(), entry.getValue()));
        this.unifier =  builder.build();
    }
    public UnifierImpl(ImmutableMultimap<Var, Var> map){ this(map.entries());}
    public UnifierImpl(Multimap<Var, Var> map){ this(map.entries());}
    public UnifierImpl(Map<Var, Var> map){ this(map.entrySet());}
    public UnifierImpl(Unifier u){ this(u.mappings());}

    @Override
    public String toString(){
        return unifier.toString();
    }

    @Override
    public boolean equals(Object obj){
        if (obj == null || this.getClass() != obj.getClass()) return false;
        if (obj == this) return true;
        UnifierImpl u2 = (UnifierImpl) obj;
        return this.unifier.equals(u2.unifier);
    }

    @Override
    public int hashCode(){
        return unifier.hashCode();
    }

    @Override
    public boolean isEmpty() {
        return unifier.isEmpty();
    }

    @Override
    public Set<Var> keySet() {
        return unifier.keySet();
    }

    @Override
    public Collection<Var> values() {
        return unifier.values();
    }

    @Override
    public ImmutableSet<Map.Entry<Var, Var>> mappings(){ return unifier.entries();}

    @Override
    public Collection<Var> get(Var key) {
        return unifier.get(key);
    }

    @Override
    public boolean containsKey(Var key) {
        return unifier.containsKey(key);
    }

    @Override
    public boolean containsValue(Var value) {
        return unifier.containsValue(value);
    }

    @Override
    public boolean containsAll(Unifier u) { return mappings().containsAll(u.mappings());}

    @Override
    public Unifier merge(Unifier d) {
        return new UnifierImpl(
                Sets.union(
                        this.mappings(),
                        d.mappings())
        );
    }

    @Override
    public Unifier combine(Unifier d) {
        if (Collections.disjoint(this.values(), d.keySet())){
            return new UnifierImpl(this).merge(d);
        }
        Multimap<Var, Var> mergedMappings = HashMultimap.create();
        Unifier inverse = this.inverse();
        this.mappings().stream().filter(m -> !d.containsKey(m.getValue())).forEach(m -> mergedMappings.put(m.getKey(), m.getValue()));
        d.mappings().stream()
                .flatMap(m -> {
                    Var lVar = m.getKey();
                    if (inverse.containsKey(lVar)){
                        return inverse.get(lVar).stream()
                                .map(v -> new Pair<>(v, m.getValue()));
                    } else {
                        return Stream.of(new Pair<>(m.getKey(), m.getValue()));
                    }
                })
                .forEach(m -> mergedMappings.put(m.getKey(), m.getValue()));
        return new UnifierImpl(mergedMappings);
    }

    @Override
    public Unifier inverse() {
        return new UnifierImpl(
                unifier.entries().stream()
                        .map(e -> new AbstractMap.SimpleImmutableEntry<>(e.getValue(), e.getKey()))
                        .collect(Collectors.toSet())
        );
    }

    @Override
    public int size(){ return unifier.size();}
}
