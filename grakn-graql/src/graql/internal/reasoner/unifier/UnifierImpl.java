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

package grakn.core.graql.internal.reasoner.unifier;

import grakn.core.graql.Var;
import grakn.core.graql.admin.Unifier;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 *
 * <p>
 * Implementation of the {@link Unifier} interface.
 * </p>
 *
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
    public UnifierImpl(ImmutableMultimap<Var, Var> map){ this(map.entries());}
    public UnifierImpl(Multimap<Var, Var> map){ this(map.entries());}
    public UnifierImpl(Map<Var, Var> map){ this(map.entrySet());}
    private UnifierImpl(Collection<Map.Entry<Var, Var>> mappings){
        this.unifier = ImmutableSetMultimap.<Var, Var>builder().putAll(mappings).build();
    }

    public static UnifierImpl trivial(){return new UnifierImpl();}
    public static UnifierImpl nonExistent(){ return null;}

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
    public Unifier inverse() {
        return new UnifierImpl(
                unifier.entries().stream()
                        .map(e -> new AbstractMap.SimpleImmutableEntry<>(e.getValue(), e.getKey()))
                        .collect(Collectors.toSet())
        );
    }

}
