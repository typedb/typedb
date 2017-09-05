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
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
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
 * Implementation of {@link Unifier} interface.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public class UnifierImpl implements Unifier {
    
    private final Multimap<Var, Var> unifier = ArrayListMultimap.create();

    /**
     * Identity unifier.
     */
    public UnifierImpl(){}
    public UnifierImpl(Map<Var, Var> map){
        map.entrySet().forEach(m -> unifier.put(m.getKey(), m.getValue()));
    }
    public UnifierImpl(Unifier u){
        merge(u);
    }

    @Override
    public String toString(){
        return unifier.toString();
    }

    @Override
    public boolean equals(Object obj){
        if (obj == null || this.getClass() != obj.getClass()) return false;
        if (obj == this) return true;
        UnifierImpl u2 = (UnifierImpl) obj;
        return this.map().equals(u2.map());
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
    public Map<Var, Collection<Var>> map() {
        return unifier.asMap();
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
    public Collection<Map.Entry<Var, Var>> mappings(){ return unifier.entries();}

    @Override
    public boolean addMapping(Var key, Var value){
        return unifier.put(key, value);
    }

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
        d.mappings().forEach(m -> unifier.put(m.getKey(), m.getValue()));
        return this;
    }

    @Override
    public Unifier combine(Unifier d) {
        if (Collections.disjoint(this.values(), d.keySet())){
            return new UnifierImpl(this).merge(d);
        }
        Unifier merged = new UnifierImpl();
        Unifier inverse = this.inverse();
        this.mappings().stream().filter(m -> !d.containsKey(m.getValue())).forEach(m -> merged.addMapping(m.getKey(), m.getValue()));
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
                .forEach(m -> merged.addMapping(m.getKey(), m.getValue()));
        return merged;
    }

    @Override
    public Unifier removeTrivialMappings() {
        Set<Var> toRemove = unifier.entries().stream()
                .filter(e -> e.getKey() == e.getValue())
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());
        toRemove.forEach(unifier::removeAll);
        return this;
    }

    @Override
    public Unifier inverse() {
        Unifier inverse = new UnifierImpl();
        unifier.entries().forEach(e -> inverse.addMapping(e.getValue(), e.getKey()));
        return inverse;
    }

    @Override
    public int size(){ return unifier.size();}
}
