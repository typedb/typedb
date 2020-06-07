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
 *
 */

package grakn.core.graql.reasoner.unifier;

import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.kb.concept.api.Concept;
import grakn.core.kb.graql.reasoner.unifier.Unifier;
import graql.lang.statement.Variable;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 *
 * <p>
 * Implementation of the Unifier interface.
 * </p>
 *
 *
 */
public class UnifierImpl implements Unifier {
    
    private final ImmutableSetMultimap<Variable, Variable> unifier;

    /**
     * Identity unifier.
     */
    public UnifierImpl(){
        this.unifier = ImmutableSetMultimap.of();
    }
    public UnifierImpl(ImmutableMultimap<Variable, Variable> map){ this(map.entries());}
    public UnifierImpl(Multimap<Variable, Variable> map){ this(map.entries());}
    public UnifierImpl(Map<Variable, Variable> map){ this(map.entrySet());}
    private UnifierImpl(Collection<Map.Entry<Variable, Variable>> mappings){
        this.unifier = ImmutableSetMultimap.<Variable, Variable>builder().putAll(mappings).build();
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
    public boolean isNonInjective() {
        return unifier.inverse().asMap().values().stream().anyMatch(s -> s.size() > 1);
    }

    @Override
    public Set<Variable> keySet() {
        return unifier.keySet();
    }

    @Override
    public Collection<Variable> values() {
        return unifier.values();
    }

    @Override
    public ImmutableSet<Map.Entry<Variable, Variable>> mappings(){ return unifier.entries();}

    @Override
    public Collection<Variable> get(Variable key) {
        return unifier.get(key);
    }

    @Override
    public boolean containsKey(Variable key) {
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

    public ConceptMap apply(ConceptMap answer) {
        if (this.isEmpty()) return answer;
        Map<Variable, Concept> unified = new HashMap<>();

        for (Map.Entry<Variable, Concept> e : answer.map().entrySet()) {
            Variable var = e.getKey();
            Concept con = e.getValue();
            Collection<Variable> uvars = unifier.get(var);
            if (uvars.isEmpty() && !unifier.values().contains(var)) {
                Concept put = unified.put(var, con);
                if (put != null && !put.equals(con)) return new ConceptMap();
            } else {
                for (Variable uv : uvars) {
                    Concept put = unified.put(uv, con);
                    if (put != null && !put.equals(con)) return new ConceptMap();
                }
            }
        }
        return new ConceptMap(unified, answer.explanation(), answer.getPattern());
    }
}
