/*
 * Copyright (C) 2021 Vaticle
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

package com.vaticle.typedb.core.pattern.equivalence;

import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.iterator.Iterators;
import com.vaticle.typedb.core.pattern.variable.Variable;
import com.vaticle.typedb.core.traversal.common.Identifier.Variable.Retrievable;

import javax.annotation.Nullable;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.vaticle.typedb.common.collection.Collections.set;

public class AlphaEquivalence {

    private final Map<Variable, Variable> map;
    private final Map<Variable, Variable> reverseMap;

    private AlphaEquivalence(Map<Variable, Variable> map, Map<Variable, Variable> reverseMap) {
        assert map.size() == reverseMap.size();
        assert map.keySet().equals(set(reverseMap.values()));
        this.map = map;
        this.reverseMap = reverseMap;
    }

    public static AlphaEquivalence empty() {
        return new AlphaEquivalence(new HashMap<>(), new HashMap<>());
    }

    public FunctionalIterator<AlphaEquivalence> alphaEqualIf(boolean condition) {
        if (condition) return Iterators.single(this);
        else return Iterators.empty();
    }

    public FunctionalIterator<AlphaEquivalence> extendIfCompatible(AlphaEquivalence alphaMap) {
        Optional<Map<Variable, Variable>> m = mergeVariableMapping(variableMapping(), alphaMap.variableMapping());
        Optional<Map<Variable, Variable>> r = mergeVariableMapping(reverseVariableMapping(), alphaMap.reverseVariableMapping());
        if (m.isPresent() && r.isPresent()) {
            if (m.get().size() != r.get().size()) return Iterators.empty();
            return Iterators.single(new AlphaEquivalence(m.get(), r.get()));
        } else {
            return Iterators.empty();
        }
    }

    private static Optional<Map<Variable, Variable>> mergeVariableMapping(Map<Variable, Variable> existing,
                                                                          Map<Variable, Variable> toMerge) {
        for (Map.Entry<Variable, Variable> e : toMerge.entrySet()) {
            Variable var = toMerge.get(e.getKey());
            if (existing.containsKey(e.getKey())) {
                if (!var.equals(e.getValue())) return Optional.empty();
            } else {
                existing.put(e.getKey(), var);
            }
        }
        return Optional.of(existing);
    }

    public AlphaEquivalence extend(Variable from, Variable to) {
        assert from.reference().isName() == to.reference().isName();
        Map<Variable, Variable> newMap = variableMapping();
        newMap.put(from, to);
        Map<Variable, Variable> reverseMap = reverseVariableMapping();
        reverseMap.put(to, from);
        return new AlphaEquivalence(newMap, reverseMap);
    }

    public static  <T extends AlphaEquivalent<T>> FunctionalIterator<AlphaEquivalence> alphaEquals(@Nullable T member1,
                                                                                                   @Nullable T member2) {
        if (member1 == null && member2 == null) return Iterators.single(empty());
        if (member1 == null || member2 == null) return Iterators.empty();
        else return member1.alphaEquals(member2);
    }

    public Map<Variable, Variable> variableMapping() {
        return new HashMap<>(map);
    }

    private Map<Variable, Variable> reverseVariableMapping() {
        return new HashMap<>(reverseMap);
    }

    public Map<Retrievable, Retrievable> retrievableMapping() {
        return variableMapping().entrySet().stream()
                .map(e -> new AbstractMap.SimpleEntry<>(
                        e.getKey().id(),
                        e.getValue().id()))
                .filter(e -> {
                    assert e.getKey().isRetrievable() == e.getValue().isRetrievable();
                    return e.getKey().isRetrievable();
                })
                .collect(Collectors.toMap(
                        e -> e.getKey().asRetrievable(),
                        e -> e.getValue().asRetrievable()
                ));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AlphaEquivalence that = (AlphaEquivalence) o;
        return map.equals(that.map) && reverseMap.equals(that.reverseMap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(map, reverseMap);
    }
}
