/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 *
 */

package io.mindmaps.graql.internal.query;

import io.mindmaps.graql.admin.Conjunction;
import io.mindmaps.graql.admin.Disjunction;
import io.mindmaps.graql.admin.PatternAdmin;
import io.mindmaps.graql.admin.VarAdmin;

import java.util.Set;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toSet;

class DisjunctionImpl<T extends PatternAdmin> implements Disjunction<T> {

    private final Set<T> patterns;

    DisjunctionImpl(Set<T> patterns) {
        this.patterns = patterns;
    }

    @Override
    public Set<T> getPatterns() {
        return patterns;
    }

    @Override
    public Disjunction<Conjunction<VarAdmin>> getDisjunctiveNormalForm() {
        // Concatenate all disjunctions into one big disjunction
        Set<Conjunction<VarAdmin>> dnf = patterns.stream()
                .flatMap(p -> p.getDisjunctiveNormalForm().getPatterns().stream())
                .collect(toSet());

        return Patterns.disjunction(dnf);
    }

    @Override
    public boolean isDisjunction() {
        return true;
    }

    @Override
    public Disjunction<?> asDisjunction() {
        return this;
    }

    @Override
    public boolean equals(Object obj) {
        return (obj instanceof DisjunctionImpl) && patterns.equals(((DisjunctionImpl) obj).patterns);
    }

    @Override
    public int hashCode() {
        return patterns.hashCode();
    }

    @Override
    public String toString() {
        return patterns.stream().map(p -> "{" + p.toString() + "}").collect(Collectors.joining(" or "));
    }

    @Override
    public PatternAdmin admin() {
        return this;
    }
}
