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
 */

package io.mindmaps.graql.internal.query;

import io.mindmaps.graql.admin.Conjunction;
import io.mindmaps.graql.admin.Disjunction;
import io.mindmaps.graql.admin.PatternAdmin;
import io.mindmaps.graql.admin.VarAdmin;

import java.util.Collection;
import java.util.Set;

public class Patterns {

    private Patterns() {}

    public static <T extends PatternAdmin> Conjunction<T> conjunction(Set<T> patterns) {
        return new ConjunctionImpl<>(patterns);
    }

    public static <T extends PatternAdmin> Disjunction<T> disjunction(Set<T> patterns) {
        return new DisjunctionImpl<>(patterns);
    }

    public static VarAdmin var() {
        return new VarImpl();
    }

    public static VarAdmin var(String name) {
        return new VarImpl(name);
    }

    public static VarAdmin mergeVars(Collection<VarAdmin> vars) {
        return new VarImpl(vars);
    }
}
