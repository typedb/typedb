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

package io.mindmaps.graql.internal.util;

import io.mindmaps.graql.Pattern;
import io.mindmaps.graql.Var;
import io.mindmaps.graql.admin.PatternAdmin;
import io.mindmaps.graql.admin.VarAdmin;

import java.util.Collection;
import java.util.Set;

import static java.util.stream.Collectors.toSet;

/**
 * Helper methods for converting classes to admin equivalents
 */
public class AdminConverter {

    /**
     * @param patterns a collection of patterns to change to admin
     * @return a collection of Pattern.Admin from the given patterns
     */
    public static Set<PatternAdmin> getPatternAdmins(Collection<? extends Pattern> patterns) {
        return patterns.stream().map(Pattern::admin).collect(toSet());
    }

    /**
     * @param patterns a collection of vars to change to admin
     * @return a collection of Var.Admin from the given patterns
     */
    public static Set<VarAdmin> getVarAdmins(Collection<? extends Var> patterns) {
        return patterns.stream().map(Var::admin).collect(toSet());
    }
}
