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

package grakn.core.graql.internal.util;

import grakn.core.graql.query.pattern.Pattern;
import grakn.core.graql.query.pattern.PatternAdmin;
import grakn.core.graql.query.pattern.VarPattern;

import java.util.Collection;

import static java.util.stream.Collectors.toList;

/**
 * Helper methods for converting classes to admin equivalents
 *
 */
public class AdminConverter {

    /**
     * @param patterns a collection of patterns to change to admin
     * @return a collection of Pattern.Admin from the given patterns
     */
    public static Collection<PatternAdmin> getPatternAdmins(Collection<? extends Pattern> patterns) {
        return patterns.stream().map(Pattern::admin).collect(toList());
    }

    /**
     * @param patterns a collection of vars to change to admin
     * @return a collection of {@link VarPattern} from the given patterns
     */
    public static Collection<VarPattern> getVarAdmins(Collection<? extends VarPattern> patterns) {
        return patterns.stream().map(VarPattern::admin).collect(toList());
    }
}
