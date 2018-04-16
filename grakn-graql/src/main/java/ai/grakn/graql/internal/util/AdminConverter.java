/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.graql.internal.util;

import ai.grakn.graql.Pattern;
import ai.grakn.graql.VarPattern;
import ai.grakn.graql.admin.PatternAdmin;
import ai.grakn.graql.admin.VarPatternAdmin;

import java.util.Collection;

import static java.util.stream.Collectors.toList;

/**
 * Helper methods for converting classes to admin equivalents
 *
 * @author Felix Chapman
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
     * @return a collection of {@link VarPatternAdmin} from the given patterns
     */
    public static Collection<VarPatternAdmin> getVarAdmins(Collection<? extends VarPattern> patterns) {
        return patterns.stream().map(VarPattern::admin).collect(toList());
    }
}
