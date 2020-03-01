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

package grakn.core.graql.executor.property;

import com.google.common.collect.Sets;
import grakn.core.graql.planning.gremlin.sets.EquivalentFragmentSets;
import grakn.core.kb.graql.executor.property.PropertyExecutor;
import grakn.core.kb.graql.planning.gremlin.EquivalentFragmentSet;
import graql.lang.property.NeqProperty;
import graql.lang.statement.Variable;

import java.util.Set;

public class NeqExecutor  implements PropertyExecutor {

    private final Variable var;
    private final NeqProperty property;

    NeqExecutor(Variable var, NeqProperty property) {
        this.var = var;
        this.property = property;
    }

    @Override
    public Set<EquivalentFragmentSet> matchFragments() {
        return Sets.newHashSet(
                EquivalentFragmentSets.notInternalFragmentSet(property, var),
                EquivalentFragmentSets.notInternalFragmentSet(property, property.statement().var()),
                EquivalentFragmentSets.neq(property, var, property.statement().var())
        );
    }

}
