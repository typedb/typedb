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

package grakn.core.graql.query.pattern.property;

import grakn.core.graql.query.pattern.Pattern;
import grakn.core.graql.query.pattern.Variable;
import grakn.core.graql.admin.Atomic;
import grakn.core.graql.admin.ReasonerQuery;
import grakn.core.graql.query.pattern.Statement;
import grakn.core.graql.internal.gremlin.EquivalentFragmentSet;
import grakn.core.common.exception.ErrorMessage;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Set;

/**
 * Abstract property for the patterns within rules.
 *
 */
public abstract class RuleProperty extends VarProperty {

    public abstract Pattern pattern();

    @Override
    public String getProperty() {
        return pattern().toString();
    }

    @Override
    public boolean isUnique() {
        return true;
    }

    @Override
    public Collection<EquivalentFragmentSet> match(Variable start) {
        throw new UnsupportedOperationException(ErrorMessage.MATCH_INVALID.getMessage(this.getName()));
    }

    @Nullable
    @Override
    public Atomic mapToAtom(Statement var, Set<Statement> vars, ReasonerQuery parent) {
        return null;
    }
}
