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
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.graql.internal.pattern.property;

/*-
 * #%L
 * grakn-graql
 * %%
 * Copyright (C) 2016 - 2018 Grakn Labs Ltd
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */

import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.Var;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;


/**
 * Represents the {@code then} (right-hand side) property on a {@link ai.grakn.concept.Rule}.
 *
 * This property can be inserted and not queried.
 *
 * The then side describes the right-hand of an implication, stating that when the when side of a rule is
 * true the then side must hold.
 *
 * @author Felix Chapman
 */
@AutoValue
public abstract class ThenProperty extends RuleProperty {

    public static final String NAME = "then";

    public static ThenProperty of(Pattern then) {
        return new AutoValue_ThenProperty(then);
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public Collection<PropertyExecutor> define(Var var) throws GraqlQueryException {
        PropertyExecutor.Method method = executor -> {
            // This allows users to skip stating `$ruleVar sub rule` when they say `$ruleVar then { ... }`
            executor.builder(var).isRule().then(pattern());
        };

        return ImmutableSet.of(PropertyExecutor.builder(method).produces(var).build());
    }
}
