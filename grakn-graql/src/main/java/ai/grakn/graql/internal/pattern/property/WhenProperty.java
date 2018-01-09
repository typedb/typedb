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

import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.Var;
import com.google.auto.value.AutoValue;

/**
 * Represents the {@code when} property on a {@link ai.grakn.concept.Rule}.
 *
 * This property can be inserted and not queried.
 *
 * The when side describes the left-hand of an implication, stating that when the when side of a rule is true
 * the then side must hold.
 *
 * @author Felix Chapman
 */
@AutoValue
public abstract class WhenProperty extends RuleProperty {

    public static final String NAME = "when";

    public static WhenProperty of(Pattern pattern) {
        return new AutoValue_WhenProperty(pattern);
    }

    @Override
    public String getName(){
        return NAME;
    }

    @Override
    public PropertyExecutor define(Var var) throws GraqlQueryException {
        PropertyExecutor.Method method = executor -> {
            executor.builder(var).when(pattern());
        };

        return PropertyExecutor.builder(method).produces(var).build();
    }
}
