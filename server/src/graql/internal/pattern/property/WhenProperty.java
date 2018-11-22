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

package grakn.core.graql.internal.pattern.property;

import grakn.core.graql.concept.Rule;
import grakn.core.graql.exception.GraqlQueryException;
import grakn.core.graql.query.Pattern;
import grakn.core.graql.query.Var;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;

/**
 * Represents the {@code when} property on a {@link Rule}.
 *
 * This property can be inserted and not queried.
 *
 * The when side describes the left-hand of an implication, stating that when the when side of a rule is true
 * the then side must hold.
 *
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
    public Collection<PropertyExecutor> define(Var var) throws GraqlQueryException {
        PropertyExecutor.Method method = executor -> {
            // This allows users to skip stating `$ruleVar sub rule` when they say `$ruleVar when { ... }`
            executor.builder(var).isRule().when(pattern());
        };

        return ImmutableSet.of(PropertyExecutor.builder(method).produces(var).build());
    }
}
