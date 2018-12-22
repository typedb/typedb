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

package grakn.core.graql.internal.executor.property;

import com.google.common.collect.ImmutableSet;
import grakn.core.graql.concept.Role;
import grakn.core.graql.concept.Type;
import grakn.core.graql.internal.executor.WriteExecutor;
import grakn.core.graql.internal.gremlin.EquivalentFragmentSet;
import grakn.core.graql.internal.gremlin.sets.EquivalentFragmentSets;
import grakn.core.graql.query.pattern.Variable;
import grakn.core.graql.query.pattern.property.PlaysProperty;
import grakn.core.graql.query.pattern.property.VarProperty;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class PlaysExecutor implements PropertyExecutor.Definable, PropertyExecutor.Matchable {

    private final Variable var;
    private final PlaysProperty property;

    public PlaysExecutor(Variable var, PlaysProperty property) {
        this.var = var;
        this.property = property;
    }

    @Override
    public Set<PropertyExecutor.Writer> defineExecutors() {
        return ImmutableSet.of(new DefinePlays());
    }

    @Override
    public Set<PropertyExecutor.Writer> undefineExecutors() {
        return ImmutableSet.of(new UndefinePlays());
    }

    @Override
    public Set<EquivalentFragmentSet> matchFragments() {
        return ImmutableSet.of(
                EquivalentFragmentSets.plays(property, var, property.role().var(), property.isRequired())
        );
    }

    private abstract class PlaysWriter {

        public Variable var() {
            return var;
        }

        public VarProperty property() {
            return property;
        }

        public Set<Variable> requiredVars() {
            Set<Variable> required = new HashSet<>();
            required.add(var);
            required.add(property.role().var());

            return Collections.unmodifiableSet(required);
        }

        public Set<Variable> producedVars() {
            return ImmutableSet.of();
        }
    }

    private class DefinePlays extends PlaysWriter implements PropertyExecutor.Writer {

        @Override
        public void execute(WriteExecutor executor) {
            Role role = executor.getConcept(property.role().var()).asRole();
            executor.getConcept(var).asType().plays(role);
        }
    }

    private class UndefinePlays extends PlaysWriter implements PropertyExecutor.Writer {

        @Override
        public void execute(WriteExecutor executor) {
            Type type = executor.getConcept(var).asType();
            Role role = executor.getConcept(property.role().var()).asRole();

            if (!type.isDeleted() && !role.isDeleted()) {
                type.unplay(role);
            }
        }
    }
}
