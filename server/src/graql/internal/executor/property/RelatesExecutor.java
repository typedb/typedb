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

import grakn.core.graql.concept.RelationType;
import grakn.core.graql.concept.Role;
import grakn.core.graql.internal.executor.WriteExecutor;
import grakn.core.graql.query.pattern.Variable;
import grakn.core.graql.query.pattern.property.RelatesProperty;
import grakn.core.graql.query.pattern.property.VarProperty;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class RelatesExecutor implements PropertyExecutor.Definable {

    private final Variable var;
    private final RelatesProperty property;

    public RelatesExecutor(Variable var, RelatesProperty property) {
        this.var = var;
        this.property = property;
    }

    @Override
    public Set<PropertyExecutor.Writer> defineExecutors() {
        Set<PropertyExecutor.Writer> defineExecutors = new HashSet<>();
        defineExecutors.add(new DefineRelates());
        defineExecutors.add(new DefineRole());

        if (property.superRole() != null) {
            defineExecutors.add(new DefineSuperRole());
        }

        return Collections.unmodifiableSet(defineExecutors);
    }

    @Override
    public Set<PropertyExecutor.Writer> undefineExecutors() {
        return Collections.unmodifiableSet(Collections.singleton(new UndefineRelates()));
    }

    private abstract class RelatesWriter {

        public Variable var() {
            return var;
        }

        public VarProperty property() {
            return property;
        }
    }

    private class DefineRelates extends RelatesWriter implements PropertyExecutor.Writer {

        @Override
        public Set<Variable> requiredVars() {
            Set<Variable> required = new HashSet<>();
            required.add(var);
            required.add(property.role().var());

            return Collections.unmodifiableSet(required);
        }

        @Override
        public Set<Variable> producedVars() {
            return Collections.unmodifiableSet(Collections.emptySet());
        }

        @Override
        public void execute(WriteExecutor executor) {
            Role role = executor.getConcept(property.role().var()).asRole();
            executor.getConcept(var).asRelationshipType().relates(role);
        }
    }

    private class DefineRole extends RelatesWriter implements PropertyExecutor.Writer {

        @Override
        public Set<Variable> requiredVars() {
            return Collections.unmodifiableSet(Collections.emptySet());
        }

        @Override
        public Set<Variable> producedVars() {
            return Collections.unmodifiableSet(Collections.singleton(property.role().var()));
        }

        @Override
        public void execute(WriteExecutor executor) {
            executor.getBuilder(property.role().var()).isRole();
        }
    }

    private class DefineSuperRole extends RelatesWriter implements PropertyExecutor.Writer {

        @Override
        public Set<Variable> requiredVars() {
            return Collections.unmodifiableSet(Collections.singleton(property.superRole().var()));
        }

        @Override
        public Set<Variable> producedVars() {
            return Collections.unmodifiableSet(Collections.singleton(property.role().var()));
        }

        @Override
        public void execute(WriteExecutor executor) {
            Role superRole = executor.getConcept(property.superRole().var()).asRole();
            executor.getBuilder(property.role().var()).sub(superRole);
        }
    }

    private class UndefineRelates extends RelatesWriter implements PropertyExecutor.Writer {

        @Override
        public Set<Variable> requiredVars() {
            Set<Variable> required = new HashSet<>();
            required.add(var);
            required.add(property.role().var());

            return Collections.unmodifiableSet(required);
        }

        @Override
        public Set<Variable> producedVars() {
            return Collections.unmodifiableSet(Collections.emptySet());
        }

        @Override
        public void execute(WriteExecutor executor) {
            RelationType relationshipType = executor.getConcept(var).asRelationshipType();
            Role role = executor.getConcept(property.role().var()).asRole();

            if (!relationshipType.isDeleted() && !role.isDeleted()) {
                relationshipType.unrelate(role);
            }
        }
    }
}
