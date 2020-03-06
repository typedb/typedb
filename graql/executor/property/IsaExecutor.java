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

import com.google.common.collect.ImmutableSet;
import grakn.core.graql.planning.gremlin.sets.EquivalentFragmentSets;
import grakn.core.kb.concept.api.Concept;
import grakn.core.kb.concept.api.SchemaConcept;
import grakn.core.kb.concept.api.Type;
import grakn.core.kb.graql.exception.GraqlSemanticException;
import grakn.core.kb.graql.executor.WriteExecutor;
import grakn.core.kb.graql.executor.property.PropertyExecutor;
import grakn.core.kb.graql.planning.gremlin.EquivalentFragmentSet;
import graql.lang.property.IsaProperty;
import graql.lang.property.VarProperty;
import graql.lang.statement.Variable;

import java.util.Set;

public class IsaExecutor implements PropertyExecutor.Insertable {

    private final Variable var;
    private final IsaProperty property;

    IsaExecutor(Variable var, IsaProperty property) {
        this.var = var;
        this.property = property;
    }

    @Override
    public Set<EquivalentFragmentSet> matchFragments() {
        Variable directTypeVar = new Variable();
        if (!property.isExplicit()) {
            return ImmutableSet.of(
                    EquivalentFragmentSets.isa(property, var, directTypeVar, true),
                    EquivalentFragmentSets.sub(property, directTypeVar, property.type().var())
            );
        } else {
            return ImmutableSet.of(
                    EquivalentFragmentSets.isa(property, var, property.type().var(), true)
            );
        }
    }

    @Override
    public Set<PropertyExecutor.Writer> insertExecutors() {
        return ImmutableSet.of(new InsertIsa());
    }

    private class InsertIsa implements PropertyExecutor.Writer {

        @Override
        public Variable var() {
            return var;
        }

        @Override
        public VarProperty property() {
            return property;
        }

        @Override
        public Set<Variable> requiredVars() {
            return ImmutableSet.of(property.type().var());
        }

        @Override
        public Set<Variable> producedVars() {
            return ImmutableSet.of(var);
        }

        @Override
        public void execute(WriteExecutor executor) {
            Type type = executor.getConcept(property.type().var()).asType();
            if (executor.isConceptDefined(var)) {
                Concept concept = executor.getConcept(var); // retrieve the existing concept
                // we silently "allow" redefining concepts, while actually doing a no-op, as long as the type hasn't changed
                if (type.subs().map(SchemaConcept::label).noneMatch(label -> label.equals(concept.asThing().type().label()))) {
                    //downcasting is bad
                    throw GraqlSemanticException.conceptDowncast(concept.asThing().type(), type);
                }
                //upcasting we silently accept
            } else {
                executor.getBuilder(var).isa(type);
            }
        }
    }
}
