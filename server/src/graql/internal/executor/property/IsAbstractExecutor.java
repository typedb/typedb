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

import grakn.core.graql.concept.Concept;
import grakn.core.graql.concept.Type;
import grakn.core.graql.exception.GraqlQueryException;
import grakn.core.graql.internal.executor.Writer;
import grakn.core.graql.query.pattern.Variable;
import grakn.core.graql.query.pattern.property.IsAbstractProperty;
import grakn.core.graql.query.pattern.property.VarProperty;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class IsAbstractExecutor implements PropertyExecutor.Definable {

    private final Variable var;
    private final IsAbstractProperty property;

    public IsAbstractExecutor(Variable var, IsAbstractProperty property) {
        this.var = var;
        this.property = property;
    }

    @Override
    public Set<PropertyExecutor.WriteExecutor> defineExecutors() {
        return Collections.unmodifiableSet(Collections.singleton(new DefineIsAbstract()));
    }

    @Override
    public Set<PropertyExecutor.WriteExecutor> undefineExecutors() {
        return Collections.unmodifiableSet(Collections.singleton(new UndefineIsAbstract()));
    }

    private abstract class AbstractWriteExecutor {

        public Variable var() {
            return var;
        }

        public VarProperty property() {
            return property;
        }

        public Set<Variable> requiredVars() {
            return Collections.unmodifiableSet(Collections.singleton(var));
        }

        public Set<Variable> producedVars() {
            return Collections.unmodifiableSet(Collections.emptySet());
        }
    }

    private class DefineIsAbstract extends AbstractWriteExecutor implements PropertyExecutor.WriteExecutor {

        @Override
        public void execute(Writer writer) {
            Concept concept = writer.getConcept(var);
            if (concept.isType()) {
                concept.asType().isAbstract(true);
            } else {
                throw GraqlQueryException.insertAbstractOnNonType(concept.asSchemaConcept());
            }
        }
    }

    private class UndefineIsAbstract extends AbstractWriteExecutor implements PropertyExecutor.WriteExecutor {

        @Override
        public void execute(Writer writer) {
            Type type = writer.getConcept(var).asType();
            if (!type.isDeleted()) {
                type.isAbstract(false);
            }
        }
    }
}
