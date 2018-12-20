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

import grakn.core.graql.concept.SchemaConcept;
import grakn.core.graql.internal.executor.ConceptBuilder;
import grakn.core.graql.internal.executor.Writer;
import grakn.core.graql.query.pattern.Variable;
import grakn.core.graql.query.pattern.property.SubAbstractProperty;
import grakn.core.graql.query.pattern.property.VarProperty;

import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

public class SubAbstractExecutor implements PropertyExecutor.Definable {

    private final Variable var;
    private final SubAbstractProperty property;

    public SubAbstractExecutor(Variable var, SubAbstractProperty property) {
        this.var = var;
        this.property = property;
    }

    @Override
    public Set<PropertyExecutor.WriteExecutor> defineExecutors() {
        return Collections.unmodifiableSet(Collections.singleton(new DefineSub()));
    }

    @Override
    public Set<PropertyExecutor.WriteExecutor> undefineExecutors() {
        return Collections.unmodifiableSet(Collections.singleton(new UndefineSub()));
    }

    private abstract class AbstractWriteExecutor {

        public Variable var() {
            return var;
        }

        public VarProperty property() {
            return property;
        }
    }

    private class DefineSub extends AbstractWriteExecutor implements PropertyExecutor.WriteExecutor {

        @Override
        public Set<Variable> requiredVars() {
            return Collections.unmodifiableSet(Collections.singleton(property.superType().var()));
        }

        @Override
        public Set<Variable> producedVars() {
            return Collections.unmodifiableSet(Collections.singleton(var));
        }

        @Override
        public void execute(Writer writer) {
            SchemaConcept superConcept = writer.getConcept(property.superType().var()).asSchemaConcept();

            Optional<ConceptBuilder> builder = writer.tryBuilder(var);

            if (builder.isPresent()) {
                builder.get().sub(superConcept);
            } else {
                ConceptBuilder.setSuper(writer.getConcept(var).asSchemaConcept(), superConcept);
            }
        }
    }

    private class UndefineSub extends AbstractWriteExecutor implements PropertyExecutor.WriteExecutor {

        @Override
        public Set<Variable> requiredVars() {
            Set<Variable> required = new HashSet<>();
            required.add(var);
            required.add(property.superType().var());

            return Collections.unmodifiableSet(required);
        }

        @Override
        public Set<Variable> producedVars() {
            return Collections.unmodifiableSet(Collections.emptySet());
        }

        @Override
        public void execute(Writer writer) {
            SchemaConcept concept = writer.getConcept(var).asSchemaConcept();

            SchemaConcept expectedSuperConcept = writer.getConcept(property.superType().var()).asSchemaConcept();
            SchemaConcept actualSuperConcept = concept.sup();

            if (!concept.isDeleted() && expectedSuperConcept.equals(actualSuperConcept)) {
                concept.delete();
            }
        }
    }
}
