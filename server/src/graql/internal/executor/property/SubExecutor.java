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
import grakn.core.graql.concept.SchemaConcept;
import grakn.core.graql.internal.executor.ConceptBuilder;
import grakn.core.graql.internal.executor.WriteExecutor;
import grakn.core.graql.internal.gremlin.EquivalentFragmentSet;
import grakn.core.graql.internal.gremlin.sets.EquivalentFragmentSets;
import grakn.core.graql.query.pattern.Variable;
import grakn.core.graql.query.pattern.property.SubProperty;
import grakn.core.graql.query.pattern.property.VarProperty;

import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

public class SubExecutor implements PropertyExecutor.Definable, PropertyExecutor.Matchable {

    private final Variable var;
    private final SubProperty property;

    public SubExecutor(Variable var, SubProperty property) {
        this.var = var;
        this.property = property;
    }

    @Override
    public Set<PropertyExecutor.Writer> defineExecutors() {
        return ImmutableSet.of(new DefineSub());
    }

    @Override
    public Set<PropertyExecutor.Writer> undefineExecutors() {
        return ImmutableSet.of(new UndefineSub());
    }

    @Override
    public Set<EquivalentFragmentSet> matchFragments() {
        return ImmutableSet.of(EquivalentFragmentSets.sub(property, var, property.type().var()));
    }

    public static class SubExplicitExecutor extends SubExecutor {

        public SubExplicitExecutor(Variable var, SubProperty property) {
            super(var, property);
        }

        @Override
        public Set<EquivalentFragmentSet> matchFragments() {
            return ImmutableSet.of(EquivalentFragmentSets.sub(super.property, super.var, super.property.type().var(), true));
        }
    }

    private abstract class SubWriter {

        public Variable var() {
            return var;
        }

        public VarProperty property() {
            return property;
        }
    }

    private class DefineSub extends SubWriter implements PropertyExecutor.Writer {

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
            SchemaConcept superConcept = executor.getConcept(property.type().var()).asSchemaConcept();

            Optional<ConceptBuilder> builder = executor.tryBuilder(var);

            if (builder.isPresent()) {
                builder.get().sub(superConcept);
            } else {
                ConceptBuilder.setSuper(executor.getConcept(var).asSchemaConcept(), superConcept);
            }
        }
    }

    private class UndefineSub extends SubWriter implements PropertyExecutor.Writer {

        @Override
        public Set<Variable> requiredVars() {
            Set<Variable> required = new HashSet<>();
            required.add(var);
            required.add(property.type().var());

            return Collections.unmodifiableSet(required);
        }

        @Override
        public Set<Variable> producedVars() {
            return ImmutableSet.of();
        }

        @Override
        public void execute(WriteExecutor executor) {
            SchemaConcept concept = executor.getConcept(var).asSchemaConcept();

            SchemaConcept expectedSuperConcept = executor.getConcept(property.type().var()).asSchemaConcept();
            SchemaConcept actualSuperConcept = concept.sup();

            if (!concept.isDeleted() && expectedSuperConcept.equals(actualSuperConcept)) {
                concept.delete();
            }
        }
    }
}
