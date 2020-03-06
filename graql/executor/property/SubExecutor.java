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
import grakn.core.kb.concept.api.GraknConceptException;
import grakn.core.kb.concept.api.SchemaConcept;
import grakn.core.kb.graql.executor.ConceptBuilder;
import grakn.core.kb.graql.executor.WriteExecutor;
import grakn.core.kb.graql.executor.property.PropertyExecutor;
import grakn.core.kb.graql.planning.gremlin.EquivalentFragmentSet;
import graql.lang.property.SubProperty;
import graql.lang.property.VarProperty;
import graql.lang.statement.Variable;

import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

public class SubExecutor  implements PropertyExecutor.Definable {

    private final Variable var;
    private final SubProperty property;

    SubExecutor(Variable var, SubProperty property) {
        this.var = var;
        this.property = property;
    }

    @Override
    public Set<EquivalentFragmentSet> matchFragments() {
        return ImmutableSet.of(EquivalentFragmentSets.sub(property, var, property.type().var(), property.isExplicit()));
    }

    @Override
    public Set<PropertyExecutor.Writer> defineExecutors() {
        return ImmutableSet.of(new DefineSub());
    }

    @Override
    public Set<PropertyExecutor.Writer> undefineExecutors() {
        return ImmutableSet.of(new UndefineSub());
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
                SchemaConcept schemaConcept = executor.getConcept(var).asSchemaConcept();
                if (superConcept.isEntityType()) {
                    schemaConcept.asEntityType().sup(superConcept.asEntityType());
                } else if (superConcept.isRelationType()) {
                    schemaConcept.asRelationType().sup(superConcept.asRelationType());
                } else if (superConcept.isRole()) {
                    schemaConcept.asRole().sup(superConcept.asRole());
                } else if (superConcept.isAttributeType()) {
                    schemaConcept.asAttributeType().sup(superConcept.asAttributeType());
                } else if (superConcept.isRule()) {
                    schemaConcept.asRule().sup(superConcept.asRule());
                } else {
                    throw GraknConceptException.invalidSuperType(schemaConcept.label(), superConcept);
                }
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
                executor.toDelete(concept);
            }
        }
    }
}
