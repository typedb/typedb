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
import grakn.core.graql.admin.Atomic;
import grakn.core.graql.admin.ReasonerQuery;
import grakn.core.graql.concept.ConceptId;
import grakn.core.graql.concept.SchemaConcept;
import grakn.core.graql.internal.executor.ConceptBuilder;
import grakn.core.graql.internal.executor.WriteExecutor;
import grakn.core.graql.internal.gremlin.EquivalentFragmentSet;
import grakn.core.graql.internal.gremlin.sets.EquivalentFragmentSets;
import grakn.core.graql.internal.reasoner.atom.binary.SubAtom;
import grakn.core.graql.internal.reasoner.atom.predicate.IdPredicate;
import grakn.core.graql.query.pattern.property.SubProperty;
import grakn.core.graql.query.pattern.property.VarProperty;
import grakn.core.graql.query.pattern.statement.Statement;
import grakn.core.graql.query.pattern.statement.Variable;

import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static grakn.core.graql.internal.reasoner.utils.ReasonerUtils.getIdPredicate;

public class SubExecutor implements PropertyExecutor.Definable {

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
    public Atomic atomic(ReasonerQuery parent, Statement statement, Set<Statement> otherStatements) {
        IdPredicate predicate = getIdPredicate(property.type().var(), property.type(), otherStatements, parent);
        ConceptId predicateId = predicate != null ? predicate.getPredicate() : null;
        return SubAtom.create(var.asUserDefined(), property.type().var(), predicateId, parent);
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
