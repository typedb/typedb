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
import grakn.core.graql.internal.reasoner.atom.Atomic;
import grakn.core.graql.internal.reasoner.query.ReasonerQuery;
import grakn.core.graql.concept.ConceptId;
import grakn.core.graql.concept.Type;
import grakn.core.graql.internal.executor.WriteExecutor;
import grakn.core.graql.internal.gremlin.EquivalentFragmentSet;
import grakn.core.graql.internal.gremlin.sets.EquivalentFragmentSets;
import grakn.core.graql.internal.reasoner.atom.binary.IsaAtom;
import grakn.core.graql.internal.reasoner.atom.predicate.IdPredicate;
import grakn.core.graql.query.property.IsaProperty;
import grakn.core.graql.query.property.RelationProperty;
import grakn.core.graql.query.property.VarProperty;
import grakn.core.graql.query.statement.Statement;
import grakn.core.graql.query.statement.Variable;

import java.util.Set;

import static grakn.core.graql.internal.reasoner.utils.ReasonerUtils.getIdPredicate;

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
    public Atomic atomic(ReasonerQuery parent, Statement statement, Set<Statement> otherStatements) {
        //IsaProperty is unique within a var, so skip if this is a relation
        if (statement.hasProperty(RelationProperty.class)) return null;

        Variable varName = var.asUserDefined();
        Variable typeVar = property.type().var();

        IdPredicate predicate = getIdPredicate(typeVar, property.type(), otherStatements, parent);
        ConceptId predicateId = predicate != null ? predicate.getPredicate() : null;

        //isa part
        Statement isaVar;

        if (property.isExplicit()) {
            isaVar = new Statement(varName).isaX(new Statement(typeVar));
        } else {
            isaVar = new Statement(varName).isa(new Statement(typeVar));
        }

        return IsaAtom.create(varName, typeVar, isaVar, predicateId, parent);
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
            executor.getBuilder(var).isa(type);
        }
    }
}
