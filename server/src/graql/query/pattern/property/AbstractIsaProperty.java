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

package grakn.core.graql.query.pattern.property;

import com.google.common.collect.ImmutableSet;
import grakn.core.graql.admin.Atomic;
import grakn.core.graql.admin.ReasonerQuery;
import grakn.core.graql.concept.ConceptId;
import grakn.core.graql.concept.SchemaConcept;
import grakn.core.graql.concept.Type;
import grakn.core.graql.exception.GraqlQueryException;
import grakn.core.graql.internal.reasoner.atom.binary.IsaAtom;
import grakn.core.graql.internal.reasoner.atom.predicate.IdPredicate;
import grakn.core.graql.query.pattern.Statement;
import grakn.core.graql.query.pattern.Variable;
import grakn.core.server.Transaction;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Set;
import java.util.stream.Stream;

import static grakn.core.graql.internal.reasoner.utils.ReasonerUtils.getIdPredicate;

abstract class AbstractIsaProperty extends VarProperty {

    public abstract Statement type();

    @Override
    public final String getProperty() {
        return type().getPrintableName();
    }

    @Override
    public final boolean isUnique() {
        return true;
    }

    @Override
    public final Stream<Statement> getTypes() {
        return Stream.of(type());
    }

    @Override
    public final Stream<Statement> innerStatements() {
        return Stream.of(type());
    }

    @Override
    public final Collection<PropertyExecutor> insert(Variable var) throws GraqlQueryException {
        PropertyExecutor.Method method = executor -> {
            Type type = executor.get(this.type().var()).asType();
            executor.builder(var).isa(type);
        };

        return ImmutableSet.of(PropertyExecutor.builder(method).requires(type().var()).produces(var).build());
    }

    @Override
    public final void checkValidProperty(Transaction graph, Statement var) throws GraqlQueryException {
        type().getTypeLabel().ifPresent(typeLabel -> {
            SchemaConcept theSchemaConcept = graph.getSchemaConcept(typeLabel);
            if (theSchemaConcept != null && !theSchemaConcept.isType()) {
                throw GraqlQueryException.cannotGetInstancesOfNonType(typeLabel);
            }
        });
    }

    @Override
    public boolean mapsToAtom(Statement var) {
        //IsaProperty is unique within a var, so skip if this is a relation
        return !var.hasProperty(RelationshipProperty.class);
    }

    @Nullable
    @Override
    public final Atomic mapToAtom(Statement var, Set<Statement> vars, ReasonerQuery parent) {
        //IsaProperty is unique within a var, so skip if this is a relation
        if (!mapsToAtom(var)) return null;

        Variable varName = var.var().asUserDefined();
        Statement typePattern = this.type();
        Variable typeVariable = typePattern.var();

        IdPredicate predicate = getIdPredicate(typeVariable, typePattern, vars, parent);
        ConceptId predicateId = predicate != null ? predicate.getPredicate() : null;

        //isa part
        Statement isaVar = statementForAtom(varName, typeVariable);
        return IsaAtom.create(varName, typeVariable, isaVar, predicateId, parent);
    }

    protected abstract Statement statementForAtom(Variable varName, Variable typeVariable);
}
