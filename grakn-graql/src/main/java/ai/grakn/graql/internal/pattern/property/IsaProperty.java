/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.graql.internal.pattern.property;

import ai.grakn.GraknGraph;
import ai.grakn.concept.OntologyConcept;
import ai.grakn.concept.Concept;
import ai.grakn.concept.Thing;
import ai.grakn.concept.Type;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.UniqueVarProperty;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.internal.gremlin.EquivalentFragmentSet;
import ai.grakn.graql.internal.gremlin.sets.EquivalentFragmentSets;
import ai.grakn.graql.internal.query.InsertQueryExecutor;
import ai.grakn.graql.internal.reasoner.atom.binary.type.IsaAtom;
import ai.grakn.graql.internal.reasoner.atom.predicate.IdPredicate;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Set;
import java.util.stream.Stream;

import static ai.grakn.graql.internal.reasoner.utils.ReasonerUtils.getIdPredicate;

/**
 * Represents the {@code isa} property on a {@link Thing}.
 *
 * This property can be queried and inserted.
 *
 * THe property is defined as a relationship between an {@link Thing} and a {@link Type}.
 *
 * When matching, any subtyping is respected. For example, if we have {@code $bob isa man}, {@code man sub person},
 * {@code person sub entity} then it follows that {@code $bob isa person} and {@code bob isa entity}.
 *
 * @author Felix Chapman
 */
public class IsaProperty extends AbstractVarProperty implements UniqueVarProperty, NamedProperty {

    private final VarPatternAdmin type;

    public IsaProperty(VarPatternAdmin type) {
        this.type = type;
    }

    public VarPatternAdmin getType() {
        return type;
    }

    @Override
    public String getName() {
        return "isa";
    }

    @Override
    public String getProperty() {
        return type.getPrintableName();
    }

    @Override
    public Collection<EquivalentFragmentSet> match(Var start) {
        return ImmutableSet.of(EquivalentFragmentSets.isa(this, start, type.getVarName()));
    }

    @Override
    public Stream<VarPatternAdmin> getTypes() {
        return Stream.of(type);
    }

    @Override
    public Stream<VarPatternAdmin> getInnerVars() {
        return Stream.of(type);
    }

    @Override
    public void insert(InsertQueryExecutor insertQueryExecutor, Concept concept) throws GraqlQueryException {
        Type type = insertQueryExecutor.getConcept(this.type).asType();
        Thing thing = concept.asThing();
        if (!thing.type().equals(type)) {
            throw GraqlQueryException.insertNewType(thing, type);
        }
    }

    @Override
    public void checkValidProperty(GraknGraph graph, VarPatternAdmin var) throws GraqlQueryException {
        type.getTypeLabel().ifPresent(typeLabel -> {
            OntologyConcept theOntologyConcept = graph.getOntologyConcept(typeLabel);
            if (theOntologyConcept != null && theOntologyConcept.isRole()) {
                throw GraqlQueryException.queryInstanceOfRoleType(typeLabel);
            }
        });
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        IsaProperty that = (IsaProperty) o;

        return type.equals(that.type);

    }

    @Override
    public int hashCode() {
        return type.hashCode();
    }

    @Nullable
    @Override
    public Atomic mapToAtom(VarPatternAdmin var, Set<VarPatternAdmin> vars, ReasonerQuery parent) {
        //IsaProperty is unique within a var, so skip if this is a relation
        if (var.hasProperty(RelationProperty.class)) return null;

        Var varName = var.getVarName().asUserDefined();
        VarPatternAdmin typeVar = this.getType();
        Var typeVariable = typeVar.getVarName().asUserDefined();
        IdPredicate predicate = getIdPredicate(typeVariable, typeVar, vars, parent);

        //isa part
        VarPatternAdmin isaVar = varName.isa(typeVariable).admin();
        return new IsaAtom(isaVar, typeVariable, predicate, parent);
    }
}
