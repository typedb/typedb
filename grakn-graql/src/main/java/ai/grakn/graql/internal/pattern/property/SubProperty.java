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

import ai.grakn.concept.Concept;
import ai.grakn.graql.Graql;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.UniqueVarProperty;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.VarName;
import ai.grakn.graql.internal.gremlin.EquivalentFragmentSet;
import ai.grakn.graql.internal.gremlin.fragment.Fragments;
import ai.grakn.graql.internal.query.InsertQueryExecutor;
import ai.grakn.graql.internal.reasoner.atom.binary.TypeAtom;
import ai.grakn.graql.internal.reasoner.atom.predicate.IdPredicate;
import ai.grakn.util.ErrorMessage;
import com.google.common.collect.Sets;

import java.util.Collection;
import java.util.Set;
import java.util.stream.Stream;

import static ai.grakn.graql.internal.reasoner.Utility.getIdPredicate;

public class SubProperty extends AbstractVarProperty implements NamedProperty, UniqueVarProperty {

    private final VarAdmin superType;

    public SubProperty(VarAdmin superType) {
        this.superType = superType;
    }

    public VarAdmin getSuperType() {
        return superType;
    }

    @Override
    public String getName() {
        return "sub";
    }

    @Override
    public String getProperty() {
        return superType.getPrintableName();
    }

    @Override
    public Collection<EquivalentFragmentSet> match(VarName start) {
        return Sets.newHashSet(EquivalentFragmentSet.create(
                Fragments.outSub(start, superType.getVarName()),
                Fragments.inSub(superType.getVarName(), start)
        ));
    }

    @Override
    public Stream<VarAdmin> getTypes() {
        return Stream.of(superType);
    }

    @Override
    public Stream<VarAdmin> getInnerVars() {
        return Stream.of(superType);
    }

    @Override
    public void insert(InsertQueryExecutor insertQueryExecutor, Concept concept) throws IllegalStateException {
        Concept superConcept = insertQueryExecutor.getConcept(superType);

        if (concept.isEntityType()) {
            concept.asEntityType().superType(superConcept.asEntityType());
        } else if (concept.isRelationType()) {
            concept.asRelationType().superType(superConcept.asRelationType());
        } else if (concept.isRoleType()) {
            concept.asRoleType().superType(superConcept.asRoleType());
        } else if (concept.isResourceType()) {
            concept.asResourceType().superType(superConcept.asResourceType());
        } else if (concept.isRuleType()) {
            concept.asRuleType().superType(superConcept.asRuleType());
        } else {
            throw new IllegalStateException(ErrorMessage.INSERT_METATYPE.getMessage(concept.getId(), superType.getId()));
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SubProperty that = (SubProperty) o;

        return superType.equals(that.superType);

    }

    @Override
    public int hashCode() {
        return superType.hashCode();
    }

    @Override
    public Atomic mapToAtom(VarAdmin var, Set<VarAdmin> vars, ReasonerQuery parent) {
        VarName varName = var.getVarName();
        VarAdmin typeVar = this.getSuperType();
        VarName typeVariable = typeVar.getVarName();
        IdPredicate predicate = getIdPredicate(typeVariable, typeVar, vars, parent);

        VarAdmin resVar = Graql.var(varName).sub(Graql.var(typeVariable)).admin();
        return new TypeAtom(resVar, predicate, parent);
    }
}
