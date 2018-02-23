/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
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

package ai.grakn.graql.internal.reasoner.atom.binary;

import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.SchemaConcept;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.PatternAdmin;
import ai.grakn.graql.admin.Unifier;
import ai.grakn.graql.internal.pattern.Patterns;
import ai.grakn.graql.internal.reasoner.UnifierImpl;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.internal.reasoner.atom.predicate.IdPredicate;

import ai.grakn.graql.internal.reasoner.atom.predicate.Predicate;
import com.google.auto.value.extension.memoized.Memoized;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 *
 * <p>
 * Implementation for binary atoms with single id typePredicate for a schema concept. Binary atoms take the form:
 *
 * <>($varName, $predicateVariable), type($predicateVariable)
 *
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public abstract class Binary extends Atom {

    public abstract Var getPredicateVariable();
    @Nullable public abstract IdPredicate getTypePredicate();

    @Override
    public void checkValid() {
        if (getTypePredicate() != null) getTypePredicate().checkValid();
    }

    @Nullable
    @Memoized
    @Override
    public SchemaConcept getSchemaConcept(){
        if (getTypePredicate() == null) return null;
        Concept concept = getParentQuery().tx().getConcept(getTypeId());
        if (concept == null) throw GraqlQueryException.idNotFound(getTypeId());
        return concept.asType();
    }

    @Override
    public ConceptId getTypeId(){ return getTypePredicate() != null? getTypePredicate().getPredicate() : null;}

    @Override
    public boolean isAlphaEquivalent(Object obj) {
        if (obj == null || this.getClass() != obj.getClass()) return false;
        if (obj == this) return true;
        Binary a2 = (Binary) obj;
        return  (isUserDefined() == a2.isUserDefined())
                && Objects.equals(this.getTypeId(), a2.getTypeId())
                && hasEquivalentPredicatesWith(a2);
    }

    @Override
    public int alphaEquivalenceHashCode() {
        int hashCode = 1;
        hashCode = hashCode * 37 + (this.getTypeId() != null? this.getTypeId().hashCode() : 0);
        return hashCode;
    }

    @Override
    public boolean isStructurallyEquivalent(Object obj) {
        if (obj == null || this.getClass() != obj.getClass()) return false;
        if (obj == this) return true;
        Binary a2 = (Binary) obj;
        return  (isUserDefined() == a2.isUserDefined())
                && Objects.equals(this.getTypeId(), a2.getTypeId())
                && predicateBindingsAreEquivalent(a2);
    }

    @Override
    public int structuralEquivalenceHashCode() {
        return alphaEquivalenceHashCode();
    }

    boolean hasEquivalentPredicatesWith(Binary atom) {
        //check if there is a substitution for varName
        IdPredicate thisVarPredicate = this.getIdPredicate(getVarName());
        IdPredicate varPredicate = atom.getIdPredicate(atom.getVarName());

        IdPredicate thisTypePredicate = this.getTypePredicate();
        IdPredicate typePredicate = atom.getTypePredicate();
        return ((thisVarPredicate == null && varPredicate == null || thisVarPredicate != null && thisVarPredicate.isAlphaEquivalent(varPredicate)))
                && (thisTypePredicate == null && typePredicate == null || thisTypePredicate != null && thisTypePredicate.isAlphaEquivalent(typePredicate));
    }

    boolean predicateBindingsAreEquivalent(Binary atom) {
        //check if there is a substitution for varName
        IdPredicate thisVarPredicate = this.getIdPredicate(getVarName());
        IdPredicate varPredicate = atom.getIdPredicate(atom.getVarName());

        IdPredicate thisTypePredicate = this.getTypePredicate();
        IdPredicate typePredicate = atom.getTypePredicate();
        return (thisVarPredicate == null) == (varPredicate == null)
                && (thisTypePredicate == null) == (typePredicate == null);
    }

    @Override
    protected Pattern createCombinedPattern(){
        Set<PatternAdmin> vars = Sets.newHashSet(getPattern().admin());
        if (getTypePredicate() != null) vars.add(getTypePredicate().getPattern().admin());
        return Patterns.conjunction(vars);
    }

    @Override
    public Set<Var> getVarNames() {
        Set<Var> vars = new HashSet<>();
        if (getVarName().isUserDefinedName()) vars.add(getVarName());
        if (getPredicateVariable().isUserDefinedName()) vars.add(getPredicateVariable());
        return vars;
    }

    @Override
    public Stream<Predicate> getInnerPredicates(){
        return getTypePredicate() != null? Stream.of(getTypePredicate()) : Stream.empty();
    }

    @Override
    public Unifier getUnifier(Atom parentAtom) {
        if (!(parentAtom instanceof Binary)) {
            throw GraqlQueryException.unificationAtomIncompatibility();
        }

        Multimap<Var, Var> varMappings = HashMultimap.create();
        Var childVarName = this.getVarName();
        Var parentVarName = parentAtom.getVarName();
        Var childPredicateVarName = this.getPredicateVariable();
        Var parentPredicateVarName = parentAtom.getPredicateVariable();

        if (parentVarName.isUserDefinedName()
                && childVarName.isUserDefinedName()
                && !childVarName.equals(parentVarName)) {
            varMappings.put(childVarName, parentVarName);
        }
        if (parentPredicateVarName.isUserDefinedName()
                && childPredicateVarName.isUserDefinedName()
                && !childPredicateVarName.equals(parentPredicateVarName)) {
            varMappings.put(childPredicateVarName, parentPredicateVarName);
        }
        return new UnifierImpl(varMappings);
    }
}
